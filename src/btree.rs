/*

A b-tree page is divided into regions in the following order:

The 100-byte database file header (found on page 1 only)
The 8 or 12 byte b-tree page header
The cell pointer array
Unallocated space
The cell content area
The reserved region.
The 100-byte database file header is found only on page 1, which is always a table b-tree page. All other b-tree pages in the database file omit this 100-byte header.

The reserved region is an area of unused space at the end of every page (except the locking page) that extensions can use to hold per-page information. The size of the reserved region is determined by the one-byte unsigned integer found at an offset of 20 into the database file header. The size of the reserved region is usually zero.

The b-tree page header is 8 bytes in size for leaf pages and 12 bytes for interior pages. All multibyte values in the page header are big-endian. The b-tree page header is composed of the following fields:

B-tree Page Header Format
Offset	Size	Description
0	1	The one-byte flag at offset 0 indicating the b-tree page type.
A value of 2 (0x02) means the page is an interior index b-tree page.
A value of 5 (0x05) means the page is an interior table b-tree page.
A value of 10 (0x0a) means the page is a leaf index b-tree page.
A value of 13 (0x0d) means the page is a leaf table b-tree page.
Any other value for the b-tree page type is an error.
1	2	The two-byte integer at offset 1 gives the start of the first freeblock on the page, or is zero if there are no freeblocks.
3	2	The two-byte integer at offset 3 gives the number of cells on the page.
5	2	The two-byte integer at offset 5 designates the start of the cell content area. A zero value for this integer is interpreted as 65536.
7	1	The one-byte integer at offset 7 gives the number of fragmented free bytes within the cell content area.
8	4	The four-byte page number at offset 8 is the right-most pointer. This value appears in the header of interior b-tree pages only and is omitted from all other pages.
The cell pointer array of a b-tree page immediately follows the b-tree page header. Let K be the number of cells on the btree. The cell pointer array consists of K 2-byte integer  cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and the right-most cell (the cell with the largest key) last.

Cell content is stored in the cell content region of the b-tree page. SQLite strives to place cells as far toward the end of the b-tree page as it can, in order to leave space for future growth of the cell pointer array. The area in between the last cell pointer array entry and the beginning of the first cell is the unallocated region.

If a page contains no cells (which is only possible for a root page of a table that contains no rows) then the offset to the cell content area will equal the page size minus the bytes of reserved space. If the database uses a 65536-byte page size and the reserved space is zero (the usual value for reserved space) then the cell content offset of an empty page wants to be 65536. However, that integer is too large to be stored in a 2-byte unsigned integer, so a value of 0 is used in its place.

A freeblock is a structure used to identify unallocated space within a b-tree page. Freeblocks are organized as a chain. The first 2 bytes of a freeblock are a big-endian integer which is the offset in the b-tree page of the next freeblock in the chain, or zero if the freeblock is the last on the chain. The third and fourth bytes of each freeblock form a big-endian integer which is the size of the freeblock in bytes, including the 4-byte header. Freeblocks are always connected in order of increasing offset. The second field of the b-tree page header is the offset of the first freeblock, or zero if there are no freeblocks on the page. In a well-formed b-tree page, there will always be at least one cell before the first freeblock.

A freeblock requires at least 4 bytes of space. If there is an isolated group of 1, 2, or 3 unused bytes within the cell content area, those bytes comprise a fragment. The total number of bytes in all fragments is stored in the fifth field of the b-tree page header. In a well-formed b-tree page, the total number of bytes in fragments may not exceed 60.

The total amount of free space on a b-tree page consists of the size of the unallocated region plus the total size of all freeblocks plus the number of fragmented free bytes. SQLite may from time to time reorganize a b-tree page so that there are no freeblocks or fragment bytes, all unused bytes are contained in the unallocated space region, and all cells are packed tightly at the end of the page. This is called "defragmenting" the b-tree page.

A variable-length integer or "varint" is a static Huffman encoding of 64-bit twos-complement integers that uses less space for small positive values. A varint is between 1 and 9 bytes in length. The varint consists of either zero or more bytes which have the high-order bit set followed by a single byte with the high-order bit clear, or nine bytes, whichever is shorter. The lower seven bits of each of the first eight bytes and all 8 bits of the ninth byte are used to reconstruct the 64-bit twos-complement integer. Varints are big-endian: bits taken from the earlier byte of the varint are more significant than bits taken from the later bytes.

The format of a cell depends on which kind of b-tree page the cell appears on. The following table shows the elements of a cell, in order of appearance, for the various b-tree page types.

Table B-Tree Leaf Cell (header 0x0d):

A varint which is the total number of bytes of payload, including any overflow
A varint which is the integer key, a.k.a. "rowid"
The initial portion of the payload that does not spill to overflow pages.
A 4-byte big-endian integer page number for the first page of the overflow page list - omitted if all payload fits on the b-tree page.
Table B-Tree Interior Cell (header 0x05):

A 4-byte big-endian page number which is the left child pointer.
A varint which is the integer key

*/

use anyhow::{bail, Result};
use std::cell::RefCell;
use std::io::{Seek, SeekFrom};
use std::path::Iter;
use std::rc::Rc;
use std::{convert::TryInto, fs::File, io::Read};

use crate::cell::{
    DataCell, IndexInteriorCell, IndexLeafCell, InteriorCell, TableInteriorCell, TableLeafCell,
};
use crate::page::{BtreePage, PageHeader, PageType};
use crate::record::ReadableRecord;
use log::debug;

/*
 * A b-tree page is divided into regions in the following order:
 * The 100-byte database file header (found on page 1 only)
 * The 8 or 12 byte b-tree page header
 * The cell pointer array
 * Unallocated space
 * The cell content area
 * The reserved region.
 *
 * The cell pointer array consists of K 2-byte integer offsets to the cell contents.
 * The cell pointers are arranged in key order with left-most cell (the cell with the smallest key) first and the right-most cell
 * (the cell with the largest key) last.
 *
 * Reserved Space in a page:
 * SQLite has the ability to set aside a small number of extra bytes at the end of every page for use by extensions.
 * These extra bytes are used, for example, by the SQLite Encryption Extension to store a nonce and/or cryptographic
 * checksum associated with each page.
 * The "reserved space" size in the 1-byte integer at offset 20 is the number of bytes of space at the end of each page
 * to reserve for extensions. This value is usually 0. The value can be odd.
 * The "usable size" of a database page is the page size specified by the 2-byte integer at offset 16 in the header less
 * the "reserved" space size recorded in the 1-byte integer at offset 20 in the header.
 * The usable size of a page might be an odd number. However, the usable size is not allowed to be less than 480.
 * In other words, if the page size is 512, then the reserved space size cannot exceed 32.
*
*/

#[derive(Debug, Clone)]
enum BtreeType {
    Table,
    Index,
}

#[derive(Debug, Clone)]
pub struct Btree {
    btree_type: BtreeType,
    db_file_name: String,
    db_file_handle: Rc<RefCell<File>>,
    page_size: usize,
    root_page: BtreePage,
}

impl Btree {
    // schema table is special because it has an extra 100 bytes of database header
    pub fn read_schema_table(
        db_file_name: &str,
        page_size: usize,
        reserved_bytes_per_page: u8,
    ) -> Result<Self> {
        Btree::read_page_to_tree(db_file_name, page_size, 0, 100, reserved_bytes_per_page)
    }

    pub fn read_table(
        db_file_name: &str,
        page_size: usize,
        page_offset: usize,
        reserved_bytes_per_page: u8,
    ) -> Result<Self> {
        debug!("Reading Btree Root Page at Offset {}", page_offset);
        Btree::read_page_to_tree(
            db_file_name,
            page_size,
            page_offset,
            0,
            reserved_bytes_per_page,
        )
    }

    fn read_page_to_tree(
        db_file_name: &str,
        page_size: usize,
        page_offset: usize,
        header_offset: usize,
        reserved_bytes_per_page: u8,
    ) -> Result<Self> {
        let mut db_file_handle = File::open(db_file_name)?;
        let mut buffer: Vec<u8> = vec![0; page_size];

        // seek to offset page
        if page_offset as u64
            != db_file_handle.seek(std::io::SeekFrom::Start(page_offset.try_into()?))?
        {
            bail!("failed to seek to page offset");
        }

        if page_size != db_file_handle.read(&mut buffer)? {
            bail!("failed to read expected bytes for table page");
        }

        let root_page = BtreePage::new(buffer, header_offset, reserved_bytes_per_page)?;

        Ok(Btree {
            btree_type: match root_page.page_type {
                PageType::LeafTable | PageType::InteriorTable => BtreeType::Table,
                PageType::LeafIndex | PageType::InteriorIndex => BtreeType::Index,
            },
            db_file_name: db_file_name.to_string(),
            db_file_handle: Rc::new(RefCell::new(db_file_handle)),
            page_size,
            root_page,
        })
    }

    // As table btree this struct is responsible for knowing how to parse the cell_content from page and be able to parse it
    // sepcifically as the Table B-Tree Cell type for interior or leaf
    pub fn get_rows(&self, is_root_db_page: bool) -> Result<Vec<ReadableRecord>> {
        let mut all_cells = Vec::new();

        self.traverse_table_btree(&self.root_page, &mut all_cells, is_root_db_page)?;

        Ok(all_cells
            .iter()
            .map(|cell| cell.get_readable_record())
            .collect())
    }

    fn traverse_table_btree(
        &self,
        curr_page: &BtreePage,
        cells: &mut Vec<DataCell>,
        is_root_db_page: bool,
    ) -> Result<()> {
        let page_byte_buffer = curr_page.get_raw_bytes_buffer();
        match &curr_page.page_header {
            PageHeader::Interior(interior_header) => {
                let num_cells = interior_header.common_header.num_cells;
                // skip the page by 32+offset bytes to skip metadata block
                let start_cell_pointer_region = if is_root_db_page { 100 + 12 } else { 12 };
                let end_cell_pointer_region = start_cell_pointer_region + (num_cells as usize * 2);
                let cell_pointers =
                    &page_byte_buffer[start_cell_pointer_region..end_cell_pointer_region];

                debug!("Interior Page with {} cells", num_cells);

                for i in 0..num_cells {
                    let cell_offset = u16::from_be_bytes(
                        cell_pointers[(i * 2) as usize..(i * 2 + 2) as usize].try_into()?,
                    );
                    let tent: &[u8] = &page_byte_buffer[cell_offset.try_into()?..];
                    let interior_cell = match self.btree_type {
                        BtreeType::Table => {
                            let (cell, _) = TableInteriorCell::from_be_bytes(tent)?;
                            InteriorCell::Table(cell)
                        }
                        BtreeType::Index => {
                            let (cell, _) = IndexInteriorCell::from_be_bytes(tent)?;
                            let index_interior_cell = InteriorCell::Index(cell.clone());
                            cells.push(DataCell::IndexInterior(cell));
                            index_interior_cell
                        }
                    };

                    // use the db handle to read the said page number
                    let mut new_page_byte_buffer = vec![0; self.page_size];
                    // explicit block to drop the mutable borrow of db_file_handle before we move on to the next call
                    // that uses db_file_handle mutably. Since recursive calls are made one at a time, we do not hold
                    // a mutable access to db_file_handle Rc<RefCell<File>> when someone else is using it.
                    {
                        let mut db_file_handle = self.db_file_handle.borrow_mut();
                        self.read_page_into_buffer(
                            &mut db_file_handle,
                            &mut new_page_byte_buffer,
                            interior_cell.get_left_child_page_number(),
                        )?;
                    }

                    let new_page =
                        BtreePage::new(new_page_byte_buffer, 0, curr_page.reserved_bytes_per_page)?;

                    // use the cell to read the new page directed by the cell, and recursively traverse the tree left to right
                    self.traverse_table_btree(&new_page, cells, false)?;
                }

                // read the right most child separately
                let mut right_page_byte_buffer = vec![0; self.page_size];
                let right_most_pointer_page_number = interior_header.right_most_pointer;
                {
                    let mut db_file_handle = self.db_file_handle.borrow_mut();
                    self.read_page_into_buffer(
                        &mut db_file_handle,
                        &mut right_page_byte_buffer,
                        right_most_pointer_page_number,
                    )?;
                }

                let right_page =
                    BtreePage::new(right_page_byte_buffer, 0, curr_page.reserved_bytes_per_page)?;

                self.traverse_table_btree(&right_page, cells, false)?;

                Ok(())
            }
            PageHeader::Leaf(leaf_header) => {
                // get access to the content area in here
                let num_cells = leaf_header.common_header.num_cells;
                // read cell pointer array
                let start_cell_pointer = if is_root_db_page { 100 + 8 } else { 8 }; // 100 from DB Header if is root db page which has extra header on page
                let end_cell_pointer = start_cell_pointer + (num_cells as usize * 2);
                let cell_pointers = &page_byte_buffer[start_cell_pointer..end_cell_pointer];

                debug!("Leaf Page with {} cells", num_cells);

                for i in 0..num_cells {
                    let cell_offset = u16::from_be_bytes(
                        cell_pointers[i as usize * 2..i as usize * 2 + 2].try_into()?,
                    );
                    let cell_content = &page_byte_buffer[cell_offset as usize..];

                    // Based on btree type use the appropriate type for cell parsing
                    let cell = match self.btree_type {
                        BtreeType::Table => {
                            let (cell, _) = TableLeafCell::from_be_bytes(
                                self.db_file_name.clone(),
                                cell_content,
                                self.page_size.try_into()?,
                                curr_page.reserved_bytes_per_page,
                            )?;
                            DataCell::Table(cell)
                        }
                        BtreeType::Index => {
                            let (cell, _) = IndexLeafCell::from_be_bytes(
                                self.db_file_name.clone(),
                                cell_content,
                                self.page_size.try_into()?,
                                curr_page.reserved_bytes_per_page,
                            )?;
                            DataCell::IndexLeaf(cell)
                        }
                    };

                    cells.push(cell);
                }

                Ok(())
            }
        }
    }

    fn read_page_into_buffer(
        &self,
        db_file_handle: &mut File,
        buf: &mut Vec<u8>,
        page_num: u32,
    ) -> Result<()> {
        let offset_page_number: u64 = (page_num - 1).try_into()?;
        let next_page_addr: u64 = offset_page_number * self.page_size as u64;
        db_file_handle.seek(SeekFrom::Start(next_page_addr))?;
        db_file_handle.read(buf)?;
        Ok(())
    }
}

// TODO: An abstraction for a next() method for the Btree struct that returns the next node. This introduces state, but lets us do stuff in a more controlled manner
pub struct BtreeIterator {
    btree: Btree,
    curr_page: BtreePage,
    curr_cell: usize,
}

impl BtreeIterator {
    pub fn new(btree: Btree) -> Self {
        BtreeIterator {
            btree: btree.clone(),
            curr_page: btree.root_page.clone(),
            curr_cell: 0,
        }
    }
}
