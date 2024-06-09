use crate::{
    record::{OverflowRecord, ReadableRecord, Record},
    sql_data_types::VarInt,
};
use anyhow::Result;

pub struct TableLeafCell {
    pub total_bytes_of_payload: VarInt,
    pub integer_key: VarInt,
    pub payload: ReadableRecord,
}

impl TableLeafCell {
    // returns the (cell content and the number of bytes read) from cell_content buffer
    pub fn from_be_bytes(
        db_file_name: String,
        cell_content: &[u8],
        page_size: u16,
        reserved_bytes_per_page: u8,
    ) -> Result<(Self, u64)> {
        let total_bytes_of_payload = VarInt::from_be_bytes(cell_content)?;
        let bytes_read = total_bytes_of_payload.1 as usize;

        let integer_key = VarInt::from_be_bytes(&cell_content[bytes_read..])?;
        let mut bytes_read = bytes_read + integer_key.1 as usize;

        /*
         * The amount of payload that spills onto overflow pages also depends on the page type.
         * For the following computations,
         * let U be the usable size of a database page, the total page size less the reserved space at the end of each page,
         * let P be the payload size.
         * In the following, symbol X represents the maximum amount of payload that can be stored directly on the b-tree page without
         * spilling onto an overflow page and symbol M represents the minimum amount of payload that must be stored on the btree page
         * before spilling is allowed.
         *
         * Table Leaf Cell Spillage:
         * Let X be U-35. If the payload size P is less than or equal to X then the entire payload is stored on the b-tree leaf page.
         * Let M be ((U-12)*32/255)-23 and let K be M+((P-M)%(U-4)).
         * If P is greater than X then the number of bytes stored on the table b-tree leaf page is K if K is less or equal to X or M otherwise.
         * The number of bytes stored on the leaf page is never less than M.
         */
        let usable_page_size = page_size - reserved_bytes_per_page as u16;
        let x = usable_page_size - 35;
        let m: u64 = ((usable_page_size - 12) as u64 * 32 / 255) - 23;
        let k = m as i64
            + ((total_bytes_of_payload.0 as i64 - m as i64) % (usable_page_size as i64 - 4));
        let bytes_stored_on_leaf_page = if total_bytes_of_payload.0 <= x as i64 {
            total_bytes_of_payload.0
        } else {
            if k <= x as i64 {
                k
            } else {
                m as i64
            }
        };

        let record = if total_bytes_of_payload.0 > x.try_into()? {
            let record = OverflowRecord::from_be_bytes(
                bytes_stored_on_leaf_page,
                &cell_content[bytes_read..],
                db_file_name.clone(),
                page_size,
            )?;
            bytes_read += record.1 as usize;
            ReadableRecord::Lazy(record.0)
        } else {
            let record = Record::from_be_bytes(&cell_content[bytes_read..])?;
            bytes_read += record.1 as usize;
            ReadableRecord::Fit(record.0)
        };

        Ok((
            Self {
                total_bytes_of_payload,
                integer_key,
                payload: record,
            },
            bytes_read as u64,
        ))
    }
}

/*
Table B-Tree Interior Cell (header 0x05):
A 4-byte big-endian page number which is the left child pointer.
A varint which is the integer key
*/
pub struct TableInteriorCell {
    pub left_child_page_number: u32,
    pub integer_key: VarInt,
}

impl TableInteriorCell {
    pub fn from_be_bytes(cell_content: &[u8]) -> Result<(Self, u64)> {
        let left_child_page_number = u32::from_be_bytes(cell_content[..4].try_into()?);

        let integer_key = VarInt::from_be_bytes(&cell_content[4..])?;

        Ok((
            Self {
                left_child_page_number,
                integer_key: integer_key.clone(),
            },
            4 + integer_key.1 as u64,
        ))
    }
}

// Index Cells

pub struct IndexLeafCell {
    pub total_bytes_of_payload: VarInt,
    pub payload: ReadableRecord,
}

impl IndexLeafCell {
    pub fn from_be_bytes(
        db_file_name: String,
        cell_content: &[u8],
        page_size: u16,
        reserved_bytes_per_page: u8,
    ) -> Result<(Self, u64)> {
        let total_bytes_of_payload = VarInt::from_be_bytes(cell_content)?;
        let bytes_read = total_bytes_of_payload.1 as usize;
        /*
        Index B-Tree Leaf Or Interior Cell:
        The amount of payload that spills onto overflow pages also depends on the page type.
        For the following computations, let U be the usable size of a database page, the
        total page size less the reserved space at the end of each page.
        And let P be the payload size. In the following, symbol X represents
        the maximum amount of payload that can be stored directly on the b-tree
        page without spilling onto an overflow page and symbol M represents the minimum amount
        of payload that must be stored on the btree page before spilling is allowed.

        Let X be ((U-12)*64/255)-23.
        If the payload size P is less than or equal to X then the entire payload is stored on the b-tree page.
        Let M be ((U-12)*32/255)-23 and let K be M+((P-M)%(U-4)). If P is greater than X then the number of bytes
        stored on the index b-tree page is K if K is less than or equal to X or M otherwise.
        The number of bytes stored on the index page is never less than M.
        */
        let usable_page_size = page_size - reserved_bytes_per_page as u16;
        let x = usable_page_size - 23;
        let m: u64 = ((usable_page_size - 12) as u64 * 32 / 255) - 23;
        let k = m as i64
            + ((total_bytes_of_payload.0 as i64 - m as i64) % (usable_page_size as i64 - 4));
        let bytes_stored_on_leaf_page = if total_bytes_of_payload.0 <= x as i64 {
            total_bytes_of_payload.0
        } else {
            if k <= x as i64 {
                k
            } else {
                m as i64
            }
        };

        let record = if total_bytes_of_payload.0 > x.try_into()? {
            let record = OverflowRecord::from_be_bytes(
                bytes_stored_on_leaf_page,
                &cell_content[bytes_read..],
                db_file_name.clone(),
                page_size,
            )?;
            ReadableRecord::Lazy(record.0)
        } else {
            let record = Record::from_be_bytes(&cell_content[bytes_read..])?;
            ReadableRecord::Fit(record.0)
        };

        Ok((
            Self {
                total_bytes_of_payload,
                payload: record,
            },
            bytes_read as u64,
        ))
    }
}

pub struct IndexInteriorCell {
    pub left_child_page_number: u32,
    pub total_bytes_of_payload: VarInt,
    pub payload: ReadableRecord,
}

impl IndexInteriorCell {
    pub fn from_be_bytes(cell_content: &[u8]) -> Result<(Self, u64)> {
        let left_child_page_number = u32::from_be_bytes(cell_content[..4].try_into()?);

        let total_bytes_of_payload = VarInt::from_be_bytes(&cell_content[4..])?;
        let bytes_read = total_bytes_of_payload.1 as u64;

        Ok((
            Self {
                left_child_page_number,
                total_bytes_of_payload,
                payload: ReadableRecord::Fit(Record::from_be_bytes(&cell_content[4..])?.0),
            },
            4 + bytes_read,
        ))
    }
}

// Enum to standardize cell aggregations
pub enum LeafCell {
    Table(TableLeafCell),
    Index(IndexLeafCell),
}

impl LeafCell {
    pub fn get_readable_record(&self) -> ReadableRecord {
        match self {
            LeafCell::Table(cell) => cell.payload.clone(),
            LeafCell::Index(cell) => cell.payload.clone(),
        }
    }
}

pub enum InteriorCell {
    Table(TableInteriorCell),
    Index(IndexInteriorCell),
}

impl InteriorCell {
    pub fn get_left_child_page_number(&self) -> u32 {
        match self {
            InteriorCell::Table(cell) => cell.left_child_page_number,
            InteriorCell::Index(cell) => cell.left_child_page_number,
        }
    }
}