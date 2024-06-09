use anyhow::{anyhow, Result};
use std::convert::TryInto;

#[derive(Debug, Clone)]
pub enum PageType {
    InteriorIndex,
    InteriorTable,
    LeafIndex,
    LeafTable,
}

impl PageType {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            2 => Some(PageType::InteriorIndex),
            5 => Some(PageType::InteriorTable),
            10 => Some(PageType::LeafIndex),
            13 => Some(PageType::LeafTable),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub enum PageHeader {
    Leaf(LeafPageHeader),
    Interior(InteriorPageHeader),
}

#[derive(Debug, Clone)]
pub struct CommonPageHeader {
    start_of_first_free_block: u16,
    pub num_cells: u16,
    pub start_of_cell_content_area: u16,
    num_fragmented_free_bytes: u8,
}

impl CommonPageHeader {
    fn from_buffer(page_buffer: &Vec<u8>, offset: usize) -> Result<Self> {
        let start_of_first_free_block =
            u16::from_be_bytes(page_buffer[1 + offset..3 + offset].try_into()?);
        let num_cells = u16::from_be_bytes(page_buffer[3 + offset..5 + offset].try_into()?);
        let start_of_cell_content_area =
            u16::from_be_bytes(page_buffer[5 + offset..7 + offset].try_into()?);
        let num_fragmented_free_bytes = u8::from_be(page_buffer[7 + offset]);
        Ok(CommonPageHeader {
            start_of_first_free_block,
            num_cells,
            start_of_cell_content_area,
            num_fragmented_free_bytes,
        })
    }
}

#[derive(Debug, Clone)]
pub struct LeafPageHeader {
    pub common_header: CommonPageHeader,
}

#[derive(Debug, Clone)]
pub struct InteriorPageHeader {
    pub common_header: CommonPageHeader,
    pub right_most_pointer: u32,
}

#[derive(Debug, Clone)]
pub struct BtreePage {
    pub page_type: PageType,
    pub page_header: PageHeader,
    raw_byte_buffer: Vec<u8>,
    pub reserved_bytes_per_page: u8,
}

impl BtreePage {
    /*
     * Give a buffer and an offset to the header read in the header and return the obj
     */
    pub fn new(
        page_byte_buffer: Vec<u8>,
        offset: usize,
        reserved_bytes_per_page: u8,
    ) -> Result<Self> {
        let page_type = PageType::from_u8(page_byte_buffer[0 + offset])
            .ok_or(anyhow!("invalid page type of btree page"))?;

        let common_header = CommonPageHeader::from_buffer(&page_byte_buffer, offset)?;

        let page_header = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => {
                let right_most_pointer =
                    u32::from_be_bytes(page_byte_buffer[8 + offset..12 + offset].try_into()?);
                PageHeader::Interior(InteriorPageHeader {
                    common_header,
                    right_most_pointer,
                })
            }
            PageType::LeafIndex | PageType::LeafTable => {
                PageHeader::Leaf(LeafPageHeader { common_header })
            }
        };

        Ok(Self {
            page_type,
            page_header,
            raw_byte_buffer: page_byte_buffer,
            reserved_bytes_per_page,
        })
    }

    // returns only the byte array for the cell content into th
    pub fn get_raw_bytes_buffer(&self) -> &Vec<u8> {
        &self.raw_byte_buffer
    }
}
