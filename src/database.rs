/*
Metadata Info
Offset	Size	Description
0	16	The header string: "SQLite format 3\000"
16	2	The database page size in bytes. Must be a power of two between 512 and 32768 inclusive, or the value 1 representing a page size of 65536.
18	1	File format write version. 1 for legacy; 2 for WAL.
19	1	File format read version. 1 for legacy; 2 for WAL.
20	1	Bytes of unused "reserved" space at the end of each page. Usually 0.
21	1	Maximum embedded payload fraction. Must be 64.
22	1	Minimum embedded payload fraction. Must be 32.
23	1	Leaf payload fraction. Must be 32.
24	4	File change counter.
28	4	Size of the database file in pages. The "in-header database size".
32	4	Page number of the first freelist trunk page.
36	4	Total number of freelist pages.
40	4	The schema cookie.
44	4	The schema format number. Supported schema formats are 1, 2, 3, and 4.
48	4	Default page cache size.
52	4	The page number of the largest root b-tree page when in auto-vacuum or incremental-vacuum modes, or zero otherwise.
56	4	The database text encoding. A value of 1 means UTF-8. A value of 2 means UTF-16le. A value of 3 means UTF-16be.
60	4	The "user version" as read and set by the user_version pragma.
64	4	True (non-zero) for incremental-vacuum mode. False (zero) otherwise.
68	4	The "Application ID" set by PRAGMA application_id.
72	20	Reserved for expansion. Must be zero.
92	4	The version-valid-for number.
96	4	SQLITE_VERSION_NUMBER
*/

use crate::btree::Btree;
use crate::sql_data_types::{SerialData, SerialType};
use anyhow::{bail, Result};
use nom::character::complete::tab;
use std::convert::TryInto;
use std::fs::File;
use std::io::Read;

#[derive(Debug)]
pub enum FileFormatVersion {
    LEGACY,
    WAL,
}

impl FileFormatVersion {
    pub fn from_u8(val: u8) -> Option<Self> {
        match val {
            1 => Some(Self::LEGACY),
            2 => Some(Self::WAL),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum SchemaFormatNumber {
    Fmt1,
    Fmt2,
    Fmt3,
    Fmt4,
}

impl SchemaFormatNumber {
    pub fn from_u32(val: u32) -> Option<Self> {
        match val {
            1 => Some(Self::Fmt1),
            2 => Some(Self::Fmt2),
            3 => Some(Self::Fmt3),
            4 => Some(Self::Fmt4),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum DatabaseTextEncoding {
    Utf8,
    Utf16le,
    Utf16be,
}

impl DatabaseTextEncoding {
    pub fn from_u32(val: u32) -> Option<Self> {
        match val {
            1 => Some(Self::Utf8),
            2 => Some(Self::Utf16le),
            3 => Some(Self::Utf16be),
            _ => None,
        }
    }
}

// only making this class for ser-deser help
#[derive(Debug)]
pub enum IsIncrementalVacuumMode {
    True,
    False,
}

impl IsIncrementalVacuumMode {
    pub fn from_u32(val: u32) -> Self {
        match val {
            0 => Self::False,
            _ => Self::True,
        }
    }
}

#[derive(Debug)]
pub struct DataBaseMetadata {
    // 0 - 16
    pub header_str: [u8; 16],
    // 16 - 18
    pub page_size: u16,
    // 18 - 19
    pub file_format_write_version: FileFormatVersion,
    // 19 - 20
    pub file_format_read_version: FileFormatVersion,
    // 20 - 21, 1 byte integer
    pub bytes_unused_reserved_space_at_page_end: u8,
    // 21 - 22
    pub max_embedded_payload_fraction: u8,
    // 22 - 23
    pub min_embedded_payload_fraction: u8,
    // 23 - 24
    pub leaf_payload_fraction: u8,
    // 24 - 28
    pub file_change_counter: u32,
    // 28 - 32
    pub db_size_in_pages: u32,
    // 32 - 36
    pub first_freelist_trunk_page_num: u32,
    // 36 - 40
    pub total_freelist_pages: u32,
    // 40 - 44
    pub schema_cookie: u32,
    // 44 - 48
    pub schema_format_number: SchemaFormatNumber,
    // 48 - 52
    pub default_page_cache_size: u32,
    // 52 - 56
    pub page_num_largest_root_btee_in_vacccum: u32,
    // 56 - 60
    pub database_text_encoding: DatabaseTextEncoding,
    // 60 - 64
    pub user_version: u32,
    // 64 - 68
    pub incremental_vacuum_mode: IsIncrementalVacuumMode,
    // 68 - 72
    pub application_id: u32,
    // 72 - 92
    pub expansion_reserved: [u8; 20],
    // 92 - 96
    pub version_valid_for: u32,
    // 96 - 100
    pub sqlite_vesion_number: u32,
}

impl DataBaseMetadata {
    pub fn read_from_file(file_name: &str) -> Result<DataBaseMetadata> {
        let mut file = File::open(file_name)?;
        let mut buffer = [0u8; 100];
        // reads 0-100
        file.read_exact(&mut buffer)?;

        let header_str: [u8; 16] = buffer[0..16].try_into().unwrap();
        let page_size = u16::from_be_bytes(buffer[16..18].try_into().unwrap());
        let file_format_write_version = FileFormatVersion::from_u8(buffer[18]).unwrap();
        let file_format_read_version = FileFormatVersion::from_u8(buffer[19]).unwrap();
        let bytes_unused_reserved_space_at_page_end = buffer[20];
        let max_embedded_payload_fraction = buffer[21];
        let min_embedded_payload_fraction = buffer[22];
        let leaf_payload_fraction = buffer[23];
        let file_change_counter = u32::from_be_bytes(buffer[24..28].try_into().unwrap());
        let db_size_in_pages = u32::from_be_bytes(buffer[28..32].try_into().unwrap());
        let first_freelist_trunk_page_num = u32::from_be_bytes(buffer[32..36].try_into().unwrap());
        let total_freelist_pages = u32::from_be_bytes(buffer[36..40].try_into().unwrap());
        let schema_cookie = u32::from_be_bytes(buffer[40..44].try_into().unwrap());
        let schema_format_number =
            SchemaFormatNumber::from_u32(u32::from_be_bytes(buffer[44..48].try_into().unwrap()))
                .unwrap();
        let default_page_cache_size = u32::from_be_bytes(buffer[48..52].try_into().unwrap());
        let page_num_largest_root_btee_in_vacccum =
            u32::from_be_bytes(buffer[52..56].try_into().unwrap());
        let database_text_encoding =
            DatabaseTextEncoding::from_u32(u32::from_be_bytes(buffer[56..60].try_into().unwrap()))
                .unwrap();
        let user_version = u32::from_be_bytes(buffer[60..64].try_into().unwrap());
        let incremental_vacuum_mode = IsIncrementalVacuumMode::from_u32(u32::from_be_bytes(
            buffer[64..68].try_into().unwrap(),
        ));
        let application_id = u32::from_be_bytes(buffer[68..72].try_into().unwrap());
        let expansion_reserved: [u8; 20] = buffer[72..92].try_into().unwrap();
        let version_valid_for = u32::from_be_bytes(buffer[92..96].try_into().unwrap());
        let sqlite_vesion_number = u32::from_be_bytes(buffer[96..100].try_into().unwrap());

        Ok(DataBaseMetadata {
            header_str,
            page_size,
            file_format_write_version,
            file_format_read_version,
            bytes_unused_reserved_space_at_page_end,
            max_embedded_payload_fraction,
            min_embedded_payload_fraction,
            leaf_payload_fraction,
            file_change_counter,
            db_size_in_pages,
            first_freelist_trunk_page_num,
            total_freelist_pages,
            schema_cookie,
            schema_format_number,
            default_page_cache_size,
            page_num_largest_root_btee_in_vacccum,
            database_text_encoding,
            user_version,
            incremental_vacuum_mode,
            application_id,
            expansion_reserved,
            version_valid_for,
            sqlite_vesion_number,
        })
    }
}

// While we may call this a database struct this is actually just holding metadata shit
// most of the actual stuff is happening in our btree
pub struct Database {
    pub db_file: String,
    pub metadata: DataBaseMetadata,
    //  sqlite_schema table contains the root page number for every other table and index in the database file.
    schema_table_btree: Btree,
}

// Indexes and Tables are both just Tables in the master table, but the index is just a different type.
// So we can just use the same struct for both
#[derive(Debug, Clone)]
pub struct TableInfo {
    pub obj_type: String,
    pub name: String,
    pub table_name: String,
    pub root_page_num: i64,
    pub sql: String,
}

impl Database {
    pub fn from_file(db_file_name: &String) -> Result<Self> {
        let metadata = DataBaseMetadata::read_from_file(db_file_name)?;

        let schema_table_btree = Btree::read_schema_table(
            db_file_name,
            metadata.page_size.try_into()?,
            metadata.bytes_unused_reserved_space_at_page_end,
        )?;

        Ok(Database {
            metadata,
            db_file: db_file_name.clone(),
            schema_table_btree,
        })
    }

    pub fn get_master_table(&self) -> Result<Vec<TableInfo>> {
        let mut results = Vec::new();
        let mut records = self.schema_table_btree.get_rows(true)?;
        // now since we know the schema of the schema table we can map the record to TableInfo
        for record in &mut records {
            let record_data = record.read_record()?;
            if record_data.len() != 5 {
                bail!("Invalid record");
            }

            let obj_type = match &record_data[0] {
                SerialData::Text(txt) => txt.clone(),
                _ => bail!("Invalid obj_type"),
            };

            let name = match &record_data[1] {
                SerialData::Text(txt) => txt.clone(),
                _ => bail!("Invalid name"),
            };

            let table_name = match &record_data[2] {
                SerialData::Text(txt) => txt.clone(),
                _ => bail!("Invalid table_name"),
            };

            let root_page_num = match &record_data[3] {
                SerialData::I64(num) => *num,
                SerialData::I8(num) => i64::from(*num),
                SerialData::I16(num) => i64::from(*num),
                SerialData::I24(num) => i64::from(*num),
                SerialData::I48(num) => i64::from(*num),
                _ => bail!("Invalid root_page_num"),
            };

            let sql = match &record_data[4] {
                SerialData::Text(txt) => txt.clone(),
                _ => bail!("Invalid sql"),
            };

            results.push(TableInfo {
                obj_type,
                name,
                table_name,
                root_page_num,
                sql,
            });
        }

        Ok(results)
    }

    pub fn get_table(&self, table_name: &str) -> Result<Btree> {
        self.btree_from_info(|x: &TableInfo| x.obj_type == "table" && x.table_name == table_name)
    }

    pub fn get_index(&self, index_name: &str) -> Result<Btree> {
        self.btree_from_info(|x: &TableInfo| x.obj_type == "index" && x.name == index_name)
    }

    pub fn get_indices_for_table(&self, table_name: &str) -> Result<Vec<Btree>> {
        let mut results = Vec::new();
        let records = self.get_master_table()?;
        for record in records {
            if record.obj_type == "index" && record.table_name == table_name {
                results.push(Btree::read_table(
                    &self.db_file,
                    self.metadata.page_size.try_into()?,
                    ((record.root_page_num - 1) * self.metadata.page_size as i64).try_into()?,
                    self.metadata.bytes_unused_reserved_space_at_page_end,
                )?);
            }
        }

        Ok(results)
    }

    pub fn get_table_columns(&self, table_name: &str) -> Result<Vec<(String, SerialType)>> {
        // Parse the Create SQL message to do this?
        let table_finder = |x: &TableInfo| x.obj_type == "table" && x.table_name == table_name;
        let table_info = self.get_obj_info(table_finder)?;

        // now use the stored create statement to parse shit
        let schema = find_schema_from_create_stmt(table_info.sql)?;

        todo!()
    }

    fn btree_from_info<F>(&self, predicate: F) -> Result<Btree>
    where
        F: Fn(&TableInfo) -> bool,
    {
        let info = self.get_obj_info(predicate)?;

        Btree::read_table(
            &self.db_file,
            self.metadata.page_size.try_into()?,
            ((info.root_page_num - 1) * self.metadata.page_size as i64).try_into()?,
            self.metadata.bytes_unused_reserved_space_at_page_end,
        )
    }

    fn get_obj_info<F>(&self, predicate: F) -> Result<TableInfo>
    where
        F: Fn(&TableInfo) -> bool,
    {
        match self.get_master_table()?.iter().find(|x| predicate(x)) {
            Some(t) => Ok(t.clone()),
            None => bail!("Obj not found in master table"),
        }
    }
}
