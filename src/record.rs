/*
The data for a table b-tree leaf page and the key of an index b-tree page was characterized above as an arbitrary sequence of bytes. The prior discussion mentioned one key being less than another, but did not define what "less than" meant. The current section will address these omissions.

Payload, either table b-tree data or index b-tree keys, is always in the "record format". The record format defines a sequence of values corresponding to columns in a table or index. The record format specifies the number of columns, the datatype of each column, and the content of each column.

The record format makes extensive use of the variable-length integer or varint representation of 64-bit signed integers defined above.

A record contains a header and a body, in that order. The header begins with a single varint which determines the total number of bytes in the header. The varint value is the size of the header in bytes including the size varint itself. Following the size varint are one or more additional varints, one per column. These additional varints are called "serial type" numbers and determine the datatype of each column, according to the following chart:

Serial Type Codes Of The Record Format
Serial Type	Content Size	Meaning
0	0	Value is a NULL.
1	1	Value is an 8-bit twos-complement integer.
2	2	Value is a big-endian 16-bit twos-complement integer.
3	3	Value is a big-endian 24-bit twos-complement integer.
4	4	Value is a big-endian 32-bit twos-complement integer.
5	6	Value is a big-endian 48-bit twos-complement integer.
6	8	Value is a big-endian 64-bit twos-complement integer.
7	8	Value is a big-endian IEEE 754-2008 64-bit floating point number.
8	0	Value is the integer 0. (Only available for schema format 4 and higher.)
9	0	Value is the integer 1. (Only available for schema format 4 and higher.)
10,11	variable	Reserved for internal use. These serial type codes will never appear in a well-formed database file, but they might be used in transient and temporary database files that SQLite sometimes generates for its own use. The meanings of these codes can shift from one release of SQLite to the next.
N≥12 and even	(N-12)/2	Value is a BLOB that is (N-12)/2 bytes in length.
N≥13 and odd	(N-13)/2	Value is a string in the text encoding and (N-13)/2 bytes in length. The nul terminator is not stored.
The header size varint and serial type varints will usually consist of a single byte. The serial type varints for large strings and BLOBs might extend to two or three byte varints, but that is the exception rather than the rule. The varint format is very efficient at coding the record header.

The values for each column in the record immediately follow the header. For serial types 0, 8, 9, 12, and 13, the value is zero bytes in length. If all columns are of these types then the body section of the record is empty.

A record might have fewer values than the number of columns in the corresponding table. This can happen, for example, after an ALTER TABLE ... ADD COLUMN SQL statement has increased the number of columns in the table schema without modifying preexisting rows in the table. Missing values at the end of the record are filled in using the default value for the corresponding columns defined in the table schema.

The record format defines a sequence of values corresponding to columns in a table or index. The record format specifies the number of columns, the datatype of each column, and the content of each column.

The record format defines a sequence of values corresponding to columns in a table or index. The record format specifies the number of columns, the datatype of each column, and the content of each column.

*/

use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
};

use anyhow::{bail, Result};

use crate::sql_data_types::{SerialData, SerialDataError, SerialType, VarInt, VarIntError};

use log::debug;
use std::convert::TryInto;

#[derive(Debug, Clone)]
pub struct Record {
    serial_data: Vec<SerialData>,
}

impl Record {
    pub fn from_be_bytes(bytes: &[u8]) -> Result<(Self, u64)> {
        let mut total_offset = 0;
        let header_size_varint = VarInt::from_be_bytes(&bytes[total_offset..])?;
        total_offset += header_size_varint.1 as usize;

        let mut serial_types = Vec::new();
        while total_offset < header_size_varint.0 as usize {
            let serial_type_varint = VarInt::from_be_bytes(&bytes[total_offset..])?;
            total_offset += serial_type_varint.1 as usize;

            serial_types.push(SerialType::from_varint(serial_type_varint)?);
        }

        let body = &bytes[total_offset..];

        let mut offset = 0;
        // now from serial types array read the body and create serial_data
        let mut serial_data = Vec::new();
        for serial_type in serial_types {
            let (data, bytes_read) = serial_type.serial_type_to_serial_data(&body[offset..])?;
            offset += bytes_read;
            serial_data.push(data);
        }
        Ok((Record { serial_data }, (total_offset + offset).try_into()?))
    }
}

#[derive(Clone, Debug)]
pub struct OverflowRecord {
    record_header_size: u64,
    raw_record_payload: Vec<u8>,
    overflow_page: u32,
    db_file_name: String,
    page_size: u16,
}

impl OverflowRecord {
    pub fn from_be_bytes(
        bytes_stored_on_leaf: i64,
        bytes: &[u8],
        db_file_name: String,
        page_size: u16,
    ) -> Result<(Self, u64)> {
        // dont read the full payload in memory just the metadata
        let record_header_size_op = VarInt::from_be_bytes(bytes)?;
        let record_header_size = record_header_size_op.0;
        let bytes_read = record_header_size_op.1;

        let init_raw_record_payload_size = bytes_stored_on_leaf - bytes_read as i64;
        let start_of_record_payload = bytes_read as usize;
        let end_of_record_payload = (bytes_read as i64 + init_raw_record_payload_size) as usize;
        let init_raw_record_payload: Vec<u8> =
            bytes[start_of_record_payload..end_of_record_payload].to_vec();

        let overflow_page = u32::from_be_bytes([
            bytes[end_of_record_payload],
            bytes[end_of_record_payload + 1],
            bytes[end_of_record_payload + 2],
            bytes[end_of_record_payload + 3],
        ]);

        // return the bytes_stored_on_leaf for consistency
        Ok((
            OverflowRecord {
                record_header_size: record_header_size.try_into()?,
                raw_record_payload: init_raw_record_payload,
                overflow_page,
                db_file_name,
                page_size,
            },
            bytes_stored_on_leaf.try_into()?,
        ))
    }

    // reads record that can overflow to multiple linked list pages from the root record portion
    fn read_record(&mut self) -> Result<Vec<SerialData>> {
        // create a file handle because overflowing records need to be seeking the db file on disk for the linked list reads
        let mut db_file_handle = File::open(self.db_file_name.clone())?;
        let page_size = self.page_size;
        // read the header given that we know the header size already
        let mut total_offset: usize = 0;
        let mut local_offset: usize = 0;
        let mut serial_types = Vec::new();
        while (total_offset as u64) < self.record_header_size - 1 {
            if local_offset >= self.raw_record_payload.len() {
                if self.overflow_page == 0 {
                    bail!("Overflow record is missing a page");
                }
                // read the next page, retaining nothing from the current page
                db_file_handle.seek(std::io::SeekFrom::Start(
                    (self.overflow_page - 1) as u64 * page_size as u64,
                ))?;
                // see if the overflow page has metadata for another page after this in the linked list
                let mut next_page_num_repr = [0; 4];
                db_file_handle.read_exact(&mut next_page_num_repr)?;
                let next_page_num = u32::from_be_bytes(next_page_num_repr);
                self.overflow_page = next_page_num;
                let mut next_page_bytes = vec![0; page_size as usize];
                db_file_handle.read(&mut next_page_bytes)?;
                self.raw_record_payload = next_page_bytes;
                local_offset = 0;
            }

            let serial_type_varint =
                match VarInt::from_be_bytes(&self.raw_record_payload[local_offset..]) {
                    Ok(varint) => varint,
                    Err(err) => match err {
                        VarIntError::Incomplete => {
                            if self.overflow_page == 0 {
                                bail!("Overflow record is missing a page");
                            }
                            // read in the next page
                            let next_page_addr_bytes =
                                (self.overflow_page - 1) as u64 * page_size as u64;
                            db_file_handle.seek(std::io::SeekFrom::Start(next_page_addr_bytes))?;
                            let mut next_page_bytes = [0; 4];
                            db_file_handle.read_exact(&mut next_page_bytes)?;
                            let next_page = u32::from_be_bytes(next_page_bytes);
                            self.overflow_page = next_page;
                            // retain the bytes in previous buffer from local offset till end of buffer
                            let previous_buffer = self.raw_record_payload[local_offset..].to_vec();
                            // the -4 accounts for the metadata on each page for the next overflow page address stored in the first 4 bytes
                            let mut next_page_bytes = vec![0; page_size as usize - 4];

                            db_file_handle.read(&mut next_page_bytes)?;
                            self.raw_record_payload = previous_buffer;
                            self.raw_record_payload.extend(next_page_bytes);
                            local_offset = 0;
                            continue;
                        }
                        e => bail!(e),
                    },
                };

            let bytes_read = serial_type_varint.1 as usize;
            local_offset += bytes_read;
            total_offset += bytes_read;
            serial_types.push(SerialType::from_varint(serial_type_varint)?);
        }

        let mut serial_data = Vec::new();
        let mut i = 0;
        while i < serial_types.len() {
            let serial_type = &serial_types[i];
            let (data, bytes_read) = match serial_type
                .serial_type_to_serial_data(&self.raw_record_payload[local_offset..])
            {
                Ok(res) => res,
                Err(err) => match err.downcast_ref::<SerialDataError>() {
                    Some(SerialDataError::OutOfBounds) => {
                        debug!("load extra page");

                        if self.overflow_page == 0 {
                            bail!("Overflow record is missing a page");
                        }
                        // read in the next page
                        let page_to_read_addr_bytes =
                            (self.overflow_page - 1) as u64 * page_size as u64;
                        db_file_handle.seek(SeekFrom::Start(page_to_read_addr_bytes))?;

                        let mut next_page_number_as_bytes = [0; 4];
                        db_file_handle.read_exact(&mut next_page_number_as_bytes)?;

                        // I am choosing to let the buffer read extra bytes since we know pages are sized as chunks of max_page_size
                        // the - 4 accounts for the metadata on each page for the next overflow page address stored in the first 4 bytes
                        let mut next_page_bytes = vec![0; page_size as usize - 4];
                        db_file_handle.read(&mut next_page_bytes)?;

                        let next_page = u32::from_be_bytes(next_page_number_as_bytes);
                        self.overflow_page = next_page;

                        // retain the bytes in previous buffer from local offset till end of buffer
                        let mut new_buffer = self.raw_record_payload[local_offset..].to_vec();
                        new_buffer.extend(next_page_bytes);

                        self.raw_record_payload = new_buffer;
                        local_offset = 0;

                        continue;
                    }
                    // Any other error while converting is not recoverable or expected
                    _ => bail!(err),
                },
            };

            local_offset += bytes_read;
            serial_data.push(data);
            i += 1;
        }

        Ok(serial_data)
    }
}

// lets us standardize the interface for reading records that may overflow or not overflow

#[derive(Clone, Debug)]
pub enum ReadableRecord {
    Fit(Record),
    Lazy(OverflowRecord),
}

impl ReadableRecord {
    pub fn read_record(&mut self) -> Result<Vec<SerialData>> {
        match self {
            ReadableRecord::Fit(fitting) => Ok(fitting.serial_data.clone()),
            ReadableRecord::Lazy(overflowing) => overflowing.read_record(),
        }
    }
}
