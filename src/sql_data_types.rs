use anyhow::bail;

/*
A variable-length integer or "varint" is a static Huffman encoding of 64-bit
twos-complement integers that uses less space for small positive values.
A varint is between 1 and 9 bytes in length. The varint consists of either zero
or more bytes which have the high-order bit set followed by a single byte with
the high-order bit clear, or nine bytes, whichever is shorter. The lower seven
bits of each of the first eight bytes and all 8 bits of the ninth byte are used
to reconstruct the 64-bit twos-complement integer.

Varints are big-endian: bits taken from the earlier byte of the varint are more
significant than bits taken from the later bytes.
*/
// VarInt is a struct that holds a 64-bit integer and the number of bytes used to encode it.
#[derive(Debug, Clone)]
pub struct VarInt(pub i64, pub u8);

#[derive(Debug)]
pub enum VarIntError {
    Empty,
    Incomplete,
    TooLong,
}

impl std::fmt::Display for VarIntError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            VarIntError::Empty => write!(f, "VarInt cannot be empty"),
            VarIntError::TooLong => write!(f, "VarInt cannot exceed 9 bytes"),
            VarIntError::Incomplete => write!(f, "VarInt incomplete representation"),
        }
    }
}

impl std::error::Error for VarIntError {}

impl VarInt {
    // read from big endian bytes into a i64
    pub fn from_be_bytes(bytes: &[u8]) -> Result<Self, VarIntError> {
        if bytes.is_empty() {
            return Err(VarIntError::Empty.into());
        }

        /*
         * The value of the VarInt is stored in the lower 7 bits of each byte.
         * The high bit is used to indicate if there are more bytes to read.
         * The value is read from the bytes in big-endian order.
         * "value" is used to accumulate all the bits read from the bytes and then casted to i64.
         * we read the big endian bytes left to right, and make space in value by shifting to the left 7 bytes each time,
         * and then adding the next byte to the right of it.
         */
        let mut bytes_used_to_encode = 0;
        let mut value = 0;
        let mut complete_repr = false;
        for byte in bytes {
            // Remove the high bit and add the lower 7 bits to the value
            // The high bit is removed by AND'ing with 0b0111_1111 (0x7F)
            value = (value << 7) | (byte & 0b0111_1111) as i64;
            bytes_used_to_encode += 1;

            // we cannot have more than 9 bytes to encode a VarInt
            if (bytes_used_to_encode == 9) && (byte & 0b1000_0000) == 1 {
                return Err(VarIntError::TooLong.into());
            }

            // If the high bit is not set, this is the last byte
            if (byte & 0b1000_0000) == 0 {
                complete_repr = true;
                break;
            }
        }

        if !complete_repr {
            return Err(VarIntError::Incomplete.into());
        }

        Ok(VarInt(value, bytes_used_to_encode))
    }
}

/*
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
*/

#[derive(Debug, Clone)]
pub enum SerialType {
    Null,
    I8,
    I16,
    I24,
    I32,
    I48,
    I64,
    F64,
    Zero,
    One,
    Reserved,
    Blob(i64), // size
    Text(i64), // size
}

impl SerialType {
    pub fn from_varint(varint: VarInt) -> anyhow::Result<Self> {
        let serial_type = match varint.0 {
            0 => SerialType::Null,
            1 => SerialType::I8,
            2 => SerialType::I16,
            3 => SerialType::I24,
            4 => SerialType::I32,
            5 => SerialType::I48,
            6 => SerialType::I64,
            7 => SerialType::F64,
            8 => SerialType::Zero,
            9 => SerialType::One,
            10 | 11 => SerialType::Reserved,
            n if n >= 12 && n % 2 == 0 => {
                let size = (n - 12) / 2;
                SerialType::Blob(size)
            }
            n if n >= 13 && n % 2 == 1 => {
                let size = (n - 13) / 2;
                SerialType::Text(size)
            }
            _ => bail!("Invalid serial type"),
        };

        Ok(serial_type)
    }
}

#[derive(Debug, Clone)]
pub enum SerialData {
    Null,
    I8(i8),
    I16(i16),
    I24(i32),
    I32(i32),
    I48(i64),
    I64(i64),
    F64(f64),
    Zero,
    One,
    Reserved,
    Blob(Vec<u8>),
    Text(String),
}

#[derive(Debug)]
pub enum SerialDataError {
    OutOfBounds,
}

impl std::error::Error for SerialDataError {}

impl std::fmt::Display for SerialDataError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SerialDataError::OutOfBounds => write!(f, "Out of bounds error"),
        }
    }
}

impl SerialType {
    pub fn serial_type_to_serial_data(&self, body: &[u8]) -> anyhow::Result<(SerialData, usize)> {
        if body.is_empty() {
            return Err(SerialDataError::OutOfBounds.into());
        }

        match self {
            SerialType::Null => Ok((SerialData::Null, 0)),
            SerialType::I8 => {
                let data = body[0].try_into()?;
                Ok((SerialData::I8(data), 1))
            }
            SerialType::I16 => {
                if 2 > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let data = i16::from_be_bytes(body[..2].try_into()?);
                Ok((SerialData::I16(data), 2))
            }
            SerialType::I24 => {
                if 3 > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let mut _32_byte_repr_of_24_byte: [u8; 4] = [0; 4];
                _32_byte_repr_of_24_byte[1..4].copy_from_slice(&body[..3]);
                let data = i32::from_be_bytes(_32_byte_repr_of_24_byte);
                Ok((SerialData::I24(data), 3))
            }
            SerialType::I32 => {
                if 4 > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let data = i32::from_be_bytes(body[..4].try_into()?);
                Ok((SerialData::I32(data), 4))
            }
            SerialType::I48 => {
                if 6 > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let mut _64_byte_repr_of_48_byte: [u8; 8] = [0; 8];
                _64_byte_repr_of_48_byte[2..8].copy_from_slice(&body[..6]);
                let data = i64::from_be_bytes(_64_byte_repr_of_48_byte);
                Ok((SerialData::I48(data), 6))
            }
            SerialType::I64 => {
                if 8 > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let data = i64::from_be_bytes(body[..8].try_into()?);
                Ok((SerialData::I64(data), 8))
            }
            SerialType::F64 => {
                if 8 > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let data = f64::from_be_bytes(body[..8].try_into()?);
                Ok((SerialData::F64(data), 8))
            }
            SerialType::Zero => Ok((SerialData::Zero, 0)),
            SerialType::One => Ok((SerialData::One, 0)),
            SerialType::Reserved => Ok((SerialData::Reserved, 0)),
            SerialType::Blob(size) => {
                let end_offset = *size as usize;
                if end_offset > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let blob = body[..end_offset].to_vec();
                Ok((SerialData::Blob(blob), *size as usize))
            }
            SerialType::Text(size) => {
                let end_offset = *size as usize;
                if end_offset > body.len() {
                    return Err(SerialDataError::OutOfBounds.into());
                }

                let text = String::from_utf8(body[..end_offset].to_vec())?;

                Ok((SerialData::Text(text), *size as usize))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_try_from_varint_valid() {
        // Test valid varints
        let varint_bytes: &[u8] = &[0b0000_0001];
        let varint = VarInt::from_be_bytes(varint_bytes).unwrap();
        assert_eq!(varint.0, 1);
        assert_eq!(varint.1, 1);

        let varint_bytes: &[u8] = &[0b0000_0000];
        let varint = VarInt::from_be_bytes(varint_bytes).unwrap();
        assert_eq!(varint.0, 0);
        assert_eq!(varint.1, 1);

        let varint_bytes: &[u8] = &[0b0000_0100];
        let varint = VarInt::from_be_bytes(varint_bytes).unwrap();
        assert_eq!(varint.0, 4);
        assert_eq!(varint.1, 1);

        let varint_bytes: &[u8] = &[0b1000_0001, 0b0000_0001];
        let varint = VarInt::from_be_bytes(varint_bytes).unwrap();
        assert_eq!(varint.0, 129);
        assert_eq!(varint.1, 2);
    }
}
