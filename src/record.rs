use std::ops::Deref;

use anyhow::{anyhow, Result};
use nom::{bytes::complete::take_while_m_n, number::complete::u8, IResult};

use crate::{header::TextEncoding, values::Value};

fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
    let mut result = 0;
    let mut bytes_read = 0;

    let mut bs = bytes.iter().copied();

    loop {
        let byte = bs
            .next()
            .ok_or_else(|| anyhow!("Unexpected end of bytes"))?;
        bytes_read += 1;

        if bytes_read == 9 {
            result = (result << 7) | u64::from(byte);
            break;
        }

        result = (result << 7) | u64::from(byte & 0b0111_1111);

        if byte & 0b1000_0000 == 0 {
            break;
        }
    }

    Ok((result, &bytes[bytes_read..], bytes_read))
}

fn _nom_varint(input: &[u8]) -> IResult<&[u8], u64> {
    let (mut rest, prefix) = take_while_m_n(1, 9, |b| b & 0b1000_0000 == 0b1000_0000)(input)?;

    let (last, prefix, last_shift) = if prefix.len() == 9 {
        let (last, prefix) = prefix.split_last().unwrap();
        (*last, prefix, 8)
    } else {
        let (new_rest, last) = u8(rest)?;
        rest = new_rest;
        (last, prefix, 7)
    };

    let prefix = prefix
        .iter()
        .fold(0_u64, |acc, b| (acc << 7) | u64::from(b & 0b0111_1111));

    Ok((rest, (prefix << last_shift) | u64::from(last)))
}

#[derive(Clone, Debug, PartialEq)]
pub struct Record {
    values: Box<[Value]>,
}

impl Record {
    pub fn from_bytes(
        text_encoding: TextEncoding,
        row_id: Option<u64>,
        bytes: &[u8],
    ) -> Result<Self> {
        let values = Self::values_from_bytes(text_encoding, row_id, bytes)?;
        Ok(Self {
            values: values.into_boxed_slice(),
        })
    }

    pub fn new(values: Vec<Value>) -> Self {
        Self {
            values: values.into_boxed_slice(),
        }
    }

    pub(crate) fn values_from_bytes(
        text_encoding: TextEncoding,
        row_id: Option<u64>,
        bytes: &[u8],
    ) -> Result<Vec<Value>> {
        let (header_size, bytes, header_size_length) = read_varint(bytes)?;
        let (mut header, mut bytes) =
            bytes.split_at(usize::try_from(header_size)? - header_size_length);

        let mut values = Vec::new();

        while !header.is_empty() {
            let (serial_type, rest, _) = read_varint(header)?;
            header = rest;
            let (rest, mut value) = Value::from_bytes(text_encoding, serial_type, bytes)?;
            bytes = rest;

            if values.is_empty() {
                if let (Value::Null, Some(row_id)) = (&value, row_id) {
                    value = Value::Int(row_id as i64);
                }
            }

            values.push(value);
        }

        Ok(values)
    }
}

impl Deref for Record {
    type Target = [Value];

    fn deref(&self) -> &Self::Target {
        &self.values
    }
}
