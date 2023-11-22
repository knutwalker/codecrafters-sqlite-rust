use anyhow::{anyhow, bail, Result};
use nom::{
    bytes::complete::take,
    combinator::{map, map_res, success},
    number::complete::{be_f64, be_i16, be_i24, be_i32, be_i64, i8},
    Finish, IResult,
};
use std::{cmp::Ordering, fmt::Display, rc::Rc};

use crate::header::TextEncoding;

#[derive(Copy, Clone, Debug, Default, PartialEq)]
pub struct TotalFloat(pub f64);

impl Eq for TotalFloat {}

impl Ord for TotalFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for TotalFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
pub enum Val {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(TotalFloat),
    Text(String),
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Null,
    Int(i64),
    Float(TotalFloat),
    Text(Rc<str>),
    Blob(Rc<[u8]>),
}

impl PartialEq<Value> for Val {
    fn eq(&self, other: &Value) -> bool {
        match (self, other) {
            (Val::Null, Value::Null) => true,
            (Val::Bool(false), Value::Int(0)) => true,
            (Val::Bool(true), Value::Int(1)) => true,
            (Val::Int(lhs), Value::Int(rhs)) => lhs == rhs,
            (Val::Float(lhs), Value::Float(rhs)) => lhs == rhs,
            (Val::Text(lhs), Value::Text(rhs)) => **lhs == **rhs,
            _ => false,
        }
    }
}

impl PartialOrd<Value> for Val {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match (self, other) {
            (Val::Null, Value::Null) => Some(Ordering::Equal),
            (Val::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Val::Bool(false), Value::Int(0)) => Some(Ordering::Equal),
            (Val::Bool(true), Value::Int(1)) => Some(Ordering::Equal),
            (Val::Int(lhs), Value::Int(rhs)) => Some(lhs.cmp(rhs)),
            (Val::Float(lhs), Value::Float(rhs)) => lhs.partial_cmp(rhs),
            (Val::Text(lhs), Value::Text(rhs)) => Some(lhs.as_str().cmp(rhs)),
            _ => None,
        }
    }
}

impl Value {
    pub fn from_bytes(
        text_encoding: TextEncoding,
        serial_type: u64,
        bytes: &[u8],
    ) -> Result<(&[u8], Self)> {
        Self::parse(text_encoding, serial_type, bytes)
            .finish()
            .map_err(|e| {
                anyhow!(
                    "Failed to parse a value of serial type {}: {:?}",
                    serial_type,
                    e
                )
            })
    }

    fn parse(text_encoding: TextEncoding, serial_type: u64, bytes: &[u8]) -> IResult<&[u8], Self> {
        match serial_type {
            0 => Ok((bytes, Self::Null)),
            1 => map(i8, |i| Self::Int(i64::from(i)))(bytes),
            2 => map(be_i16, |i| Self::Int(i64::from(i)))(bytes),
            3 => map(be_i24, |i| Self::Int(i64::from(i)))(bytes),
            4 => map(be_i32, |i| Self::Int(i64::from(i)))(bytes),

            5 => map(Self::be_i48, Self::Int)(bytes),
            6 => map(be_i64, Self::Int)(bytes),
            7 => map(be_f64, |f| Self::Float(TotalFloat(f)))(bytes),
            8 => Ok((bytes, Self::Int(0))),
            9 => Ok((bytes, Self::Int(1))),
            10 | 11 => map_res(success(()), |()| -> Result<_> {
                bail!("Cannot read from an internal serial type: {}", serial_type)
            })(bytes),
            otherwise => Self::text_or_blob(text_encoding, otherwise, bytes),
        }
    }

    fn text_or_blob(
        text_encoding: TextEncoding,
        serial: u64,
        bytes: &[u8],
    ) -> IResult<&[u8], Self> {
        let len = (serial >> 1) - 6;
        match (serial & 0x1) == 0x1 {
            true => map(
                map_res(take(len), |data| text_encoding.read(data)),
                |text| Self::Text(text.into()),
            )(bytes),
            false => map(take(len), |data: &[u8]| Self::Blob(data.into()))(bytes),
        }
    }

    fn be_i48(input: &[u8]) -> IResult<&[u8], i64> {
        map(take(6_usize), |bytes| {
            let mut data = [0; 8];
            data[2..].copy_from_slice(bytes);
            i64::from_be_bytes(data)
        })(input)
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v.0),
            Value::Text(v) => write!(f, "{}", v),
            Value::Blob(v) => write!(f, "{:?}", v),
        }
    }
}
