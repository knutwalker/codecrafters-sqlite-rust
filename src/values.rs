use std::{cmp::Ordering, fmt::Display};

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

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Value {
    Null,
    Int(i64),
    Float(TotalFloat),
    Text(*const u8, usize),
    Blob(*const u8, usize),
}

impl Value {
    pub fn as_int(&self) -> Option<i64> {
        match self {
            Value::Int(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(v.0),
            _ => None,
        }
    }

    pub fn as_text<'a>(self) -> Option<&'a str> {
        match self {
            Value::Text(ptr, len) => unsafe {
                Some(std::str::from_utf8_unchecked(std::slice::from_raw_parts(
                    ptr, len,
                )))
            },
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl PartialEq<Value> for Val {
    fn eq(&self, other: &Value) -> bool {
        match self {
            Val::Null => other.is_null(),
            Val::Bool(val) => other.as_int().is_some_and(|i| i == i64::from(*val)),
            Val::Int(val) => other.as_int().is_some_and(|i| i == *val),
            Val::Float(val) => other.as_float().is_some_and(|f| f == val.0),
            Val::Text(val) => other.as_text().is_some_and(|s| s == *val),
        }
    }
}

impl PartialOrd<Value> for Val {
    fn partial_cmp(&self, other: &Value) -> Option<Ordering> {
        match self {
            Val::Null => other.is_null().then_some(Ordering::Equal),
            Val::Bool(lhs) => other.as_int().map(|rhs| i64::from(*lhs).cmp(&rhs)),
            Val::Int(lhs) => other.as_int().map(|rhs| lhs.cmp(&rhs)),
            Val::Float(lhs) => other.as_float().map(|rhs| lhs.0.total_cmp(&rhs)),
            Val::Text(lhs) => other.as_text().map(|rhs| lhs.as_str().cmp(rhs)),
        }
    }
}

impl Value {
    pub fn payload_size(serial_type: u64) -> usize {
        match serial_type {
            0 | 8 | 9 | 10 | 11 => 0,
            1 => 1,
            2 => 2,
            3 => 3,
            4 => 4,
            5 => 6,
            6 | 7 => 8,
            otherwise => ((otherwise >> 1) - 6) as usize,
        }
    }

    pub fn from_bytes(serial_type: u64, bytes: &[u8]) -> (&[u8], Self) {
        match serial_type {
            0 => (bytes, Self::Null),
            1 => int::<1>(bytes),
            2 => int::<2>(bytes),
            3 => int::<3>(bytes),
            4 => int::<4>(bytes),
            5 => int::<6>(bytes),
            6 => int::<8>(bytes),
            7 => float(bytes),
            8 => (bytes, Self::Int(0)),
            9 => (bytes, Self::Int(1)),
            10 | 11 => panic!("Cannot read from an internal serial type: {}", serial_type),
            otherwise => text_or_blob(otherwise, bytes),
        }
    }
}

fn int<const N: usize>(bytes: &[u8]) -> (&[u8], Value) {
    let (bytes, rest) = bytes.split_at(N);
    let mut data = [0; 8];
    data[8 - N..].copy_from_slice(bytes);
    (rest, Value::Int(i64::from_be_bytes(data)))
}

fn float(bytes: &[u8]) -> (&[u8], Value) {
    let (bytes, rest) = bytes.split_at(8);
    let data: [u8; 8] = bytes.try_into().unwrap();
    (rest, Value::Float(TotalFloat(f64::from_be_bytes(data))))
}

fn text_or_blob(serial: u64, bytes: &[u8]) -> (&[u8], Value) {
    let len = (serial >> 1) - 6;
    let (bytes, rest) = bytes.split_at(len as usize);
    let value = match (serial & 0x1) == 0x1 {
        true => Value::Text(bytes.as_ptr(), len as usize),
        false => Value::Blob(bytes.as_ptr(), len as usize),
    };
    (rest, value)
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v.0),
            v @ Value::Text(..) => write!(f, "{}", v.as_text().unwrap()),
            Value::Blob(..) => write!(f, "BLOB"),
        }
    }
}
