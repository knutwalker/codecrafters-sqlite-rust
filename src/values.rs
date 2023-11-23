use std::cmp::Ordering;

#[derive(Copy, Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Typ {
    Null,
    Int,
    Float,
    Bool,
    Text,
    Blob,
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

impl PartialEq<()> for Val {
    fn eq(&self, _: &()) -> bool {
        matches!(self, Val::Null)
    }
}

impl PartialOrd<()> for Val {
    fn partial_cmp(&self, _: &()) -> Option<Ordering> {
        match self {
            Val::Null => Some(Ordering::Equal),
            _ => Some(Ordering::Greater),
        }
    }
}

impl PartialEq<bool> for Val {
    fn eq(&self, other: &bool) -> bool {
        match self {
            Val::Bool(val) => val == other,
            _ => false,
        }
    }
}

impl PartialOrd<bool> for Val {
    fn partial_cmp(&self, other: &bool) -> Option<Ordering> {
        match self {
            Val::Bool(val) => val.partial_cmp(other),
            _ => None,
        }
    }
}

impl PartialEq<i64> for Val {
    fn eq(&self, other: &i64) -> bool {
        match self {
            Val::Int(val) => val == other,
            _ => false,
        }
    }
}

impl PartialOrd<i64> for Val {
    fn partial_cmp(&self, other: &i64) -> Option<Ordering> {
        match self {
            Val::Int(val) => val.partial_cmp(other),
            _ => None,
        }
    }
}

impl PartialEq<f64> for Val {
    fn eq(&self, other: &f64) -> bool {
        match self {
            Val::Float(val) => val.0 == *other,
            _ => false,
        }
    }
}

impl PartialOrd<f64> for Val {
    fn partial_cmp(&self, other: &f64) -> Option<Ordering> {
        match self {
            Val::Float(val) => Some(val.0.total_cmp(other)),
            _ => None,
        }
    }
}

impl PartialEq<str> for Val {
    fn eq(&self, other: &str) -> bool {
        match self {
            Val::Text(val) => val == other,
            _ => false,
        }
    }
}

impl PartialOrd<str> for Val {
    fn partial_cmp(&self, other: &str) -> Option<Ordering> {
        match self {
            Val::Text(val) => val.as_str().partial_cmp(other),
            _ => None,
        }
    }
}

impl PartialEq<[u8]> for Val {
    fn eq(&self, _other: &[u8]) -> bool {
        false
    }
}

impl PartialOrd<[u8]> for Val {
    fn partial_cmp(&self, _other: &[u8]) -> Option<Ordering> {
        None
    }
}

impl Val {
    pub fn typ(&self) -> Typ {
        match self {
            Val::Null => Typ::Null,
            Val::Bool(..) => Typ::Bool,
            Val::Int(..) => Typ::Int,
            Val::Float(..) => Typ::Float,
            Val::Text(..) => Typ::Text,
        }
    }
}

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
