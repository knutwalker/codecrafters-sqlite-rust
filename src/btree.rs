use anyhow::{anyhow, bail, ensure, Result};
use std::{cmp::Ordering, ops::Deref};

use crate::{
    header::DbHeader,
    record::Record,
    values::{Val, Value},
};

#[derive(Clone, Debug, PartialEq)]
pub struct LeafTablePage {
    cells: Box<[LeafTableCell]>,
}

impl Deref for LeafTablePage {
    type Target = [LeafTableCell];

    fn deref(&self) -> &Self::Target {
        &self.cells
    }
}

impl LeafTablePage {
    pub fn new(cells: Box<[LeafTableCell]>) -> Self {
        Self { cells }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafTableCell {
    pub row_id: u64,
    pub payload: Record,
    overflow: Option<Overflow>,
}

impl LeafTableCell {
    pub fn from_bytes(header: &DbHeader, bytes: &[u8]) -> Result<Self> {
        let (payload_size, bytes, _) = read_varint(bytes)?;
        let payload_size = usize::try_from(payload_size)
            .map_err(|_| anyhow!("Invalid payload size: {}", payload_size))?;

        let (row_id, bytes, _) = read_varint(bytes)?;

        let usable = header.page_size.saturating_sub(header.reserved_size);
        let max_inline_size = usable.saturating_sub(35);
        if payload_size <= max_inline_size {
            let payload = Record::from_bytes(header.text_encoding, Some(row_id), bytes)?;
            return Ok(Self {
                row_id,
                payload,
                overflow: None,
            });
        }

        let min_inline_size = usable
            .saturating_sub(12)
            .saturating_mul(32)
            .saturating_div(255)
            .saturating_sub(23);
        let cutoff_size = payload_size
            .saturating_sub(min_inline_size)
            .wrapping_rem(usable.saturating_sub(4))
            .saturating_add(min_inline_size);

        let stored_size = if cutoff_size <= max_inline_size {
            cutoff_size
        } else {
            min_inline_size
        };

        ensure!(bytes.len() >= stored_size + 4, "Not enough bytes, need at least {} bytes for the payload and 4 bytes for the overflow page", stored_size);
        let (bytes, overflow) = bytes.split_at(cutoff_size);
        let overflow_page = u32::from_be_bytes(overflow[..4].try_into()?);

        let payload = Record::from_bytes(header.text_encoding, Some(row_id), bytes)?;
        let overflow = Some(Overflow {
            page: overflow_page,
            spilled_length: payload_size - stored_size,
        });

        Ok(Self {
            row_id,
            payload,
            overflow,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteriorTablePage {
    pub right_ptr: u32,
    cells: Box<[InteriorTableCell]>,
}

impl Deref for InteriorTablePage {
    type Target = [InteriorTableCell];

    fn deref(&self) -> &Self::Target {
        &self.cells
    }
}

impl InteriorTablePage {
    pub fn new(right_ptr: u32, cells: Box<[InteriorTableCell]>) -> Self {
        Self { right_ptr, cells }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct InteriorTableCell {
    pub left_ptr: u32,
    pub row_id: u64,
}

impl InteriorTableCell {
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 4,
            "Not enough bytes to read a cell: expected at least 4 but only {} were available",
            bytes.len()
        );

        let (left_ptr, bytes) = bytes.split_at(4);
        let left_ptr = u32::from_be_bytes(left_ptr.try_into()?);

        let (row_id, _, _) = read_varint(bytes)?;

        Self::from_values(left_ptr, row_id)
    }

    fn from_values(left_ptr: u32, row_id: u64) -> Result<Self> {
        let left_ptr = left_ptr.checked_sub(1).ok_or_else(|| {
            anyhow!(
                "Invalid left pointer: {} (must be at least 1, but 0 is reserved)",
                left_ptr
            )
        })?;

        Ok(Self { left_ptr, row_id })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafIndexPage {
    cells: Box<[LeafIndexCell]>,
}

impl Deref for LeafIndexPage {
    type Target = [LeafIndexCell];

    fn deref(&self) -> &Self::Target {
        &self.cells
    }
}

impl LeafIndexPage {
    pub fn new(cells: Box<[LeafIndexCell]>) -> Self {
        Self { cells }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafIndexCell {
    index: usize,
    pub payload: Record,
    pub row_id: u64,
    overflow: Option<Overflow>,
}

impl LeafIndexCell {
    pub fn lower_bound(&self, filter: usize, condition: &Val) -> bool {
        self.compare(filter, condition) == Ordering::Greater
    }

    pub fn upper_bound(&self, filter: usize, condition: &Val) -> bool {
        self.compare(filter, condition) != Ordering::Less
    }

    fn compare(&self, filter: usize, lhs: &Val) -> Ordering {
        let rhs = &self.payload[filter];
        match lhs.partial_cmp(rhs) {
            Some(cmp) => cmp,
            None => panic!("Cannot compare {:?} and {:?}", lhs, rhs),
        }
    }

    pub fn from_bytes(header: &DbHeader, index: usize, bytes: &[u8]) -> Result<Self> {
        let (payload_size, bytes, _) = read_varint(bytes)?;
        let payload_size = usize::try_from(payload_size)
            .map_err(|_| anyhow!("Invalid payload size: {}", payload_size))?;

        let usable = header.page_size.saturating_sub(header.reserved_size);
        let max_inline_size = usable
            .saturating_sub(12)
            .saturating_mul(64)
            .saturating_div(255)
            .saturating_sub(23);
        if payload_size <= max_inline_size {
            let payload = Record::values_from_bytes(header.text_encoding, None, bytes)?;
            return Self::from_values(payload, index, None);
        }

        let min_inline_size = usable
            .saturating_sub(12)
            .saturating_mul(32)
            .saturating_div(255)
            .saturating_sub(23);
        let cutoff_size = payload_size
            .saturating_sub(min_inline_size)
            .wrapping_rem(usable.saturating_sub(4))
            .saturating_add(min_inline_size);

        let stored_size = if cutoff_size <= max_inline_size {
            cutoff_size
        } else {
            min_inline_size
        };

        ensure!(bytes.len() >= stored_size + 4, "Not enough bytes, need at least {} bytes for the payload and 4 bytes for the overflow page", stored_size);
        let (bytes, overflow) = bytes.split_at(cutoff_size);
        let overflow_page = u32::from_be_bytes(overflow[..4].try_into()?);

        let payload = Record::values_from_bytes(header.text_encoding, None, bytes)?;

        let overflow = Some(Overflow {
            page: overflow_page,
            spilled_length: payload_size - stored_size,
        });

        Self::from_values(payload, index, overflow)
    }

    fn from_values(
        mut payload: Vec<Value>,
        index: usize,
        overflow: Option<Overflow>,
    ) -> Result<Self> {
        let Some(Value::Int(row_id)) = payload.pop() else {
            bail!("Missing row id for index payload");
        };
        let row_id = row_id.try_into()?;
        let payload = Record::new(payload);

        Ok(Self {
            index,
            payload,
            row_id,
            overflow,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteriorIndexPage {
    pub right_ptr: u32,
    cells: Box<[InteriorIndexCell]>,
}

impl Deref for InteriorIndexPage {
    type Target = [InteriorIndexCell];

    fn deref(&self) -> &Self::Target {
        &self.cells
    }
}

impl InteriorIndexPage {
    pub fn new(right_ptr: u32, cells: Box<[InteriorIndexCell]>) -> Self {
        Self { right_ptr, cells }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct InteriorIndexCell {
    pub left_ptr: u32,
    cell: LeafIndexCell,
}

impl Deref for InteriorIndexCell {
    type Target = LeafIndexCell;

    fn deref(&self) -> &Self::Target {
        &self.cell
    }
}

impl InteriorIndexCell {
    pub fn lower_bound(&self, filter: usize, condition: &Val) -> bool {
        self.cell.lower_bound(filter, condition)
    }

    pub fn upper_bound(&self, filter: usize, condition: &Val) -> bool {
        self.cell.upper_bound(filter, condition)
    }

    pub(crate) fn from_bytes(header: &DbHeader, bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 4,
            "Not enough bytes to read a cell: expected at least 4 but only {} were available",
            bytes.len()
        );

        let (left_ptr, bytes) = bytes.split_at(4);
        let left_ptr = u32::from_be_bytes(left_ptr.try_into()?);

        let leaf = LeafIndexCell::from_bytes(header, usize::MAX, bytes)?;
        Self::from_values(left_ptr, leaf)
    }

    fn from_values(left_ptr: u32, cell: LeafIndexCell) -> Result<Self> {
        let left_ptr = left_ptr.checked_sub(1).ok_or_else(|| {
            anyhow!(
                "Invalid left pointer: {} (must be at least 1, but 0 is reserved)",
                left_ptr
            )
        })?;

        Ok(Self { left_ptr, cell })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Overflow {
    page: u32,
    spilled_length: usize,
}

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
