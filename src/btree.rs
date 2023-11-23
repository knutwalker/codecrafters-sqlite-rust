use std::{cmp::Ordering, ops::Deref};

use crate::{
    page::{CellType, Cells, LazyCells},
    record::{read_varint, FilterVisitor, LazyRecord, ReadInt},
    values::Val,
};

pub type LeafTablePage = Cells<LeafTableCell>;
pub type LeafIndexPage = Cells<LeafIndexCell>;
pub type InteriorTablePage = Cells<InteriorTableCell, u32>;
pub type InteriorIndexPage = Cells<InteriorIndexCell, u32>;

impl LeafTablePage {
    pub fn new(page_size: usize, cells: LazyCells<LeafTableCell>) -> Self {
        Self::create(page_size, cells, ())
    }
}

impl LeafIndexPage {
    pub fn new(page_size: usize, cells: LazyCells<LeafIndexCell>) -> Self {
        Self::create(page_size, cells, ())
    }
}

impl InteriorTablePage {
    pub fn new(page_size: usize, right_ptr: u32, cells: LazyCells<InteriorTableCell>) -> Self {
        Self::create(page_size, cells, right_ptr)
    }
}

impl InteriorIndexPage {
    pub fn new(page_size: usize, right_ptr: u32, cells: LazyCells<InteriorIndexCell>) -> Self {
        Self::create(page_size, cells, right_ptr)
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafTableCell {
    pub row_id: u64,
    pub payload: LazyRecord,
}

impl CellType for LeafTableCell {
    fn new(bytes: &[u8]) -> Self {
        let (_payload_size, bytes, _) = read_varint(bytes);
        let (row_id, bytes, _) = read_varint(bytes);
        let payload = LazyRecord::new(bytes);
        Self { row_id, payload }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct LeafIndexCell {
    record: LazyRecord,
}

impl CellType for LeafIndexCell {
    fn new(bytes: &[u8]) -> Self {
        let (_payload_size, bytes, _) = read_varint(bytes);
        let payload = LazyRecord::new(bytes);
        Self { record: payload }
    }
}

impl LeafIndexCell {
    pub fn lower_bound(&self, condition: &Val) -> bool {
        self.compare(condition) == Ordering::Greater
    }

    pub fn upper_bound(&self, condition: &Val) -> bool {
        self.compare(condition) != Ordering::Less
    }

    fn compare(&self, lhs: &Val) -> Ordering {
        self.record
            .consume_one(0, FilterVisitor::new(0, lhs))
            .result
    }

    pub fn row_id(&self) -> u64 {
        self.record
            .consume_one(1, ReadInt::new())
            .get()
            .expect("row_id is always an int value") as u64
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct InteriorTableCell {
    pub left_ptr: u32,
    pub row_id: u64,
}

impl CellType for InteriorTableCell {
    fn new(bytes: &[u8]) -> Self {
        let (left_ptr, bytes) = bytes.split_at(4);
        let left_ptr = u32::from_be_bytes(left_ptr.try_into().unwrap());

        let (row_id, _, _) = read_varint(bytes);

        let left_ptr = left_ptr - 1;
        Self { left_ptr, row_id }
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

impl CellType for InteriorIndexCell {
    fn new(bytes: &[u8]) -> Self {
        let (left_ptr, bytes) = bytes.split_at(4);
        let left_ptr = u32::from_be_bytes(left_ptr.try_into().unwrap());

        let cell = LeafIndexCell::new(bytes);
        let left_ptr = left_ptr - 1;
        InteriorIndexCell { left_ptr, cell }
    }
}

impl InteriorIndexCell {
    pub fn lower_bound(&self, condition: &Val) -> bool {
        self.cell.lower_bound(condition)
    }

    pub fn upper_bound(&self, condition: &Val) -> bool {
        self.cell.upper_bound(condition)
    }
}
