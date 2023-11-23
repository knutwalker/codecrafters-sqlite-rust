use std::cmp::Ordering;

use crate::{
    btree::LeafTableCell,
    record::{FilterVisitor, Printer, Visitor},
    values::{Typ, Val},
    Sqlite,
};

#[derive(Copy, Clone, Debug, PartialEq)]
pub enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

pub trait Operator {
    fn execute(&mut self, cell: &LeafTableCell);

    fn finish(self)
    where
        Self: Sized,
    {
    }

    fn filtered(self, column: usize, value: &Val, compare: Op) -> FilterOp<'_, Self>
    where
        Self: Sized,
    {
        FilterOp {
            compare,
            op: self,
            visitor: FilterVisitor::new(column, value),
        }
    }

    fn seek(
        &mut self,
        db: &mut Sqlite,
        root_page: usize,
        key: u64,
        seek: impl Fn(&mut Sqlite, usize, u64, &mut Self),
    ) where
        Self: Sized,
    {
        seek(db, root_page, key, self)
    }
}

impl<T: Operator> Operator for &mut T {
    fn execute(&mut self, cell: &LeafTableCell) {
        (**self).execute(cell)
    }
}

#[derive(Debug, Default)]
pub struct Count(usize);

impl Operator for Count {
    fn execute(&mut self, _cell: &LeafTableCell) {
        self.0 += 1;
    }

    fn finish(self) {
        println!("{}", self.0);
    }

    fn seek(
        &mut self,
        _db: &mut Sqlite,
        _root_page: usize,
        _key: u64,
        _seek: impl Fn(&mut Sqlite, usize, u64, &mut Self),
    ) {
        self.0 += 1;
    }
}

#[derive(Debug)]
pub struct SelectAll {
    primary_key: usize,
    row_id: u64,
}

impl SelectAll {
    pub fn new(primary_key: usize) -> Self {
        Self {
            primary_key,
            row_id: u64::MAX,
        }
    }
}

impl Operator for SelectAll {
    fn execute(&mut self, cell: &LeafTableCell) {
        self.row_id = cell.row_id;
        cell.payload.consume(self);
        println!()
    }
}

impl Visitor<'static> for SelectAll {
    fn read_next(&mut self, index: usize, typ: Typ) -> bool {
        if index > 0 {
            print!("|");
        }
        if index == self.primary_key && typ == Typ::Null {
            print!("{}", self.row_id);
            return false;
        }
        true
    }

    fn on_null(&mut self) {
        Printer.on_null()
    }

    fn on_bool(&mut self, value: bool) {
        Printer.on_bool(value)
    }

    fn on_int(&mut self, value: i64) {
        Printer.on_int(value)
    }

    fn on_float(&mut self, value: f64) {
        Printer.on_float(value)
    }

    fn on_text(&mut self, value: &'static str) {
        Printer.on_text(value)
    }

    fn on_blob(&mut self, value: &'static [u8]) {
        Printer.on_blob(value)
    }
}

pub struct Select<'a>(&'a [(usize, bool)], (usize, bool));

impl<'a> Select<'a> {
    pub fn new(columns: &'a [(usize, bool)]) -> Self {
        let (&last, init) = columns.split_last().unwrap();
        Self(init, last)
    }
}

impl<'a> Operator for Select<'a> {
    fn execute(&mut self, cell: &LeafTableCell) {
        for &(idx, pk) in self.0 {
            if pk {
                print!("{}", cell.row_id);
            } else {
                cell.payload.consume_one(idx, Printer);
            }
            print!("|");
        }

        let &(idx, pk) = &self.1;
        if pk {
            print!("{}", cell.row_id);
        } else {
            cell.payload.consume_one(idx, Printer);
        }

        println!();
    }

    fn seek(
        &mut self,
        db: &mut Sqlite,
        root_page: usize,
        key: u64,
        seek: impl Fn(&mut Sqlite, usize, u64, &mut Self),
    ) where
        Self: Sized,
    {
        if self.0.is_empty() && self.1 .1 {
            println!("{}", key);
        } else {
            seek(db, root_page, key, self);
        }
    }
}

pub struct FilterOp<'a, T> {
    compare: Op,
    op: T,
    visitor: FilterVisitor<'a>,
}

impl<'a, T: Operator> Operator for FilterOp<'a, T> {
    fn execute(&mut self, cell: &LeafTableCell) {
        let cmp = cell
            .payload
            .consume_one(self.visitor.column, &mut self.visitor)
            .result;
        let filter_matches = match self.compare {
            Op::Eq => cmp == Ordering::Equal,
            Op::Ne => cmp != Ordering::Equal,
            Op::Lt => cmp == Ordering::Less,
            Op::Le => cmp != Ordering::Greater,
            Op::Gt => cmp == Ordering::Greater,
            Op::Ge => cmp != Ordering::Less,
        };
        if filter_matches {
            self.op.execute(cell);
        }
    }

    fn finish(self) {
        self.op.finish()
    }
}
