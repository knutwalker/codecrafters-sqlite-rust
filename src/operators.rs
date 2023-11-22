use std::cmp::Ordering;

use crate::{btree::LeafTableCell, values::Val};

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
            column,
            value,
            compare,
            op: self,
        }
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
}

#[derive(Debug)]
pub struct SelectAll {
    primary_key: usize,
}

impl SelectAll {
    pub fn new(primary_key: usize) -> Self {
        Self { primary_key }
    }
}

impl Operator for SelectAll {
    fn execute(&mut self, cell: &LeafTableCell) {
        let pk = self.primary_key;
        let mut index = 0;
        cell.payload.for_each(|val| {
            if index > 0 {
                print!("|");
            }
            if index == pk {
                print!("{}", cell.row_id);
            } else {
                print!("{}", val);
            }
            index += 1;
        });

        println!();
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
                print!("{}|", cell.row_id);
            } else {
                print!("{}|", cell.payload.value_at(idx));
            }
        }

        let &(idx, pk) = &self.1;
        if pk {
            print!("{}", cell.row_id);
        } else {
            print!("{}", cell.payload.value_at(idx));
        }

        println!();
    }
}

pub struct FilterOp<'a, T> {
    column: usize,
    value: &'a Val,
    compare: Op,
    op: T,
}

impl<'a, T: Operator> Operator for FilterOp<'a, T> {
    fn execute(&mut self, cell: &LeafTableCell) {
        let lhs = self.value;
        let rhs = cell.payload.value_at(self.column);
        let cmp = match lhs.partial_cmp(&rhs) {
            Some(cmp) => cmp,
            None => panic!("Cannot compare {:?} and {:?}", lhs, rhs),
        };
        let filter = match self.compare {
            Op::Eq => cmp == Ordering::Equal,
            Op::Ne => cmp != Ordering::Equal,
            Op::Lt => cmp == Ordering::Less,
            Op::Le => cmp != Ordering::Greater,
            Op::Gt => cmp == Ordering::Greater,
            Op::Ge => cmp != Ordering::Less,
        };
        if filter {
            self.op.execute(cell);
        }
    }

    fn finish(self) {
        self.op.finish()
    }
}
