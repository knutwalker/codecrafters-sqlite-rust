use anyhow::{anyhow, Result};
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
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()>;

    fn finish(self) -> Result<()>;

    fn filtered(self, column: usize, value: Val, compare: Op) -> FilterOp<Self>
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
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        (**self).execute(cell)
    }

    fn finish(self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct Count(usize);

impl Count {
    pub fn new(count: usize) -> Self {
        Self(count)
    }
}

impl Operator for Count {
    fn execute(&mut self, _cell: &LeafTableCell) -> Result<()> {
        self.0 += 1;
        Ok(())
    }

    fn finish(self) -> Result<()> {
        println!("{}", self.0);
        Ok(())
    }
}

#[derive(Debug, Default)]
pub struct SelectAll;

impl Operator for SelectAll {
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        let (last, init) = cell.payload.split_last().unwrap();
        for val in init {
            print!("{}|", val);
        }
        print!("{}", last);
        println!();
        Ok(())
    }

    fn finish(self) -> Result<()> {
        Ok(())
    }
}

pub struct Select(Vec<usize>, usize);

impl Select {
    pub fn new(initial_columns: Vec<usize>, last_column: usize) -> Self {
        Self(initial_columns, last_column)
    }
}

impl Operator for Select {
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        for &idx in &self.0 {
            print!("{}|", cell.payload[idx]);
        }
        print!("{}", cell.payload[self.1]);
        println!();
        Ok(())
    }

    fn finish(self) -> Result<()> {
        Ok(())
    }
}

pub struct FilterOp<T> {
    column: usize,
    value: Val,
    compare: Op,
    op: T,
}

impl<T: Operator> Operator for FilterOp<T> {
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        let lhs = &self.value;
        let rhs = &cell.payload[self.column];
        let cmp = match lhs.partial_cmp(rhs) {
            Some(cmp) => cmp,
            None => return Err(anyhow!("Cannot compare {:?} and {:?}", lhs, rhs)),
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
            self.op.execute(cell)?;
        }
        Ok(())
    }

    fn finish(self) -> Result<()> {
        self.op.finish()
    }
}
