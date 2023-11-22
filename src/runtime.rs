use anyhow::{bail, Result};
use std::{collections::VecDeque, iter::once};

use crate::{
    operators::{Count, Op, Operator, Select, SelectAll},
    page::Page,
    values::Val,
    Sqlite,
};

#[derive(Clone, Debug, PartialEq)]
pub struct PhysicalPlan {
    root_page: usize,
    scan: Scan,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scan {
    Index {
        root_page: usize,
        column: usize,
        value: Val,
        op: LeafOp,
    },
    Table {
        column: usize,
        value: Val,
        compare: Op,
        op: LeafOp,
    },
    FullTable {
        op: LeafOp,
    },
}

#[derive(Clone, Debug, PartialEq)]
pub enum LeafOp {
    Count,
    SelectAll,
    Select { init: Vec<usize>, last: usize },
}

impl PhysicalPlan {
    pub fn new(root_page: usize, scan: Scan) -> Self {
        Self { root_page, scan }
    }

    pub fn execute(self, db: &mut Sqlite) -> Result<()> {
        match self.scan {
            Scan::Index {
                root_page,
                column,
                value,
                op,
            } => {
                let row_ids = index_scan(db, root_page, column, value)?;
                match op {
                    LeafOp::Count => Count::new(row_ids.len()).finish(),
                    LeafOp::SelectAll => seek_all(db, self.root_page, row_ids, SelectAll),
                    LeafOp::Select { init, last } => {
                        seek_all(db, self.root_page, row_ids, Select::new(init, last))
                    }
                }
            }
            Scan::Table {
                column,
                value,
                compare,
                op,
            } => match op {
                LeafOp::Count => table_scan_op(
                    db,
                    self.root_page,
                    Count::default().filtered(column, value, compare),
                ),
                LeafOp::SelectAll => table_scan_op(
                    db,
                    self.root_page,
                    SelectAll.filtered(column, value, compare),
                ),
                LeafOp::Select { init, last } => table_scan_op(
                    db,
                    self.root_page,
                    Select::new(init, last).filtered(column, value, compare),
                ),
            },
            Scan::FullTable { op } => match op {
                LeafOp::Count => table_scan_op(db, self.root_page, Count::default()),
                LeafOp::SelectAll => table_scan_op(db, self.root_page, SelectAll),
                LeafOp::Select { init, last } => {
                    table_scan_op(db, self.root_page, Select::new(init, last))
                }
            },
        }
    }
}

fn index_scan(
    db: &mut Sqlite,
    index_root: usize,
    index_column: usize,
    condition: Val,
) -> Result<Vec<u64>> {
    let mut row_ids = Vec::new();
    let mut next_pages = VecDeque::new();
    next_pages.push_back(index_root);

    while let Some(page_id) = next_pages.pop_front() {
        let page = db.page(page_id)?;

        match page {
            Page::LeafTable(_) | Page::InteriorTable(_) => bail!("Expected index page"),
            Page::LeafIndex(page) => {
                let lower = page.partition_point(|cell| cell.lower_bound(index_column, &condition));
                let upper = page.partition_point(|cell| cell.upper_bound(index_column, &condition));

                row_ids.extend(page[lower..upper].iter().map(|cell| cell.row_id));
            }
            Page::InteriorIndex(page) => {
                let lower = page.partition_point(|cell| cell.lower_bound(index_column, &condition));
                let upper = page.partition_point(|cell| cell.upper_bound(index_column, &condition));

                row_ids.extend(page[lower..upper].iter().map(|cell| cell.row_id));

                next_pages.extend(
                    page[lower..upper]
                        .iter()
                        .map(|cell| cell.left_ptr)
                        .chain(once(
                            page.get(upper).map_or(page.right_ptr, |cell| cell.left_ptr),
                        ))
                        .map(|ptr| ptr as usize),
                );
            }
        };
    }

    Ok(row_ids)
}

fn table_scan_op(db: &mut Sqlite, root_page: usize, mut operator: impl Operator) -> Result<()> {
    let mut stack = VecDeque::new();
    stack.push_back(root_page);

    while let Some(page) = stack.pop_front() {
        let page = db.page(page)?;

        match page {
            Page::LeafTable(page) => {
                for cell in page.iter() {
                    operator.execute(cell)?;
                }
            }
            Page::InteriorTable(page) => {
                stack.extend(page.iter().map(|cell| cell.left_ptr as usize));
                stack.push_back(page.right_ptr as usize);
            }
            Page::LeafIndex(_) | Page::InteriorIndex(_) => bail!("Expected table page"),
        };
    }

    operator.finish()
}

fn seek_all(
    db: &mut Sqlite,
    root_page: usize,
    keys: impl IntoIterator<Item = u64> + std::fmt::Debug,
    mut operator: impl Operator,
) -> Result<()> {
    for key in keys {
        seek_op(db, root_page, key, &mut operator)?;
    }
    operator.finish()
}

fn seek_op(
    db: &mut Sqlite,
    root_page: usize,
    key: u64,
    operator: &mut impl Operator,
) -> Result<()> {
    let mut next_page = Some(root_page);

    while let Some(page_id) = next_page.take() {
        let page = db.page(page_id)?;

        match page {
            Page::LeafIndex(_) | Page::InteriorIndex(_) => bail!("Expected table page"),
            Page::LeafTable(page) => {
                if let Ok(idx) = page.binary_search_by_key(&key, |cell| cell.row_id) {
                    operator.execute(&page[idx])?;
                };
            }
            Page::InteriorTable(page) => {
                let next_ptr = match page.binary_search_by_key(&key, |cell| cell.row_id) {
                    Ok(idx) => page[idx].left_ptr,
                    Err(idx) => page.get(idx).map_or(page.right_ptr, |cell| cell.left_ptr),
                };
                next_page = Some(next_ptr as usize);
            }
        };
    }

    Ok(())
}
