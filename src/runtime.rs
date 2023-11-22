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
    op: LeafOp,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Scan {
    Index {
        root_page: usize,
        value: Val,
    },
    Table {
        column: usize,
        value: Val,
        compare: Op,
    },
    FullTable,
}

#[derive(Clone, Debug, PartialEq)]
pub enum LeafOp {
    Count,
    SelectAll { primary_key: usize },
    Select { columns: Vec<(usize, bool)> },
}

impl PhysicalPlan {
    pub fn new(root_page: usize, scan: Scan, op: LeafOp) -> Self {
        Self {
            root_page,
            scan,
            op,
        }
    }

    pub fn execute(self, db: &mut Sqlite) {
        match &self.op {
            LeafOp::Count => self.execute_op(db, Count::default()),
            LeafOp::SelectAll { primary_key } => self.execute_op(db, SelectAll::new(*primary_key)),
            LeafOp::Select { ref columns } => self.execute_op(db, Select::new(columns)),
        }
    }

    fn execute_op(&self, db: &mut Sqlite, operator: impl Operator) {
        match self.scan {
            Scan::Index {
                root_page,
                ref value,
            } => index_scan(db, self.root_page, root_page, value, operator),
            Scan::Table {
                column,
                ref value,
                compare,
            } => table_scan(
                db,
                self.root_page,
                operator.filtered(column, value, compare),
            ),
            Scan::FullTable => table_scan(db, self.root_page, operator),
        }
    }
}

fn index_scan(
    db: &mut Sqlite,
    table_root: usize,
    index_root: usize,
    condition: &Val,
    mut operator: impl Operator,
) {
    page_index_scan(db, table_root, index_root, condition, &mut operator);
    operator.finish()
}

fn page_index_scan(
    db: &mut Sqlite,
    table_root: usize,
    page_id: usize,
    condition: &Val,
    operator: &mut impl Operator,
) {
    let page = db.page(page_id);

    match page {
        Page::LeafTable(_) | Page::InteriorTable(_) => panic!("Expected index page"),
        Page::LeafIndex(page) => {
            let lower = page.partition_point(|cell| cell.lower_bound(condition));
            let upper = page.partition_point(|cell| cell.upper_bound(condition));

            for cell in page.slice(lower..upper) {
                seek(db, table_root, cell.row_id, operator);
            }
        }
        Page::InteriorIndex(page) => {
            let lower = page.partition_point(|cell| cell.lower_bound(condition));
            let upper = page.partition_point(|cell| cell.upper_bound(condition));

            let right_ptr = page.get(upper).map_or(page.right_ptr, |cell| cell.left_ptr);

            for page in page.slice(lower..upper) {
                let row_id = page.row_id;
                page_index_scan(db, table_root, page.left_ptr as usize, condition, operator);

                seek(db, table_root, row_id, operator);
            }

            page_index_scan(db, table_root, right_ptr as usize, condition, operator);
        }
    };
}

fn table_scan(db: &mut Sqlite, root_page: usize, mut operator: impl Operator) {
    page_scan(db, root_page, &mut operator);
    operator.finish()
}

fn page_scan(db: &mut Sqlite, page: usize, operator: &mut impl Operator) {
    let page = db.page(page);

    match page {
        Page::LeafTable(page) => {
            for cell in page.iter() {
                operator.execute(&cell);
            }
        }
        Page::InteriorTable(page) => {
            let last_page = page.right_ptr as usize;
            for cell in page.iter() {
                page_scan(db, cell.left_ptr as usize, operator);
            }
            page_scan(db, last_page, operator);
        }
        Page::LeafIndex(_) | Page::InteriorIndex(_) => panic!("Expected table page"),
    };
}

fn seek(db: &mut Sqlite, root_page: usize, key: u64, operator: &mut impl Operator) {
    let mut next_page = Some(root_page);

    while let Some(page_id) = next_page.take() {
        let page = db.page(page_id);

        match page {
            Page::LeafIndex(_) | Page::InteriorIndex(_) => panic!("Expected table page"),
            Page::LeafTable(page) => {
                if let Ok(idx) = page.binary_search(&key, |cell| cell.row_id) {
                    operator.execute(&page.at(idx));
                };
            }
            Page::InteriorTable(page) => {
                let next_ptr = match page.binary_search(&key, |cell| cell.row_id) {
                    Ok(idx) => page.at(idx).left_ptr,
                    Err(idx) => page.get(idx).map_or(page.right_ptr, |cell| cell.left_ptr),
                };
                next_page = Some(next_ptr as usize);
            }
        };
    }
}
