use anyhow::{anyhow, bail, Result};
use itertools::{process_results, Itertools};

use crate::{
    operators::Op,
    page::Page,
    runtime::{LeafOp, PhysicalPlan, Scan},
    schema::{Schema, TableType},
    values::Val,
    Sqlite,
};

#[derive(Debug, Clone, PartialEq)]
pub struct Plan<'a> {
    op: LogicalOp<'a>,
    table: &'a str,
    filter: Option<Filter<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogicalOp<'a> {
    Count,
    SelectAll,
    Select(Vec<&'a str>),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Filter<'a> {
    op: Op,
    column: &'a str,
    value: Val,
}

impl<'a> Filter<'a> {
    pub fn new(op: Op, column: &'a str, value: Val) -> Self {
        Self { op, column, value }
    }
}

impl<'a> Plan<'a> {
    pub fn new(op: LogicalOp<'a>, table: &'a str, filter: Option<Filter<'a>>) -> Self {
        Self { op, table, filter }
    }

    pub fn translate(self, db: &mut Sqlite) -> Result<PhysicalPlan> {
        let from_table = self.table;

        let Page::LeafTable(root_page) = db.page(0)? else {
            bail!("Root page is not a leaf table page");
        };

        let (mut table_schemas, index_schemas): (Vec<_>, Vec<_>) = process_results(
            root_page
                .iter()
                .map(|cell| Schema::from_record(&cell.payload))
                .filter_ok(|create_table| create_table.table_name() == from_table),
            |i| i.partition(|s| s.typ() == TableType::Table),
        )?;

        let table_schema = table_schemas
            .pop()
            .ok_or_else(|| anyhow!("Table not found: {}", from_table))?;

        if !table_schemas.is_empty() {
            bail!("Ambiguous table name: {}", from_table);
        }

        let table_def = table_schema.table_def()?;

        let op = match self.op {
            LogicalOp::Count => LeafOp::Count,
            LogicalOp::SelectAll => LeafOp::SelectAll,
            LogicalOp::Select(columns) => {
                let mut columns = columns
                    .into_iter()
                    .map(|col| {
                        table_def
                            .iter()
                            .position(|c| c.name == col)
                            .ok_or_else(|| anyhow!("Column not found: {}", col))
                    })
                    .collect::<Result<Vec<_>>>()?;

                if columns.is_empty() {
                    bail!("No columns selected");
                }

                if columns.iter().copied().eq(0..table_def.len()) {
                    LeafOp::SelectAll
                } else {
                    let last = columns.pop().unwrap();
                    LeafOp::Select {
                        init: columns,
                        last,
                    }
                }
            }
        };

        if let Some(filter) = self.filter {
            for index in index_schemas {
                let index_def = index.index_def()?;
                if *index_def == [filter.column] {
                    let scan = Scan::Index {
                        root_page: index.root_page(),
                        column: 0,
                        value: filter.value,
                        op,
                    };

                    return Ok(PhysicalPlan::new(table_schema.root_page(), scan));
                }
            }

            let column = table_def
                .iter()
                .position(|c| c.name == filter.column)
                .ok_or_else(|| anyhow!("Column not found: {}", filter.column))?;

            let scan = Scan::Table {
                column,
                value: filter.value,
                compare: filter.op,
                op,
            };

            return Ok(PhysicalPlan::new(table_schema.root_page(), scan));
        }

        Ok(PhysicalPlan::new(
            table_schema.root_page(),
            Scan::FullTable { op },
        ))
    }
}
