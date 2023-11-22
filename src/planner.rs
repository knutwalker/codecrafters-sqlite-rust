use anyhow::{anyhow, bail, Result};

use crate::{
    operators::Op,
    page::Page,
    runtime::{LeafOp, PhysicalPlan, Scan},
    schema::{Schema, TableDef, TableType},
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

        let Page::LeafTable(root_page) = db.page(0) else {
            panic!("Root page is not a leaf table page");
        };

        let (mut table_schemas, index_schemas): (Vec<_>, Vec<_>) = root_page
            .iter()
            .map(|cell| Schema::from_record(cell.payload))
            .filter(|schema| schema.table_name() == from_table)
            .partition(|schema| schema.typ() == TableType::Table);

        let table_schema = table_schemas
            .pop()
            .ok_or_else(|| anyhow!("Table not found: {}", from_table))?;

        if !table_schemas.is_empty() {
            bail!("Ambiguous table name: {}", from_table);
        }

        let table_def = table_schema.table_def()?;

        let op = translate_op(&table_def, self.op)?;

        if let Some(filter) = self.filter {
            #[cfg(not(disable_index))]
            for index in index_schemas {
                let index_def = index.index_def()?;
                if *index_def == [filter.column] {
                    let scan = Scan::Index {
                        root_page: index.root_page(),
                        value: filter.value,
                    };

                    return Ok(PhysicalPlan::new(table_schema.root_page(), scan, op));
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
            };

            return Ok(PhysicalPlan::new(table_schema.root_page(), scan, op));
        }

        Ok(PhysicalPlan::new(
            table_schema.root_page(),
            Scan::FullTable,
            op,
        ))
    }
}

fn translate_op(table: &TableDef, op: LogicalOp) -> Result<LeafOp> {
    Ok(match op {
        LogicalOp::Count => LeafOp::Count,
        LogicalOp::SelectAll => {
            let primary_key = table
                .iter()
                .position(|c| c.primary_key)
                .unwrap_or(usize::MAX);

            LeafOp::SelectAll { primary_key }
        }
        LogicalOp::Select(columns) => {
            let columns = columns
                .into_iter()
                .map(|col| {
                    table
                        .iter()
                        .enumerate()
                        .find_map(|(index, c)| (c.name == col).then_some((index, c.primary_key)))
                        .ok_or_else(|| anyhow!("Column not found: {}", col))
                })
                .collect::<Result<Vec<_>>>()?;

            if columns.is_empty() {
                bail!("No columns selected");
            }

            if columns.iter().map(|(idx, _)| *idx).eq(0..table.len()) {
                translate_op(table, LogicalOp::SelectAll)?
            } else {
                LeafOp::Select { columns }
            }
        }
    })
}
