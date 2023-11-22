use anyhow::{anyhow, bail, ensure, Result};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until1, take_while1},
    character::complete::{multispace0, multispace1},
    combinator::{all_consuming, map, opt, value},
    multi::separated_list1,
    sequence::{delimited, pair, preceded, terminated, tuple},
    Finish, IResult,
};
use std::{ops::Deref, rc::Rc};

use crate::values::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    typ: TableType,
    name: Rc<str>,
    table_name: Rc<str>,
    root_page: usize,
    sql: Rc<str>,
}

impl Schema {
    pub fn from_record(record: &[Value]) -> Result<Self> {
        ensure!(
            record.len() == 5,
            "Invalid number of values in a create table record: {}",
            record.len()
        );

        let typ = match &record[0] {
            Value::Text(typ) => TableType::from_text(typ)?,
            otherwise => bail!("Invalid table type: {:?}", otherwise),
        };

        let name = match &record[1] {
            Value::Text(name) => name.clone(),
            otherwise => bail!("Invalid table name: {:?}", otherwise),
        };

        let table_name = match &record[2] {
            Value::Text(table_name) => table_name.clone(),
            otherwise => bail!("Invalid table name: {:?}", otherwise),
        };

        let root_page = match &record[3] {
            Value::Int(root_page) => usize::try_from(*root_page)?.saturating_sub(1),
            otherwise => bail!("Invalid root page: {:?}", otherwise),
        };

        let sql = match &record[4] {
            Value::Text(sql) => sql.clone(),
            otherwise => bail!("Invalid sql: {:?}", otherwise),
        };

        Ok(Self {
            typ,
            name,
            table_name,
            root_page,
            sql,
        })
    }

    pub fn typ(&self) -> TableType {
        self.typ
    }

    pub fn into_table_name(self) -> Rc<str> {
        self.table_name
    }

    pub fn table_name(&self) -> &str {
        self.table_name.as_ref()
    }

    pub fn root_page(&self) -> usize {
        self.root_page
    }

    pub fn table_def(&self) -> Result<TableDef<'_>> {
        assert!(self.typ == TableType::Table);
        TableDef::from_sql(self.sql.as_ref())
    }

    pub fn index_def(&self) -> Result<IndexDef<'_>> {
        assert!(self.typ == TableType::Index);
        IndexDef::from_sql(self.sql.as_ref())
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum TableType {
    Table,
    Index,
    View,
    Trigger,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableDef<'a> {
    pub name: &'a str,
    columns: Vec<ColumnDef<'a>>,
}

impl<'a> Deref for TableDef<'a> {
    type Target = [ColumnDef<'a>];

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

impl<'a> TableDef<'a> {
    fn from_sql(sql: &'a str) -> Result<Self> {
        Ok(parse_create_table_schema(sql)
            .finish()
            .map_err(|e| anyhow!("Unknown table schema: {} ({})", sql, e))?
            .1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexDef<'a> {
    columns: Vec<&'a str>,
}

impl<'a> Deref for IndexDef<'a> {
    type Target = [&'a str];

    fn deref(&self) -> &Self::Target {
        &self.columns
    }
}

impl<'a> IndexDef<'a> {
    fn from_sql(sql: &'a str) -> Result<Self> {
        Ok(parse_create_index_schema(sql)
            .finish()
            .map_err(|e| anyhow!("Unknown index schema: {} ({})", sql, e))?
            .1)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ColumnDef<'a> {
    pub name: &'a str,
    typ: Option<&'a str>,
    constraints: (),
}

fn parse_create_table_schema(sql: &str) -> IResult<&str, TableDef<'_>> {
    all_consuming(map(
        tuple((
            value(
                (),
                pair(
                    terminated(tag_no_case("CREATE"), multispace1),
                    terminated(tag_no_case("TABLE"), multispace1),
                ),
            ),
            quoted_name,
            delimited(
                delimited(multispace0, tag("("), multispace0),
                separated_list1(delimited(multispace0, tag(","), multispace0), column_def),
                delimited(multispace0, tag(")"), multispace0),
            ),
        )),
        |(_, name, columns)| TableDef { name, columns },
    ))(sql)
}

fn name(s: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_ascii_alphanumeric() || c == '_')(s)
}

fn ws_name(s: &str) -> IResult<&str, &str> {
    take_while1(|c: char| c.is_ascii_alphanumeric() || c == '_' || c == ' ')(s)
}

fn quoted_name(s: &str) -> IResult<&str, &str> {
    alt((delimited(tag("\""), ws_name, tag("\"")), name))(s)
}

fn column_def(s: &str) -> IResult<&str, ColumnDef<'_>> {
    map(
        tuple((
            quoted_name,
            opt(preceded(multispace1, name)),
            value((), opt(take_until1(","))),
        )),
        |(name, typ, constraints)| ColumnDef {
            name,
            typ,
            constraints,
        },
    )(s)
}

fn parse_create_index_schema(sql: &str) -> IResult<&str, IndexDef<'_>> {
    all_consuming(map(
        tuple((
            value(
                (),
                tuple((
                    terminated(tag_no_case("CREATE"), multispace1),
                    terminated(tag_no_case("INDEX"), multispace1),
                    quoted_name,
                    delimited(multispace1, tag_no_case("ON"), multispace1),
                    quoted_name,
                )),
            ),
            delimited(
                delimited(multispace0, tag("("), multispace0),
                separated_list1(delimited(multispace0, tag(","), multispace0), quoted_name),
                delimited(multispace0, tag(")"), multispace0),
            ),
        )),
        |(_, columns)| IndexDef { columns },
    ))(sql)
}

impl TableType {
    fn from_text(typ: &str) -> Result<Self> {
        match typ {
            "table" => Ok(Self::Table),
            "index" => Ok(Self::Index),
            "view" => Ok(Self::View),
            "trigger" => Ok(Self::Trigger),
            otherwise => bail!("Invalid table type: {}", otherwise),
        }
    }
}
