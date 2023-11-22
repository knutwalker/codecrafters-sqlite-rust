use anyhow::{anyhow, bail, Result};
use nom::{
    branch::alt,
    bytes::complete::{tag, tag_no_case, take_until1, take_while1},
    character::complete::{multispace0, multispace1},
    combinator::{all_consuming, map, opt, value},
    multi::separated_list1,
    sequence::{delimited, pair, preceded, terminated, tuple},
    Finish, IResult,
};
use std::ops::Deref;

use crate::record::LazyRecord;

#[derive(Debug, PartialEq)]
pub struct Schema {
    record: LazyRecord,
}

impl Schema {
    pub fn typ(&self) -> TableType {
        let Some(typ) = self.record.value_at(0).as_text() else {
            unreachable!("type is always a text value")
        };

        match TableType::from_text(typ) {
            Ok(typ) => typ,
            Err(e) => panic!("Invalid table type: {}", e),
        }
    }

    pub fn table_name(&self) -> &str {
        let Some(table_name) = self.record.value_at(2).as_text() else {
            unreachable!("table_name is always a text value")
        };

        table_name
    }

    pub fn table_def(&self) -> Result<TableDef<'_>> {
        let Some(sql) = self.record.value_at(4).as_text() else {
            unreachable!("sql is always a text value")
        };

        TableDef::from_sql(sql)
    }

    pub fn index_def(&self) -> Result<IndexDef<'_>> {
        let Some(sql) = self.record.value_at(4).as_text() else {
            unreachable!("sql is always a text value")
        };

        IndexDef::from_sql(sql)
    }

    pub fn root_page(&self) -> usize {
        let Some(root_page) = self.record.value_at(3).as_int() else {
            unreachable!("root_page is always an int value")
        };

        root_page as usize - 1
    }

    pub fn from_record(record: LazyRecord) -> Self {
        Self { record }
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
    pub primary_key: bool,
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
            opt(value((), preceded(multispace1, tag_no_case("PRIMARY KEY")))),
            value((), opt(take_until1(","))),
        )),
        |(name, typ, pk, _)| ColumnDef {
            name,
            typ,
            primary_key: pk.is_some(),
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
