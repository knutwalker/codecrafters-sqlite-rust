use anyhow::{anyhow, bail, ensure, Result};
use itertools::{process_results, Itertools};
use nom::{
    branch::alt,
    bytes::complete::{escaped_transform, tag, tag_no_case, take_until1, take_while1},
    character::complete::{i64, multispace0, multispace1, none_of, one_of},
    combinator::{all_consuming, cut, map, opt, value},
    multi::separated_list1,
    number::complete::double,
    sequence::{delimited, pair, preceded, terminated, tuple},
    Finish, IResult,
};
use std::{
    borrow::Cow,
    cmp::Ordering,
    collections::VecDeque,
    ffi::CStr,
    fmt::Display,
    fs::File,
    io::{Read as _, Seek as _, SeekFrom},
    iter::once,
    mem::size_of,
    rc::Rc,
};

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let file = File::open(&args[1])?;
    let mut db = Sqlite::new(file)?;

    // Parse command and act accordingly
    let command = args[2].as_str();
    match command {
        ".dbinfo" => {
            let page_size = db.header.page_size;
            println!("database page size: {}", page_size);

            let Page::LeafTable(root_page) = db.page(0)? else {
                bail!("Root page is not a leaf table page");
            };

            let num_tables = process_results(
                root_page
                    .cells
                    .iter()
                    .map(|cell| Schema::from_record(&cell.payload))
                    .filter_ok(|ct| ct.typ == TableType::Table),
                |tables| tables.count(),
            )?;

            println!("number of tables: {}", num_tables);
        }
        ".tables" => {
            let Page::LeafTable(root_page) = db.page(0)? else {
                bail!("Root page is not a leaf table page");
            };

            process_results(
                root_page
                    .cells
                    .iter()
                    .map(|cell| Schema::from_record(&cell.payload))
                    .filter_map_ok(|ct| (ct.typ == TableType::Table).then_some(ct.table_name)),
                |tables| tables.for_each(|table| println!("{}", table)),
            )?;
        }
        query => {
            let plan = parse_query(query)?;
            let plan = plan.translate(&mut db)?;
            plan.execute(&mut db)?;
        }
    };

    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
struct Plan<'a> {
    op: LogicalOp<'a>,
    table: &'a str,
    filter: Option<Filter<'a>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum LogicalOp<'a> {
    Count,
    SelectAll,
    Select(Vec<&'a str>),
}

#[derive(Clone, Debug, PartialEq)]
struct PhysicalPlan {
    root_page: usize,
    scan: Scan,
}

#[derive(Clone, Debug, PartialEq)]
enum Scan {
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
enum LeafOp {
    Count,
    SelectAll,
    Select { init: Vec<usize>, last: usize },
}

impl Plan<'_> {
    fn translate(self, db: &mut Sqlite) -> Result<PhysicalPlan> {
        let from_table = self.table;

        let Page::LeafTable(root_page) = db.page(0)? else {
            bail!("Root page is not a leaf table page");
        };

        let (mut table_schemas, index_schemas): (Vec<_>, Vec<_>) = process_results(
            root_page
                .cells
                .iter()
                .map(|cell| Schema::from_record(&cell.payload))
                .filter_ok(|create_table| &*create_table.table_name == from_table),
            |i| i.partition(|s| s.typ == TableType::Table),
        )?;

        let table_schema = table_schemas
            .pop()
            .ok_or_else(|| anyhow!("Table not found: {}", from_table))?;

        if !table_schemas.is_empty() {
            bail!("Ambiguous table name: {}", from_table);
        }

        let table_def = TableDef::from_sql(&table_schema.sql)?;

        let op = match self.op {
            LogicalOp::Count => LeafOp::Count,
            LogicalOp::SelectAll => LeafOp::SelectAll,
            LogicalOp::Select(columns) => {
                let mut columns = columns
                    .into_iter()
                    .map(|col| {
                        table_def
                            .columns
                            .iter()
                            .position(|c| c.name == col)
                            .ok_or_else(|| anyhow!("Column not found: {}", col))
                    })
                    .collect::<Result<Vec<_>>>()?;

                if columns.is_empty() {
                    bail!("No columns selected");
                }

                if columns.iter().copied().eq(0..table_def.columns.len()) {
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
                let index_def = IndexDef::from_sql(&index.sql)?;
                if index_def.columns == [filter.column] {
                    let scan = Scan::Index {
                        root_page: index.root_page,
                        column: 0,
                        value: filter.value,
                        op,
                    };

                    return Ok(PhysicalPlan {
                        root_page: table_schema.root_page,
                        scan,
                    });
                }
            }

            let column = table_def
                .columns
                .iter()
                .position(|c| c.name == filter.column)
                .ok_or_else(|| anyhow!("Column not found: {}", filter.column))?;

            let scan = Scan::Table {
                column,
                value: filter.value,
                compare: filter.op,
                op,
            };

            return Ok(PhysicalPlan {
                root_page: table_schema.root_page,
                scan,
            });
        }

        Ok(PhysicalPlan {
            root_page: table_schema.root_page,
            scan: Scan::FullTable { op },
        })
    }
}

impl PhysicalPlan {
    fn execute(self, db: &mut Sqlite) -> Result<()> {
        match self.scan {
            Scan::Index {
                root_page,
                column,
                value,
                op,
            } => {
                let row_ids = Self::index_scan(db, root_page, column, value)?;
                match op {
                    LeafOp::Count => Count(row_ids.len()).finish(),
                    LeafOp::SelectAll => Self::seek_all(db, self.root_page, row_ids, SelectAll),
                    LeafOp::Select { init, last } => {
                        Self::seek_all(db, self.root_page, row_ids, Select(init, last))
                    }
                }
            }
            Scan::Table {
                column,
                value,
                compare,
                op,
            } => match op {
                LeafOp::Count => Self::table_scan_op(
                    db,
                    self.root_page,
                    Count(0).filtered(column, value, compare),
                ),
                LeafOp::SelectAll => Self::table_scan_op(
                    db,
                    self.root_page,
                    SelectAll.filtered(column, value, compare),
                ),
                LeafOp::Select { init, last } => Self::table_scan_op(
                    db,
                    self.root_page,
                    Select(init, last).filtered(column, value, compare),
                ),
            },
            Scan::FullTable { op } => match op {
                LeafOp::Count => Self::table_scan_op(db, self.root_page, Count(0)),
                LeafOp::SelectAll => Self::table_scan_op(db, self.root_page, SelectAll),
                LeafOp::Select { init, last } => {
                    Self::table_scan_op(db, self.root_page, Select(init, last))
                }
            },
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
                    let lower = page
                        .cells
                        .partition_point(|cell| cell.lower_bound(index_column, &condition));
                    let upper = page
                        .cells
                        .partition_point(|cell| cell.upper_bound(index_column, &condition));

                    row_ids.extend(page.cells[lower..upper].iter().map(|cell| cell.row_id));
                }
                Page::InteriorIndex(page) => {
                    let lower = page
                        .cells
                        .partition_point(|cell| cell.lower_bound(index_column, &condition));
                    let upper = page
                        .cells
                        .partition_point(|cell| cell.upper_bound(index_column, &condition));

                    row_ids.extend(page.cells[lower..upper].iter().map(|cell| cell.cell.row_id));

                    next_pages.extend(
                        page.cells[lower..upper]
                            .iter()
                            .map(|cell| cell.left_ptr)
                            .chain(once(
                                page.cells
                                    .get(upper)
                                    .map_or(page.right_ptr, |cell| cell.left_ptr),
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
                    for cell in &*page.cells {
                        operator.execute(cell)?;
                    }
                }
                Page::InteriorTable(page) => {
                    stack.extend(page.cells.iter().map(|cell| cell.left_ptr as usize));
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
            Self::seek_op(db, root_page, key, &mut operator)?;
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
                    if let Ok(idx) = page.cells.binary_search_by_key(&key, |cell| cell.row_id) {
                        operator.execute(&page.cells[idx])?;
                    };
                }
                Page::InteriorTable(page) => {
                    let next_ptr = match page.cells.binary_search_by_key(&key, |cell| cell.row_id) {
                        Ok(idx) => page.cells[idx].left_ptr,
                        Err(idx) => page
                            .cells
                            .get(idx)
                            .map_or(page.right_ptr, |cell| cell.left_ptr),
                    };
                    next_page = Some(next_ptr as usize);
                }
            };
        }

        Ok(())
    }
}

trait Operator {
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

struct Count(usize);

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

struct SelectAll;

impl Operator for SelectAll {
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        let (last, init) = cell.payload.values.split_last().unwrap();
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

struct Select(Vec<usize>, usize);

impl Operator for Select {
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        for &idx in &self.0 {
            print!("{}|", cell.payload.values[idx]);
        }
        print!("{}", cell.payload.values[self.1]);
        println!();
        Ok(())
    }

    fn finish(self) -> Result<()> {
        Ok(())
    }
}

struct FilterOp<T> {
    column: usize,
    value: Val,
    compare: Op,
    op: T,
}

impl<T: Operator> Operator for FilterOp<T> {
    fn execute(&mut self, cell: &LeafTableCell) -> Result<()> {
        let lhs = &self.value;
        let rhs = &cell.payload.values[self.column];
        let cmp = match compare(lhs, rhs) {
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

fn compare(lhs: &Val, rhs: &Value) -> Option<Ordering> {
    match (lhs, rhs) {
        (Val::Null, Value::Null) => Some(Ordering::Equal),
        (Val::Null, _) => Some(Ordering::Less),
        (_, Value::Null) => Some(Ordering::Greater),
        (Val::Bool(false), Value::Int(0)) => Some(Ordering::Equal),
        (Val::Bool(true), Value::Int(1)) => Some(Ordering::Equal),
        (Val::Int(lhs), Value::Int(rhs)) => Some(lhs.cmp(rhs)),
        (Val::Float(lhs), Value::Float(rhs)) => lhs.partial_cmp(rhs),
        (Val::Text(lhs), Value::Text(rhs)) => Some(lhs.as_str().cmp(rhs)),
        (Val::Text(lhs), Value::Blob(rhs)) => {
            std::str::from_utf8(rhs).ok().map(|r| lhs.as_str().cmp(r))
        }
        _ => None,
    }
}

fn parse_query(q: &str) -> Result<Plan<'_>> {
    Ok(query(q)
        .finish()
        .map_err(|e| anyhow!("Invalid query: {}", e))?
        .1)
}

fn query(query: &str) -> IResult<&str, Plan<'_>> {
    all_consuming(map(
        tuple((
            terminated(tag_no_case("SELECT"), multispace1),
            alt((
                value(LogicalOp::Count, tag_no_case("COUNT(*)")),
                value(LogicalOp::SelectAll, tag("*")),
                map(
                    separated_list1(delimited(multispace0, tag(","), multispace0), name),
                    LogicalOp::Select,
                ),
            )),
            delimited(multispace1, tag_no_case("FROM"), multispace1),
            name,
            opt(preceded(
                delimited(multispace1, tag_no_case("WHERE"), multispace1),
                filter,
            )),
        )),
        |(_, op, _, table, filter)| Plan { table, op, filter },
    ))(query)
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
#[derive(Clone, Debug, PartialEq)]
struct Filter<'a> {
    op: Op,
    column: &'a str,
    value: Val,
}

#[derive(Copy, Clone, Debug, PartialEq)]
enum Op {
    Eq,
    Ne,
    Lt,
    Le,
    Gt,
    Ge,
}

#[derive(Copy, Clone, Debug, Default, PartialEq)]
struct TotalFloat(f64);

impl Eq for TotalFloat {}

impl Ord for TotalFloat {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialOrd for TotalFloat {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Default)]
enum Val {
    #[default]
    Null,
    Bool(bool),
    Int(i64),
    Float(TotalFloat),
    Text(String),
}

fn filter(s: &str) -> IResult<&str, Filter<'_>> {
    map(
        tuple((name, delimited(multispace0, op, multispace0), expr)),
        |(column, op, value)| Filter { op, column, value },
    )(s)
}

fn op(s: &str) -> IResult<&str, Op> {
    alt((
        value(Op::Eq, tuple((tag("="), opt(tag("="))))),
        value(Op::Ne, alt((tag("!="), tag("<>")))),
        value(Op::Le, tag("<=")),
        value(Op::Lt, tag("<")),
        value(Op::Ge, tag(">=")),
        value(Op::Gt, tag(">")),
    ))(s)
}

fn expr(s: &str) -> IResult<&str, Val> {
    alt((
        value(Val::Null, tag_no_case("NULL")),
        value(Val::Bool(true), tag_no_case("TRUE")),
        value(Val::Bool(false), tag_no_case("FALSE")),
        map(i64, Val::Int),
        map(double, |o| Val::Float(TotalFloat(o))),
        map(string, Val::Text),
    ))(s)
}

fn string(s: &str) -> IResult<&str, String> {
    alt((
        preceded(
            tag("\""),
            cut(terminated(
                escaped_transform(none_of(r#"\""#), '\\', one_of(r#""n\"#)),
                tag("\""),
            )),
        ),
        preceded(
            tag("'"),
            cut(terminated(
                escaped_transform(none_of(r#"\'"#), '\\', one_of(r#"'n\"#)),
                tag("'"),
            )),
        ),
    ))(s)
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TableDef<'a> {
    name: &'a str,
    columns: Vec<ColumnDef<'a>>,
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
struct IndexDef<'a> {
    columns: Vec<&'a str>,
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
struct ColumnDef<'a> {
    name: &'a str,
    typ: Option<&'a str>,
    constraints: (),
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Schema {
    typ: TableType,
    name: Rc<str>,
    table_name: Rc<str>,
    root_page: usize,
    sql: Rc<str>,
}

impl Schema {
    fn from_record(record: &Record) -> Result<Self> {
        ensure!(
            record.values.len() == 5,
            "Invalid number of values in a create table record: {}",
            record.values.len()
        );

        let typ = match &record.values[0] {
            Value::Text(typ) => TableType::from_text(typ)?,
            otherwise => bail!("Invalid table type: {:?}", otherwise),
        };

        let name = match &record.values[1] {
            Value::Text(name) => name.clone(),
            otherwise => bail!("Invalid table name: {:?}", otherwise),
        };

        let table_name = match &record.values[2] {
            Value::Text(table_name) => table_name.clone(),
            otherwise => bail!("Invalid table name: {:?}", otherwise),
        };

        let root_page = match &record.values[3] {
            Value::Int(root_page) => usize::try_from(*root_page)?.saturating_sub(1),
            otherwise => bail!("Invalid root page: {:?}", otherwise),
        };

        let sql = match &record.values[4] {
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
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
enum TableType {
    Table,
    Index,
    View,
    Trigger,
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

struct Sqlite {
    file: File,
    header: DbHeader,
    pages: Box<[LazyPage]>,
}

impl Sqlite {
    fn new(mut file: File) -> Result<Self> {
        file.seek(SeekFrom::Start(0))?;

        let mut header = [0; 100];
        file.read_exact(&mut header)?;

        let header = DbHeader::from_bytes(&file, header)?;

        let pages = vec![LazyPage::default(); header.db_size].into_boxed_slice();

        Ok(Self {
            file,
            header,
            pages,
        })
    }

    fn page(&mut self, page_number: usize) -> Result<&Page> {
        ensure!(
            page_number < self.header.db_size,
            "Invalid page number: {}, number of pages: {}",
            page_number,
            self.header.db_size
        );

        self.pages[page_number].load(&mut self.file, &self.header, page_number)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct DbHeader {
    page_size: usize,
    db_size: usize,
    reserved_size: usize,
    text_encoding: TextEncoding,
    sqlite_version: (u16, u16, u16),
}

impl DbHeader {
    fn from_bytes(file: &File, bytes: [u8; 100]) -> Result<Self> {
        let header = unsafe { std::mem::transmute::<_, DbHeaderStruct>(bytes) };
        header.verify(file)
    }
}

#[derive(Debug, Clone, Default)]
enum LazyPage {
    #[default]
    Unloaded,
    Loaded(Page),
}

impl LazyPage {
    fn load(&mut self, file: &mut File, db_header: &DbHeader, page_number: usize) -> Result<&Page> {
        match self {
            LazyPage::Loaded(page) => Ok(page),
            LazyPage::Unloaded => {
                let page_size = db_header.page_size;
                let file_offset = page_number * page_size;
                file.seek(SeekFrom::Start(u64::try_from(file_offset)?))?;

                let mut page = vec![0; page_size];
                file.read_exact(&mut page)?;

                ensure!(
                    page.len() == page_size,
                    "Not enough data to read a full page: expected {} but only {} was available",
                    page_size,
                    page.len(),
                );

                let page = &page[..page_size - db_header.reserved_size];
                let page_start = 100 * usize::from(page_number == 0);
                let page = Page::from_bytes(db_header, page, page_start)?;
                *self = Self::Loaded(page);

                let LazyPage::Loaded(page) = self else {
                    unreachable!()
                };
                Ok(page)
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Page {
    LeafTable(LeafTablePage),
    InteriorTable(InteriorTablePage),
    LeafIndex(LeafIndexPage),
    InteriorIndex(InteriorIndexPage),
}

#[derive(Clone, Debug, PartialEq)]
struct LeafTablePage {
    cells: Box<[LeafTableCell]>,
}

#[derive(Clone, Debug, PartialEq)]
struct InteriorTablePage {
    right_ptr: u32,
    cells: Box<[InteriorTableCell]>,
}

#[derive(Clone, Debug, PartialEq)]
struct LeafIndexPage {
    cells: Box<[LeafIndexCell]>,
}

#[derive(Clone, Debug, PartialEq)]
struct InteriorIndexPage {
    right_ptr: u32,
    cells: Box<[InteriorIndexCell]>,
}

impl Page {
    fn from_bytes(db_header: &DbHeader, page: &[u8], header_start: usize) -> Result<Self> {
        ensure!(
            page.len() >= header_start + size_of::<PageHeaderStruct>(),
            "Page of length {} is not big enough",
            page.len(),
        );

        let content = &page[header_start..];
        let (header, content) = content.split_at(size_of::<PageHeaderStruct>());
        let header: &[u8; size_of::<PageHeaderStruct>()] = header.try_into()?;
        let header = unsafe { std::mem::transmute_copy::<_, PageHeaderStruct>(header) };

        let page_type = match header.page_type {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            otherwise => bail!("Invalid page type: {}", otherwise),
        };

        let (right_ptr, content) = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => {
                ensure!(
                    content.len() >= 4,
                    "Page of length {} is not big enough",
                    page.len()
                );
                let (right_ptr, content) = content.split_at(4);
                let right_ptr = u32::from_be_bytes(right_ptr.try_into()?) - 1;
                (right_ptr, content)
            }
            _ => (u32::MAX, content),
        };

        let number_of_cells = usize::from(u16::from_be_bytes(header.number_of_cells));
        let content_start = usize::from(u16::from_be_bytes(header.cell_content_area_offset));

        ensure!(
            content.len() >= number_of_cells * 2 && page.len() >= content_start,
            "Page of length {} is not big enough",
            page.len(),
        );

        let cells = content[..number_of_cells * 2]
            .chunks_exact(2)
            .map(|v| u16::from_be_bytes([v[0], v[1]]))
            .map(usize::from)
            .map(|pos| &page[pos..]);

        Ok(match page_type {
            PageType::LeafTable => {
                let cells = cells
                    .map(|cell| LeafTableCell::from_bytes(db_header, cell))
                    .collect::<Result<Vec<_>>>()?;
                Self::LeafTable(LeafTablePage {
                    cells: cells.into_boxed_slice(),
                })
            }
            PageType::InteriorTable => {
                let cells = cells
                    .map(InteriorTableCell::from_bytes)
                    .collect::<Result<Vec<_>>>()?;
                Self::InteriorTable(InteriorTablePage {
                    right_ptr,
                    cells: cells.into_boxed_slice(),
                })
            }
            PageType::LeafIndex => {
                let cells = cells
                    .enumerate()
                    .map(|(index, cell)| LeafIndexCell::from_bytes(db_header, index, cell))
                    .collect::<Result<Vec<_>>>()?;
                Self::LeafIndex(LeafIndexPage {
                    cells: cells.into_boxed_slice(),
                })
            }
            PageType::InteriorIndex => {
                let cells = cells
                    .map(|cell| InteriorIndexCell::from_bytes(db_header, cell))
                    .collect::<Result<Vec<_>>>()?;
                Self::InteriorIndex(InteriorIndexPage {
                    right_ptr,
                    cells: cells.into_boxed_slice(),
                })
            }
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct LeafTableCell {
    row_id: u64,
    payload: Record,
    overflow: Option<Overflow>,
}

impl LeafTableCell {
    fn from_bytes(header: &DbHeader, bytes: &[u8]) -> Result<Self> {
        let (payload_size, bytes, _) = read_varint(bytes)?;
        let payload_size = usize::try_from(payload_size)
            .map_err(|_| anyhow!("Invalid payload size: {}", payload_size))?;

        let (row_id, bytes, _) = read_varint(bytes)?;

        let usable = header.page_size.saturating_sub(header.reserved_size);
        let max_inline_size = usable.saturating_sub(35);
        if payload_size <= max_inline_size {
            let payload = Record::from_bytes(header.text_encoding, Some(row_id), bytes)?;
            return Ok(Self {
                row_id,
                payload,
                overflow: None,
            });
        }

        let min_inline_size = usable
            .saturating_sub(12)
            .saturating_mul(32)
            .saturating_div(255)
            .saturating_sub(23);
        let cutoff_size = payload_size
            .saturating_sub(min_inline_size)
            .wrapping_rem(usable.saturating_sub(4))
            .saturating_add(min_inline_size);

        let stored_size = if cutoff_size <= max_inline_size {
            cutoff_size
        } else {
            min_inline_size
        };

        ensure!(bytes.len() >= stored_size + 4, "Not enough bytes, need at least {} bytes for the payload and 4 bytes for the overflow page", stored_size);
        let (bytes, overflow) = bytes.split_at(cutoff_size);
        let overflow_page = u32::from_be_bytes(overflow[..4].try_into()?);

        let payload = Record::from_bytes(header.text_encoding, Some(row_id), bytes)?;
        let overflow = Some(Overflow {
            page: overflow_page,
            spilled_length: payload_size - stored_size,
        });

        Ok(Self {
            row_id,
            payload,
            overflow,
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct LeafIndexCell {
    index: usize,
    payload: Record,
    row_id: u64,
    overflow: Option<Overflow>,
}

impl LeafIndexCell {
    fn lower_bound(&self, filter: usize, condition: &Val) -> bool {
        self.compare(filter, condition) == Ordering::Greater
    }

    fn upper_bound(&self, filter: usize, condition: &Val) -> bool {
        self.compare(filter, condition) != Ordering::Less
    }

    fn compare(&self, filter: usize, lhs: &Val) -> Ordering {
        let rhs = &self.payload.values[filter];
        match compare(lhs, rhs) {
            Some(cmp) => cmp,
            None => panic!("Cannot compare {:?} and {:?}", lhs, rhs),
        }
    }

    fn from_bytes(header: &DbHeader, index: usize, bytes: &[u8]) -> Result<Self> {
        let (payload_size, bytes, _) = read_varint(bytes)?;
        let payload_size = usize::try_from(payload_size)
            .map_err(|_| anyhow!("Invalid payload size: {}", payload_size))?;

        let usable = header.page_size.saturating_sub(header.reserved_size);
        let max_inline_size = usable
            .saturating_sub(12)
            .saturating_mul(64)
            .saturating_div(255)
            .saturating_sub(23);
        if payload_size <= max_inline_size {
            let payload = Record::values_from_bytes(header.text_encoding, None, bytes)?;
            return Self::from_values(payload, index, None);
        }

        let min_inline_size = usable
            .saturating_sub(12)
            .saturating_mul(32)
            .saturating_div(255)
            .saturating_sub(23);
        let cutoff_size = payload_size
            .saturating_sub(min_inline_size)
            .wrapping_rem(usable.saturating_sub(4))
            .saturating_add(min_inline_size);

        let stored_size = if cutoff_size <= max_inline_size {
            cutoff_size
        } else {
            min_inline_size
        };

        ensure!(bytes.len() >= stored_size + 4, "Not enough bytes, need at least {} bytes for the payload and 4 bytes for the overflow page", stored_size);
        let (bytes, overflow) = bytes.split_at(cutoff_size);
        let overflow_page = u32::from_be_bytes(overflow[..4].try_into()?);

        let payload = Record::values_from_bytes(header.text_encoding, None, bytes)?;

        let overflow = Some(Overflow {
            page: overflow_page,
            spilled_length: payload_size - stored_size,
        });

        Self::from_values(payload, index, overflow)
    }

    fn from_values(
        mut payload: Vec<Value>,
        index: usize,
        overflow: Option<Overflow>,
    ) -> Result<Self> {
        let Some(Value::Int(row_id)) = payload.pop() else {
            bail!("Missing row id for index payload");
        };
        let row_id = row_id.try_into()?;
        let payload = Record {
            values: payload.into_boxed_slice(),
        };

        Ok(Self {
            index,
            payload,
            row_id,
            overflow,
        })
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
struct InteriorTableCell {
    left_ptr: u32,
    row_id: u64,
}

impl InteriorTableCell {
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 4,
            "Not enough bytes to read a cell: expected at least 4 but only {} were available",
            bytes.len()
        );

        let (left_ptr, bytes) = bytes.split_at(4);
        let left_ptr = u32::from_be_bytes(left_ptr.try_into()?);

        let (row_id, _, _) = read_varint(bytes)?;

        Self::from_values(left_ptr, row_id)
    }

    fn from_values(left_ptr: u32, row_id: u64) -> Result<Self> {
        let left_ptr = left_ptr.checked_sub(1).ok_or_else(|| {
            anyhow!(
                "Invalid left pointer: {} (must be at least 1, but 0 is reserved)",
                left_ptr
            )
        })?;

        Ok(Self { left_ptr, row_id })
    }
}

#[derive(Clone, Debug, PartialEq)]
struct InteriorIndexCell {
    left_ptr: u32,
    cell: LeafIndexCell,
}

impl InteriorIndexCell {
    fn lower_bound(&self, filter: usize, condition: &Val) -> bool {
        self.cell.lower_bound(filter, condition)
    }

    fn upper_bound(&self, filter: usize, condition: &Val) -> bool {
        self.cell.upper_bound(filter, condition)
    }

    fn from_bytes(header: &DbHeader, bytes: &[u8]) -> Result<Self> {
        ensure!(
            bytes.len() >= 4,
            "Not enough bytes to read a cell: expected at least 4 but only {} were available",
            bytes.len()
        );

        let (left_ptr, bytes) = bytes.split_at(4);
        let left_ptr = u32::from_be_bytes(left_ptr.try_into()?);

        let leaf = LeafIndexCell::from_bytes(header, usize::MAX, bytes)?;
        Self::from_values(left_ptr, leaf)
    }

    fn from_values(left_ptr: u32, cell: LeafIndexCell) -> Result<Self> {
        let left_ptr = left_ptr.checked_sub(1).ok_or_else(|| {
            anyhow!(
                "Invalid left pointer: {} (must be at least 1, but 0 is reserved)",
                left_ptr
            )
        })?;

        Ok(Self { left_ptr, cell })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Overflow {
    page: u32,
    spilled_length: usize,
}

fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
    let mut result = 0;
    let mut bytes_read = 0;

    let mut bs = bytes.iter().copied();

    loop {
        let byte = bs
            .next()
            .ok_or_else(|| anyhow!("Unexpected end of bytes"))?;
        bytes_read += 1;

        if bytes_read == 9 {
            result = (result << 7) | u64::from(byte);
            break;
        }

        result = (result << 7) | u64::from(byte & 0b0111_1111);

        if byte & 0b1000_0000 == 0 {
            break;
        }
    }

    Ok((result, &bytes[bytes_read..], bytes_read))
}

#[derive(Clone, Debug, PartialEq)]
struct Record {
    values: Box<[Value]>,
}

impl Record {
    fn from_bytes(text_encoding: TextEncoding, row_id: Option<u64>, bytes: &[u8]) -> Result<Self> {
        let values = Self::values_from_bytes(text_encoding, row_id, bytes)?;
        Ok(Self {
            values: values.into_boxed_slice(),
        })
    }

    fn values_from_bytes(
        text_encoding: TextEncoding,
        row_id: Option<u64>,
        bytes: &[u8],
    ) -> Result<Vec<Value>> {
        let (header_size, bytes, header_size_length) = read_varint(bytes)?;
        let (mut header, mut bytes) =
            bytes.split_at(usize::try_from(header_size)? - header_size_length);

        let mut values = Vec::new();

        while !header.is_empty() {
            let (serial_type, rest, _) = read_varint(header)?;
            header = rest;
            let (mut value, rest) = Value::from_bytes(text_encoding, serial_type, bytes)?;
            bytes = rest;

            if values.is_empty() {
                if let (Value::Null, Some(row_id)) = (&value, row_id) {
                    value = Value::Int(row_id as i64);
                }
            }

            values.push(value);
        }

        Ok(values)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Value {
    Null,
    Int(i64),
    Float(TotalFloat),
    Text(Rc<str>),
    Blob(Rc<[u8]>),
}

impl Value {
    fn from_bytes(
        text_encoding: TextEncoding,
        serial_type: u64,
        bytes: &[u8],
    ) -> Result<(Self, &[u8])> {
        match serial_type {
            0 => Ok((Self::Null, bytes)),
            1 => Self::read_int(bytes, 1).map(|(value, bytes)| (Self::Int(value), bytes)),
            2 => Self::read_int(bytes, 2).map(|(value, bytes)| (Self::Int(value), bytes)),
            3 => Self::read_int(bytes, 3).map(|(value, bytes)| (Self::Int(value), bytes)),
            4 => Self::read_int(bytes, 4).map(|(value, bytes)| (Self::Int(value), bytes)),
            5 => Self::read_int(bytes, 5).map(|(value, bytes)| (Self::Int(value), bytes)),
            6 => Self::read_int(bytes, 8).map(|(value, bytes)| (Self::Int(value), bytes)),
            7 => {
                let (value, bytes) = Self::read_int(bytes, 8)?;
                let value = f64::from_bits(value as u64);
                Ok((Self::Float(TotalFloat(value)), bytes))
            }
            8 => Ok((Self::Int(0), bytes)),
            9 => Ok((Self::Int(1), bytes)),
            10 | 11 => {
                bail!("Cannot read from an internal serial type: {}", serial_type)
            }
            otherwise => {
                let len = usize::try_from((otherwise >> 1) - 6)?;
                ensure!(
                    len <= bytes.len(),
                    "Not enough bytes to read data of length {}, only {} are available",
                    len,
                    bytes.len()
                );

                let (data, bytes) = bytes.split_at(len);
                match (otherwise & 0x1) == 0x1 {
                    true => Ok((Self::Text(text_encoding.read(data)?.into()), bytes)),
                    false => Ok((Self::Blob(data.into()), bytes)),
                }
            }
        }
    }

    fn read_int(bytes: &[u8], len: usize) -> Result<(i64, &[u8])> {
        ensure!(
            bytes.len() >= len,
            "Not enough bytes to read a big-endian {}-bit integer: {:?}",
            len * 8,
            bytes
        );
        ensure!(
            len <= 8,
            "Cannot read a big-endian {}-bit integer, must be at most 64 bits",
            len * 8
        );

        let (value, bytes) = bytes.split_at(len);
        let mut data = [0; 8];
        data[8 - len..].copy_from_slice(value);
        let value = i64::from_be_bytes(data);

        Ok((value, bytes))
    }
}

impl Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Int(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v.0),
            Value::Text(v) => write!(f, "{}", v),
            Value::Blob(v) => write!(f, "{:?}", v),
        }
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
enum TextEncoding {
    #[default]
    Utf8 = 1,
    Utf16le = 2,
    Utf16be = 3,
}

impl TextEncoding {
    fn read(self, bytes: &[u8]) -> Result<Cow<str>> {
        fn convert(bytes: &[u8], mapper: impl Fn([u8; 2]) -> u16) -> Result<String> {
            if bytes.len() % 2 != 0 {
                bail!("Invalid utf16 string length: {:?}", bytes.len());
            }

            Ok(
                char::decode_utf16(bytes.chunks_exact(2).map(|v| mapper([v[0], v[1]])))
                    .map(|r| r.unwrap_or(char::REPLACEMENT_CHARACTER))
                    .collect::<String>(),
            )
        }

        match self {
            Self::Utf8 => Ok(std::str::from_utf8(bytes)?.into()),
            Self::Utf16le => Ok(convert(bytes, u16::from_le_bytes)?.into()),
            Self::Utf16be => Ok(convert(bytes, u16::from_be_bytes)?.into()),
        }
    }
}

#[derive(Debug, Clone)]
#[repr(C, packed)]
struct DbHeaderStruct {
    magic: [u8; 16],
    page_size: [u8; 2],
    file_format_write_version: u8,
    file_format_read_version: u8,
    reserved_space: u8,
    max_payload_fraction: u8,
    min_payload_fraction: u8,
    leaf_payload_fraction: u8,
    file_change_counter: [u8; 4],
    database_size: [u8; 4],
    first_freelist_page: [u8; 4],
    number_of_freelist_pages: [u8; 4],
    schema_cookie: [u8; 4],
    schema_format_number: [u8; 4],
    default_page_cache_size: [u8; 4],
    largest_root_btree_page_number: [u8; 4],
    text_encoding: [u8; 4],
    user_version: [u8; 4],
    incremental_vacuum_mode: [u8; 4],
    application_id: [u8; 4],
    unused: [u8; 20],
    version_valid_for: [u8; 4],
    sqlite_version_number: [u8; 4],
}

impl DbHeaderStruct {
    fn verify(self, file: &File) -> Result<DbHeader> {
        let magic = CStr::from_bytes_with_nul(&self.magic)?.to_str()?;
        ensure!(
            magic == "SQLite format 3",
            "Invalid sqlite header: {}",
            magic
        );

        ensure!(
            self.file_format_read_version <= 2,
            "Unsupported file format version: {}",
            self.file_format_read_version
        );

        let mut page_size = u32::from(u16::from_be_bytes(self.page_size));
        if page_size == 1 {
            page_size = 65536;
        }

        ensure!(
            (512..=65536).contains(&page_size) && page_size.is_power_of_two(),
            "Invalid page size: {}",
            page_size
        );

        if page_size.saturating_sub(u32::from(self.reserved_space)) < 480 {
            bail!("Invalid reserved space: {}", self.reserved_space);
        }

        ensure!(
            self.reserved_space == 0,
            "Invalid reserved space: {}",
            self.reserved_space
        );

        ensure!(
            self.max_payload_fraction == 64,
            "Invalid max payload fraction: {}",
            self.max_payload_fraction
        );
        ensure!(
            self.min_payload_fraction == 32,
            "Invalid min payload fraction: {}",
            self.min_payload_fraction
        );
        ensure!(
            self.leaf_payload_fraction == 32,
            "Invalid leaf payload fraction: {}",
            self.leaf_payload_fraction
        );

        let file_change_counter = u32::from_be_bytes(self.file_change_counter);
        let version_for_valid = u32::from_be_bytes(self.version_valid_for);

        let mut db_size = u32::from_be_bytes(self.database_size);

        if db_size != 0 && file_change_counter != version_for_valid {
            let calculated_db_size = file.metadata()?.len() / u64::from(page_size);
            db_size = u32::try_from(calculated_db_size)
                .map_err(|_| anyhow!("Invalid database size: {}", calculated_db_size))?;
        }

        match u32::from_be_bytes(self.schema_format_number) {
            1..=4 => {}
            otherwise => bail!("Invalid schema format number: {}", otherwise),
        };

        if u32::from_be_bytes(self.largest_root_btree_page_number) != 0 {
            let mode = u32::from_be_bytes(self.incremental_vacuum_mode);
            ensure!(
                mode == 0 || mode == 1,
                "Invalid incremental vacuum mode: {}",
                mode
            );
        } else {
            let mode = u32::from_be_bytes(self.incremental_vacuum_mode);
            ensure!(mode == 0, "Invalid incremental vacuum mode: {}", mode);
        }

        let text_encoding = match u32::from_be_bytes(self.text_encoding) {
            1 => TextEncoding::Utf8,
            2 => TextEncoding::Utf16le,
            3 => TextEncoding::Utf16be,
            otherwise => bail!("Invalid text encoding: {}", otherwise),
        };

        let version = u32::from_be_bytes(self.sqlite_version_number);

        let patch = version % 1000;
        let version = version / 1000;

        let minor = version % 1000;
        let version = version / 1000;

        let major = version % 1000;
        let version = version / 1000;

        let (major, minor, patch) = (
            u16::try_from(major)?,
            u16::try_from(minor)?,
            u16::try_from(patch)?,
        );

        ensure!(
            version == 0,
            "Invalid sqlite version: {}",
            u32::from_be_bytes(self.sqlite_version_number)
        );

        if self.unused.iter().any(|&b| b != 0) {
            bail!("Invalid unused space: {:?}", self.unused);
        }

        Ok(DbHeader {
            page_size: usize::try_from(page_size)?,
            db_size: usize::try_from(db_size)?,
            reserved_size: usize::from(self.reserved_space),
            text_encoding,
            sqlite_version: (major, minor, patch),
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(C, packed)]
struct PageHeaderStruct {
    page_type: u8,
    first_freeblock_offset: [u8; 2],
    number_of_cells: [u8; 2],
    cell_content_area_offset: [u8; 2],
    fragmented_free_bytes: u8,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0a,
    LeafTable = 0x0d,
}

impl std::fmt::Debug for Sqlite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sqlite")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}
