use anyhow::{anyhow, bail, ensure, Result};
use std::{
    borrow::Cow,
    cmp::Reverse,
    ffi::CStr,
    fs::File,
    io::{Read as _, Seek as _, SeekFrom},
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

    // Parse command and act accordingly
    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            let file = File::open(&args[1])?;

            let mut db = Sqlite::new(file)?;
            let page_size = db.header.page_size;

            println!("database page size: {}", page_size);

            let mut num_tables = 0;
            let root_page = db.page(0)?;
            for cell in root_page.cells.iter() {
                let create_table = match cell {
                    Cell::LeafTable(cell) => CreateTable::from_record(&cell.payload)?,
                };
                if matches!(create_table.typ, TableType::Table) {
                    num_tables += 1;
                }
            }

            println!("number of tables: {}", num_tables);

            // for page_id in 0..db.header.db_size {
            //     let page = db.page(page_id)?;
            //     println!("page {}: {:#?}", page_id, page);
            // }
        }
        ".tables" => {
            let file = File::open(&args[1])?;

            let mut db = Sqlite::new(file)?;
            let root_page = db.page(0)?;

            for cell in root_page.cells.iter() {
                let create_table = match cell {
                    Cell::LeafTable(cell) => CreateTable::from_record(&cell.payload)?,
                };
                if matches!(create_table.typ, TableType::Table) {
                    print!("{} ", create_table.table_name);
                }
            }
        }
        query => {
            let table = (|| {
                let mut query = query.split_whitespace();
                let _select = query.next().filter(|s| s.eq_ignore_ascii_case("SELECT"))?;
                let _count = query
                    .next()
                    .filter(|s| s.eq_ignore_ascii_case("COUNT(*)"))?;
                let _from = query.next().filter(|s| s.eq_ignore_ascii_case("FROM"))?;

                query.next()
            })()
            .ok_or_else(|| anyhow!("Invalid query: {}", query))?;

            let file = File::open(&args[1])?;

            let mut db = Sqlite::new(file)?;
            let root_page = db.page(0)?;

            let table_def = root_page
                .cells
                .iter()
                .find_map(|cell| match cell {
                    Cell::LeafTable(cell) => {
                        let create_table = match CreateTable::from_record(&cell.payload) {
                            Ok(create_table) => create_table,
                            Err(_) => return None,
                        };
                        if &*create_table.table_name == table {
                            Some(create_table)
                        } else {
                            None
                        }
                    }
                })
                .ok_or_else(|| anyhow!("Table not found: {}", table))?;

            let page = db.page(table_def.root_page)?;
            println!("{}", page.cells.len());
        }
    }

    Ok(())
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct CreateTable {
    typ: TableType,
    name: Rc<str>,
    table_name: Rc<str>,
    root_page: usize,
    sql: Rc<str>,
}

impl CreateTable {
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
            Value::Int(root_page) => usize::try_from(*root_page)? - 1,
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
                let page = Page::from_bytes(db_header.text_encoding, page, page_start)?;
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
struct Page {
    cells: Box<[Cell]>,
}

impl Page {
    fn from_bytes(text_encoding: TextEncoding, page: &[u8], header_start: usize) -> Result<Self> {
        ensure!(
            page.len() >= header_start + size_of::<PageHeaderStruct>(),
            "Page of length {} is not big enough",
            page.len(),
        );

        let content = &page[header_start..];
        let (header, mut content) = content.split_at(size_of::<PageHeaderStruct>());
        let header: &[u8; size_of::<PageHeaderStruct>()] = header.try_into()?;
        let header = unsafe { std::mem::transmute_copy::<_, PageHeaderStruct>(header) };

        let page_type = match header.page_type {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            otherwise => bail!("Invalid page type: {}", otherwise),
        };

        if matches!(page_type, PageType::InteriorIndex | PageType::InteriorTable) {
            content = content
                .get(4..)
                .ok_or_else(|| anyhow!("Page of length {} is not big enough", page.len()))?;
        }

        let number_of_cells = usize::from(u16::from_be_bytes(header.number_of_cells));
        let content_start = usize::from(u16::from_be_bytes(header.cell_content_area_offset));

        ensure!(
            content.len() >= number_of_cells * 2 && page.len() >= content_start,
            "Page of length {} is not big enough",
            page.len(),
        );

        let mut cell_positions = content[..number_of_cells * 2]
            .chunks_exact(2)
            .map(|v| u16::from_be_bytes([v[0], v[1]]))
            .map(usize::from)
            .collect::<Vec<_>>();

        cell_positions.sort_unstable_by_key(|o| Reverse(*o));

        let mut cells = Vec::with_capacity(number_of_cells);

        let mut page = page;
        for pos in cell_positions {
            ensure!(
                pos >= content_start,
                "Cell offset {} cannot be before content start ({})",
                pos,
                content_start
            );

            let (rest, cell) = page.split_at(pos);
            page = rest;

            let cell = Cell::from_bytes(text_encoding, page_type, cell)?;
            cells.push(cell);
        }

        Ok(Self {
            cells: cells.into_boxed_slice(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Cell {
    LeafTable(LeafTableCell),
}

impl Cell {
    fn from_bytes(text_encoding: TextEncoding, typ: PageType, bytes: &[u8]) -> Result<Self> {
        match typ {
            PageType::InteriorIndex => todo!(),
            PageType::InteriorTable => todo!(),
            PageType::LeafIndex => todo!(),
            PageType::LeafTable => Ok(Self::LeafTable(LeafTableCell::from_bytes(
                text_encoding,
                bytes,
            )?)),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
struct LeafTableCell {
    row_id: u64,
    payload: Record,
    overflow: Option<Overflow>,
}

impl LeafTableCell {
    fn from_bytes(text_encoding: TextEncoding, bytes: &[u8]) -> Result<Self> {
        let (payload_size, bytes, _) = read_varint(bytes)?;
        let payload_size = usize::try_from(payload_size)
            .map_err(|_| anyhow!("Invalid payload size: {}", payload_size))?;

        let (row_id, mut bytes, _) = read_varint(bytes)?;

        let mut overflow = None;

        if payload_size > bytes.len() {
            let overflow_point = bytes.len().checked_sub(4).ok_or_else(|| {
                anyhow!(
                    "Invalid payload size: {} (need at least 4 bytes, but only got {})",
                    payload_size,
                    bytes.len()
                )
            })?;
            let (payload, overflow_page) = bytes.split_at(overflow_point);
            let overflow_page = u32::from_be_bytes(overflow_page.try_into()?);
            overflow = Some(Overflow {
                page: overflow_page,
                spilled_length: payload_size - payload.len(),
            });
            bytes = payload;
        }

        let payload = Record::from_bytes(text_encoding, bytes)?;

        Ok(Self {
            row_id,
            payload,
            overflow,
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
struct Overflow {
    page: u32,
    spilled_length: usize,
}

fn read_varint(bytes: &[u8]) -> Result<(u64, &[u8], usize)> {
    let mut result = 0;
    let mut shift = 0;
    let mut bytes_read = 0;

    let mut bs = bytes.iter().copied();

    loop {
        let byte = bs
            .next()
            .ok_or_else(|| anyhow!("Unexpected end of bytes"))?;
        bytes_read += 1;

        if bytes_read == 9 {
            result = (result << shift) | u64::from(byte);
            break;
        }

        result = (result << shift) | u64::from(byte & 0b0111_1111);
        shift += 7;

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
    fn from_bytes(text_encoding: TextEncoding, bytes: &[u8]) -> Result<Self> {
        let (header_size, bytes, header_size_length) = read_varint(bytes)?;
        let (mut header, mut bytes) =
            bytes.split_at(usize::try_from(header_size)? - header_size_length);

        let mut values = Vec::new();

        while !header.is_empty() {
            let (serial_type, rest, _) = read_varint(header)?;
            header = rest;
            let (value, rest) = Value::from_bytes(text_encoding, serial_type, bytes)?;
            bytes = rest;

            values.push(value);
        }

        Ok(Self {
            values: values.into_boxed_slice(),
        })
    }
}

#[derive(Clone, Debug, PartialEq)]
enum Value {
    Null,
    Int(i64),
    Float(f64),
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
                Ok((Self::Float(value), bytes))
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
