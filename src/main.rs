use crate::schema::{Schema, TableType};

use self::{
    header::DbHeader,
    page::{LazyPage, Page},
    query_parser::parse_query,
};

use anyhow::{bail, ensure, Result};
use itertools::{process_results, Itertools as _};
use std::{
    fs::File,
    io::{Read as _, Seek as _, SeekFrom},
};

mod btree;
mod header;
mod operators;
mod page;
mod planner;
mod query_parser;
mod record;
mod runtime;
mod schema;
mod values;

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
                    .iter()
                    .map(|cell| Schema::from_record(&cell.payload))
                    .filter_ok(|ct| ct.typ() == TableType::Table),
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
                    .iter()
                    .map(|cell| Schema::from_record(&cell.payload))
                    .filter_map_ok(|ct| {
                        (ct.typ() == TableType::Table).then(|| ct.into_table_name())
                    }),
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

impl std::fmt::Debug for Sqlite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sqlite")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}
