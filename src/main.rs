use crate::schema::{Schema, TableType};

use self::{
    header::DbHeader,
    page::{LazyPage, Page},
    query_parser::parse_query,
};

use anyhow::{bail, Result};
use memmap2::{Mmap, MmapOptions};
use std::fs::File;

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

            let Page::LeafTable(root_page) = db.page(0) else {
                bail!("Root page is not a leaf table page");
            };

            let num_tables = root_page
                .iter()
                .filter(|cell| Schema::from_record(cell.payload).typ() == TableType::Table)
                .count();

            println!("number of tables: {}", num_tables);
        }
        ".tables" => {
            let Page::LeafTable(root_page) = db.page(0) else {
                bail!("Root page is not a leaf table page");
            };

            root_page
                .iter()
                .map(|cell| Schema::from_record(cell.payload))
                .filter(|schema| schema.typ() == TableType::Table)
                .for_each(|schema| println!("{}", schema.table_name()));
        }
        query => {
            let plan = parse_query(query)?;
            let plan = plan.translate(&mut db)?;
            plan.execute(&mut db);
        }
    };

    Ok(())
}

pub struct Sqlite {
    file: Mmap,
    header: DbHeader,
    pages: Box<[LazyPage]>,
}

impl Sqlite {
    fn new(file: File) -> Result<Self> {
        let map = unsafe { MmapOptions::new().populate().map(&file) }?;
        map.advise(memmap2::Advice::Random)?;

        let header = DbHeader::from_bytes(&file, map[..100].try_into().unwrap())?;

        let mut pages = Vec::with_capacity(header.db_size);
        pages.resize_with(header.db_size, LazyPage::default);
        let pages = pages.into_boxed_slice();

        Ok(Self {
            file: map,
            header,
            pages,
        })
    }

    fn page(&mut self, page_number: usize) -> &Page {
        assert!(
            page_number < self.header.db_size,
            "Invalid page number: {}, number of pages: {}",
            page_number,
            self.header.db_size
        );

        self.pages[page_number].load(&self.file, &self.header, page_number)
    }
}

impl std::fmt::Debug for Sqlite {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Sqlite")
            .field("header", &self.header)
            .finish_non_exhaustive()
    }
}
