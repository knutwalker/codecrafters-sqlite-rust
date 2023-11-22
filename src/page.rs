use anyhow::{bail, ensure, Result};
use std::{
    fs::File,
    io::{Read as _, Seek as _, SeekFrom},
    mem::size_of,
};

use crate::{
    btree::{
        InteriorIndexCell, InteriorIndexPage, InteriorTableCell, InteriorTablePage, LeafIndexCell,
        LeafIndexPage, LeafTableCell, LeafTablePage,
    },
    header::DbHeader,
};

#[derive(Debug, Clone, Default)]
pub enum LazyPage {
    #[default]
    Unloaded,
    Loaded(Page),
}

impl LazyPage {
    pub fn load(
        &mut self,
        file: &mut File,
        db_header: &DbHeader,
        page_number: usize,
    ) -> Result<&Page> {
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
pub enum Page {
    LeafTable(LeafTablePage),
    InteriorTable(InteriorTablePage),
    LeafIndex(LeafIndexPage),
    InteriorIndex(InteriorIndexPage),
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
                Self::LeafTable(LeafTablePage::new(cells.into_boxed_slice()))
            }
            PageType::InteriorTable => {
                let cells = cells
                    .map(InteriorTableCell::from_bytes)
                    .collect::<Result<Vec<_>>>()?;
                Self::InteriorTable(InteriorTablePage::new(right_ptr, cells.into_boxed_slice()))
            }
            PageType::LeafIndex => {
                let cells = cells
                    .enumerate()
                    .map(|(index, cell)| LeafIndexCell::from_bytes(db_header, index, cell))
                    .collect::<Result<Vec<_>>>()?;
                Self::LeafIndex(LeafIndexPage::new(cells.into_boxed_slice()))
            }
            PageType::InteriorIndex => {
                let cells = cells
                    .map(|cell| InteriorIndexCell::from_bytes(db_header, cell))
                    .collect::<Result<Vec<_>>>()?;
                Self::InteriorIndex(InteriorIndexPage::new(right_ptr, cells.into_boxed_slice()))
            }
        })
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[repr(u8)]
enum PageType {
    InteriorIndex = 0x02,
    InteriorTable = 0x05,
    LeafIndex = 0x0a,
    LeafTable = 0x0d,
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
