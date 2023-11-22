use std::{mem::size_of, ops::RangeBounds};

use crate::{
    btree::{InteriorIndexPage, InteriorTablePage, LeafIndexPage, LeafTablePage},
    header::DbHeader,
};

#[derive(Debug, Default)]
pub enum LazyPage {
    #[default]
    Unloaded,
    Loaded(Page),
}

impl LazyPage {
    pub fn load(&mut self, file: &[u8], db_header: &DbHeader, page_number: usize) -> &Page {
        match self {
            LazyPage::Loaded(page) => page,
            LazyPage::Unloaded => {
                let page_size = db_header.page_size;
                let file_offset = page_number * page_size;

                let page_data = &file[file_offset..file_offset + page_size];

                let page = &page_data[..page_size - db_header.reserved_size];
                let page_start = 100 * usize::from(page_number == 0);
                let page = Page::from_bytes(db_header, page, page_start);
                *self = Self::Loaded(page);

                let LazyPage::Loaded(page) = self else {
                    unreachable!()
                };
                page
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
    fn from_bytes(db_header: &DbHeader, page: &[u8], header_start: usize) -> Self {
        let content = &page[header_start..];
        let (header, content) = content.split_at(size_of::<PageHeaderStruct>());
        let header: &[u8; size_of::<PageHeaderStruct>()] = header.try_into().unwrap();
        let header = unsafe { std::mem::transmute_copy::<_, PageHeaderStruct>(header) };

        let page_type = match header.page_type {
            0x02 => PageType::InteriorIndex,
            0x05 => PageType::InteriorTable,
            0x0a => PageType::LeafIndex,
            0x0d => PageType::LeafTable,
            otherwise => panic!("Invalid page type: {}", otherwise),
        };

        let (right_ptr, content) = match page_type {
            PageType::InteriorIndex | PageType::InteriorTable => {
                let (right_ptr, content) = content.split_at(4);
                let right_ptr = u32::from_be_bytes(right_ptr.try_into().unwrap()) - 1;
                (right_ptr, content)
            }
            _ => (u32::MAX, content),
        };

        let number_of_cells = usize::from(u16::from_be_bytes(header.number_of_cells));
        let _content_start = usize::from(u16::from_be_bytes(header.cell_content_area_offset));

        let ps = db_header.page_size;
        match page_type {
            PageType::LeafTable => {
                let cells = LazyCells::new(page, content, number_of_cells);
                Self::LeafTable(LeafTablePage::new(ps, cells))
            }
            PageType::InteriorTable => {
                let cells = LazyCells::new(page, content, number_of_cells);
                Self::InteriorTable(InteriorTablePage::new(ps, right_ptr, cells))
            }
            PageType::LeafIndex => {
                let cells = LazyCells::new(page, content, number_of_cells);
                Self::LeafIndex(LeafIndexPage::new(db_header.page_size, cells))
            }
            PageType::InteriorIndex => {
                let cells = LazyCells::new(page, content, number_of_cells);
                Self::InteriorIndex(InteriorIndexPage::new(ps, right_ptr, cells))
            }
        }
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

pub trait CellType {
    fn new(bytes: &[u8]) -> Self
    where
        Self: Sized;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct LazyCells<T> {
    page: *const u8,
    cells: u16,
    number_of_cells: usize,
    _type: std::marker::PhantomData<T>,
}

impl<T: CellType> LazyCells<T> {
    fn new(page: &[u8], content: &[u8], number_of_cells: usize) -> Self {
        let cells = content.as_ptr() as usize - page.as_ptr() as usize;
        Self {
            page: page.as_ptr(),
            cells: cells as u16,
            number_of_cells,
            _type: std::marker::PhantomData,
        }
    }

    pub fn cells(&self, page_size: usize) -> impl Iterator<Item = T> {
        let page = self.page(page_size);
        self.cells_ptr()
            .iter()
            .map(move |&ptr| T::new(&page[ptr.swap_bytes() as usize..]))
    }

    pub fn cell_at(&self, page_size: usize, index: usize) -> Option<T> {
        let page = self.page(page_size);
        let ptr = self.cells_ptr().get(index)?.swap_bytes();
        Some(T::new(&page[ptr as usize..]))
    }

    pub fn cells_from<R>(&self, page_size: usize, index: R) -> impl Iterator<Item = T>
    where
        R: RangeBounds<usize>,
        [u16]: std::ops::Index<R, Output = [u16]>,
    {
        let page = self.page(page_size);
        let cells = self.cells_ptr();
        cells[index]
            .iter()
            .map(move |&ptr| T::new(&page[ptr.swap_bytes() as usize..]))
    }

    pub fn binary_search<F, B>(&self, page_size: usize, key: &B, mut f: F) -> Result<usize, usize>
    where
        F: FnMut(T) -> B,
        B: std::cmp::Ord,
    {
        let page = self.page(page_size);
        let cells = self.cells_ptr();
        cells.binary_search_by_key(key, move |&ptr| {
            f(T::new(&page[ptr.swap_bytes() as usize..]))
        })
    }

    pub fn partition_point<F>(&self, page_size: usize, mut f: F) -> usize
    where
        F: FnMut(T) -> bool,
    {
        let page = self.page(page_size);
        let cells = self.cells_ptr();
        cells.partition_point(move |&ptr| f(T::new(&page[ptr.swap_bytes() as usize..])))
    }

    fn cells_ptr(&self) -> &'static [u16] {
        let cell_ptr = unsafe { self.page.add(self.cells.into()) };
        assert_eq!(cell_ptr.align_offset(std::mem::align_of::<u16>()), 0);
        unsafe { std::slice::from_raw_parts(cell_ptr.cast::<u16>(), self.number_of_cells) }
    }

    fn page(&self, page_size: usize) -> &'static [u8] {
        unsafe { std::slice::from_raw_parts(self.page, page_size) }
    }
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct Cells<T, P = ()> {
    page_size: usize,
    cells: LazyCells<T>,
    pub right_ptr: P,
}

impl<T: CellType, P> Cells<T, P> {
    pub fn create(page_size: usize, cells: LazyCells<T>, right_ptr: P) -> Self {
        Self {
            page_size,
            cells,
            right_ptr,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = T> {
        self.cells.cells(self.page_size)
    }

    pub fn at(&self, index: usize) -> T {
        self.cells.cell_at(self.page_size, index).unwrap()
    }

    pub fn get(&self, index: usize) -> Option<T> {
        self.cells.cell_at(self.page_size, index)
    }

    pub fn slice<R>(&self, index: R) -> impl Iterator<Item = T>
    where
        R: RangeBounds<usize>,
        [u16]: std::ops::Index<R, Output = [u16]>,
    {
        self.cells.cells_from(self.page_size, index)
    }

    pub fn binary_search<F, B>(&self, key: &B, f: F) -> Result<usize, usize>
    where
        F: FnMut(T) -> B,
        B: std::cmp::Ord,
    {
        self.cells.binary_search(self.page_size, key, f)
    }

    pub fn partition_point<F>(&self, f: F) -> usize
    where
        F: FnMut(T) -> bool,
    {
        self.cells.partition_point(self.page_size, f)
    }
}
