use anyhow::{anyhow, bail, ensure, Result};
use std::{borrow::Cow, ffi::CStr, fs::File};

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct DbHeader {
    pub page_size: usize,
    pub db_size: usize,
    pub reserved_size: usize,
    pub text_encoding: TextEncoding,
    pub sqlite_version: (u16, u16, u16),
}

impl DbHeader {
    pub fn from_bytes(file: &File, bytes: [u8; 100]) -> Result<Self> {
        let header = unsafe { std::mem::transmute::<_, DbHeaderStruct>(bytes) };
        header.verify(file)
    }
}

#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub enum TextEncoding {
    #[default]
    Utf8 = 1,
    Utf16le = 2,
    Utf16be = 3,
}

impl TextEncoding {
    // TODO: get rid, fail on utf16, inline utf8 and remove Rc/Cow
    pub fn read(self, bytes: &[u8]) -> Result<Cow<str>> {
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
