use std::{cmp::Ordering, slice::from_raw_parts, str::from_utf8_unchecked};

use crate::values::{Typ, Val};

pub fn read_varint(bytes: &[u8]) -> (u64, &[u8], usize) {
    let mut result = 0;
    let mut bytes_read = 0;

    loop {
        let byte = bytes[bytes_read];
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

    (result, &bytes[bytes_read..], bytes_read)
}

#[derive(Copy, Clone, Debug, PartialEq)]
pub struct LazyRecord {
    bytes: *const u8,
    header: usize,
}

impl LazyRecord {
    pub fn new(bytes: &[u8]) -> Self {
        let (header_size, payload, header_size_length) = read_varint(bytes);

        let bytes = payload.as_ptr();
        let header = header_size as usize - header_size_length;

        Self { bytes, header }
    }

    pub fn consume<'a>(&self, mut visitor: impl Visitor<'a>) {
        let mut payload = unsafe { self.bytes.add(self.header) };
        let mut header = unsafe { std::slice::from_raw_parts(self.bytes, self.header) };

        let mut index = 0;

        while !header.is_empty() {
            let (serial_type, next_header, _) = read_varint(header);
            let payload_size = read(serial_type, &mut visitor, index, payload);
            header = next_header;
            payload = unsafe { payload.add(payload_size) };
            index += 1;
        }
    }

    pub fn consume_one<'a, C: Visitor<'a>>(&self, index: usize, mut visitor: C) -> C {
        let mut header = unsafe { std::slice::from_raw_parts(self.bytes, self.header) };
        let bytes = unsafe { self.bytes.add(self.header) };

        let mut payload_offset = 0;

        for idx in 0..index {
            let (serial_type, rest, _) = read_varint(header);
            let payload_size = read(serial_type, IgnoreAll, idx, bytes);

            payload_offset += payload_size;
            header = rest;
        }

        let (serial_type, _, _) = read_varint(header);
        read(serial_type, &mut visitor, index, unsafe {
            self.bytes.add(self.header + payload_offset)
        });

        visitor
    }
}

fn read<'a>(
    serial_type: u64,
    mut visitor: impl Visitor<'a>,
    index: usize,
    bytes: *const u8,
) -> usize {
    match serial_type {
        0 => {
            if visitor.read_next(index, Typ::Null) {
                visitor.on_null()
            }
            0
        }
        1 => read_int::<1>(bytes, index, visitor),
        2 => read_int::<2>(bytes, index, visitor),
        3 => read_int::<3>(bytes, index, visitor),
        4 => read_int::<4>(bytes, index, visitor),
        5 => read_int::<6>(bytes, index, visitor),
        6 => read_int::<8>(bytes, index, visitor),
        7 => read_float(bytes, index, visitor),
        8 | 9 => {
            if visitor.read_next(index, Typ::Bool) {
                visitor.on_bool(serial_type == 9)
            }
            0
        }
        10 | 11 => panic!("Cannot read from an internal serial type: {}", serial_type),
        _ => {
            let len = ((serial_type >> 1) - 6) as usize;
            if serial_type & 0x1 == 0x1 && visitor.read_next(index, Typ::Text) {
                visitor.on_text(unsafe { from_utf8_unchecked(from_raw_parts(bytes, len)) })
            } else if visitor.read_next(index, Typ::Blob) {
                visitor.on_blob(unsafe { from_raw_parts(bytes, len) })
            }
            len
        }
    }
}

fn read_int<'a, const LEN: usize>(
    bytes: *const u8,
    index: usize,
    mut visitor: impl Visitor<'a>,
) -> usize {
    if visitor.read_next(index, Typ::Int) {
        let mut data = [0; 8];
        data[8 - LEN..].copy_from_slice(unsafe { from_raw_parts(bytes, LEN) });
        let value = i64::from_be_bytes(data);
        visitor.on_int(value)
    }
    LEN
}

fn read_float<'a>(bytes: *const u8, index: usize, mut visitor: impl Visitor<'a>) -> usize {
    if visitor.read_next(index, Typ::Float) {
        let data: [u8; 8] = unsafe { from_raw_parts(bytes, 8) }.try_into().unwrap();
        let value = f64::from_be_bytes(data);
        visitor.on_float(value)
    }
    8
}

#[allow(unused_variables)]
pub trait Visitor<'a> {
    fn read_next(&mut self, index: usize, typ: Typ) -> bool {
        true
    }

    fn on_null(&mut self) {}

    fn on_bool(&mut self, value: bool) {}

    fn on_int(&mut self, value: i64) {}

    fn on_float(&mut self, value: f64) {}

    fn on_text(&mut self, value: &'a str) {}

    fn on_blob(&mut self, value: &'a [u8]) {}
}

impl<'a, T: Visitor<'a>> Visitor<'a> for &mut T {
    fn read_next(&mut self, index: usize, typ: Typ) -> bool {
        (**self).read_next(index, typ)
    }

    fn on_null(&mut self) {
        (**self).on_null()
    }

    fn on_bool(&mut self, value: bool) {
        (**self).on_bool(value)
    }

    fn on_int(&mut self, value: i64) {
        (**self).on_int(value)
    }

    fn on_float(&mut self, value: f64) {
        (**self).on_float(value)
    }

    fn on_text(&mut self, value: &'a str) {
        (**self).on_text(value)
    }

    fn on_blob(&mut self, value: &'a [u8]) {
        (**self).on_blob(value)
    }
}

pub struct FilterVisitor<'a> {
    pub column: usize,
    value: &'a Val,
    pub result: Ordering,
}

impl<'a> FilterVisitor<'a> {
    pub fn new(column: usize, value: &'a Val) -> Self {
        Self {
            column,
            value,
            result: Ordering::Equal,
        }
    }

    fn matches<X: ?Sized>(&mut self, value: &X)
    where
        Val: std::cmp::PartialOrd<X>,
    {
        self.result = self.value.partial_cmp(value).unwrap();
    }
}

impl<'a> Visitor<'a> for FilterVisitor<'a> {
    fn read_next(&mut self, index: usize, typ: Typ) -> bool {
        if self.column == index {
            assert!(
                self.value.typ().comparable(typ),
                "Cannot compare a value of type {:?} to {:?}",
                typ,
                self.value
            );
            true
        } else {
            false
        }
    }

    fn on_null(&mut self) {
        self.matches(&())
    }

    fn on_bool(&mut self, value: bool) {
        self.matches(&value)
    }

    fn on_int(&mut self, value: i64) {
        self.matches(&value)
    }

    fn on_float(&mut self, value: f64) {
        self.matches(&value)
    }

    fn on_text(&mut self, value: &'a str) {
        self.matches(value)
    }

    fn on_blob(&mut self, value: &'a [u8]) {
        self.matches(value)
    }
}

pub struct ReadText<'a>(Option<&'a str>);

impl<'a> ReadText<'a> {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn get(self) -> Option<&'a str> {
        self.0
    }
}

impl<'a> Visitor<'a> for ReadText<'a> {
    fn on_text(&mut self, value: &'a str) {
        self.0 = Some(value);
    }
}

pub struct ReadInt(Option<i64>);

impl ReadInt {
    pub fn new() -> Self {
        Self(None)
    }

    pub fn get(self) -> Option<i64> {
        self.0
    }
}

impl Visitor<'static> for ReadInt {
    fn on_int(&mut self, value: i64) {
        self.0 = Some(value);
    }
}

pub struct Printer;

impl Visitor<'static> for Printer {
    fn on_bool(&mut self, value: bool) {
        print!("{}", value);
    }

    fn on_int(&mut self, value: i64) {
        print!("{}", value);
    }

    fn on_float(&mut self, value: f64) {
        print!("{}", value);
    }

    fn on_text(&mut self, value: &'static str) {
        print!("{}", value);
    }

    fn on_blob(&mut self, _value: &'static [u8]) {
        print!("<BLOB>");
    }
}

struct IgnoreAll;

impl Visitor<'static> for IgnoreAll {
    fn read_next(&mut self, _index: usize, _typ: Typ) -> bool {
        false
    }
}
