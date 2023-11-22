use crate::values::Value;

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

    pub fn value_at(&self, index: usize) -> Value {
        let mut payload_offset = 0;

        let mut header = unsafe { std::slice::from_raw_parts(self.bytes, self.header) };
        for _ in 0..index {
            let (serial_type, rest, _) = read_varint(header);
            payload_offset += Value::payload_size(serial_type);
            header = rest;
        }

        let (serial_type, _, _) = read_varint(header);
        let payload_size = Value::payload_size(serial_type);
        let payload = unsafe { self.bytes.add(self.header + payload_offset) };
        let payload = unsafe { std::slice::from_raw_parts(payload, payload_size) };
        let (_, value) = Value::from_bytes(serial_type, payload);

        value
    }

    pub fn values(&self) -> LazyRecordValues<'_> {
        LazyRecordValues {
            header: unsafe { std::slice::from_raw_parts(self.bytes, self.header) },
            payload: unsafe { self.bytes.add(self.header) },
        }
    }

    pub fn for_each(&self, mut op: impl FnMut(Value)) {
        let mut payload = unsafe { self.bytes.add(self.header) };
        let mut header = unsafe { std::slice::from_raw_parts(self.bytes, self.header) };
        while !header.is_empty() {
            let (serial_type, next_header, _) = read_varint(header);
            let payload_size = Value::payload_size(serial_type);
            let payload_data = unsafe { std::slice::from_raw_parts(payload, payload_size) };
            let (_, value) = Value::from_bytes(serial_type, payload_data);

            op(value);

            header = next_header;
            payload = unsafe { payload.add(payload_size) };
        }
    }
}

pub struct LazyRecordValues<'a> {
    header: &'a [u8],
    payload: *const u8,
}

impl<'a> Iterator for LazyRecordValues<'a> {
    type Item = Value;

    fn next(&mut self) -> Option<Self::Item> {
        if self.header.is_empty() {
            return None;
        }

        let (serial_type, next_header, _) = read_varint(self.header);
        let payload_size = Value::payload_size(serial_type);
        let payload_data = unsafe { std::slice::from_raw_parts(self.payload, payload_size) };
        let (_, value) = Value::from_bytes(serial_type, payload_data);

        self.header = next_header;
        self.payload = unsafe { self.payload.add(payload_size) };

        Some(value)
    }
}
