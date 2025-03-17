use crate::core::types::Page;
use byteorder::{ByteOrder, LittleEndian};

impl Page {
    pub fn new(blocksize: u32) -> Self {
        Page {buffer: vec![0; blocksize.try_into().unwrap()]}
    }

    pub fn new_bytes(buffer: Vec<u8>) -> Self {
        Page {buffer}
    }

    pub fn get_int(&self, offset: u32) -> Option<u32> {
        if self.buffer.len() >= (offset + 4) as usize {
            let slice = &self.buffer[offset as usize..(offset + 4) as usize];
            Some(LittleEndian::read_u32(slice))
        } else {
            None
        }
    }

    pub fn get_bytes(&self, offset: u32) -> Option<&[u8]> {
        let byte_len: Option<u32> = self.get_int(offset);

        match byte_len {
            Some(len) => {
                if self.buffer.len() >= (offset + 4 + len) as usize {
                    let slice = &self.buffer[(offset + 4) as usize..(offset + 4 + len) as usize];
                    Some(slice)
                } else {
                    None
                }
            }
            None => None
        }
    }

    pub fn get_string(&self, offset: u32) -> Option<String> {
        let bytes: Option<&[u8]> = self.get_bytes(offset);

        match bytes {
            Some(byte_slice) => {
                String::from_utf8(byte_slice.to_vec()).ok()
            }
            None => None
        }
    }

    pub fn set_int(&mut self, offset: u32, n: u32) -> () {
        let bytes = n.to_le_bytes();
        self.buffer[offset as usize..(offset + 4) as usize].copy_from_slice(&bytes)
    }

    pub fn set_bytes(&mut self, offset: u32, bytes: &[u8]) -> () {
        let length: u32 = bytes.len() as u32;
        self.set_int(offset, length);
        self.buffer[(offset + 4) as usize..(offset + 4 + length) as usize].copy_from_slice(bytes);
    }

    pub fn set_string(&mut self, offset: u32, string: String) -> () {
        let bytes = string.as_bytes();
        self.set_bytes(offset, bytes);
    }

    pub(crate) fn contents(&mut self) -> &mut [u8] {
        return &mut self.buffer;
    }

}
