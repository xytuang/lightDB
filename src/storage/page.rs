use bytebuffer::ByteBuffer;

pub struct Page {
    buffer: ByteBuffer,
}

impl Page {
    // Create empty page
    pub fn new_empty(blksize: u32) -> Self {
        let mut buffer = ByteBuffer::new();
        buffer.resize(blksize as usize);
        Page { buffer }
    }

    // Create page from existing buffer
    pub fn new_from_vec(vec: Vec<u8>) -> Self {
        let buffer = ByteBuffer::from_vec(vec);
        Page { buffer }
    }

    // Gets an int from this page. Returns None if out of bounds or invalid read
    pub fn get_int(&mut self, offset: u32) -> Option<u32> {
        self.buffer.set_rpos(offset as usize);
        let res = self.buffer.read_u32();
        return res.ok();
    }

    // Writes an int to this page
    pub fn set_int(&mut self, offset: u32, n: u32) -> () {
        self.buffer.set_wpos(offset as usize);
        self.buffer.write_u32(n)
    }

    // Gets a string from this page. Returns None if out of bounds or invalid read
    pub fn get_string(&mut self, offset: u32) -> Option<String> {
        self.buffer.set_rpos(offset as usize);
        let res = self.buffer.read_string();
        return res.ok();
    }

    // Writes a string to this page
    pub fn set_string(&mut self, offset: u32, string: &str) -> () {
        self.buffer.set_wpos(offset as usize);
        self.buffer.write_string(string)
    }

    // Gets bytes from this page. Returns None if out of bounds or invalid read
    pub fn get_bytes(&mut self, offset: u32) -> Option<Vec<u8>> {
        let opt = self.get_string(offset);

        return match opt {
            Some(x) => Some(x.into_bytes()),
            None => None,
        };
    }

    // Writes a string to this page
    pub fn set_bytes(&mut self, offset: u32, bytes: Vec<u8>) -> () {
        let string = String::from_utf8(bytes);
        if string.is_err() {
            return;
        }
        self.set_string(offset, &string.unwrap())
    }

    // exposes buffer of this page for reading and writing
    pub fn get_buffer(&mut self) -> &mut ByteBuffer {
        return &mut self.buffer;
    }
}
