use std::collections::HashMap;
use std::fs::File;
use std::sync::Mutex;

pub struct FileMgr {
    blksize: u32,
    is_new: bool,
    directory: File,
    open_files: HashMap<String, File>,
}

impl FileMgr {
    pub fn new(directory: File, blksize: u32) {}

    pub fn read() {}

    pub fn write() {}

    pub fn append() {}

    pub fn get_is_new(&self) -> bool {
        self.is_new
    }

    pub fn get_blksize() -> u32 {
        self.blksize;
    }

    pub fn get_file() {}
}
