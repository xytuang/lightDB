use std::collections::HashMap;
use std::fs::File;
use std::sync::{Arc, Mutex, RwLock};
use std::io::{SeekFrom, Seek, Read};
use crate::storage::{block::Block, page::Page};

pub struct FileMgr {
    blksize: u32,
    is_new: bool,
    directory: File,
    open_files: Mutex<HashMap<String, Arc<RwLock<File>>>>,
}

impl FileMgr {
    pub fn new(directory: File, blksize: u32) {

    }

    // Reads block into page
    pub fn read(&mut self, blk: &Block, p: &mut Page) {
        let file_lock = self.get_file(blk.get_fname());
        let read_guard = file_lock.read().unwrap();
        let mut f = &*read_guard;
        let mut buffer = &mut p.get_buffer();
        let offset = (blk.get_blknum() * self.blksize) as u64;
        let tmp: &mut [u8] = &mut vec![0; buffer.len()]; //a temp buffer to store content from file
        
        buffer.set_wpos(0); // move write pos to the beginning of this page
        f.seek(SeekFrom::Start(offset));
        f.read_exact(tmp);
        buffer.write_bytes(tmp);
    }

    // Writes from page to block
    pub fn write(&mut self) {

    }

    pub fn append(&mut self) {}

    pub fn get_is_new(&self) -> bool {
        self.is_new
    }

    pub fn get_blksize(&self) -> u32 {
        self.blksize
    }

    pub fn get_file(&mut self, fname: &str) -> Arc<RwLock<File>> {
        let mut files = self.open_files.lock().unwrap();
        files.entry(fname.to_string())
            .or_insert_with(|| {
                let f = File::create_new(fname.to_string()).unwrap();
                Arc::new(RwLock::new(f))
            }).clone()
    }
}
