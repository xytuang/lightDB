use std::collections::HashMap;
use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

//Identify block by filename and logical block number
#[derive(Debug)]
pub struct Block {
    pub fname: String,
    pub blknum: u32
}

//Holds contents of disk block
//Can hold ints, strings, bytes
pub struct Page {
    pub buffer: Vec<u8>
}

pub struct FileMgr {
    pub db_directory: PathBuf,                                  //name of database
    pub blocksize: u32,                                         //denotes size of one block
    pub is_new: bool,                                           //indicates if a new folder was created
    pub open_files: RwLock<HashMap<String, Arc<RwLock<File>>>>  //mapping from filenames to open files
}
