use lightDB::core::filemgr::*;
use lightDB::core::block::*;
use lightDB::core::page::*;
use lightDB::core::types::{FileMgr, Block, Page};
use lightDB::error::Error;
use lightDB::utils::utils::max_len;

use std::path::PathBuf;
use std::env;

#[test]
fn test_filemgr() {
    let mut db_directory: PathBuf = env::current_dir().expect("Failed to get current directory");

    db_directory.push("filetest");

    let mut fm = FileMgr::new(db_directory, 800);
    let mut blk = Block::new("testfile", 2);
    let mut p1 = Page::new(fm.get_blocksize());
    let pos1: u32 = 88;

    p1.set_string(pos1,"abcdefghijklm");

    let size: u32 = max_len("abcdefghijklm");
    let pos2 = size + pos1;

    p1.set_int(pos2, 345);
    fm.write(blk, p1);

    let mut p2 = Page::new(fm.get_blocksize());

    fm.read(blk, p2);

    assert!(p2.get_int(pos2) == 345);
    assert!(p2.get_string(pos1) == "abcdefghijklm");
}
