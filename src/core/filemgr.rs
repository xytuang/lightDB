use crate::core::{FileMgr, Block, Page};
use std::fs::{self,File};
use std::path::PathBuf;

impl FileMgr {
    pub fn new(db_directory: PathBuf, blocksize: u32) -> Self {
        let is_new = !db_directory.exists();

        if is_new {
            fs::create_dir_all(&db_directory).expect("Failed to create directory");
        }

        // Remove temporary files
        if let Ok(entries) = fs::read_dir(&db_directory) {
            for entry in entries.flatten() {
                if let Some(filename) = entry.file_name().to_str() {
                    if filename.starts_with("temp") {
                        let _ = fs::remove_file(entry.path()); // Ignore errors for now
                    }
                }
            }
        }

        let open_files: HashMap<String, File> = HashMap::new();

        FileMgr {
            db_directory,
            blocksize,
            is_new,
            open_files,
        }
    }
    //Reads contents of blk into p
    pub fn read(&self, blk: &Block, p: &mut Page) -> () {

    }

    //Writes contents of p to blk
    pub fn write(&self, blk: &mut Block, p: &Page) -> () {

    }

    pub fn append(&self, fname: String) -> Block {

    }

    //Returns true if new folder is created for database, false otherwise
    //Used to initialize database
    pub fn is_new(&self) -> bool {
    }

    pub fn length(&self, fname: String) -> u32 {

    }

    pub fn get_blocksize(&self) -> u32 {

    }
}
