use crate::core::types::{FileMgr, Block, Page};
use crate::error::error::Error;
use std::fs::{self,File, OpenOptions};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::collections::HashMap;
use std::io;
use std::sync::PoisonError;
use std::sync::RwLockReadGuard;

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

        let open_files: RwLock<HashMap<String, Arc<RwLock<File>>>> = RwLock::new(HashMap::new());

        FileMgr {
            db_directory,
            blocksize,
            is_new,
            open_files,
        }
    }
    //Reads contents of blk into p
    pub fn read(&self, blk: &Block, p: &mut Page) -> Result<(), Error> {
        if let Ok(file) = self.get_file(blk.get_fname()) {
            let f = file.read().map_err(|e: PoisonError<RwLockReadGuard<'_, File>>| { io::Error::new(io::ErrorKind::Other, e)})?;

            f.seek(blk.get_blknum() * self.blocksize).map_err(|e| Error::SeekFailed(e))?;
            f.read(p.contents()).map_err(|e| Error::ReadFailed(e))?;

            Ok(())
        } else {
            Err(Error::FileNotFound)
        }
    }

    //Writes contents of p to blk
    pub fn write(&self, blk: &mut Block, p: &Page) -> Result<(), Error> {
        if let Ok(file) = self.get_file(blk.get_fname()) {
            let f = file.write().map_err(|e| Error::LockPoisoned(e))?;

            f.seek(blk.get_blknum() * self.blocksize).map_err(|e| Error::SeekFailed(e))?;
            f.write(p.contents()).map_err(|e| Error::WriteFailed(e))?;

            Ok(())
        } else {
            Err(Error::FileNotFound)
        }
    }

    pub fn append(&self, fname: &str) -> Result<Block, Error> {
        let new_blknum = self.get_num_blocks(fname)?;
        let blk = Block::new(String::from(fname), new_blknum);

        if let Ok(file) = self.get_file(blk.get_fname()) {
            let f = file.write().map_err(|e| Error::LockPoisoned(e))?;

            f.seek(blk.get_blknum() * self.blocksize).map_err(|e| Error::SeekFailed(e))?;

            let vec: Vec<u8>  = vec![0; self.blocksize.try_into().unwrap()];
            let b: &[u8] = &vec;

            f.write(b).map_err(|e| Error::WriteFailed(e))?;

            Ok(blk)
        } else {
            Err(Error::FileNotFound)
        }
    }

    //Returns true if new folder is created for database, false otherwise
    //Used to initialize database
    pub fn is_new(&self) -> bool {
        return self.is_new;
    }

    //Returns number of blocks in file fname
    pub fn get_num_blocks(&self, fname: &str) -> Result<u32, Error> {

        if let Ok(file) = self.get_file(fname) {
            let f = file.read().map_err(|e| Error::LockPoisoned(e))?;
            let num_blocks: u32 = (f.metadata().unwrap().len()) as u32 / self.blocksize;

            return Ok(num_blocks);

        } else {
            Err(Error::FileNotFound)
        }
    }

    pub fn get_blocksize(&self) -> u32 {
        return self.blocksize;
    }

    fn get_file(&self, fname: &str) -> Result<Arc<RwLock<File>>, Error> {
        let mut files = self.open_files.write().unwrap();

        if let Some(file) = files.get(fname) {
            return Ok(file.clone());
        } else {
            let file_path = self.db_directory.join(fname);

            match OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .open(file_path.clone()) {
                    Ok(file) => {
                        let f = Arc::new(RwLock::new(file));

                        files.insert(fname.to_string(), f.clone());
                        return Ok(f);
                    },
                    Err(e) => Err(Error::FileCreationFailed(e))
            }
        }
    }
}
