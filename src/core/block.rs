use crate::core::types::Block
use std::fmt
use std::hash::{Hash, Hasher}

impl Block {
    pub fn new(fname: String, blknum: u32) -> Self {
        Block {fname, blknum}
    }

    pub fn get_fname(&self) -> String {
        return self.fname;
    }

    pub fn get_blknum(&self) -> u32 {
        return self.blknum;
    }
}

impl PartialEq for Block {

    fn eq(&self, other: &Self) -> bool {
        return self.fname == other.fname
    }
}

impl Hash for Block {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.fname.hash(state);
        self.blknum.hash(state);
    }
}

impl fmt::Display for Block {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Block: {}, {}", self.fname, self.blknum)
    }
}


