#[derive(PartialEq, Debug)]
pub struct Block {
    fname: String,
    blknum: u32,
}

impl Block {
    pub fn new(fname: String, blknum: u32) -> Self {
        Block { fname, blknum }
    }

    pub fn get_fname(&self) -> &str {
        return &self.fname;
    }

    pub fn get_blknum(&self) -> u32 {
        return self.blknum;
    }
}
