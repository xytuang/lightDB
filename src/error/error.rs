use std::io;

pub enum Error {
    FileNotFound,
    SeekFailed(io::Error),
    WriteFailed(io::Error),
    ReadFailed(io::Error),
    LockPoisoned(io::Error),
    FileCreationFailed(io::Error)
}
