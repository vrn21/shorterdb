use std::io;
use thiserror::Error;

/// Error type for kvs.
#[derive(Error, Debug)]
pub enum ShortDBErrors {
    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    /// WAL file is corrupted (partial write detected).
    #[error("WAL corruption detected")]
    WalCorruption,

    /// SST file is corrupted.
    #[error("SST corruption: {0}")]
    SstCorruption(String),

    /// UTF-8 encoding error.
    #[error("UTF-8 error: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, ShortDBErrors>;
