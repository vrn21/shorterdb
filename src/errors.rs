// use serde_json;
use std::io;
use thiserror::Error;

/// Error type for kvs.
#[derive(Error, Debug)]
pub enum ShortDBErrors {
    /// IO error.
    #[error("{0}")]
    Io(#[from] io::Error),
    /// Serialization or deserialization error.
    // #[error("{0}")]
    // Serde(#[from] serde_json::Error),
    /// Removing non-existent key error.
    #[error("Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicates a corrupted log or a program bug.
    #[error("Unexpected command type")]
    UnexpectedCommandType,
    //value is not set, it is given when we try get after set
    #[error("Value not set")]
    ValueNotSet,
}

/// Result type for kvs.
pub type Result<T> = std::result::Result<T, ShortDBErrors>;
