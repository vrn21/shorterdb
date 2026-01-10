//! # ShorterDB
//!
//! A lightweight embedded key-value store built with SkipLists and LSM architecture.
//!
//! ## Example
//!
//! ```rust,no_run
//! use shorterdb::ShorterDB;
//! use std::path::Path;
//!
//! let mut db = ShorterDB::new(Path::new("./my_db")).unwrap();
//! db.set(b"key", b"value").unwrap();
//! let value = db.get(b"key").unwrap();
//! assert_eq!(value, Some(b"value".to_vec()));
//! ```

pub mod errors;
pub mod kv;

pub use errors::{Result, ShortDBErrors};
pub use kv::db::ShorterDB;
