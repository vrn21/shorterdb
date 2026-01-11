//! Database runner implementations.
//!
//! This module contains implementations of the Database trait
//! for various database backends.

pub mod rocksdb;
pub mod shorterdb;

pub use self::rocksdb::RocksDBRunner;
pub use self::shorterdb::ShorterDBRunner;
