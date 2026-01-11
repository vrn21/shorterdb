//! Database trait for uniform benchmarking interface.
//!
//! This trait abstracts over different database implementations,
//! providing a common interface for benchmarking.

use anyhow::Result;
use std::path::Path;

/// Trait for database implementations to be benchmarked.
///
/// All methods use byte slices (`&[u8]`) for maximum flexibility
/// and to avoid allocations where possible.
pub trait Database {
    /// Open or create a database at the given path.
    ///
    /// # Errors
    ///
    /// Returns an error if the database cannot be opened or created.
    fn open<P: AsRef<Path>>(path: P) -> Result<Self>
    where
        Self: Sized;

    /// Write a key-value pair to the database.
    ///
    /// # Errors
    ///
    /// Returns an error if the write fails.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()>;

    /// Read a value by key.
    ///
    /// Returns `Ok(None)` if the key doesn't exist.
    ///
    /// # Errors
    ///
    /// Returns an error if the read operation fails.
    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Delete a key from the database.
    ///
    /// Returns `true` if the key existed, `false` otherwise.
    ///
    /// # Errors
    ///
    /// Returns an error if the delete operation fails.
    fn delete(&mut self, key: &[u8]) -> Result<bool>;

    /// Flush any buffered writes to disk.
    ///
    /// This is a hint; implementations may choose to flush automatically.
    ///
    /// # Errors
    ///
    /// Returns an error if the flush fails.
    fn flush(&mut self) -> Result<()>;

    /// Close the database gracefully.
    ///
    /// # Errors
    ///
    /// Returns an error if cleanup fails.
    fn close(self) -> Result<()>;

    /// Get the name of this database implementation.
    ///
    /// Used for display in benchmark results.
    fn name(&self) -> &'static str;
}
