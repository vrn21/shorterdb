//! RocksDB database runner for benchmarking.
//!
//! This module provides a wrapper around RocksDB that implements
//! the Database trait for uniform benchmarking.

use anyhow::{Context, Result};
use rocksdb::{Options, DB};
use std::path::Path;

use crate::db::Database;

/// RocksDB wrapper implementing the Database trait.
pub struct RocksDBRunner {
    db: DB,
}

impl Database for RocksDBRunner {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        // Use default options for fair comparison
        let mut opts = Options::default();
        opts.create_if_missing(true);

        let db = DB::open(&opts, path).context("Failed to open RocksDB")?;

        Ok(Self { db })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db
            .put(key, value)
            .context("Failed to write to RocksDB")?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let result = self.db.get(key).context("Failed to read from RocksDB")?;
        Ok(result)
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        // RocksDB doesn't return whether key existed
        // Check first, then delete
        let existed = self.db.get(key)?.is_some();

        self.db
            .delete(key)
            .context("Failed to delete from RocksDB")?;

        Ok(existed)
    }

    fn flush(&mut self) -> Result<()> {
        self.db.flush().context("Failed to flush RocksDB")?;
        Ok(())
    }

    fn close(self) -> Result<()> {
        // RocksDB closes automatically on drop
        // But we can explicitly flush before dropping
        drop(self.db);
        Ok(())
    }

    fn name(&self) -> &'static str {
        "RocksDB"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_basic_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut db = RocksDBRunner::open(temp_dir.path())?;

        // Test write
        db.put(b"key1", b"value1")?;

        // Test read
        let value = db.get(b"key1")?;
        assert_eq!(value, Some(b"value1".to_vec()));

        // Test delete
        let existed = db.delete(b"key1")?;
        assert!(existed);

        // Verify deletion
        let value = db.get(b"key1")?;
        assert_eq!(value, None);

        db.close()?;
        Ok(())
    }
}
