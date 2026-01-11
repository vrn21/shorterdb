//! ShorterDB database runner for benchmarking.
//!
//! This module provides a wrapper around ShorterDB that implements
//! the Database trait for uniform benchmarking.

use anyhow::Result;
use shorterdb::ShorterDB as DB;
use std::path::Path;

use crate::db::Database;

/// ShorterDB wrapper implementing the Database trait.
pub struct ShorterDBRunner {
    db: DB,
}

impl Database for ShorterDBRunner {
    fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let db = DB::new(path)?;
        Ok(Self { db })
    }

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.db.set(key, value)?;
        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.db.get(key)
    }

    fn delete(&mut self, key: &[u8]) -> Result<bool> {
        self.db.delete(key)
    }

    fn flush(&mut self) -> Result<()> {
        // ShorterDB handles flushing automatically via background flusher
        // This is a no-op but kept for trait compliance
        Ok(())
    }

    fn close(mut self) -> Result<()> {
        // Explicitly close the database
        self.db.close()?;
        Ok(())
    }

    fn name(&self) -> &'static str {
        "ShorterDB"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_basic_operations() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let mut db = ShorterDBRunner::open(temp_dir.path())?;

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
