use super::{
    memtable::{Memtable, Value},
    sst::{SstValue, SST},
    wal::{WalEntry, WalOp, WAL},
};
use crate::errors::Result;
use std::fs;
use std::path::Path;

/// Default memtable size threshold (4MB)
const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// The main database handle.
pub struct ShorterDB {
    /// In-memory write buffer
    memtable: Memtable,

    /// Write-ahead log for durability
    wal: WAL,

    /// Sorted String Tables (on-disk storage)
    sst: SST,
}

impl ShorterDB {
    /// Open a database with default memtable size (4MB).
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        Self::with_memtable_size(data_dir, DEFAULT_MEMTABLE_SIZE)
    }

    /// Open a database with custom memtable size threshold (in bytes).
    ///
    /// When the memtable exceeds this size, it will be flushed to disk.
    pub fn with_memtable_size<P: AsRef<Path>>(data_dir: P, memtable_size: usize) -> Result<Self> {
        let data_dir = data_dir.as_ref();

        // Create directory if needed
        fs::create_dir_all(data_dir)?;

        // Open WAL
        let wal = WAL::open(data_dir)?;

        // Open SST
        let sst = SST::open(data_dir)?;

        // Create memtable with size limit (minimum 1KB to prevent pathological flush behavior)
        let memtable = Memtable::new(memtable_size.max(1024));

        // Recover from WAL (entries since last flush)
        for entry in wal.read_entries()? {
            match entry.op {
                WalOp::Set => memtable.set(&entry.key, &entry.value),
                WalOp::Delete => memtable.delete(&entry.key),
            }
        }

        Ok(Self { memtable, wal, sst })
    }

    /// Get a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();

        // 1. Check memtable first (newest data)
        if let Some(value) = self.memtable.get(key) {
            return match value {
                Value::Data(bytes) => Ok(Some(bytes.to_vec())),
                Value::Tombstone => Ok(None),
            };
        }

        // 2. Check SST (older data)
        match self.sst.get(key)? {
            Some(SstValue::Data(bytes)) => Ok(Some(bytes)),
            Some(SstValue::Tombstone) => Ok(None),
            None => Ok(None),
        }
    }

    /// Set a key-value pair.
    pub fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();

        // 1. Write to WAL first (durability)
        self.wal.write(&WalEntry::set(key, value))?;

        // 2. Write to memtable
        self.memtable.set(key, value);

        // 3. Flush if needed
        if self.memtable.needs_flush() {
            self.flush_memtable()?;
        }

        Ok(())
    }

    /// Delete a key.
    ///
    /// Returns `true` if the key existed, `false` otherwise.
    pub fn delete(&mut self, key: impl AsRef<[u8]>) -> Result<bool> {
        let key = key.as_ref();

        // Check if key exists (for return value only)
        let existed = self.get(key)?.is_some();

        // 1. Write tombstone to WAL
        self.wal.write(&WalEntry::delete(key))?;

        // 2. Write tombstone to memtable
        self.memtable.delete(key);

        // 3. Flush if needed
        if self.memtable.needs_flush() {
            self.flush_memtable()?;
        }

        Ok(existed)
    }

    /// Gracefully close the database.
    pub fn close(&mut self) -> Result<()> {
        // Flush if there's data in memtable
        if !self.memtable.is_empty() {
            self.flush_memtable()?;
        }

        // Sync WAL
        self.wal.sync()?;

        Ok(())
    }

    /// Flush memtable to SST.
    fn flush_memtable(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        // 1. Write memtable to new SST file
        self.sst.write_memtable(&self.memtable)?;

        // 2. Rotate WAL (data is now in SST)
        self.wal.rotate()?;

        // 3. Clear memtable
        self.memtable.clear();

        Ok(())
    }
}

/// Drop automatically calls close() for safety.
impl Drop for ShorterDB {
    fn drop(&mut self) {
        // Best-effort close (can't propagate errors from Drop)
        let _ = self.close();
    }
}
