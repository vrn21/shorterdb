use super::{
    memtable::{Memtable, Value},
    sst::SST,
    wal::{WALEntry, WAL},
};
use crate::errors::Result;
use bytes::Bytes;
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
        let wal = WAL::new(data_dir)?;

        // Open SST
        let sst = SST::open(format!("{:?}", data_dir));

        // Create memtable with size limit
        let memtable = Memtable::new(memtable_size);

        // TODO: Recover from WAL (only entries since last flush)
        // for entry in wal.read_entries()? {
        //     match entry.op {
        //         WalOp::Set => memtable.set(&entry.key, &entry.value),
        //         WalOp::Delete => memtable.delete(&entry.key),
        //     }
        // }

        Ok(Self { memtable, wal, sst })
    }

    /// Get a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();

        // 1. Check memtable first
        if let Some(value) = self.memtable.get(key) {
            return match value {
                Value::Data(bytes) => Ok(Some(bytes.to_vec())),
                Value::Tombstone => Ok(None),
            };
        }

        // 2. Check SST
        Ok(self.sst.get(key))
    }

    /// Set a key-value pair.
    pub fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();

        // 1. Write to WAL first (durability)
        let entry = WALEntry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };
        self.wal.write(&entry)?;

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
        let tombstone_entry = WALEntry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(b"tombstone"), // WAL still uses bytes for now
        };
        self.wal.write(&tombstone_entry)?;

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

        // Sync WAL (best effort for now)
        // TODO: Add sync method to WAL

        Ok(())
    }

    /// Flush memtable to SST.
    fn flush_memtable(&mut self) -> Result<()> {
        if self.memtable.is_empty() {
            return Ok(());
        }

        // Get max_size before taking ownership of memtable
        let max_size = self.memtable.max_size();

        // 1. Write memtable to SST - SST's set() will iterate over entries directly
        // Swap the current memtable with a fresh one
        let old_memtable = std::mem::replace(&mut self.memtable, Memtable::new(max_size));
        self.sst.queue.push_back(old_memtable);
        self.sst.set();

        // 2. TODO: Rotate WAL (delete old entries that are now in SST)
        // self.wal.rotate()?;

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
