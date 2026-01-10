use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex};

use log::{debug, info, warn};

use super::{
    flusher::Flusher,
    memtable::{Memtable, Value},
    sst::{Sst, SstValue},
    wal::{Wal, WalEntry, WalOp},
};
use crate::errors::Result;

/// Default memtable size threshold (4MB)
const DEFAULT_MEMTABLE_SIZE: usize = 4 * 1024 * 1024;

/// The main database handle for ShorterDB.
///
/// `ShorterDB` provides an embedded key-value store with the following features:
/// - **LSM-Tree Architecture**: fast in-memory writes (memtable) flushed to disk (SST).
/// - **Durability**: Write-Ahead Log (WAL) ensures no data loss on crash.
/// - **Thread Safety**: Safe for concurrent use (though current API requires `&mut self` for writes).
///
/// # Architecture
///
/// ```text
/// Write -> WAL -> Memtable -> (Flush) -> SST Files
/// Read  -> Memtable -> Immutable Memtable -> SST Files
/// ```
///
/// # Example
///
/// ```no_run
/// use shorterdb::ShorterDB;
/// use std::path::Path;
///
/// fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut db = ShorterDB::new("/tmp/mydb")?;
///
///     // Write data
///     db.set("key", "value")?;
///
///     // Read data
///     if let Some(val) = db.get("key")? {
///         println!("Value: {:?}", val);
///     }
///
///     Ok(())
/// }
/// ```
pub struct ShorterDB {
    /// In-memory write buffer
    memtable: Memtable,

    /// Write-ahead log for durability (shared with flusher)
    wal: Arc<Mutex<Wal>>,

    /// Sorted String Tables on-disk storage (shared with flusher)
    sst: Arc<Mutex<Sst>>,

    /// Background flusher
    flusher: Flusher,
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
        info!("Opening database at {:?}", data_dir);

        // Create directory if needed
        fs::create_dir_all(data_dir)?;

        // Minimum 1KB to prevent pathological flush behavior
        let memtable_size = memtable_size.max(1024);
        debug!("Memtable size threshold: {} bytes", memtable_size);

        // Open WAL
        let wal = Arc::new(Mutex::new(Wal::open(data_dir)?));

        // Open SST
        let sst = Arc::new(Mutex::new(Sst::open(data_dir)?));

        // Create memtable
        let memtable = Memtable::new(memtable_size);

        // Create flusher with shared access to SST and WAL
        let flusher = Flusher::new(Arc::clone(&sst), Arc::clone(&wal));

        // Recover from WAL (entries since last flush)
        let recovered_count = {
            let wal_guard = wal.lock().unwrap_or_else(|e| e.into_inner());
            let entries = wal_guard.read_entries()?;
            let count = entries.len();
            for entry in entries {
                match entry.op {
                    WalOp::Set => memtable.set(&entry.key, &entry.value),
                    WalOp::Delete => memtable.delete(&entry.key),
                }
            }
            count
        };

        if recovered_count > 0 {
            info!("Recovered {} entries from WAL", recovered_count);
        }

        info!("Database opened successfully");

        Ok(Self {
            memtable,
            wal,
            sst,
            flusher,
        })
    }

    /// Get a value by key.
    pub fn get(&self, key: impl AsRef<[u8]>) -> Result<Option<Vec<u8>>> {
        let key = key.as_ref();

        // 1. Check active memtable first (newest data)
        if let Some(value) = self.memtable.get(key) {
            return self.handle_value(value);
        }

        // 2. Check immutable memtable (being flushed)
        if let Some(imm) = self.flusher.get_immutable() {
            if let Some(value) = imm.get(key) {
                return self.handle_value(value);
            }
        }

        // 3. Check SST (older data)
        let sst_guard = self.sst.lock().unwrap_or_else(|e| e.into_inner());
        match sst_guard.get(key)? {
            Some(SstValue::Data(bytes)) => Ok(Some(bytes)),
            Some(SstValue::Tombstone) => Ok(None),
            None => Ok(None),
        }
    }

    /// Convert memtable Value to Option<Vec<u8>>.
    #[inline]
    fn handle_value(&self, value: Value) -> Result<Option<Vec<u8>>> {
        match value {
            Value::Data(bytes) => Ok(Some(bytes.to_vec())),
            Value::Tombstone => Ok(None),
        }
    }

    /// Set a key-value pair.
    pub fn set(&mut self, key: impl AsRef<[u8]>, value: impl AsRef<[u8]>) -> Result<()> {
        let key = key.as_ref();
        let value = value.as_ref();

        // 1. Write to WAL first (durability)
        {
            let mut wal_guard = self.wal.lock().unwrap_or_else(|e| e.into_inner());
            wal_guard.write(&WalEntry::set(key, value))?;
        }

        // 2. Write to memtable
        self.memtable.set(key, value);

        // 3. Trigger flush if needed
        self.maybe_flush();

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
        {
            let mut wal_guard = self.wal.lock().unwrap_or_else(|e| e.into_inner());
            wal_guard.write(&WalEntry::delete(key))?;
        }

        // 2. Write tombstone to memtable
        self.memtable.delete(key);

        // 3. Trigger flush if needed
        self.maybe_flush();

        Ok(existed)
    }

    /// Check if flush is needed and schedule it.
    fn maybe_flush(&mut self) {
        if self.memtable.needs_flush() {
            let max_size = self.memtable.max_size();
            debug!("Memtable full, scheduling flush");

            // Swap memtable with a new empty one
            let old = std::mem::replace(&mut self.memtable, Memtable::new(max_size));

            // Schedule background flush (non-blocking unless stalled)
            self.flusher.schedule(old);
        }
    }

    /// Gracefully close the database.
    pub fn close(&mut self) -> Result<()> {
        info!("Closing database");

        // Flush remaining data in memtable
        if !self.memtable.is_empty() {
            let max_size = self.memtable.max_size();
            let old = std::mem::replace(&mut self.memtable, Memtable::new(max_size));
            self.flusher.schedule(old);
        }

        // Wait for all flushes to complete
        self.flusher.wait_for_completion();

        // Shutdown flusher
        self.flusher.shutdown();

        // Sync WAL
        {
            let mut wal_guard = self.wal.lock().unwrap_or_else(|e| e.into_inner());
            wal_guard.sync()?;
        }

        info!("Database closed");
        Ok(())
    }
}

/// Drop automatically calls close() for safety.
impl Drop for ShorterDB {
    fn drop(&mut self) {
        // Best-effort close (can't propagate errors from Drop)
        if let Err(e) = self.close() {
            warn!("Error during database close: {}", e);
        }
    }
}
