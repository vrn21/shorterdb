use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::atomic::{AtomicUsize, Ordering};

/// Represents a value in the memtable/WAL/SST.
///
/// This is the canonical way to represent "deleted" in Rust -
/// using an enum rather than a magic value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Value {
    /// The key has this data
    Data(Bytes),
    /// The key was deleted
    Tombstone,
}

impl Value {
    /// Check if this is a tombstone (deletion marker).
    #[inline]
    pub fn is_tombstone(&self) -> bool {
        matches!(self, Value::Tombstone)
    }

    /// Size in bytes (for memory tracking).
    #[inline]
    pub fn size(&self) -> usize {
        match self {
            Value::Data(b) => b.len(),
            Value::Tombstone => 0,
        }
    }
}

/// Overhead per entry (skip list node, Arc overhead, etc.)
/// This is an estimate - doesn't need to be exact.
const ENTRY_OVERHEAD: usize = 64;

/// In-memory sorted write buffer.
///
/// Note: Size tracking is intentionally approximate. Concurrent writes may cause
/// slight inaccuracies, but this is acceptable since the size is only used to
/// determine when to flush (not for correctness).
pub struct Memtable {
    /// Sorted key-value storage
    entries: SkipMap<Bytes, Value>,

    /// Current approximate size in bytes (may be slightly inaccurate under contention)
    size_bytes: AtomicUsize,

    /// Threshold to trigger flush
    max_size_bytes: usize,
}

impl Memtable {
    /// Create a new memtable with the given size threshold.
    pub fn new(max_size_bytes: usize) -> Self {
        Self {
            entries: SkipMap::new(),
            size_bytes: AtomicUsize::new(0),
            max_size_bytes,
        }
    }

    /// Look up a key.
    ///
    /// Returns:
    /// - `Some(Value::Data(_))` if key exists with value
    /// - `Some(Value::Tombstone)` if key was deleted
    /// - `None` if key not in memtable (check SST next)
    pub fn get(&self, key: &[u8]) -> Option<Value> {
        self.entries.get(key).map(|e| e.value().clone())
    }

    /// Set a key-value pair.
    pub fn set(&self, key: &[u8], value: &[u8]) {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_bytes = Bytes::copy_from_slice(value);
        let new_size = key.len() + value.len() + ENTRY_OVERHEAD;

        // Get old size if key exists
        let old_size = self
            .entries
            .get(key)
            .map(|e| key.len() + e.value().size() + ENTRY_OVERHEAD)
            .unwrap_or(0);

        // Insert new value
        self.entries.insert(key_bytes, Value::Data(value_bytes));

        // Update size (add new, subtract old)
        if new_size > old_size {
            self.size_bytes
                .fetch_add(new_size - old_size, Ordering::Relaxed);
        } else {
            self.size_bytes
                .fetch_sub(old_size - new_size, Ordering::Relaxed);
        }
    }

    /// Delete a key by inserting a tombstone.
    pub fn delete(&self, key: &[u8]) {
        let key_bytes = Bytes::copy_from_slice(key);
        let new_size = key.len() + ENTRY_OVERHEAD; // Tombstone has no value size

        // Get old size if key exists
        let old_size = self
            .entries
            .get(key)
            .map(|e| key.len() + e.value().size() + ENTRY_OVERHEAD)
            .unwrap_or(0);

        // Insert tombstone
        self.entries.insert(key_bytes, Value::Tombstone);

        // Update size
        if new_size > old_size {
            self.size_bytes
                .fetch_add(new_size - old_size, Ordering::Relaxed);
        } else {
            self.size_bytes
                .fetch_sub(old_size - new_size, Ordering::Relaxed);
        }
    }

    /// Check if flush is needed.
    #[inline]
    pub fn needs_flush(&self) -> bool {
        self.size_bytes.load(Ordering::Relaxed) >= self.max_size_bytes
    }

    /// Check if empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Get the max size threshold.
    #[inline]
    pub fn max_size(&self) -> usize {
        self.max_size_bytes
    }

    /// Iterate over all entries (for flushing to SST).
    pub fn iter(&self) -> impl Iterator<Item = (Bytes, Value)> + '_ {
        self.entries
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
    }
}
