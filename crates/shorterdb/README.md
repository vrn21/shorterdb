# ShorterDB

A lightweight embedded key-value store for Rust, inspired by the Log-Structured Merge-Tree (LSM-Tree) architecture.

[![Crates.io](https://img.shields.io/crates/v/shorterdb.svg)](https://crates.io/crates/shorterdb)
[![Documentation](https://docs.rs/shorterdb/badge.svg)](https://docs.rs/shorterdb)
[![License](https://img.shields.io/crates/l/shorterdb.svg)](../../LICENSE)

## Overview

ShorterDB is an embedded key-value database that combines simplicity with proven database design patterns. Built for learning, experimentation, and lightweight applications that need persistent storage.

## Architecture

ShorterDB implements an LSM-Tree inspired architecture with three core components:

```
┌─────────────────────────────────────────────────────────────────┐
│                         ShorterDB                               │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                     MEMTABLE                             │   │
│  │              (In-Memory SkipList)                        │   │
│  │     All writes go here first for fast performance        │   │
│  └────────────────────────┬────────────────────────────────┘   │
│                           │                                     │
│         ┌─────────────────┼─────────────────┐                   │
│         ▼                 │                 ▼                   │
│  ┌─────────────┐          │          ┌─────────────┐           │
│  │    WAL      │          │          │   FLUSHER   │           │
│  │  (Append-   │          │          │  (Background│           │
│  │   Only Log) │          │          │   Thread)   │           │
│  └─────────────┘          │          └──────┬──────┘           │
│                           │                 │                   │
│                           │                 ▼                   │
│                           │     ┌───────────────────┐           │
│                           │     │       SST         │           │
│                           │     │  (Sorted String   │           │
│                           │     │   Tables on Disk) │           │
│                           │     └───────────────────┘           │
└─────────────────────────────────────────────────────────────────┘
```

---

## Core Components

### Memtable

The **Memtable** is an in-memory sorted data structure that serves as the write buffer for all incoming operations.

| Property | Implementation |
|----------|----------------|
| **Data Structure** | Lock-free SkipList (`crossbeam-skiplist`) |
| **Ordering** | Keys are always sorted in byte order |
| **Concurrency** | Thread-safe reads without locks |
| **Size Tracking** | Approximate byte-based tracking with atomic counters |
| **Threshold** | Configurable (default 4MB), triggers flush when exceeded |

**Key Features:**
- All writes (set/delete) are first inserted into the memtable
- Deletes are represented as **tombstones** (markers indicating deletion)
- Provides the fastest read path since data is in memory
- When full, becomes **immutable** and is flushed to SST in the background

---

### Write-Ahead Log (WAL)

The **WAL** is an append-only log file that ensures durability by persisting every write before it's applied to the memtable.

| Property | Implementation |
|----------|----------------|
| **Format** | Binary: `[op(1B)][key_len(4B)][key][value_len(4B)][value]` |
| **Sync Mode** | `fsync` after every write for durability |
| **Recovery** | Replayed on startup to restore uncommitted state |
| **Rotation** | Truncated after successful SST flush |

**Key Features:**
- Guarantees no data loss on crash (writes survive before acknowledgment)
- Supports both `Set` and `Delete` operations
- Corruption-tolerant recovery (stops at first invalid entry)
- Sanity checks prevent OOM from corrupted length fields (100MB max)

---

### Sorted String Table (SST)

**SST files** are immutable, sorted key-value files stored on disk. They represent the long-term persistent storage layer.

| Property | Implementation |
|----------|----------------|
| **File Format** | `[Data Entries][Sparse Index][Footer(24B)]` |
| **Index** | Sparse index every 16 entries for O(log n) lookups |
| **Magic Number** | `SSTFILE\0` for corruption detection |
| **Tombstones** | Preserved until compaction |

**File Structure:**
```
┌────────────────────────────────┐
│         Data Entries           │  Sorted key-value pairs
│   [key_len][key][val_len]      │
│   [value][type_marker]         │
├────────────────────────────────┤
│         Sparse Index           │  Every 16th key → offset
│   [key_len][key][offset(8B)]   │
├────────────────────────────────┤
│           Footer               │  data_end(8B) + index_off(8B)
│                                │  + magic(8B) = 24 bytes
└────────────────────────────────┘
```

**Key Features:**
- Binary search on sparse index for efficient lookups
- Supports both data entries and tombstones
- L0 compaction when >4 files accumulate (merges and removes tombstones)
- Files are never modified, only created and deleted

---

## ACID Properties

### Atomicity

Each individual operation (`set`, `delete`) is atomic:

- **Single-key atomicity**: A write either fully completes or doesn't happen
- **WAL-first**: Operations are logged before being applied
- **No partial writes**: If crash occurs mid-operation, recovery ignores incomplete WAL entries

> **Limitation**: Multi-key transactions are not supported. Each operation is independent.

---

### Consistency

The database maintains a consistent view of data:

- **Ordered key-value store**: Keys are always sorted, enabling range scans
- **Tombstone semantics**: Deletes are properly propagated through all layers
- **Read consistency**: Queries check memtable → immutable memtable → SST in order
- **No phantom reads**: A key is either present with its latest value or absent

---

### Isolation

ShorterDB provides **snapshot-like** isolation for reads:

- **Read path**: Checks layers in order (newest to oldest), returns first match
- **Write path**: New writes don't affect in-progress reads
- **Background flush**: Uses immutable memtable copy, reads continue on original

> **Isolation Level**: Roughly equivalent to "Read Committed" — you see committed data, but concurrent writes may be visible.

---

### Durability

Writes are durable once `set()` or `delete()` returns:

| Guarantee | Mechanism |
|-----------|-----------|
| **Write durability** | WAL is `fsync`'d after every write |
| **Crash recovery** | WAL is replayed on startup to restore state |
| **SST durability** | Files are `sync_all`'d after creation |
| **WAL rotation** | Only cleared after SST is confirmed on disk |

**Recovery Flow:**
```
1. Open database
2. Load existing SST files
3. Replay WAL entries into fresh memtable
4. Resume normal operation
```

---

## Usage

```rust
use shorterdb::ShorterDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open database with default settings (4MB memtable)
    let mut db = ShorterDB::new(Path::new("./my_db"))?;

    // Set values (accepts &str or &[u8])
    db.set("user:1", "alice")?;
    db.set("user:2", "bob")?;

    // Get values
    if let Some(value) = db.get("user:1")? {
        println!("Found: {}", std::str::from_utf8(&value)?);
    }

    // Delete values
    let existed = db.delete("user:1")?;
    println!("Key existed: {}", existed);

    // Graceful shutdown (also called automatically on drop)
    db.close()?;

    Ok(())
}
```

## Examples

```bash
# Run the embedded example
cargo run --example embedded

# Run the interactive REPL
cargo run --example repl
```

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
