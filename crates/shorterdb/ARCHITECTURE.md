# ShorterDB Architecture

ShorterDB implements a Log-Structured Merge-Tree (LSM-Tree) inspired architecture, designed for high write throughput and durability.

## High-Level Design

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
│                           │                                     │
└─────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. Memtable (In-Memory Buffer)

The **Memtable** serves as the primary write buffer.

- **Data Structure**: Lock-free SkipList (`crossbeam-skiplist`).
- **Function**: Accepts all `set` and `delete` operations.
- **Concurrency**: Lock-free reads; supports high concurrency.
- **Flush Trigger**: When the size exceeds the threshold (default 4MB), it becomes immutable and is scheduled for flushing to disk.

### 2. Write-Ahead Log (WAL)

The **WAL** ensures data durability/persistence.

- **Mechanism**: Every write to the Memtable is simultaneously appended to the WAL file.
- **Sync**: Writes are `fsync`'d for durability guarantees.
- **Recovery**: On startup, the WAL is replayed to reconstruct the Memtable state, ensuring no data loss in case of a crash.
- **Rotation**: The WAL is truncated/cleared only after the Memtable has been successfully flushed to an SST file.

### 3. Sorted String Table (SST)

**SSTs** form the persistent storage layer on disk.

- **Immutability**: Once written, SST files are never modified.
- **Format**:
    - **Data Block**: Sorted key-value pairs.
    - **Sparse Index**: Facilitates efficient disk reads (binary search).
    - **Footer**: Metadata and magic numbers for integrity.
- **Levels**: Organized into levels (currently simplistic L0) to manage compaction.

## ACID Properties

ShorterDB provides specific ACID guarantees suitable for a key-value store:

-   **Atomicity**: Single-key operations (`set`, `delete`) are atomic. They either fully succeed or fail.
-   **Consistency**: The database maintains internal consistency. Deletes (tombstones) are correctly propagated, and ordering is maintained.
-   **Isolation**: Snapshot-like isolation for reads. Readers perceive the state at the start of their operation and are not blocked by writers.
-   **Durability**: Strong durability. Writes are acknowledged only after being persisted to the WAL.

## Flush and Compaction

-   **Flush**: Background thread handles the conversion of immutable Memtables to SST files to avoid blocking writes.
-   **Compaction**: Basic compaction (merging multiple SST files) prevents potential "read amplification" (reading too many files) and reclaims space taken by obsolete or deleted records.
