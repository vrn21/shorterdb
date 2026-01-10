# ShorterDB

A high-performance, embedded key-value store for Rust, built with a Log-Structured Merge-Tree (LSM-Tree) architecture.

[![Crates.io](https://img.shields.io/crates/v/shorterdb.svg)](https://crates.io/crates/shorterdb)
[![Documentation](https://docs.rs/shorterdb/badge.svg)](https://docs.rs/shorterdb)
[![License](https://img.shields.io/crates/l/shorterdb.svg)](https://github.com/Hrefto/shorterdb/blob/main/LICENSE)

## Features

- **Embedded**: Runs directly in your application process (no external server required).
- **Persistent**: Data is durably stored using a Write-Ahead Log (WAL) and SSTables.
- **Fast**: In-memory writers using lock-free SkipLists.
- **Simple API**: Minimalistic `get`, `set`, and `delete` interface.
- **Thread-Safe**: Designed for concurrent access.

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
shorterDB = "0.2.0"
```

## Quick Start

```rust
use shorterdb::ShorterDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open the database (creates directory if missing)
    let mut db = ShorterDB::new(Path::new("/tmp/my_db"))?;

    // Store a key-value pair
    db.set(b"username", b"ferris")?;

    // Retrieve the value
    if let Some(value) = db.get(b"username")? {
        println!("Found user: {}", String::from_utf8_lossy(&value));
    }

    // Delete the key
    db.delete(b"username")?;

    Ok(())
}
```

## Architecture

ShorterDB uses a classic LSM-Tree design with a Memtable, Write-Ahead Log, and background Flusher.

For deep technical details on the internal design, file formats, and ACID guarantees, see [ARCHITECTURE.md](ARCHITECTURE.md).

## Examples

The project workspace includes examples you can run locally:

```bash
# Basic embedded usage
cargo run -p examples --bin embedded

# Interactive REPL
cargo run -p examples --bin repl
```

## License

Licensed under either of [Apache License, Version 2.0](https://github.com/Hrefto/shorterdb/blob/main/LICENSE) or [MIT license](https://github.com/Hrefto/shorterdb/blob/main/LICENSE) at your option.
