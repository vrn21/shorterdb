# ShorterDB

A lightweight embedded key-value store for Rust.

[![Crates.io](https://img.shields.io/crates/v/shorterdb.svg)](https://crates.io/crates/shorterdb)
[![Documentation](https://docs.rs/shorterdb/badge.svg)](https://docs.rs/shorterdb)
[![License](https://img.shields.io/crates/l/shorterdb.svg)](LICENSE)

## Overview

ShorterDB is an embedded key-value database designed for simplicity and ease of use. It's built for learning, experimentation, and lightweight applications that need persistent storage without the overhead of a full database server.

## Quick Start

```rust
use shorterdb::ShorterDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = ShorterDB::new(Path::new("./my_db"))?;

    db.set(b"user:1", b"alice")?;
    db.set(b"user:2", b"bob")?;

    if let Some(value) = db.get(b"user:1")? {
        println!("Found: {}", String::from_utf8_lossy(&value));
    }

    db.delete(b"user:1")?;

    Ok(())
}
```

## Features

- **Zero configuration** — Just create a database and start using it
- **Embedded** — No separate server process, runs in your application
- **Persistent** — Data is durably stored with Write-Ahead Logging
- **Fast reads** — In-memory caching with automatic background flushing
- **Simple API** — Only `get`, `set`, and `delete` operations
- **gRPC support** — Optional remote access (see examples)

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
shorterdb = "0.1.0"
```

## Examples

Check out the [`examples/`](examples/) directory:

- **[embedded](examples/embedded)** — Basic usage
- **[grpc](examples/grpc)** — Remote access via gRPC
- **[repl_csv](examples/repl_csv)** — CSV import with REPL

Run an example:

```bash
cargo run --example embedded
```

## Documentation

[View the full API documentation on docs.rs](https://docs.rs/shorterdb)

Contributions are welcome! Please feel free to submit a Pull Request.
