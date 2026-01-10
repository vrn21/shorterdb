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
- **gRPC support** — Optional remote access via `shorterdb-grpc` crate

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
shorterDB = "0.2.0"
```

## Workspace Structure

This project is organized as a Cargo workspace:

```
shorterdb/
├── crates/
│   ├── shorterdb/          # Core database engine (minimal dependencies)
│   └── shorterdb-grpc/     # gRPC server (optional networking)
└── examples/               # Usage examples
```

### Building

```bash
# Build just the core engine (fast, minimal deps)
cargo build -p shorterdb

# Build the gRPC server
cargo build -p shorterdb-grpc

# Build everything
cargo build --workspace

# Run tests
cargo test --workspace
```

### Running the gRPC Server

```bash
# Using cargo
cargo run -p shorterdb-grpc

# Using Docker
docker build -t shorterdb-grpc .
docker run -p 50051:50051 shorterdb-grpc
```

## Examples

Check out the [`examples/`](examples/) directory:

- **[embedded](examples/embedded.rs)** — Basic embedded database usage
- **[repl](examples/repl.rs)** — Interactive REPL for testing

Run an example:

```bash
cargo run -p shorterdb --example embedded
cargo run -p shorterdb --example repl
```

## Documentation

[View the full API documentation on docs.rs](https://docs.rs/shorterdb)

Contributions are welcome! Please feel free to submit a Pull Request.
