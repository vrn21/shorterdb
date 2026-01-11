# ShorterDB Benchmarks

A high-performance benchmarking suite comparing **ShorterDB** (an embedded LSM-Tree key-value store in Rust) against **RocksDB**.

## Features

- **Embedded Mode**: Runs both databases in embedded mode for fair comparison.
- **Deterministic Data**: Uses seeded RNG for reproducible benchmarks.
- **Comprehensive Workloads**:
  - **Sequential Write**: Populate with 1,000,000 key-value pairs
  - **Random Read**: 500,000 read operations (50% coverage)
  - **Random Update**: 100,000 update operations (10% coverage)
  - **Random Delete**: 50,000 delete operations (5% coverage)
- **Metrics**: Throughput (ops/sec) and total duration.

## Usage

To run the full benchmark suite:

```bash
cargo run --release -p benchmarks
```

> **Note**: Always use `--release` for accurate performance measurements.

### Customization

The default configuration uses 1M key-value pairs with 100-byte values. To customize the parameters, edit the configuration in `src/main.rs`:

```rust
let config = BenchmarkConfig {
    num_pairs: 10_000_000, // Increase dataset size
    value_size: 1024,      // Increase value size
    ..BenchmarkConfig::default()
};
```

## Requirements

- **RocksDB**: Requires `librocksdb` installed on your system.
  - macOS: `brew install rocksdb`
  - Linux: `sudo apt-get install librocksdb-dev`
