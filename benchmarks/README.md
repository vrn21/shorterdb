# Benchmarks

Performance benchmarking suite for ShorterDB vs RocksDB.

## Phase 1: Foundation (Current)

Foundation components for benchmarking:
- ✅ Configuration system with validation
- ✅ Deterministic data generation
- ✅ Database trait abstraction
- ⏳ Database implementations (Phase 2)
- ⏳ Workload execution (Phase 3)
- ⏳ Metrics and reporting (Phase 4)

## Quick Start

```bash
# Run Phase 1 validation
cargo run -p benchmarks

# To customize config, edit the values in src/main.rs
```

## Testing

```bash
# Run unit tests
cargo test -p benchmarks

# Check compilation
cargo check -p benchmarks

# Run with debug output
RUST_LOG=debug cargo run -p benchmarks
```

## Configuration

Default configuration in `src/main.rs`:
- **num_pairs**: 1,000,000
- **value_size**: 100 bytes
- **data_dir**: `./benchmark_data`
- **results_dir**: `./results`

To customize, edit the config in `main.rs`.

## Development

This is a workspace member. See main repo README for workspace setup.

### Code Organization

- `config.rs`: Configuration with validation
- `data.rs`: Deterministic data generation
- `db.rs`: Database trait (implementations in Phase 2)
- `main.rs`: CLI entry point

### Design Principles

- Simple and robust over clever
- Fail fast with clear errors
- No premature optimization
- Idiomatic Rust patterns
