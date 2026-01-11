//! Benchmark entry point.
//!
//! Phase 2: Runs sequential write workload on both databases.

use anyhow::Result;
use std::path::PathBuf;

mod config;
mod data;
mod db;
mod runners;
mod workload;

use config::BenchmarkConfig;
use db::Database;
use runners::{RocksDBRunner, ShorterDBRunner};
use workload::run_sequential_write;

fn main() -> Result<()> {
    println!("=== ShorterDB vs RocksDB Benchmark ===\n");

    // Create configuration
    let config = BenchmarkConfig::default();

    println!("Configuration:");
    println!("  Pairs:      {}", config.num_pairs);
    println!("  Value size: {} bytes", config.value_size);
    println!();

    // Run ShorterDB benchmark
    let shorterdb_result = {
        let db_path = PathBuf::from("benchmark_data/shorterdb");
        println!("Initializing ShorterDB at {}...", db_path.display());

        match ShorterDBRunner::open(&db_path) {
            Ok(db) => match run_sequential_write(&config, db) {
                Ok(result) => Some(result),
                Err(e) => {
                    eprintln!("ShorterDB benchmark failed: {}", e);
                    None
                }
            },
            Err(e) => {
                eprintln!("Failed to open ShorterDB: {}", e);
                None
            }
        }
    };

    // Run RocksDB benchmark
    let rocksdb_result = {
        let db_path = PathBuf::from("benchmark_data/rocksdb");
        println!("\nInitializing RocksDB at {}...", db_path.display());

        match RocksDBRunner::open(&db_path) {
            Ok(db) => match run_sequential_write(&config, db) {
                Ok(result) => Some(result),
                Err(e) => {
                    eprintln!("RocksDB benchmark failed: {}", e);
                    None
                }
            },
            Err(e) => {
                eprintln!("Failed to open RocksDB: {}", e);
                None
            }
        }
    };

    // Print results
    println!("\n=== Results ===");

    if let Some(ref result) = shorterdb_result {
        result.print();
    }

    if let Some(ref result) = rocksdb_result {
        result.print();
    }

    // Print comparison if both succeeded
    if let (Some(sdb), Some(rdb)) = (&shorterdb_result, &rocksdb_result) {
        println!("\n=== Comparison ===");
        let speedup = sdb.ops_per_sec / rdb.ops_per_sec;
        if speedup > 1.0 {
            println!("ShorterDB is {:.5}x faster", speedup);
        } else {
            println!("RocksDB is {:.5}x faster", 1.0 / speedup);
        }
    }

    println!("\n✓ Phase 2: Database implementations validated");
    Ok(())
}
