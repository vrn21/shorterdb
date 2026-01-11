//! Benchmark entry point.
//!
//! Runs full CRUD benchmark suite on both databases.

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
use workload::run_full_benchmark;

fn main() -> Result<()> {
    println!("╔═══════════════════════════════════════════════════╗");
    println!("║   ShorterDB vs RocksDB Performance Benchmark     ║");
    println!("╚═══════════════════════════════════════════════════╝\n");

    let config = BenchmarkConfig::default();

    println!("Configuration:");
    println!("  Total KV pairs:  {}", config.num_pairs);
    println!("  Value size:      {} bytes", config.value_size);

    // Calculate CRUD operation counts
    let total = config.num_pairs;
    let num_reads = (total / 2).max(1000);
    let num_updates = (total / 10).max(100);
    let num_deletes = (total / 20).max(50);

    println!(
        "  CRUD operations: {} reads, {} updates, {} deletes\n",
        num_reads, num_updates, num_deletes
    );

    // Run ShorterDB benchmark
    let shorterdb_results = {
        let db_path = PathBuf::from("benchmark_data/shorterdb");
        match ShorterDBRunner::open(&db_path) {
            Ok(db) => match run_full_benchmark(&config, db) {
                Ok(results) => Some(results),
                Err(e) => {
                    eprintln!("❌ ShorterDB benchmark failed: {}", e);
                    None
                }
            },
            Err(e) => {
                eprintln!("❌ Failed to open ShorterDB: {}", e);
                None
            }
        }
    };

    // Run RocksDB benchmark
    let rocksdb_results = {
        let db_path = PathBuf::from("benchmark_data/rocksdb");
        match RocksDBRunner::open(&db_path) {
            Ok(db) => match run_full_benchmark(&config, db) {
                Ok(results) => Some(results),
                Err(e) => {
                    eprintln!("❌ RocksDB benchmark failed: {}", e);
                    None
                }
            },
            Err(e) => {
                eprintln!("❌ Failed to open RocksDB: {}", e);
                None
            }
        }
    };

    // Print results table
    println!("\n╔═══════════════════════════════════════════════════╗");
    println!("║                     RESULTS                       ║");
    println!("╚═══════════════════════════════════════════════════╝");

    if let (Some(sdb_results), Some(rdb_results)) = (&shorterdb_results, &rocksdb_results) {
        // Throughput table
        println!("\n┌─────────────────┬──────────────┬──────────────┐");
        println!("│ Workload        │ ShorterDB    │ RocksDB      │");
        println!("│                 │ (ops/sec)    │ (ops/sec)    │");
        println!("├─────────────────┼──────────────┼──────────────┤");

        for (sdb, rdb) in sdb_results.iter().zip(rdb_results.iter()) {
            println!(
                "│ {:<15} │ {:>12.0} │ {:>12.0} │",
                sdb.workload_name, sdb.ops_per_sec, rdb.ops_per_sec
            );
        }

        println!("└─────────────────┴──────────────┴──────────────┘");

        // Latency table (average time per operation)
        println!("\n┌─────────────────┬──────────────┬──────────────┐");
        println!("│ Workload        │ ShorterDB    │ RocksDB      │");
        println!("│                 │ (avg ms/op)  │ (avg ms/op)  │");
        println!("├─────────────────┼──────────────┼──────────────┤");

        for (sdb, rdb) in sdb_results.iter().zip(rdb_results.iter()) {
            println!(
                "│ {:<15} │ {:>12.3} │ {:>12.3} │",
                sdb.workload_name, sdb.avg_latency_ms, rdb.avg_latency_ms
            );
        }

        println!("└─────────────────┴──────────────┴──────────────┘");

        // Summary
        println!("\n📊 Summary:");
        for (sdb, rdb) in sdb_results.iter().zip(rdb_results.iter()) {
            let ratio = sdb.ops_per_sec / rdb.ops_per_sec;
            let (winner, speedup) = if ratio > 1.0 {
                ("ShorterDB", ratio)
            } else {
                ("RocksDB", 1.0 / ratio)
            };
            println!(
                "   {} - {} is {:.2}x faster",
                sdb.workload_name, winner, speedup
            );
        }
    } else if let Some(results) = shorterdb_results {
        println!("\nShorterDB Results:");
        for result in results {
            result.print();
        }
    } else if let Some(results) = rocksdb_results {
        println!("\nRocksDB Results:");
        for result in results {
            result.print();
        }
    }

    println!("\n✓ Benchmark complete");
    Ok(())
}
