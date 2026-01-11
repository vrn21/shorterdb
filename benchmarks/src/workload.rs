//! Workload implementations for benchmarking.
//!
//! Provides sequential write and CRUD benchmarks on populated databases.

use anyhow::Result;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::time::{Duration, Instant};

use crate::config::BenchmarkConfig;
use crate::data::DataGenerator;
use crate::db::Database;

/// Result from running a workload.
#[derive(Debug)]
pub struct WorkloadResult {
    pub database_name: String,
    pub workload_name: String,
    pub total_operations: usize,
    pub duration: Duration,
    pub ops_per_sec: f64,
    pub avg_latency_ms: f64,
}

impl WorkloadResult {
    fn new(
        database_name: String,
        workload_name: String,
        total_operations: usize,
        duration: Duration,
    ) -> Self {
        let secs = duration.as_secs_f64();
        let ops_per_sec = if secs > 0.0 {
            total_operations as f64 / secs
        } else {
            0.0
        };

        // Calculate average latency per operation in milliseconds
        let avg_latency_ms = if total_operations > 0 {
            (secs * 1000.0) / total_operations as f64
        } else {
            0.0
        };

        Self {
            database_name,
            workload_name,
            total_operations,
            duration,
            ops_per_sec,
            avg_latency_ms,
        }
    }

    /// Print the result in a compact format.
    pub fn print(&self) {
        println!(
            "  {} - {:.2}s - {:.0} ops/sec",
            self.workload_name,
            self.duration.as_secs_f64(),
            self.ops_per_sec
        );
    }
}

/// Run sequential write workload on a database.
///
/// Inserts all key-value pairs in sequential order (key_0000000 to key_NNNNNNN).
/// Database remains open for subsequent CRUD operations.
pub fn run_sequential_write<D: Database>(
    config: &BenchmarkConfig,
    db: &mut D,
) -> Result<WorkloadResult> {
    let db_name = db.name().to_string();
    println!(
        "\n[{}] Sequential Write ({} inserts)",
        db_name, config.num_pairs
    );

    let mut gen = DataGenerator::new(config.value_size, None);
    let start = Instant::now();

    for i in 0..config.num_pairs {
        let (key, value) = gen.generate_pair(i);
        db.put(&key, &value)?;

        if i > 0 && i % 100_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let rate = i as f64 / elapsed;
            println!(
                "  Progress: {}/{} ({:.0} ops/sec)",
                i, config.num_pairs, rate
            );
        }
    }

    db.flush()?;
    let duration = start.elapsed();

    Ok(WorkloadResult::new(
        db_name,
        "Sequential Write".to_string(),
        config.num_pairs,
        duration,
    ))
}

/// Run random read workload on populated database.
///
/// Performs random reads of existing keys.
pub fn run_random_read<D: Database>(
    config: &BenchmarkConfig,
    db: &D,
    num_reads: usize,
) -> Result<WorkloadResult> {
    let db_name = db.name().to_string();
    println!("  Random Read ({} reads)", num_reads);

    let mut rng = StdRng::seed_from_u64(42);
    let start = Instant::now();

    for _ in 0..num_reads {
        let index = rng.gen_range(0..config.num_pairs);
        let key = DataGenerator::generate_key(index);
        db.get(&key)?;
    }

    let duration = start.elapsed();

    Ok(WorkloadResult::new(
        db_name,
        "Random Read".to_string(),
        num_reads,
        duration,
    ))
}

/// Run random update workload on populated database.
///
/// Updates existing keys with new random values.
pub fn run_random_update<D: Database>(
    config: &BenchmarkConfig,
    db: &mut D,
    num_updates: usize,
) -> Result<WorkloadResult> {
    let db_name = db.name().to_string();
    println!("  Random Update ({} updates)", num_updates);

    let mut rng = StdRng::seed_from_u64(43);
    let mut gen = DataGenerator::new(config.value_size, Some(999));
    let start = Instant::now();

    for i in 0..num_updates {
        let index = rng.gen_range(0..config.num_pairs);
        let key = DataGenerator::generate_key(index);
        let (_, new_value) = gen.generate_pair(i);
        db.put(&key, &new_value)?;
    }

    db.flush()?;
    let duration = start.elapsed();

    Ok(WorkloadResult::new(
        db_name,
        "Random Update".to_string(),
        num_updates,
        duration,
    ))
}

/// Run random delete workload on populated database.
///
/// Deletes random keys from the database.
pub fn run_random_delete<D: Database>(
    config: &BenchmarkConfig,
    db: &mut D,
    num_deletes: usize,
) -> Result<WorkloadResult> {
    let db_name = db.name().to_string();
    println!("  Random Delete ({} deletes)", num_deletes);

    let mut rng = StdRng::seed_from_u64(44);
    let start = Instant::now();

    for _ in 0..num_deletes {
        let index = rng.gen_range(0..config.num_pairs);
        let key = DataGenerator::generate_key(index);
        db.delete(&key)?;
    }

    db.flush()?;
    let duration = start.elapsed();

    Ok(WorkloadResult::new(
        db_name,
        "Random Delete".to_string(),
        num_deletes,
        duration,
    ))
}

/// Run full CRUD benchmark suite.
///
/// 1. Sequential write (populate database)
/// 2. Random reads
/// 3. Random updates
/// 4. Random deletes
pub fn run_full_benchmark<D: Database>(
    config: &BenchmarkConfig,
    mut db: D,
) -> Result<Vec<WorkloadResult>> {
    let mut results = Vec::new();

    // 1. Sequential write (populate database)
    let write_result = run_sequential_write(config, &mut db)?;
    results.push(write_result);

    // 2. CRUD operations on populated database
    println!("\nCRUD Benchmarks:");

    // Calculate operation counts (use smaller numbers for small datasets)
    let total = config.num_pairs;
    let num_reads = (total / 2).max(1000); // 50% of data or at least 1K
    let num_updates = (total / 10).max(100); // 10% of data or at least 100
    let num_deletes = (total / 20).max(50); // 5% of data or at least 50

    // Read: random reads
    let read_result = run_random_read(config, &db, num_reads)?;
    results.push(read_result);

    // Update: random updates
    let update_result = run_random_update(config, &mut db, num_updates)?;
    results.push(update_result);

    // Delete: random deletes
    let delete_result = run_random_delete(config, &mut db, num_deletes)?;
    results.push(delete_result);

    // Clean shutdown
    db.close()?;

    Ok(results)
}
