//! Basic workload implementation for benchmarking.
//!
//! Phase 2: Simple sequential write workload with timing.
//! Future phases: Add read workloads, mixed workloads, etc.

use anyhow::Result;
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

        Self {
            database_name,
            workload_name,
            total_operations,
            duration,
            ops_per_sec,
        }
    }

    /// Print the result in a human-readable format.
    pub fn print(&self) {
        println!(
            "\n{} - {} Workload:",
            self.database_name, self.workload_name
        );
        println!("  Operations: {}", self.total_operations);
        println!("  Duration:   {:.2}s", self.duration.as_secs_f64());
        println!("  Throughput: {:.0} ops/sec", self.ops_per_sec);
    }
}

/// Run sequential write workload on a database.
///
/// Inserts all key-value pairs in sequential order (key_0000000 to key_NNNNNNN).
pub fn run_sequential_write<D: Database>(
    config: &BenchmarkConfig,
    mut db: D,
) -> Result<WorkloadResult> {
    let db_name = db.name().to_string();
    println!("\nRunning sequential write workload on {}...", db_name);

    let mut gen = DataGenerator::new(config.value_size, None);

    // Start timing
    let start = Instant::now();

    // Insert all pairs
    for i in 0..config.num_pairs {
        let (key, value) = gen.generate_pair(i);
        db.put(&key, &value)?;

        // Progress indication (every 100k operations)
        if i > 0 && i % 100_000 == 0 {
            let elapsed = start.elapsed().as_secs_f64();
            let current_rate = i as f64 / elapsed;
            println!(
                "  Progress: {}/{} ({:.0} ops/sec)",
                i, config.num_pairs, current_rate
            );
        }
    }

    // Flush to ensure all data is persisted
    db.flush()?;

    // Stop timing
    let duration = start.elapsed();

    // Clean shutdown
    db.close()?;

    Ok(WorkloadResult::new(
        db_name,
        "Sequential Write".to_string(),
        config.num_pairs,
        duration,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::BenchmarkConfig;
    use tempfile::TempDir;

    // Mock database for testing
    struct MockDB {
        write_count: usize,
    }

    impl Database for MockDB {
        fn open<P: AsRef<std::path::Path>>(_path: P) -> Result<Self> {
            Ok(Self { write_count: 0 })
        }

        fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<()> {
            self.write_count += 1;
            Ok(())
        }

        fn get(&self, _key: &[u8]) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }

        fn delete(&mut self, _key: &[u8]) -> Result<bool> {
            Ok(false)
        }

        fn flush(&mut self) -> Result<()> {
            Ok(())
        }

        fn close(self) -> Result<()> {
            Ok(())
        }

        fn name(&self) -> &'static str {
            "MockDB"
        }
    }

    #[test]
    fn test_sequential_write_workload() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let config = BenchmarkConfig::new(
            1000, // Small number for quick test
            100,
            temp_dir.path().join("data"),
            temp_dir.path().join("results"),
        )?;

        let db = MockDB::open(temp_dir.path())?;
        let result = run_sequential_write(&config, db)?;

        assert_eq!(result.database_name, "MockDB");
        assert_eq!(result.total_operations, 1000);
        assert!(result.duration.as_secs() < 10); // Should be fast
        assert!(result.ops_per_sec > 0.0);

        Ok(())
    }
}
