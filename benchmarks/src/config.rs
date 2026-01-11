//! Benchmark configuration with validation.
//!
//! This module provides a validated configuration struct for benchmarks.
//! All validation happens at construction time to prevent invalid states.

use anyhow::{anyhow, Context, Result};
use std::path::{Path, PathBuf};

/// Benchmark configuration with validated parameters.
///
/// # Examples
///
/// ```
/// let config = BenchmarkConfig::default();
/// assert_eq!(config.num_pairs, 1_000_000);
/// ```
#[derive(Debug, Clone)]
pub struct BenchmarkConfig {
    /// Number of key-value pairs to benchmark (must be > 0)
    pub num_pairs: usize,

    /// Size of each value in bytes (must be > 0)
    pub value_size: usize,

    /// Directory for generated test data
    pub data_dir: PathBuf,

    /// Directory for benchmark results
    pub results_dir: PathBuf,
}

impl Default for BenchmarkConfig {
    fn default() -> Self {
        Self {
            num_pairs: 1_000_000,
            value_size: 100,
            data_dir: PathBuf::from("benchmark_data"),
            results_dir: PathBuf::from("results"),
        }
    }
}

impl BenchmarkConfig {
    /// Create a new configuration with validation.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - `num_pairs` is 0
    /// - `value_size` is 0
    /// - Directories cannot be created
    pub fn new(
        num_pairs: usize,
        value_size: usize,
        data_dir: PathBuf,
        results_dir: PathBuf,
    ) -> Result<Self> {
        // Validation: fail fast on invalid inputs
        if num_pairs == 0 {
            return Err(anyhow!("num_pairs must be greater than 0"));
        }
        if value_size == 0 {
            return Err(anyhow!("value_size must be greater than 0"));
        }

        let config = Self {
            num_pairs,
            value_size,
            data_dir,
            results_dir,
        };

        // Create directories if they don't exist
        config.ensure_directories()?;

        Ok(config)
    }

    /// Ensure required directories exist.
    fn ensure_directories(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir).context("Failed to create data directory")?;
        std::fs::create_dir_all(&self.results_dir).context("Failed to create results directory")?;
        Ok(())
    }

    /// Get the data directory path.
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// Get the results directory path.
    pub fn results_dir(&self) -> &Path {
        &self.results_dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = BenchmarkConfig::default();
        assert_eq!(config.num_pairs, 1_000_000);
        assert_eq!(config.value_size, 100);
    }

    #[test]
    fn test_zero_pairs_fails() {
        let result = BenchmarkConfig::new(0, 100, PathBuf::from("data"), PathBuf::from("results"));
        assert!(result.is_err());
    }

    #[test]
    fn test_zero_value_size_fails() {
        let result = BenchmarkConfig::new(1000, 0, PathBuf::from("data"), PathBuf::from("results"));
        assert!(result.is_err());
    }
}
