//! Deterministic test data generation.
//!
//! Generates reproducible key-value pairs for benchmarking using a seeded RNG.
//! Keys follow the format: `key_{:07d}` (e.g., "key_0000001")
//! Values are random bytes of configurable size.

use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

/// Default RNG seed for reproducible benchmarks.
pub const DEFAULT_SEED: u64 = 42;

/// Key-value pair generator with deterministic output.
///
/// # Examples
///
/// ```
/// let mut gen = DataGenerator::new(100, Some(42));
/// let (key, value) = gen.generate_pair(0);
/// assert_eq!(key, b"key_0000000");
/// assert_eq!(value.len(), 100);
/// ```
pub struct DataGenerator {
    value_size: usize,
    rng: StdRng,
}

impl DataGenerator {
    /// Create a new data generator.
    ///
    /// # Arguments
    ///
    /// * `value_size` - Size of each value in bytes
    /// * `seed` - Optional RNG seed (uses DEFAULT_SEED if None)
    pub fn new(value_size: usize, seed: Option<u64>) -> Self {
        let seed = seed.unwrap_or(DEFAULT_SEED);
        Self {
            value_size,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Generate a key-value pair for the given index.
    ///
    /// Keys are formatted as: `key_{index:07d}`
    /// Values are random bytes of configured size.
    ///
    /// # Examples
    ///
    /// ```
    /// let mut gen = DataGenerator::new(10, Some(42));
    /// let (key, value) = gen.generate_pair(123);
    /// assert_eq!(key, b"key_0000123");
    /// assert_eq!(value.len(), 10);
    /// ```
    pub fn generate_pair(&mut self, index: usize) -> (Vec<u8>, Vec<u8>) {
        // Key: deterministic format
        let key = format!("key_{:07}", index).into_bytes();

        // Value: random bytes (deterministic based on seed)
        let value = self.generate_value();

        (key, value)
    }

    /// Generate a random value of configured size.
    fn generate_value(&mut self) -> Vec<u8> {
        let mut value = vec![0u8; self.value_size];
        self.rng.fill(&mut value[..]);
        value
    }

    /// Generate a key for a given index without generating the value.
    ///
    /// Useful for read operations where you only need the key.
    pub fn generate_key(index: usize) -> Vec<u8> {
        format!("key_{:07}", index).into_bytes()
    }
}

/// Iterator adapter for generating pairs in sequence.
///
/// # Examples
///
/// ```
/// let pairs: Vec<_> = DataIterator::new(10, 100, Some(42))
///     .take(5)
///     .collect();
/// assert_eq!(pairs.len(), 5);
/// ```
pub struct DataIterator {
    generator: DataGenerator,
    current: usize,
    total: usize,
}

impl DataIterator {
    /// Create a new iterator over generated data.
    ///
    /// # Arguments
    ///
    /// * `num_pairs` - Total number of pairs to generate
    /// * `value_size` - Size of each value in bytes
    /// * `seed` - Optional RNG seed
    pub fn new(num_pairs: usize, value_size: usize, seed: Option<u64>) -> Self {
        Self {
            generator: DataGenerator::new(value_size, seed),
            current: 0,
            total: num_pairs,
        }
    }
}

impl Iterator for DataIterator {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.total {
            return None;
        }

        let pair = self.generator.generate_pair(self.current);
        self.current += 1;
        Some(pair)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.total - self.current;
        (remaining, Some(remaining))
    }
}

impl ExactSizeIterator for DataIterator {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_format() {
        let mut gen = DataGenerator::new(10, Some(42));
        let (key, _) = gen.generate_pair(0);
        assert_eq!(key, b"key_0000000");

        let (key, _) = gen.generate_pair(999_999);
        assert_eq!(key, b"key_0999999");
    }

    #[test]
    fn test_value_size() {
        let mut gen = DataGenerator::new(100, Some(42));
        let (_, value) = gen.generate_pair(0);
        assert_eq!(value.len(), 100);
    }

    #[test]
    fn test_deterministic_output() {
        let mut gen1 = DataGenerator::new(50, Some(42));
        let mut gen2 = DataGenerator::new(50, Some(42));

        for i in 0..100 {
            let (k1, v1) = gen1.generate_pair(i);
            let (k2, v2) = gen2.generate_pair(i);
            assert_eq!(k1, k2);
            assert_eq!(v1, v2);
        }
    }

    #[test]
    fn test_different_seeds_produce_different_values() {
        let mut gen1 = DataGenerator::new(50, Some(42));
        let mut gen2 = DataGenerator::new(50, Some(100));

        let (k1, v1) = gen1.generate_pair(0);
        let (k2, v2) = gen2.generate_pair(0);

        assert_eq!(k1, k2); // Keys are the same
        assert_ne!(v1, v2); // Values are different
    }

    #[test]
    fn test_iterator() {
        let iter = DataIterator::new(10, 50, Some(42));
        let pairs: Vec<_> = iter.collect();
        assert_eq!(pairs.len(), 10);
    }

    #[test]
    fn test_iterator_size_hint() {
        let iter = DataIterator::new(100, 50, Some(42));
        let (lower, upper) = iter.size_hint();
        assert_eq!(lower, 100);
        assert_eq!(upper, Some(100));
    }
}
