// use bytes::Bytes;
// use flate2::{write::GzEncoder, Compression};
// use rand::{thread_rng, Rng};
// use std::collections::BTreeMap;
// use std::fs::{self, OpenOptions};
// use std::io::{self, BufRead, Read, Write};
// use std::path::{Path, PathBuf};
// use std::sync::{Arc, Mutex};

// const MAX_SST_SIZE: usize = 256 * 1024; // 256 KB

// pub struct SST {
//     path: PathBuf,
//     entries: BTreeMap<Bytes, Bytes>, // In-memory representation for fast access
//     current_size: usize,
// }

// impl SST {
//     pub fn new<P: AsRef<Path>>(directory: P) -> io::Result<Self> {
//         let path = directory.as_ref().to_path_buf();
//         fs::create_dir_all(&path)?; // Ensure directory exists

//         Ok(SST {
//             path,
//             entries: BTreeMap::new(),
//             current_size: 0,
//         })
//     }

//     pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
//         let key_bytes = Bytes::copy_from_slice(key);
//         let value_bytes = Bytes::copy_from_slice(value);

//         // Insert or update the entry
//         self.entries.insert(key_bytes.clone(), value_bytes.clone());
//         self.current_size += key.len() + value.len();

//         // Check if we need to write to disk
//         if self.current_size >= MAX_SST_SIZE {
//             self.write_to_disk()?;
//             self.entries.clear(); // Clear in-memory entries after writing
//             self.current_size = 0; // Reset size counter
//         }

//         Ok(())
//     }

//     pub fn get(&self, key: &[u8]) -> Option<Bytes> {
//         self.entries.get(key).cloned()
//     }

//     fn write_to_disk(&mut self) -> io::Result<()> {
//         let filename = format!("{}.sst", generate_random_filename());
//         let file_path = self.path.join(filename);

//         let file = OpenOptions::new()
//             .create(true)
//             .write(true)
//             .truncate(true)
//             .open(file_path)?;

//         let mut encoder = GzEncoder::new(file, Compression::default());

//         for (key, value) in &self.entries {
//             encoder.write_all(key)?;
//             encoder.write_all(b"\n")?;
//             encoder.write_all(value)?;
//             encoder.write_all(b"\n")?;
//         }

//         encoder.finish()?;
//         Ok(())
//     }

//     pub fn load_from_disk(&mut self) -> io::Result<()> {
//         for entry in fs::read_dir(&self.path)? {
//             let entry = entry?;
//             if entry.path().extension().and_then(|s| s.to_str()) == Some("sst") {
//                 let file = fs::File::open(entry.path())?;
//                 let reader = io::BufReader::new(file);

//                 let mut lines = reader.lines();
//                 while let Some(key_line) = lines.next() {
//                     if let Ok(key) = key_line {
//                         if let Some(value_line) = lines.next() {
//                             if let Ok(value) = value_line {
//                                 self.set(key.as_bytes(), value.as_bytes())?;
//                             }
//                         }
//                     }
//                 }
//             }
//         }
//         Ok(())
//     }
// }

// fn generate_random_filename() -> String {
//     const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
//     const FILENAME_LENGTH: usize = 10;

//     let mut rng = thread_rng();
//     (0..FILENAME_LENGTH)
//         .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
//         .collect()
// }

use bytes::Bytes;
use flate2::read::GzDecoder;
use flate2::{write::GzEncoder, Compression};
use rand::{thread_rng, Rng};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::{self, BufRead, Read, Write};
use std::path::{Path, PathBuf};

const MAX_SST_SIZE: usize = 256 * 1024; // 256 KB

pub struct SST {
    path: PathBuf,
    entries: BTreeMap<Bytes, Bytes>, // In-memory representation for fast access
    current_size: usize,
}

impl SST {
    pub fn new<P: AsRef<Path>>(directory: P) -> io::Result<Self> {
        let path = directory.as_ref().to_path_buf();
        fs::create_dir_all(&path)?; // Ensure directory exists

        Ok(SST {
            path,
            entries: BTreeMap::new(),
            current_size: 0,
        })
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        let key_bytes = Bytes::copy_from_slice(key);
        let value_bytes = Bytes::copy_from_slice(value);

        // Insert or update the entry
        self.entries.insert(key_bytes.clone(), value_bytes.clone());
        self.current_size += key.len() + value.len();

        // Check if we need to write to disk
        if self.current_size >= MAX_SST_SIZE {
            self.write_to_disk()?;
            self.entries.clear(); // Clear in-memory entries after writing
            self.current_size = 0; // Reset size counter
        }

        Ok(())
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        // First check in-memory entries
        if let Some(value) = self.entries.get(key).cloned() {
            return Some(value);
        }

        // If not found in memory, check on disk
        for entry in fs::read_dir(&self.path).unwrap() {
            let entry = entry.unwrap();
            if entry.path().extension().and_then(|s| s.to_str()) == Some("sst") {
                if let Some(value) = self.get_from_file(entry.path(), key) {
                    return Some(value);
                }
            }
        }

        None // Return None if not found
    }

    fn get_from_file(&self, file_path: PathBuf, key: &[u8]) -> Option<Bytes> {
        let file = fs::File::open(file_path).ok()?;
        let mut decoder = GzDecoder::new(file);

        let reader = io::BufReader::new(decoder);

        let mut lines = reader.lines();

        while let Some(key_line) = lines.next() {
            if let Ok(key_str) = key_line {
                if let Some(value_line) = lines.next() {
                    if let Ok(value_str) = value_line {
                        if key_str.as_bytes() == key {
                            return Some(Bytes::from(value_str));
                        }
                    }
                }
            }
        }

        None // Return None if not found
    }

    fn write_to_disk(&mut self) -> io::Result<()> {
        let filename = format!("{}.sst", generate_random_filename());
        let file_path = self.path.join(filename);

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(file_path)?;

        let mut encoder = GzEncoder::new(file, Compression::default());

        for (key, value) in &self.entries {
            encoder.write_all(key)?;
            encoder.write_all(b"\n")?;
            encoder.write_all(value)?;
            encoder.write_all(b"\n")?;
        }

        encoder.finish()?;
        Ok(())
    }

    pub fn load_from_disk(&mut self) -> io::Result<()> {
        for entry in fs::read_dir(&self.path)? {
            let entry = entry?;
            if entry.path().extension().and_then(|s| s.to_str()) == Some("sst") {
                let file = fs::File::open(entry.path())?;
                let reader = io::BufReader::new(file);

                let mut lines = reader.lines();
                while let Some(key_line) = lines.next() {
                    if let Ok(key) = key_line {
                        if let Some(value_line) = lines.next() {
                            if let Ok(value) = value_line {
                                self.set(key.as_bytes(), value.as_bytes())?;
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

fn generate_random_filename() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    const FILENAME_LENGTH: usize = 10;

    let mut rng = thread_rng();
    (0..FILENAME_LENGTH)
        .map(|_| CHARSET[rng.gen_range(0..CHARSET.len())] as char)
        .collect()
}
