// use bytes::Bytes;
// use crossbeam_skiplist::{SkipList, SkipMap};
// use flate2::{read::GzDecoder, write::GzEncoder, Compression};
// use queues::Queue;
// use rand::{thread_rng, Rng};
// use std::collections::BTreeMap;
// use std::collections::VecDeque;
// use std::fs::{self, OpenOptions};
// use std::io::{self, BufRead, Read, Write};
// use std::path::{Path, PathBuf};

// const MAX_SST_SIZE: usize = 256 * 1024; // 256 KB

// pub struct SST {
//     pub(crate) path: PathBuf,
//     pub(crate) entries: BTreeMap<Bytes, Bytes>, // In-memory representation for fast access
//     pub(crate) index: BTreeMap<Bytes, usize>,   // Index to track key positions
//     pub(crate) current_size: usize,
//     pub(crate) queue: VecDeque<SkipMap<Bytes, Bytes>>,
// }

// impl SST {
//     pub fn new<P: AsRef<Path>>(directory: P) -> io::Result<Self> {
//         let path = directory.as_ref().to_path_buf();
//         fs::create_dir_all(&path)?; // Ensure directory exists

//         Ok(SST {
//             path,
//             entries: BTreeMap::new(),
//             index: BTreeMap::new(),
//             current_size: 0,
//             queue: VecDeque::new(),
//         })
//     }

//     pub fn set(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
//         let key_bytes = Bytes::copy_from_slice(key);
//         let value_bytes = Bytes::copy_from_slice(value);

//         // Insert or update the entry
//         self.entries.insert(key_bytes.clone(), value_bytes.clone());
//         self.index.insert(key_bytes.clone(), self.entries.len() - 1); // Update index
//         self.current_size += key.len() + value.len();

//         // Check if we need to write to disk
//         if self.current_size >= MAX_SST_SIZE {
//             self.write_to_disk()?;
//             self.entries.clear(); // Clear in-memory entries after writing
//             self.index.clear(); // Clear index after writing
//             self.current_size = 0; // Reset size counter
//         }

//         Ok(())
//     }

//     pub fn get(&self, key: &[u8]) -> Option<Bytes> {
//         // First check in-memory entries
//         if let Some(value) = self.entries.get(key).cloned() {
//             return Some(value);
//         }

//         // If not found in memory, check on disk using index
//         for entry in fs::read_dir(&self.path).unwrap() {
//             let entry = entry.unwrap();
//             if entry.path().extension().and_then(|s| s.to_str()) == Some("sst") {
//                 if let Some(value) = self.get_from_file(entry.path(), key) {
//                     return Some(value);
//                 }
//             }
//         }

//         None // Return None if not found
//     }

//     fn get_from_file(&self, file_path: PathBuf, key: &[u8]) -> Option<Bytes> {
//         let file = fs::File::open(file_path).ok()?;
//         let mut decoder = GzDecoder::new(file);

//         let reader = io::BufReader::new(decoder);

//         let mut lines = reader.lines();

//         while let Some(key_line) = lines.next() {
//             if let Ok(key_str) = key_line {
//                 if let Some(value_line) = lines.next() {
//                     if let Ok(value_str) = value_line {
//                         if key_str.as_bytes() == key {
//                             return Some(Bytes::from(value_str));
//                         }
//                     }
//                 }
//             }
//         }

//         None // Return None if not found
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

//     pub fn write_from_queue(&self) {
//         for data in self.queue.iter() {
//             // write! data somewhere efficently
//         }
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

use bincode;
use bloomfilter::Bloom;
use bytes::{Bytes, BytesMut};
use crossbeam_channel::{bounded, Receiver, Sender};
use memmap2::MmapMut;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const BLOOM_FILTER_SIZE: usize = 1000000; // 100M items
const BLOOM_FPR: f64 = 0.01; // 1% false positive rate
const WRITE_BATCH_SIZE: usize = 1000;
const INDEX_INTERVAL: usize = 1000000; // Create an index entry every 1000 entries

#[derive(Serialize, Deserialize, Clone)]
struct KeyValuePair {
    key: Vec<u8>,
    value: Vec<u8>,
    timestamp: u64,
}

#[derive(Clone)]
struct IndexEntry {
    key: Vec<u8>,
    position: u64,
}

pub struct SST {
    file: Arc<RwLock<File>>,
    mmap: Arc<RwLock<MmapMut>>,
    bloom_filter: Arc<RwLock<Bloom<Vec<u8>>>>,
    write_queue: (Sender<KeyValuePair>, Receiver<KeyValuePair>),
    index: Arc<RwLock<Vec<IndexEntry>>>,
}

impl SST {
    pub fn new(path: &Path) -> Self {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path);

        let file = match file {
            Ok(f) => f,
            Err(e) => {
                eprintln!("Error opening file: {:?}", e);
                eprintln!("Path: {:?}", path);
                eprintln!("Current working directory: {:?}", std::env::current_dir());
                panic!();
            }
        };

        let mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        let bloom_filter = Bloom::new_for_fp_rate(BLOOM_FILTER_SIZE, BLOOM_FPR);
        let (sender, receiver) = bounded(WRITE_BATCH_SIZE);

        let sst = SST {
            file: Arc::new(RwLock::new(file)),
            mmap: Arc::new(RwLock::new(mmap)),
            bloom_filter: Arc::new(RwLock::new(bloom_filter)),
            write_queue: (sender, receiver),
            index: Arc::new(RwLock::new(Vec::new())),
        };

        sst.start_write_thread();
        sst.load_index_and_bloom_filter();
        sst
    }

    fn start_write_thread(&self) {
        let receiver = self.write_queue.1.clone();
        let mmap = Arc::clone(&self.mmap);
        let bloom_filter = Arc::clone(&self.bloom_filter);
        let index = Arc::clone(&self.index);

        std::thread::spawn(move || {
            let mut batch = Vec::with_capacity(WRITE_BATCH_SIZE);
            loop {
                // dbg!("hi im in a loop");
                while let Ok(kv) = receiver.try_recv() {
                    batch.push(kv);
                    if batch.len() >= WRITE_BATCH_SIZE {
                        Self::write_batch(&mmap, &bloom_filter, &index, &batch);
                        batch.clear();
                    }
                }
                if !batch.is_empty() {
                    Self::write_batch(&mmap, &bloom_filter, &index, &batch);
                    batch.clear();
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
        });
    }

    fn load_index_and_bloom_filter(&self) {
        let mmap = self.mmap.read();
        let mut position = 0;
        let mut index_counter = 0;

        while position < mmap.len() {
            match bincode::deserialize::<KeyValuePair>(&mmap[position..]) {
                Ok(kv) => {
                    self.bloom_filter.write().set(&kv.key);

                    if index_counter % INDEX_INTERVAL == 0 {
                        self.index.write().push(IndexEntry {
                            key: kv.key.clone(),
                            position: position as u64,
                        });
                    }

                    position += bincode::serialized_size(&kv).unwrap() as usize;
                    index_counter += 1;
                }
                Err(_) => break, // Handle deserialization error
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        dbg!("really checking sst?");
        if !self.bloom_filter.read().check(&key.to_vec()) {
            dbg!("Bloom filter says key is not present");
            return None;
        }

        let mmap = self.mmap.read();
        let index = self.index.read();

        // Binary search in the index
        let search_result = index.binary_search_by(|entry| entry.key.as_slice().cmp(key));
        let start_position = match search_result {
            Ok(exact_match) => index[exact_match].position,
            Err(insertion_point) if insertion_point > 0 => index[insertion_point - 1].position,
            _ => {
                dbg!("Index search failed");
                return None;
            }
        };

        let mut position = start_position as usize;
        let i: usize = 0;
        dbg!(position);
        dbg!(mmap.len());
        while position <= mmap.len() {
            match bincode::deserialize::<KeyValuePair>(&mmap[position..]) {
                Ok(kv) => {
                    dbg!(kv.key.as_slice());
                    dbg!(key);
                    match kv.key.as_slice().cmp(key) {
                        Ordering::Equal => return Some(Bytes::from(kv.value)),
                        Ordering::Greater => break,
                        Ordering::Less => {}
                    }
                    position += bincode::serialized_size(&kv).unwrap() as usize;
                }
                Err(e) => {
                    println!("error:{}", e);
                    dbg!("though here then break");
                    break;
                } // Handle deserialization error
            }
        }
        dbg!("returning none");
        None
    }

    pub fn set(&self, key: &[u8], value: &[u8]) {
        let kv = KeyValuePair {
            key: key.to_vec(),
            value: value.to_vec(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };
        dbg!("data sent to write queue");
        dbg!(self.write_queue.0.send(kv).unwrap());
    }

    fn write_batch(
        mmap: &Arc<RwLock<MmapMut>>,
        bloom_filter: &Arc<RwLock<Bloom<Vec<u8>>>>,
        index: &Arc<RwLock<Vec<IndexEntry>>>,
        batch: &[KeyValuePair],
    ) {
        println!("Starting batch write of {} items", batch.len());

        // let mut file_guard = file.write();
        let mut mmap_guard = mmap.write();
        let mut bloom_filter_guard = bloom_filter.write();
        let mut index_guard = index.write();

        let current_position = mmap_guard.len();
        // let current_position = match file_guard.seek(SeekFrom::End(0)) {
        //     Ok(pos) => pos,
        //     Err(e) => {
        //         eprintln!("Error seeking to end of file: {:?}", e);
        //         return;
        //     }
        // };
        let mut buffer = Vec::new();

        for (i, kv) in batch.iter().enumerate() {
            let serialized = bincode::serialize(kv).unwrap();
            // if let Err(e) = file_guard.write_all(&serialized) {
            //     eprintln!("Error writing to file: {:?}", e);
            //     continue;
            // }
            buffer.extend_from_slice(&serialized);
            bloom_filter_guard.set(&kv.key);

            if i % INDEX_INTERVAL == 0 {
                index_guard.push(IndexEntry {
                    key: kv.key.clone(),
                    position: (current_position + buffer.len() - serialized.len()) as u64,
                });
            }
        }
        // if let Err(e) = file_guard.flush() {
        //     eprintln!("Error flushing file: {:?}", e);
        // }
        mmap_guard.clone_from_slice(&buffer);
        println!("Batch write completed successfully");

        // Ensure that changes are flushed to disk
        // mmap_guard.flush().unwrap();
    }
}
