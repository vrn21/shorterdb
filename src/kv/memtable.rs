// use anyhow::Result;
use crate::errors::{Result, ShortDBErrors};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::{Arc, Mutex};
pub struct Memtable {
    pub memtable: Arc<SkipMap<Bytes, Bytes>>,
    pub size: u64,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            memtable: Arc::new(SkipMap::new()),
            size: 0,
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self.memtable.get(key).map(|e| e.value().clone()) {
            Some(v) if v == Bytes::copy_from_slice(b"tombstone") => Ok(None),
            Some(v) => Ok(Some(v)),
            None => Err(ShortDBErrors::KeyNotFound),
        }
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Insert the key-value pair into the memtable
        self.memtable
            .insert(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(value));
        // let mut size = *self.size.lock().unwrap();
        self.size += 1;
        dbg!(self.size);
        // Check if the insertion was successful
        if self.memtable.get(key).is_some() {
            if self.size >= 256 {
                return Err(ShortDBErrors::FlushNeededFromMemTable); // Indicate that a flush is needed
            }
            Ok(())
        } else {
            Err(ShortDBErrors::ValueNotSet) // Use a meaningful error
        }
    }
    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        //when we say we delete a key, we set its value to tombstone
        self.memtable.insert(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(b"tombstone"),
        );

        // let mut size = *self.size.lock().unwrap();
        self.size += 1;

        // Check if the insertion was successful
        if self.size >= 256 {
            return Err(ShortDBErrors::FlushNeededFromMemTable); // Indicate that a flush is needed
        }

        Ok(())
    }
    pub fn clear(&mut self) {
        self.memtable.clear();
        // *self.size.lock().unwrap() = 0;
        self.size = 0;
    }
}
