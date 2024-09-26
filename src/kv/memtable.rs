// use anyhow::Result;
use crate::errors::{Result, ShortDBErrors};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

pub struct ShorterDB {
    pub memtable: Arc<SkipMap<Bytes, Bytes>>,
}

impl ShorterDB {
    pub fn new() -> Self {
        ShorterDB {
            memtable: Arc::new(SkipMap::new()),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self.memtable.get(key).map(|e| e.value().clone()) {
            Some(v) if v == Bytes::copy_from_slice(b"tombstone") => Ok(None),
            Some(v) => Ok(Some(v)),
            None => Err(ShortDBErrors::KeyNotFound),
        }
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        // Insert the key-value pair into the memtable
        self.memtable
            .insert(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(value));

        // Check if the insertion was successful
        if self.memtable.get(key).is_some() {
            Ok(())
        } else {
            Err(ShortDBErrors::ValueNotSet) // Use a meaningful error
        }
    }
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        //when we say we delete a key, we set its value to tombstone
        self.memtable.insert(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(b"tombstone"),
        );
        Ok(())
    }
}
