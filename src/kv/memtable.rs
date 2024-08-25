use anyhow::Result;
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

    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.memtable.get(key).map(|e| e.value().clone())
    }

    pub fn set(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.memtable
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.memtable
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(b""));
        Ok(())
    }
}
