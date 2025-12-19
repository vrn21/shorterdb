use crate::errors::{Result, ShortDBErrors};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use std::sync::Arc;

#[derive(Clone)]
pub(crate) struct Memtable {
    pub(crate) memtable: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) size: u64,
}

impl Memtable {
    pub(crate) fn new() -> Self {
        Memtable {
            memtable: Arc::new(SkipMap::new()),
            size: 0,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        match self.memtable.get(key).map(|e| e.value().clone()) {
            Some(v) if v == Bytes::copy_from_slice(b"tombstone") => Ok(None),
            Some(v) => Ok(Some(v)),
            None => Err(ShortDBErrors::KeyNotFound),
        }
    }

    pub(crate) fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        self.memtable
            .insert(Bytes::copy_from_slice(&key), Bytes::copy_from_slice(value));
        self.size += 1;
        // dbg!(self.size);
        if self.memtable.get(key).is_some() {
            if self.size >= 256 {
                return Err(ShortDBErrors::FlushNeededFromMemTable);
            }
            Ok(())
        } else {
            Err(ShortDBErrors::ValueNotSet)
        }
    }
    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<()> {
        self.memtable.insert(
            Bytes::copy_from_slice(key),
            Bytes::copy_from_slice(b"tombstone"),
        );

        self.size += 1;

        if self.size >= 256 {
            return Err(ShortDBErrors::FlushNeededFromMemTable); // Indicate that a flush is needed
        }

        Ok(())
    }
    pub(crate) fn clear(&mut self) {
        self.memtable.clear();
        // *self.size.lock().unwrap() = 0;
        self.size = 0;
    }
}
