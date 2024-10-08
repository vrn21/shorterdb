use super::{
    memtable::Memtable,
    sst::SST,
    wal::{WALEntry, WAL},
};
use crate::errors::{Result, ShortDBErrors};
use bytes::Bytes;
use std::fs;
use std::path::{Path, PathBuf};

pub struct ShorterDB {
    memtable: Memtable,
    wal: WAL,
    sst: SST,
    data_dir: PathBuf,
}

impl ShorterDB {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir)?; // Ensure the data directory exists

        let wal = WAL::new(&data_dir).unwrap();
        let sst = SST::new(&data_dir.join("./data.sst"));
        Ok(Self {
            memtable: Memtable::new(),
            wal,
            sst,
            data_dir,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        // First check in Memtable
        // if let Some(value) = self.memtable.get(key) {
        //     return Ok(Some(value));
        // }
        //

        match self.memtable.get(key) {
            Ok(None) => println!("data deleted"),
            Ok(Some(v)) => {
                return Ok(Some(v));
            }
            Err(ShortDBErrors::KeyNotFound) => println!("not found in mem"),
            Err(e) => println!("something problematic happend {}", e),
        }
        dbg!("checking in sst");
        // If not found in Memtable, check SST
        if let Some(value) = self.sst.get(key) {
            return Ok(Some(value));
        }

        Err(ShortDBErrors::KeyNotFound) // Return None if not found
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        // Create a new WALEntry
        let entry = WALEntry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };

        // Write to the WAL
        self.wal.write(&entry)?;

        // Insert into Memtable
        self.memtable.set(key, value)?;

        // Check if we need to flush Memtable to SST
        if let Err(err) = self.memtable.set(key, value) {
            match err {
                ShortDBErrors::FlushNeededFromMemTable => self.flush_memtable()?,
                _ => println!("{}", err),
            }
        }

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        // Create a tombstone entry
        let tombstone_entry = WALEntry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(b"tombstone"),
        };

        // Write tombstone to WAL
        self.wal.write(&tombstone_entry)?;

        // Delete from Memtable
        self.memtable.delete(key)?;

        // Check if we need to flush Memtable to SST
        if let Err(err) = self.memtable.delete(key) {
            match err {
                ShortDBErrors::FlushNeededFromMemTable => self.flush_memtable()?,
                _ => println!("{:?}", err),
            }
        }

        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        // Flush entries from Memtable to SST
        // for entry in self.memtable.memtable.iter() {
        //     self.sst.set(entry.key().as_ref(), entry.value().as_ref())?;
        // }

        // println!("Memtable: Flushing {} entries to SST", self.memtable.len());
        // for entry in self.memtable.memtable.iter() {
        //     self.sst.set(entry.key().as_ref(), entry.value().as_ref());
        // }
        self.memtable.clear();
        // self.sst.flush();
        // self.sst.queue.push_back(self.memtable.memtable);

        Ok(())
    }
}
