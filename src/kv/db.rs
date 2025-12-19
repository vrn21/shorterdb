use super::{
    memtable::Memtable,
    sst::SST,
    wal::{WALEntry, WAL},
};
use crate::errors::{Result, ShortDBErrors};
use bytes::Bytes;
use std::fs;
use std::path::Path;

pub struct ShorterDB {
    pub(crate) memtable: Memtable,
    pub(crate) wal: WAL,
    pub(crate) sst: SST,
    // pub(crate) data_dir: PathBuf,
}

impl ShorterDB {
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let data_dir = data_dir.as_ref().to_path_buf();
        fs::create_dir_all(&data_dir); // Ensure the data directory exists
        let wal = WAL::new(&data_dir).unwrap();
        let sst = SST::open(format!("{:?}", data_dir));
        Ok(Self {
            memtable: Memtable::new(),
            wal,
            sst,
            // data_dir,
        })
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.memtable.get(key) {
            Ok(None) => println!("data deleted"),
            Ok(Some(v)) => {
                return Ok(Some(v.to_vec()));
            }
            Err(ShortDBErrors::KeyNotFound) => println!("not found in mem"),
            Err(e) => println!("something problematic happend {}", e),
        }
        if let Some(value) = self.sst.get(key) {
            print!("checking in sst");
            return Ok(Some(value));
        }

        Err(ShortDBErrors::KeyNotFound)
    }

    pub fn set(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        let entry = WALEntry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(value),
        };
        self.wal.write(&entry);
        self.memtable.set(key, value)?;

        if let Err(err) = self.memtable.set(key, value) {
            match err {
                ShortDBErrors::FlushNeededFromMemTable => self.flush_memtable()?,
                _ => println!("some err happend"),
            }
        }
        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<()> {
        let tombstone_entry = WALEntry {
            key: Bytes::copy_from_slice(key),
            value: Bytes::copy_from_slice(b"tombstone"),
        };
        self.wal.write(&tombstone_entry);
        self.memtable.delete(key)?;
        if let Err(err) = self.memtable.delete(key) {
            match err {
                ShortDBErrors::FlushNeededFromMemTable => self.flush_memtable()?,
                _er => println!("some problem: {}", _er),
            }
        }

        Ok(())
    }

    fn flush_memtable(&mut self) -> Result<()> {
        self.sst.queue.push_back(self.memtable.clone());
        self.sst.set();
        self.memtable.clear();
        Ok(())
    }
}
