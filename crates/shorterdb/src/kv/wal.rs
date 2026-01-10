use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use log::debug;

use crate::errors::{Result, ShortDBErrors};

/// WAL operation type
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WalOp {
    Set = 1,
    Delete = 2,
}

impl WalOp {
    fn from_byte(b: u8) -> Option<Self> {
        match b {
            1 => Some(WalOp::Set),
            2 => Some(WalOp::Delete),
            _ => None,
        }
    }
}

/// A WAL entry
#[derive(Debug, Clone)]
pub struct WalEntry {
    pub op: WalOp,
    pub key: Vec<u8>,
    pub value: Vec<u8>, // Empty for Delete
}

impl WalEntry {
    /// Create a Set entry
    pub fn set(key: &[u8], value: &[u8]) -> Self {
        Self {
            op: WalOp::Set,
            key: key.to_vec(),
            value: value.to_vec(),
        }
    }

    /// Create a Delete entry
    pub fn delete(key: &[u8]) -> Self {
        Self {
            op: WalOp::Delete,
            key: key.to_vec(),
            value: Vec::new(),
        }
    }
}

/// Write-Ahead Log for durability
pub struct Wal {
    path: PathBuf,
    writer: BufWriter<File>,
}

impl Wal {
    /// Open or create WAL file.
    pub fn open<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let path = dir.as_ref().join("wal.log");
        debug!("Opening WAL at {:?}", path);

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        Ok(Self {
            path,
            writer: BufWriter::new(file),
        })
    }

    /// Write a single entry with immediate sync for durability.
    pub fn write(&mut self, entry: &WalEntry) -> Result<()> {
        self.write_entry_no_sync(entry)?;
        self.sync()?;
        Ok(())
    }

    /// Sync WAL to disk.
    pub fn sync(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.get_ref().sync_data()?;
        Ok(())
    }

    /// Clear WAL after successful SST flush (rotation).
    ///
    /// This truncates the file to zero. Only call this after the memtable
    /// has been successfully flushed to SST.
    pub fn rotate(&mut self) -> Result<()> {
        debug!("Rotating WAL");

        // Ensure current data is synced before truncating
        self.writer.flush()?;
        self.writer.get_ref().sync_all()?;

        // Truncate file to zero
        let file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&self.path)?;
        file.sync_all()?;
        drop(file);

        // Reopen with append mode for future writes
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;

        self.writer = BufWriter::new(file);
        Ok(())
    }

    /// Read all valid entries from the WAL for recovery.
    ///
    /// Stops at EOF or first corrupted/partial entry (crash recovery safe).
    pub fn read_entries(&self) -> Result<Vec<WalEntry>> {
        let file = match File::open(&self.path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(e) => return Err(e.into()),
        };

        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        loop {
            match Self::read_one_entry(&mut reader) {
                Ok(Some(entry)) => entries.push(entry),
                Ok(None) => break, // Clean EOF
                Err(_) => break,   // Partial write or corruption - stop here
            }
        }

        debug!("Read {} entries from WAL", entries.len());
        Ok(entries)
    }

    /// Write an entry without syncing (for batching).
    fn write_entry_no_sync(&mut self, entry: &WalEntry) -> Result<()> {
        // Op type (1 byte)
        self.writer.write_all(&[entry.op as u8])?;

        // Key length (4 bytes) + key
        self.writer
            .write_all(&(entry.key.len() as u32).to_le_bytes())?;
        self.writer.write_all(&entry.key)?;

        // Value length (4 bytes) + value
        self.writer
            .write_all(&(entry.value.len() as u32).to_le_bytes())?;
        self.writer.write_all(&entry.value)?;

        Ok(())
    }

    /// Read a single entry from the reader.
    fn read_one_entry(reader: &mut BufReader<File>) -> Result<Option<WalEntry>> {
        // Read op type (1 byte)
        let mut op_buf = [0u8; 1];
        match reader.read_exact(&mut op_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }

        let op = WalOp::from_byte(op_buf[0]).ok_or(ShortDBErrors::WalCorruption)?;

        // Read key length (4 bytes)
        let mut len_buf = [0u8; 4];
        reader.read_exact(&mut len_buf)?;
        let key_len = u32::from_le_bytes(len_buf) as usize;

        // Sanity check to prevent OOM on corrupted data
        if key_len > 1024 * 1024 * 100 {
            // 100MB max key
            return Err(ShortDBErrors::WalCorruption);
        }

        // Read key
        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        // Read value length (4 bytes)
        reader.read_exact(&mut len_buf)?;
        let value_len = u32::from_le_bytes(len_buf) as usize;

        // Sanity check
        if value_len > 1024 * 1024 * 100 {
            // 100MB max value
            return Err(ShortDBErrors::WalCorruption);
        }

        // Read value
        let mut value = vec![0u8; value_len];
        reader.read_exact(&mut value)?;

        Ok(Some(WalEntry { op, key, value }))
    }
}
