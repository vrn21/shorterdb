use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

pub struct WALEntry {
    pub key: Bytes,
    pub value: Bytes,
}

pub struct WAL {
    path: PathBuf,
    file: File,
}

impl WAL {
    /// Creates a new WAL in a given directory.
    pub fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let path = dir.as_ref().join("wal.log");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(WAL { path, file })
    }

    /// Writes an entry to the WAL.
    pub fn write(&mut self, entry: &WALEntry) -> io::Result<()> {
        // Serialize the entry
        self.file.write_all(&entry.key.len().to_le_bytes())?; // Key length
        self.file.write_all(entry.key.as_ref())?; // Key
        self.file.write_all(&entry.value.len().to_le_bytes())?; // Value length
        self.file.write_all(entry.value.as_ref())?; // Value
        self.file.flush()?; // Ensure data is written to disk
        Ok(())
    }

    /// Reads all entries from the WAL.
    pub fn read_entries(&self) -> io::Result<Vec<WALEntry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        let mut buffer = vec![0; 8]; // Buffer for lengths

        while let Ok(_) = reader.read_exact(&mut buffer) {
            let key_len = usize::from_le_bytes(buffer[0..8].try_into().unwrap());
            let mut key = vec![0; key_len];
            reader.read_exact(&mut key)?;

            reader.read_exact(&mut buffer)?;
            let value_len = usize::from_le_bytes(buffer[0..8].try_into().unwrap());
            let mut value = vec![0; value_len];
            reader.read_exact(&mut value)?;

            entries.push(WALEntry {
                key: Bytes::from(key),
                value: Bytes::from(value),
            });
        }

        Ok(entries)
    }
}
