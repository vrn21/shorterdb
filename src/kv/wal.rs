use bytes::Bytes;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, Read, Write};
use std::path::{Path, PathBuf};

pub(crate) struct WALEntry {
    pub(crate) key: Bytes,
    pub(crate) value: Bytes,
}

pub(crate) struct WAL {
    path: PathBuf,
    file: File,
}

impl WAL {
    pub(crate) fn new<P: AsRef<Path>>(dir: P) -> io::Result<Self> {
        let path = dir.as_ref().join("wal.log");
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(WAL { path, file })
    }

    pub(crate) fn write(&mut self, entry: &WALEntry) -> io::Result<()> {
        self.file.write_all(&entry.key.len().to_le_bytes())?;
        self.file.write_all(entry.key.as_ref())?;
        self.file.write_all(&entry.value.len().to_le_bytes())?;
        self.file.write_all(entry.value.as_ref())?;
        self.file.flush()?;
        Ok(())
    }

    pub(crate) fn read_entries(&self) -> io::Result<Vec<WALEntry>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut entries = Vec::new();

        let mut buffer = vec![0; 8];

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
