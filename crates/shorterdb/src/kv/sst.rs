//! Sorted String Table (SST) implementation for ShorterDB.
//!
//! File format: Data entries | Index entries | Footer (24 bytes)

use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use log::{debug, info, warn};

use crate::errors::{Result, ShortDBErrors};

use super::memtable::{Memtable, Value};

/// Magic number at end of every SST file: "SSTFILE\0"
const SST_MAGIC: u64 = 0x53_53_54_46_49_4C_45_00;

/// Entry type markers
const ENTRY_TYPE_VALUE: u8 = 0x01;
const ENTRY_TYPE_TOMBSTONE: u8 = 0x02;

/// Sparse index interval - one index entry every N data entries
const INDEX_INTERVAL: usize = 16;

/// Footer size: data_end(8) + index_offset(8) + magic(8)
const FOOTER_SIZE: u64 = 24;

/// Represents a value read from an SST file.
#[derive(Clone, Debug)]
pub enum SstValue {
    Data(Vec<u8>),
    Tombstone,
}

/// A single SST file on disk.
pub struct SstFile {
    path: PathBuf,
    /// Sparse index: (key, file offset)
    index: Vec<(Vec<u8>, u64)>,
    /// Where the data section ends
    data_end_offset: u64,
}

impl SstFile {
    /// Create a new SST file from memtable entries.
    pub fn create(path: &Path, memtable: &Memtable) -> Result<Self> {
        let entries: Vec<_> = memtable.iter().collect();
        debug!(
            "Creating SST file {:?} with {} entries",
            path,
            entries.len()
        );
        Self::write_entries(path, entries.iter().map(|(k, v)| (k.as_ref(), v)))
    }

    /// Create SST file from pre-sorted entries (used by compaction).
    pub fn create_from_entries(path: &Path, entries: &[(Vec<u8>, Value)]) -> Result<Self> {
        debug!(
            "Creating SST file {:?} from {} compacted entries",
            path,
            entries.len()
        );
        Self::write_entries(path, entries.iter().map(|(k, v)| (k.as_slice(), v)))
    }

    /// Write entries to an SST file (shared implementation).
    fn write_entries<'a>(
        path: &Path,
        entries: impl Iterator<Item = (&'a [u8], &'a Value)>,
    ) -> Result<Self> {
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);

        let mut index = Vec::new();
        let mut count = 0usize;
        let mut offset = 0u64;

        for (key, value) in entries {
            if count.is_multiple_of(INDEX_INTERVAL) {
                index.push((key.to_vec(), offset));
            }
            offset += Self::write_entry(&mut writer, key, value)? as u64;
            count += 1;
        }

        let data_end_offset = offset;

        // Write index
        for (key, off) in &index {
            writer.write_all(&(key.len() as u32).to_le_bytes())?;
            writer.write_all(key)?;
            writer.write_all(&off.to_le_bytes())?;
        }

        // Write footer
        writer.write_all(&data_end_offset.to_le_bytes())?;
        writer.write_all(&offset.to_le_bytes())?; // index_offset == data_end
        writer.write_all(&SST_MAGIC.to_le_bytes())?;

        writer.flush()?;
        writer.get_ref().sync_all()?;

        debug!(
            "SST file {:?} written: {} entries, {} index entries",
            path,
            count,
            index.len()
        );

        Ok(Self {
            path: path.to_path_buf(),
            index,
            data_end_offset,
        })
    }

    /// Write a single data entry, returns bytes written.
    fn write_entry<W: Write>(w: &mut W, key: &[u8], value: &Value) -> Result<usize> {
        let mut n = 0;

        w.write_all(&(key.len() as u32).to_le_bytes())?;
        n += 4;
        w.write_all(key)?;
        n += key.len();

        match value {
            Value::Data(data) => {
                w.write_all(&(data.len() as u32).to_le_bytes())?;
                n += 4;
                w.write_all(data)?;
                n += data.len();
                w.write_all(&[ENTRY_TYPE_VALUE])?;
                n += 1;
            }
            Value::Tombstone => {
                w.write_all(&0u32.to_le_bytes())?;
                n += 4;
                w.write_all(&[ENTRY_TYPE_TOMBSTONE])?;
                n += 1;
            }
        }
        Ok(n)
    }

    /// Open an existing SST file.
    pub fn open(path: &Path) -> Result<Self> {
        let file = File::open(path)?;
        let file_size = file.metadata()?.len();
        let mut reader = BufReader::new(file);

        // Read footer (last 24 bytes)
        reader.seek(SeekFrom::End(-(FOOTER_SIZE as i64)))?;
        let mut footer = [0u8; 24];
        reader.read_exact(&mut footer)?;

        let data_end_offset = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let index_offset = u64::from_le_bytes(footer[8..16].try_into().unwrap());
        let magic = u64::from_le_bytes(footer[16..24].try_into().unwrap());

        if magic != SST_MAGIC {
            return Err(ShortDBErrors::SstCorruption(format!(
                "invalid magic: {:x}",
                magic
            )));
        }

        // Read index
        reader.seek(SeekFrom::Start(index_offset))?;
        let index_size = file_size - FOOTER_SIZE - index_offset;
        let index = Self::read_index(&mut reader, index_size)?;

        debug!("Opened SST file {:?}: {} index entries", path, index.len());

        Ok(Self {
            path: path.to_path_buf(),
            index,
            data_end_offset,
        })
    }

    fn read_index(reader: &mut BufReader<File>, mut remaining: u64) -> Result<Vec<(Vec<u8>, u64)>> {
        let mut index = Vec::new();

        while remaining >= 12 {
            // minimum: 4 (len) + 0 (key) + 8 (offset)
            let mut len_buf = [0u8; 4];
            reader.read_exact(&mut len_buf)?;
            let key_len = u32::from_le_bytes(len_buf) as usize;
            remaining -= 4;

            if key_len > 1024 * 1024 || (key_len as u64) > remaining {
                return Err(ShortDBErrors::SstCorruption("key too large".into()));
            }

            let mut key = vec![0u8; key_len];
            reader.read_exact(&mut key)?;
            remaining -= key_len as u64;

            let mut off_buf = [0u8; 8];
            reader.read_exact(&mut off_buf)?;
            remaining -= 8;

            index.push((key, u64::from_le_bytes(off_buf)));
        }
        Ok(index)
    }

    /// Get a value by key.
    pub fn get(&self, key: &[u8]) -> Result<Option<SstValue>> {
        let start = self.find_start_offset(key);

        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        reader.seek(SeekFrom::Start(start))?;

        while reader.stream_position()? < self.data_end_offset {
            match Self::read_entry(&mut reader)? {
                Some((k, v)) => match k.as_slice().cmp(key) {
                    std::cmp::Ordering::Equal => return Ok(Some(v)),
                    std::cmp::Ordering::Greater => return Ok(None),
                    std::cmp::Ordering::Less => continue,
                },
                None => return Ok(None),
            }
        }
        Ok(None)
    }

    /// Find starting offset using binary search on sparse index.
    fn find_start_offset(&self, key: &[u8]) -> u64 {
        // Find first index entry > key, then use the one before it
        let i = self.index.partition_point(|(k, _)| k.as_slice() <= key);
        if i > 0 {
            self.index[i - 1].1
        } else {
            0
        }
    }

    /// Read a single entry from current position.
    fn read_entry(reader: &mut BufReader<File>) -> Result<Option<(Vec<u8>, SstValue)>> {
        let mut len_buf = [0u8; 4];
        match reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let key_len = u32::from_le_bytes(len_buf) as usize;

        let mut key = vec![0u8; key_len];
        reader.read_exact(&mut key)?;

        reader.read_exact(&mut len_buf)?;
        let val_len = u32::from_le_bytes(len_buf) as usize;

        let mut value = vec![0u8; val_len];
        reader.read_exact(&mut value)?;

        let mut type_buf = [0u8; 1];
        reader.read_exact(&mut type_buf)?;

        let v = match type_buf[0] {
            ENTRY_TYPE_VALUE => SstValue::Data(value),
            ENTRY_TYPE_TOMBSTONE => SstValue::Tombstone,
            t => return Err(ShortDBErrors::SstCorruption(format!("invalid type: {}", t))),
        };

        Ok(Some((key, v)))
    }
}

/// Manages multiple SST files organized in levels.
pub struct Sst {
    dir: PathBuf,
    levels: Vec<Vec<SstFile>>,
    next_file_id: u64,
}

impl Sst {
    /// Open SST manager, loading existing files.
    pub fn open(dir: &Path) -> Result<Self> {
        let sst_dir = dir.join("sst");
        fs::create_dir_all(&sst_dir)?;

        let mut sst = Self {
            dir: sst_dir,
            levels: vec![Vec::new()],
            next_file_id: 1,
        };
        sst.load_existing()?;

        info!(
            "SST manager opened: {} L0 files",
            sst.levels.first().map(|l| l.len()).unwrap_or(0)
        );

        Ok(sst)
    }

    fn load_existing(&mut self) -> Result<()> {
        let entries = match fs::read_dir(&self.dir) {
            Ok(e) => e,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e.into()),
        };

        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }

            let filename = match path.file_name().and_then(|s| s.to_str()) {
                Some(f) if f.ends_with(".sst") => f,
                _ => continue,
            };

            // Parse "L0_0001.sst"
            let name = filename.trim_end_matches(".sst");
            let parts: Vec<&str> = name.split('_').collect();
            if parts.len() != 2 {
                continue;
            }

            let level = parts[0].trim_start_matches('L').parse::<usize>().ok();
            let file_id = parts[1].parse::<u64>().ok();

            if let (Some(level), Some(file_id)) = (level, file_id) {
                self.next_file_id = self.next_file_id.max(file_id + 1);

                while self.levels.len() <= level {
                    self.levels.push(Vec::new());
                }

                match SstFile::open(&path) {
                    Ok(sst_file) => self.levels[level].push(sst_file),
                    Err(e) => warn!("Failed to open SST file {:?}: {}", path, e),
                }
            }
        }

        // Sort by path (which embeds file_id)
        for level in &mut self.levels {
            level.sort_by(|a, b| a.path.cmp(&b.path));
        }
        Ok(())
    }

    /// Get a value by key, checking all levels newest to oldest.
    pub fn get(&self, key: &[u8]) -> Result<Option<SstValue>> {
        for level in &self.levels {
            for sst in level.iter().rev() {
                if let Some(v) = sst.get(key)? {
                    return Ok(Some(v));
                }
            }
        }
        Ok(None)
    }

    /// Write memtable to a new SST file.
    pub fn write_memtable(&mut self, memtable: &Memtable) -> Result<()> {
        if memtable.is_empty() {
            return Ok(());
        }

        let file_id = self.next_file_id;
        self.next_file_id += 1;

        let path = self.dir.join(format!("L0_{:04}.sst", file_id));
        let sst_file = SstFile::create(&path, memtable)?;

        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        self.levels[0].push(sst_file);

        if self.levels[0].len() > 4 {
            self.compact_l0()?;
        }
        Ok(())
    }

    fn compact_l0(&mut self) -> Result<()> {
        if self.levels.is_empty() || self.levels[0].len() < 4 {
            return Ok(());
        }

        info!("Starting L0 compaction: {} files", self.levels[0].len());

        // Collect old paths before merging
        let old_paths: Vec<PathBuf> = self.levels[0].iter().map(|f| f.path.clone()).collect();

        // Merge keeping newest version of each key
        let mut merged: BTreeMap<Vec<u8>, Value> = BTreeMap::new();
        for sst in &self.levels[0] {
            let file = File::open(&sst.path)?;
            let mut reader = BufReader::new(file);

            while reader.stream_position()? < sst.data_end_offset {
                if let Some((k, v)) = SstFile::read_entry(&mut reader)? {
                    let val = match v {
                        SstValue::Data(d) => Value::Data(d.into()),
                        SstValue::Tombstone => Value::Tombstone,
                    };
                    merged.insert(k, val);
                }
            }
        }

        // Filter tombstones
        let entries: Vec<_> = merged
            .into_iter()
            .filter(|(_, v)| !v.is_tombstone())
            .collect();

        self.levels[0].clear();

        if !entries.is_empty() {
            let file_id = self.next_file_id;
            self.next_file_id += 1;
            let path = self.dir.join(format!("L0_{:04}.sst", file_id));
            let new_sst = SstFile::create_from_entries(&path, &entries)?;
            self.levels[0].push(new_sst);
        }

        // Delete old files
        for path in &old_paths {
            if let Err(e) = fs::remove_file(path) {
                warn!("Failed to delete old SST file {:?}: {}", path, e);
            }
        }

        info!(
            "L0 compaction complete: {} files -> 1 file, {} entries",
            old_paths.len(),
            entries.len()
        );

        Ok(())
    }
}
