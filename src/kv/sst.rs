use std::{
    collections::VecDeque,
    fs::{self, create_dir, remove_file, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

use std::fs::File;

use anyhow::Error;

use super::{memtable::Memtable, utils::bytes_to_string};

pub(crate) struct SST {
    pub(crate) dir: PathBuf,
    pub(crate) levels: Vec<PathBuf>,
    pub(crate) max_level_size: Vec<usize>,
    pub(crate) curr_level_size: Vec<usize>,
    pub(crate) queue: VecDeque<Memtable>,
    // parralellisation: todo!(),
}

impl SST {
    pub(crate) fn open(db_name: String) -> Self {
        let dir;
        match create_dir("./".to_string() + &db_name) {
            Ok(()) => {
                dir = PathBuf::from("./".to_string() + &db_name);
                let mut l0 = dir.clone();
                l0.push("./l0");
                create_dir(l0.clone());
                let mut levels = Vec::new();
                levels.push(l0.clone());
                let mut max_level_size = Vec::new();
                max_level_size.push(1024);
                let mut curr_level_size = Vec::new();
                curr_level_size.push(0);
                SST {
                    dir,
                    levels,
                    max_level_size,
                    queue: VecDeque::new(),
                    curr_level_size,
                }
            }
            Err(e) => {
                dir = PathBuf::from("./".to_string() + &db_name);

                let children = dir.read_dir().unwrap();
                let mut levels = Vec::new();
                let mut curr_level_size = Vec::new();
                let mut max_level_size = Vec::new();
                let mut i: usize = 0;
                for child in children {
                    let child = child.unwrap();
                    let path = child.path();
                    if path.is_dir() {
                        let level = path.clone();
                        levels.push(path.clone());
                        max_level_size.push(1024 * 10_i32.pow(i as u32) as usize);
                        let curr_no_of_kvs_in_level = path.read_dir().unwrap().count();
                        max_level_size[i] = curr_no_of_kvs_in_level;
                        i += 1;
                    }
                }

                SST {
                    dir,
                    levels,
                    max_level_size,
                    curr_level_size,
                    queue: VecDeque::new(),
                }
            }
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        dbg!("looking in sst");
        for level in self.levels.iter() {
            dbg!(&level);
            let ssts: fs::ReadDir = level.read_dir().unwrap();
            for sst in ssts {
                let mut directory = level.clone();
                let name = bytes_to_string(key);
                directory.push(name.clone());
                match Path::new(&directory).try_exists() {
                    Ok(true) => match fs::read(directory) {
                        Ok(val) => {
                            return Some(val);
                        }
                        Err(e) => {
                            println!("some error happend{}", e);
                        }
                    },
                    Ok(false) => {}
                    Err(e) => {
                        println!("error while seeking into sst files{}", e);
                    }
                }
            }
        }
        return None;
    }

    pub(crate) fn set(&mut self) {
        use super::memtable::Value;

        let mem = self.queue.pop_front().unwrap();

        for (key, value) in mem.iter() {
            // Skip tombstones - they don't need to be written to SST as files
            // (In a proper SST implementation, tombstones would be written to handle
            // older versions in lower levels, but for now we skip them)
            let value_bytes = match value {
                Value::Data(data) => data,
                Value::Tombstone => continue,
            };

            let mut path_of_kv_file = self.dir.clone();
            path_of_kv_file.push("l0");
            self.curr_level_size.push(0);
            match path_of_kv_file.is_dir() {
                false => {
                    create_dir(&path_of_kv_file).expect("sorry couldnt create the folder");
                }
                true => {
                    print!("folder already there");
                }
            }
            path_of_kv_file.push(bytes_to_string(&key));
            dbg!(&path_of_kv_file);
            let file = File::create_new(&path_of_kv_file);
            match file {
                Ok(mut f) => {
                    f.write_all(&value_bytes).unwrap();
                }
                Err(_) => {
                    print!("most probably already existing");
                    let file = OpenOptions::new()
                        .write(true)
                        .truncate(true)
                        .open(path_of_kv_file);
                    file.unwrap().write_all(&value_bytes).unwrap();
                }
            };

            self.curr_level_size[0] += 1;
            if self.curr_level_size >= self.max_level_size {
                self.compact();
            }
        }
    }

    pub(crate) fn compact(&self) {
        print!("ok compacted");
        // todo!()
    }

    pub(crate) fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        for level in self.levels.iter() {
            dbg!(&level);
            let ssts: fs::ReadDir = level.read_dir()?;
            for sst in ssts {
                let mut directory = level.clone();
                let name = bytes_to_string(key);
                directory.push(name.clone());
                if Path::new(&directory).exists() {
                    remove_file(&directory)?;
                    return Ok(());
                }
            }
        }
        Ok(())
    }
}
