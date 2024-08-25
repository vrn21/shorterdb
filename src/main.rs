pub mod kv;
use anyhow::Result;
use crossbeam_skiplist::SkipMap;
use kv::memtable::{self, ShorterDB};

fn main() -> Result<()> {
    let db = ShorterDB::new();
    db.set(b"hello", b"hi")?;
    dbg!(db.get(b"hello"));
    db.delete(b"hello")?;
    dbg!(db.get(b"hello"));

    Ok(())
}
