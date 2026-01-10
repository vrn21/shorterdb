//! Embedded database example
//!
//! Run with: `cargo run --example embedded`

use shorterdb::ShorterDB;
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the embedded ShorterDB
    let mut db = ShorterDB::new(Path::new("./example_db"))?;

    // Store key-value pairs
    db.set(b"hello", b"world")?;
    db.set(b"foo", b"bar")?;

    // Retrieve and display values
    if let Some(value) = db.get(b"hello")? {
        println!("Key: 'hello', Value: '{}'", String::from_utf8_lossy(&value));
    }

    if let Some(value) = db.get(b"foo")? {
        println!("Key: 'foo', Value: '{}'", String::from_utf8_lossy(&value));
    }

    // Delete a key
    db.delete(b"foo")?;
    println!("Deleted key 'foo'");

    // Verify deletion
    match db.get(b"foo")? {
        Some(_) => println!("Key 'foo' still exists"),
        None => println!("Key 'foo' was deleted successfully"),
    }

    println!("Embedded ShorterDB example completed.");
    Ok(())
}
