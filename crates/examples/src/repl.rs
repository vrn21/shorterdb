//! Interactive REPL for ShorterDB
//!
//! Run with: `cargo run --example repl`

use shorterdb::ShorterDB;
use std::io::{self, Write};
use std::path::Path;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut db = ShorterDB::new(Path::new("./repl_db"))?;

    println!("Welcome to the ShorterDB REPL!");
    println!("Commands:");
    println!("  set <key> <value>  - Store a key-value pair");
    println!("  get <key>          - Retrieve a value by key");
    println!("  delete <key>       - Delete a key");
    println!("  exit               - Exit the REPL");
    println!();

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        if input == "exit" || input == "quit" {
            break;
        }

        let parts: Vec<&str> = input.splitn(3, ' ').collect();

        match parts.as_slice() {
            ["set", key, value] => {
                db.set(key.as_bytes(), value.as_bytes())?;
                println!("OK");
            }
            ["get", key] => match db.get(key.as_bytes())? {
                Some(value) => println!("{}", String::from_utf8_lossy(&value)),
                None => println!("(nil)"),
            },
            ["delete", key] => {
                let existed = db.delete(key.as_bytes())?;
                if existed {
                    println!("OK (key existed)");
                } else {
                    println!("OK (key did not exist)");
                }
            }
            _ => {
                println!("Unknown command. Use 'set', 'get', 'delete', or 'exit'.");
            }
        }
    }

    println!("Goodbye!");
    Ok(())
}
