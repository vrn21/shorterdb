pub mod kv;
use anyhow::Result;
use clap::{Parser, Subcommand};
use crossbeam_skiplist::SkipMap;
use kv::memtable::{self, ShorterDB};
use std::collections::HashMap;
use std::io::{self, Write};

#[derive(Parser)]
#[command(name = "shortdb")]
#[command(about = "A simple key-value store REPL", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    Set { key: String, value: String },
    Get { key: String },
    Delete { key: String },
}

fn main() {
    let mut db = ShorterDB::new();

    println!("Welcome to the Key-Value Store REPL!");
    println!("Type 'exit' to quit.");

    loop {
        print!("> ");
        io::stdout().flush().unwrap(); // Ensure the prompt is printed immediately

        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .expect("Failed to read line");
        let input = input.trim();

        if input == "exit" {
            break;
        }

        // Parse the input command using clap
        let args: Vec<&str> = input.split_whitespace().collect();
        let cli = Cli::parse_from(args);

        match cli.command {
            Some(Commands::Set { key, value }) => {
                db.set(key.as_bytes(), value.as_bytes());
                println!("Key: {}, Value: {} Set", key, value.as_str());
            }
            Some(Commands::Get { key }) => {
                match db.get(key.as_bytes()) {
                    Some(v) => {
                        println!("Value for key: {} found: {:?}", &key, v);
                    }
                    None => {
                        println!("Value for Key: {} Not found!!", &key);
                    }
                };
            }
            Some(Commands::Delete { key }) => match db.delete(&key.as_bytes()) {
                Ok(()) => {
                    println!("Value for key: {} changed to tombstone", key);
                }
                Err((e)) => {
                    println!("Some error happend {}", e);
                }
            },
            None => println!("Unknown command. Use 'set', 'get', or 'delete'."),
        }
    }

    println!("Exiting the REPL. Goodbye!");
}
