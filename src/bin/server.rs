//! gRPC Server for ShorterDB
//!
//! Run this file using:
//! ```bash
//! cargo run --bin server
//! ```

use proto::basic_server::{Basic, BasicServer};
use proto::{GetRequest, GetResponse, SetRequest, SetResponse};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;

use shorterdb::kv::db::ShorterDB;

mod proto {
    tonic::include_proto!("commands");
}

struct DbOperations {
    db: Arc<Mutex<ShorterDB>>,
}

#[tonic::async_trait]
impl Basic for DbOperations {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> Result<tonic::Response<GetResponse>, tonic::Status> {
        let key = request.get_ref().key.clone();

        let db = self.db.lock().await;
        match db.get(key.as_bytes()) {
            Ok(Some(value)) => match std::str::from_utf8(&value) {
                Ok(string_value) => {
                    let response = GetResponse {
                        value: string_value.to_string(),
                    };
                    Ok(tonic::Response::new(response))
                }
                Err(_) => Err(tonic::Status::internal("Invalid UTF-8 sequence")),
            },
            Ok(None) => Err(tonic::Status::not_found("Key not found")),
            Err(_) => Err(tonic::Status::internal("Error reading from the database")),
        }
    }

    async fn set(
        &self,
        request: tonic::Request<SetRequest>,
    ) -> Result<tonic::Response<SetResponse>, tonic::Status> {
        let key = request.get_ref().key.clone();
        let value = request.get_ref().value.clone();

        let mut db = self.db.lock().await;
        match db.set(key.as_bytes(), value.as_bytes()) {
            Ok(_) => {
                let response = SetResponse { success: true };
                Ok(tonic::Response::new(response))
            }
            Err(_) => Err(tonic::Status::internal("Error writing to the database")),
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;

    let db = Arc::new(Mutex::new(ShorterDB::new(Path::new("./test_db"))?));

    let db_operations = DbOperations { db };

    Server::builder()
        .layer(tower_http::cors::CorsLayer::permissive())
        .add_service(BasicServer::new(db_operations))
        .serve(addr)
        .await?;

    Ok(())
}
