//! gRPC Server for ShorterDB
//!
//! Run this binary using:
//! ```bash
//! cargo run -p shorterdb-grpc
//! ```

use anyhow::Result;
use shorterdb::ShorterDB;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::transport::Server;

mod proto {
    tonic::include_proto!("commands");
}

use proto::basic_server::{Basic, BasicServer};
use proto::{DelRequest, DelResponse, GetRequest, GetResponse, SetRequest, SetResponse};

struct DbService {
    db: Arc<Mutex<ShorterDB>>,
}

#[tonic::async_trait]
impl Basic for DbService {
    async fn get(
        &self,
        request: tonic::Request<GetRequest>,
    ) -> std::result::Result<tonic::Response<GetResponse>, tonic::Status> {
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
    ) -> std::result::Result<tonic::Response<SetResponse>, tonic::Status> {
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

    async fn delete(
        &self,
        request: tonic::Request<DelRequest>,
    ) -> std::result::Result<tonic::Response<DelResponse>, tonic::Status> {
        let key = request.get_ref().key.clone();

        let mut db = self.db.lock().await;
        match db.delete(key.as_bytes()) {
            Ok(existed) => {
                let response = DelResponse {
                    key_existed: existed,
                };
                Ok(tonic::Response::new(response))
            }
            Err(_) => Err(tonic::Status::internal("Error deleting from the database")),
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let addr = "[::1]:50051".parse()?;
    let db = Arc::new(Mutex::new(ShorterDB::new(Path::new("./data"))?));

    println!("ShorterDB gRPC server listening on {}", addr);

    Server::builder()
        .layer(tower_http::cors::CorsLayer::permissive())
        .add_service(BasicServer::new(DbService { db }))
        .serve(addr)
        .await?;

    Ok(())
}
