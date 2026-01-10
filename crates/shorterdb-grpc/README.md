# ShorterDB gRPC Server

A gRPC server for remote access to ShorterDB.

[![Crates.io](https://img.shields.io/crates/v/shorterdb-grpc.svg)](https://crates.io/crates/shorterdb-grpc)
[![License](https://img.shields.io/crates/l/shorterdb-grpc.svg)](../../LICENSE)

## Overview

This crate provides a gRPC server that wraps the ShorterDB embedded database, allowing remote clients to perform key-value operations over the network.

## Features

- **gRPC API** - Standard protobuf-based interface
- **CORS support** - Built-in cross-origin resource sharing
- **Docker ready** - Dockerfile included for containerized deployment

## Running the Server

### Using Cargo

```bash
cargo run -p shorterdb-grpc
```

The server starts on `[::1]:50051` by default.

### Using Docker

```bash
# Build the image
docker build -t shorterdb-grpc .

# Run the container
docker run -p 50051:50051 -v ./data:/app/data shorterdb-grpc
```

### Using Docker Compose

```bash
docker compose up
```

## gRPC API

The server implements the `Basic` service defined in `proto/commands.proto`:

```protobuf
service Basic {
    rpc Get (GetRequest) returns (GetResponse);
    rpc Set (SetRequest) returns (SetResponse);
}

message GetRequest {
    string key = 1;
}

message GetResponse {
    string value = 1;
}

message SetRequest {
    string key = 1;
    string value = 2;
}

message SetResponse {
    bool success = 1;
}
```

## Client Example

Using `grpcurl`:

```bash
# Set a value
grpcurl -plaintext -d '{"key": "hello", "value": "world"}' \
  [::1]:50051 commands.Basic/Set

# Get a value
grpcurl -plaintext -d '{"key": "hello"}' \
  [::1]:50051 commands.Basic/Get
```

## Configuration

| Environment Variable | Default | Description |
|---------------------|---------|-------------|
| `SHORTERDB_ADDR` | `[::1]:50051` | Server listen address |
| `SHORTERDB_DATA` | `./data` | Data directory path |

## Dependencies

This crate depends on:
- `shorterdb` - Core database engine
- `tonic` - gRPC framework
- `tokio` - Async runtime
- `tower-http` - HTTP middleware (CORS)

## License

Licensed under either of Apache License, Version 2.0 or MIT license at your option.
