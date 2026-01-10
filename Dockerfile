# Multi-stage build for ShorterDB gRPC Server
FROM rust:1.85-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace manifests first for better caching
COPY Cargo.toml Cargo.lock ./
COPY crates/shorterdb/Cargo.toml crates/shorterdb/
COPY crates/shorterdb-grpc/Cargo.toml crates/shorterdb-grpc/

# Create dummy source files for dependency caching
RUN mkdir -p crates/shorterdb/src crates/shorterdb-grpc/src && \
    echo "pub fn main() {}" > crates/shorterdb/src/lib.rs && \
    echo "fn main() {}" > crates/shorterdb-grpc/src/main.rs

# Build dependencies only (cached unless Cargo.toml changes)
RUN cargo build --release -p shorterdb-grpc 2>/dev/null || true

# Copy actual source code
COPY crates/shorterdb/src crates/shorterdb/src
COPY crates/shorterdb-grpc/src crates/shorterdb-grpc/src
COPY crates/shorterdb-grpc/proto crates/shorterdb-grpc/proto
COPY crates/shorterdb-grpc/build.rs crates/shorterdb-grpc/

# Build the gRPC server
RUN cargo build --release -p shorterdb-grpc

# Runtime stage
FROM debian:bookworm-slim

WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -m -u 1000 -s /bin/bash shorterdb && \
    mkdir -p /app/data && \
    chown -R shorterdb:shorterdb /app

# Copy binary from builder
COPY --from=builder /build/target/release/shorterdb-grpc /usr/local/bin/

# Switch to non-root user
USER shorterdb

# Expose gRPC port
EXPOSE 50051

# Set data directory
VOLUME /app/data

# Run the gRPC server
CMD ["shorterdb-grpc"]
