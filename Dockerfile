# Multi-stage build for ShorterDB gRPC Server
FROM rust:1.85-slim as builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Copy manifests
COPY Cargo.toml ./

# Copy source code
COPY src ./src
COPY proto ./proto
COPY build.rs ./build.rs

# Build the gRPC server
RUN cargo build --release --bin server

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
COPY --from=builder /build/target/release/server /usr/local/bin/shorterdb-server

# Switch to non-root user
USER shorterdb

# Expose gRPC port
EXPOSE 50051

# Set data directory
VOLUME /app/data

# Run the gRPC server
CMD ["shorterdb-server"]
