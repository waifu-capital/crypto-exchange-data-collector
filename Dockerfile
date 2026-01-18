# Stage 1: Build
FROM rust:1-bookworm as builder
WORKDIR /usr/src/app

COPY Cargo.toml Cargo.lock ./
COPY src ./src

RUN cargo build --release

# Stage 2: Final
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y libsqlite3-0 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/app/target/release/crypto-exchange-data-collector /usr/local/bin/app

# Create data directory
RUN mkdir -p /app/data /app/logs

# Set the entrypoint
CMD ["/usr/local/bin/app"]
