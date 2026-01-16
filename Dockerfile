# Stage 1: Build
FROM rust:1.92.0 as builder
WORKDIR /usr/src/app

COPY . .

RUN cargo build --release

# Stage 2: Final
FROM debian:bookworm-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y libsqlite3-0 ca-certificates openssl && \
    rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /usr/src/app/target/release/crypto-exchange-data-collector /usr/local/bin/app

# Copy any additional files required (like .env)
COPY . .

# Set the entrypoint
CMD ["/usr/local/bin/app"]