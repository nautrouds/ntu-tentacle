# Build Stage
FROM rust:1.97.1-bookworm AS builder

RUN apt-get update && apt-get install -y \
    git \
    pkg-config \
    libssl-dev \
    musl-tools \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

    
WORKDIR /app

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN rustup target add x86_64-unknown-linux-musl
RUN cargo build --release --target x86_64-unknown-linux-musl
RUN rm -rf src/

COPY src ./src
RUN touch src/main.rs && cargo build --release --target x86_64-unknown-linux-musl

# Final Stage
FROM scratch

COPY --from=builder /app/target/x86_64-unknown-linux-musl/release/ntu-tentacle /entrypoint

ENTRYPOINT ["/entrypoint"]