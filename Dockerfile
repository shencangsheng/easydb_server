FROM rust:1.83.0-bullseye as builder

WORKDIR /usr/local/app

COPY . .

RUN cargo install cargo-chef \
    && cargo chef prepare --recipe-path recipe.json \
    && cargo chef cook --release --recipe-path recipe.json \
    && cargo clean && cargo build --release

FROM debian:bullseye-slim

RUN apt update && \
    apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

VOLUME ["/usr/local/app/sqlite", "/var/lib/easydb"]

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/target/release/easy_db .

EXPOSE 3000

CMD ["./easy_db"]