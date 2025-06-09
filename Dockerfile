FROM rust:1.83.0-bullseye as builder

WORKDIR /usr/local/app

COPY . .

ENV RUSTFLAGS="-C codegen-units=1"

RUN cargo clean && cargo build --release

FROM debian:bullseye-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

VOLUME ["/usr/local/app/sqlite", "/var/lib/easydb"]

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/target/release/easy_db .

EXPOSE 3000

CMD ["./easy_db"]