FROM rust:1.83.0-bullseye as builder

WORKDIR /usr/local/app

COPY . .

RUN cargo build --release

FROM debian:bullseye-slim

VOLUME ["/usr/local/app/sqlite", "/var/lib/easydb"]

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/target/release/easy_db .

EXPOSE 3000

CMD ["./easy_db"]