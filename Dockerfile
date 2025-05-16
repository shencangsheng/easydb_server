FROM rust:1.83.0-bullseye as builder

RUN apk add --no-cache musl-dev sqlite-dev build-base musl-dev linux-headers sqlite-dev

WORKDIR /usr/local/app

COPY . .

RUN cargo build --release

FROM alpine:3.21

VOLUME ["/usr/local/app/sqlite", "/var/lib/easydb"]

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/target/release/easy_db .

EXPOSE 3000

CMD ["./easy_db"]