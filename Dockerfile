FROM rust:1.83.0-alpine3.21 as builder

RUN apk add --no-cache musl-dev

WORKDIR /usr/local/app

COPY . .

RUN cargo build --release

FROM alpine:3.21

WORKDIR /usr/local/app

COPY --from=builder /usr/local/app/target/release/easy_db .

EXPOSE 3000

VOLUME "/var/lib/easydb"

CMD ["./easy_db"]