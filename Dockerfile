# 第一阶段：构建依赖项
FROM rust:1.83.0-bullseye as chef
WORKDIR /usr/local/app
RUN cargo install cargo-chef
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# 第二阶段：构建项目
FROM rust:1.83.0-bullseye as builder
WORKDIR /usr/local/app
COPY --from=chef /usr/local/app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json
COPY . .
ENV RUSTFLAGS="-C codegen-units=1"
RUN cargo build --release

# 第三阶段：创建运行时镜像
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