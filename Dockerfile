# syntax=docker/dockerfile:1.7

FROM lukemathwalker/cargo-chef:0.1.77-rust-1.95.0-bookworm AS chef
WORKDIR /app

FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

FROM chef AS builder
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        cmake \
        libclang-dev \
        libssl-dev \
        pkg-config \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --locked --recipe-path recipe.json

COPY . .
RUN cargo build --release --locked \
    --bin cloudbreak \
    --bin cloudbreak-migration

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/cloudbreak /usr/local/bin/cloudbreak
COPY --from=builder /app/target/release/cloudbreak-migration /usr/local/bin/cloudbreak-migration

ENTRYPOINT ["cloudbreak"]
