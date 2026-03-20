# Stage 1: Builder
FROM rust:1.83-bookworm AS builder

RUN apt-get update && apt-get install -y \
    libgdal-dev \
    libclang-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY . .

RUN cargo build --release
RUN strip target/release/earthgrid-core

# Stage 2: Runtime
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    libgdal34 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/earthgrid-core /usr/local/bin/earthgrid-core

EXPOSE 8400

RUN mkdir /data
VOLUME ["/data"]

ENTRYPOINT ["earthgrid-core"]
