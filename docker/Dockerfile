FROM rust:1.86-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY earthgrid-core/ ./earthgrid-core/
WORKDIR /build/earthgrid-core
RUN cargo build --release

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    libgdal32 \
    && rm -rf /var/lib/apt/lists/*

# Cargo binary name is "earthgrid"; install as earthgrid-core for compatibility.
COPY --from=builder /build/earthgrid-core/target/release/earthgrid /usr/local/bin/earthgrid-core
RUN chmod +x /usr/local/bin/earthgrid-core

EXPOSE 8400
VOLUME ["/data"]

ENV EARTHGRID_STORE_PATH=/data/store
ENV EARTHGRID_CATALOG_PATH=/data/catalog.db

ENTRYPOINT ["earthgrid-core"]
CMD ["--data-dir", "/data", "serve", "--host", "0.0.0.0", "--port", "8400"]
