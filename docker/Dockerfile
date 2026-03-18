FROM python:3.12-slim

# GDAL for rasterio + nice/ionice utils
RUN apt-get update && apt-get install -y --no-install-recommends     libgdal-dev gcc g++ util-linux supervisor &&     rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY earthgrid/ earthgrid/
COPY docs/ docs/

# Rust core binary (pre-compiled on host)
COPY earthgrid-core/target/release/earthgrid-core /usr/local/bin/earthgrid-core

RUN pip install --no-cache-dir ".[full]"

# Supervisor config to run both Python + Rust
COPY docker/supervisord.conf /etc/supervisor/conf.d/earthgrid.conf

EXPOSE 8400

VOLUME /data

ENV EARTHGRID_STORE_PATH=/data/store
ENV EARTHGRID_CATALOG_PATH=/data/catalog.db
ENV EARTHGRID_RUST_CORE_URL=http://127.0.0.1:8401

# Run both processes via supervisor at lowest priority
CMD ["nice", "-n", "19", "ionice", "-c", "3", "supervisord", "-n", "-c", "/etc/supervisor/conf.d/earthgrid.conf"]
