FROM python:3.12-slim

# GDAL for rasterio + nice/ionice utils
RUN apt-get update && apt-get install -y --no-install-recommends     libgdal-dev gcc g++ util-linux &&     rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY earthgrid/ earthgrid/

RUN pip install --no-cache-dir ".[full]"

EXPOSE 8400

VOLUME /data

ENV EARTHGRID_STORE_PATH=/data/store
ENV EARTHGRID_CATALOG_PATH=/data/catalog.db

# Run at lowest CPU + I/O priority (idle resources only)
CMD ["nice", "-n", "19", "ionice", "-c", "3", "uvicorn", "earthgrid.main:app", "--host", "0.0.0.0", "--port", "8400", "--workers", "2"]
