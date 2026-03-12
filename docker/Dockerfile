FROM python:3.12-slim

# GDAL for rasterio
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev gcc g++ && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY pyproject.toml README.md LICENSE ./
COPY earthgrid/ earthgrid/

RUN pip install --no-cache-dir .

EXPOSE 8400

VOLUME /data

ENV EARTHGRID_STORE_PATH=/data/store
ENV EARTHGRID_CATALOG_PATH=/data/catalog.db

CMD ["uvicorn", "earthgrid.main:app", "--host", "0.0.0.0", "--port", "8400"]
