"""Reconstruct files from chunks — reverse of ingest."""
from __future__ import annotations
import io
import math
from pathlib import Path

try:
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds
    HAS_RASTERIO = True
except ImportError:
    HAS_RASTERIO = False

from .chunk_store import ChunkStore
from .catalog import Catalog


def reconstruct_geotiff(
    item_id: str,
    collection_id: str,
    catalog: Catalog,
    chunk_store: ChunkStore,
) -> bytes:
    """Reconstruct a GeoTIFF from stored chunks.

    Returns the file as bytes (in-memory GeoTIFF).
    """
    if not HAS_RASTERIO:
        raise ImportError("Reconstruction requires rasterio. Install with: pip install earthgrid[geo]")

    # Get STAC item
    items = catalog.search(collections=[collection_id], limit=1000)
    item = None
    for i in items:
        if i.id == item_id:
            item = i
            break

    if not item:
        raise FileNotFoundError(f"Item {item_id} not found in {collection_id}")

    props = item.properties
    width = props["earthgrid:width"]
    height = props["earthgrid:height"]
    bands = props["earthgrid:bands"]
    dtype = props["earthgrid:dtype"]
    tile_size = props["earthgrid:tile_size"]
    tile_cols = props["earthgrid:tile_cols"]
    tile_rows = props["earthgrid:tile_rows"]
    crs = props.get("earthgrid:crs", "EPSG:4326")

    bbox = item.bbox  # [left, bottom, right, top]

    # Reconstruct raster from chunks
    full_data = np.zeros((bands, height, width), dtype=dtype)

    for idx, sha in enumerate(item.chunk_hashes):
        row_i = idx // tile_cols
        col_i = idx % tile_cols

        chunk_bytes = chunk_store.get(sha)
        if chunk_bytes is None:
            continue  # missing chunk — leave as zeros

        x_off = col_i * tile_size
        y_off = row_i * tile_size
        w = min(tile_size, width - x_off)
        h = min(tile_size, height - y_off)

        tile_data = np.frombuffer(chunk_bytes, dtype=dtype).reshape(bands, h, w)
        full_data[:, y_off:y_off + h, x_off:x_off + w] = tile_data

    # Write to in-memory GeoTIFF
    transform = from_bounds(bbox[0], bbox[1], bbox[2], bbox[3], width, height)

    buffer = io.BytesIO()
    with rasterio.open(
        buffer,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=bands,
        dtype=dtype,
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(full_data)

    buffer.seek(0)
    return buffer.read()
