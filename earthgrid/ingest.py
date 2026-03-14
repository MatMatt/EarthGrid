"""Ingest COG/GeoTIFF files into EarthGrid — band-level chunking.

Each band is split into spatial tiles independently, producing one chunk
per band per tile.  This enables:
- Band-selective downloads (NDVI needs only B04 + B08)
- Better dedup across products
- Parallel multi-node fetch at band granularity
"""
from __future__ import annotations
import hashlib
import math
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    import rasterio
    from rasterio.windows import Window
    HAS_RASTERIO = True
except ImportError:
    HAS_RASTERIO = False

from .chunk_store import ChunkStore
from .catalog import Catalog, STACItem, STACCollection

# Default chunk size: 512x512 pixels per band
DEFAULT_TILE_SIZE = 512

# Sentinel-2 band order (for band name detection)
S2_BANDS = ["B01", "B02", "B03", "B04", "B05", "B06", "B07",
            "B08", "B8A", "B09", "B11", "B12", "SCL"]


def _detect_band_names(file_path: Path, n_bands: int) -> list[str]:
    """Detect band names from filename or fall back to generic names."""
    fname = file_path.name.upper()
    # Single-band file with S2 band name in filename
    for band_id in S2_BANDS:
        if band_id in fname and n_bands == 1:
            return [band_id]
    # TCI (true color image)
    if "TCI" in fname and n_bands == 3:
        return ["B04", "B03", "B02"]
    # Landsat
    if "SR_B" in fname and n_bands == 1:
        for i in range(1, 8):
            if f"SR_B{i}" in fname:
                return [f"SR_B{i}"]
    # Generic
    return [f"B{i+1:02d}" for i in range(n_bands)]


def ingest_cog(
    file_path: Path,
    chunk_store: ChunkStore,
    catalog: Catalog,
    collection_id: str = "default",
    item_id: str | None = None,
    tile_size: int = DEFAULT_TILE_SIZE,
) -> STACItem:
    """Ingest a COG/GeoTIFF: split into per-band tiles, hash, store, catalog.

    Chunk layout: each band is independently tiled into 512x512 spatial chunks.
    chunk_hashes = {"B04": ["sha1", "sha2", ...], "B08": ["sha3", ...]}

    Returns the created STAC item.
    """
    if not HAS_RASTERIO:
        raise ImportError(
            "Geospatial ingest requires rasterio. "
            "Install with: pip install earthgrid[geo]"
        )

    file_path = Path(file_path)
    if not item_id:
        item_id = file_path.stem

    with rasterio.open(file_path) as src:
        bounds = src.bounds
        bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
        crs = str(src.crs)
        width, height = src.width, src.height
        n_bands = src.count
        dtype = str(src.dtypes[0])

        band_names = _detect_band_names(file_path, n_bands)

        # Ensure collection exists
        col = catalog.get_collection(collection_id)
        if not col:
            catalog.add_collection(STACCollection(
                id=collection_id,
                title=collection_id,
                description=f"Collection: {collection_id}",
            ))

        n_cols = math.ceil(width / tile_size)
        n_rows = math.ceil(height / tile_size)

        # Band-level chunking: one chunk per band per spatial tile
        chunk_hashes: dict[str, list[str]] = {}
        total_chunks = 0

        for band_idx in range(n_bands):
            band_name = band_names[band_idx]
            band_hashes = []

            for row_i in range(n_rows):
                for col_i in range(n_cols):
                    x_off = col_i * tile_size
                    y_off = row_i * tile_size
                    w = min(tile_size, width - x_off)
                    h = min(tile_size, height - y_off)

                    window = Window(x_off, y_off, w, h)
                    # Read single band (band_idx+1 because rasterio is 1-indexed)
                    data = src.read(band_idx + 1, window=window)
                    raw = data.tobytes()
                    sha = chunk_store.put(raw)
                    band_hashes.append(sha)
                    total_chunks += 1

            chunk_hashes[band_name] = band_hashes

    # Build STAC item
    geometry = {
        "type": "Polygon",
        "coordinates": [[
            [bbox[0], bbox[1]],
            [bbox[2], bbox[1]],
            [bbox[2], bbox[3]],
            [bbox[0], bbox[3]],
            [bbox[0], bbox[1]],
        ]],
    }

    now = datetime.now(timezone.utc).isoformat()
    properties = {
        "datetime": now,
        "earthgrid:crs": crs,
        "earthgrid:width": width,
        "earthgrid:height": height,
        "earthgrid:bands": n_bands,
        "earthgrid:band_names": band_names,
        "earthgrid:dtype": dtype,
        "earthgrid:tile_size": tile_size,
        "earthgrid:tile_cols": n_cols,
        "earthgrid:tile_rows": n_rows,
        "earthgrid:source_file": file_path.name,
        "earthgrid:chunk_format": "band-level",  # marks new format
    }

    assets = {
        "data": {
            "href": "/chunks",
            "type": "application/octet-stream",
            "title": "Band-level chunked raster data",
            "earthgrid:chunk_count": total_chunks,
            "earthgrid:bands_available": band_names,
        }
    }

    item = STACItem(
        id=item_id,
        collection=collection_id,
        geometry=geometry,
        bbox=bbox,
        properties=properties,
        assets=assets,
        chunk_hashes=chunk_hashes,
    )

    catalog.add_item(item)
    return item
