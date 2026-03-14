"""Reconstruct files from chunks — reverse of ingest.

Supports three formats:
- spatial-tile (new): each chunk = (n_bands, tile_h, tile_w), flat hash list
- band-level (legacy v2): each chunk = single band tile, dict of hash lists
- legacy (v1): each chunk = (n_bands, tile_h, tile_w), flat hash list

Band-selective reconstruction: only return the bands you need.
"""
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


def reconstruct_bands(
    item_id: str,
    collection_id: str,
    catalog: Catalog,
    chunk_store: ChunkStore,
    bands: list[str] | None = None,
) -> dict[str, "np.ndarray"]:
    """Reconstruct per-band 2D arrays from stored chunks.

    Args:
        bands: Only return these bands (None = all).
               E.g. ["B04", "B08"] for NDVI.

    Returns:
        dict of band_name → 2D numpy array
    """
    if not HAS_RASTERIO:
        raise ImportError("Reconstruction requires rasterio.")

    import numpy as np

    items = catalog.search(collections=[collection_id], limit=10000)
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
    dtype = np.dtype(props["earthgrid:dtype"])
    tile_size = props["earthgrid:tile_size"]
    tile_cols = props["earthgrid:tile_cols"]
    tile_rows = props["earthgrid:tile_rows"]
    n_bands = props["earthgrid:bands"]
    band_names = props.get("earthgrid:band_names", [f"B{i+1:02d}" for i in range(n_bands)])
    chunk_format = props.get("earthgrid:chunk_format", "legacy")

    if chunk_format in ("spatial-tile", "legacy"):
        # Both formats: chunk_hashes = ["sha1", "sha2", ...]
        # Each chunk = (n_bands, tile_h, tile_w) raw bytes
        hashes = item.chunk_hashes
        if isinstance(hashes, dict):
            # Shouldn't happen for these formats, but handle gracefully
            hashes = list(hashes.values())[0] if hashes else []

        full = np.zeros((n_bands, height, width), dtype=dtype)
        for idx, sha in enumerate(hashes):
            row_i = idx // tile_cols
            col_i = idx % tile_cols
            raw = chunk_store.get(sha)
            if raw is None:
                continue
            x_off = col_i * tile_size
            y_off = row_i * tile_size
            w = min(tile_size, width - x_off)
            h = min(tile_size, height - y_off)
            tile = np.frombuffer(raw, dtype=dtype).reshape(n_bands, h, w)
            full[:, y_off:y_off + h, x_off:x_off + w] = tile

        if bands:
            return {name: full[i] for i, name in enumerate(band_names) if name in bands}
        return {name: full[i] for i, name in enumerate(band_names)}

    elif chunk_format == "band-level":
        # Band-level format: chunk_hashes = {"B04": ["sha1", ...], "B08": [...]}
        all_band_hashes = item.chunk_hashes  # dict

        # Filter to requested bands
        if bands:
            selected = {b: all_band_hashes[b] for b in bands if b in all_band_hashes}
        else:
            selected = all_band_hashes

        result = {}
        for band_name, hashes in selected.items():
            band_data = np.zeros((height, width), dtype=dtype)
            for idx, sha in enumerate(hashes):
                row_i = idx // tile_cols
                col_i = idx % tile_cols
                raw = chunk_store.get(sha)
                if raw is None:
                    continue
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)
                tile = np.frombuffer(raw, dtype=dtype).reshape(h, w)
                band_data[y_off:y_off + h, x_off:x_off + w] = tile
            result[band_name] = band_data
        return result

    else:
        raise ValueError(f"Unknown chunk format: {chunk_format}")


def _guess_band_names_legacy(source_file: str, n_bands: int) -> list[str]:
    """Legacy band name detection."""
    fname = source_file.upper()
    s2_bands = ["B02", "B03", "B04", "B05", "B06", "B07",
                "B08", "B8A", "B09", "B11", "B12", "SCL"]
    for band_id in s2_bands:
        if band_id in fname and n_bands == 1:
            return [band_id]
    if "TCI" in fname and n_bands == 3:
        return ["B04", "B03", "B02"]
    return [f"B{i+1:02d}" for i in range(n_bands)]


def reconstruct_geotiff(
    item_id: str,
    collection_id: str,
    catalog: Catalog,
    chunk_store: ChunkStore,
    bands: list[str] | None = None,
) -> bytes:
    """Reconstruct a GeoTIFF from stored chunks.

    Args:
        bands: Only include these bands (None = all).

    Returns the file as bytes (in-memory GeoTIFF).
    """
    import numpy as np

    band_data = reconstruct_bands(item_id, collection_id, catalog, chunk_store, bands=bands)
    if not band_data:
        raise FileNotFoundError(f"No data for {item_id}")

    # Get item for metadata
    items = catalog.search(collections=[collection_id], limit=10000)
    item = next((i for i in items if i.id == item_id), None)
    props = item.properties
    width = props["earthgrid:width"]
    height = props["earthgrid:height"]
    crs = props.get("earthgrid:crs", "EPSG:4326")
    bbox = item.bbox

    band_names_out = list(band_data.keys())
    stack = np.stack([band_data[b] for b in band_names_out], axis=0)

    transform = from_bounds(bbox[0], bbox[1], bbox[2], bbox[3], width, height)

    buffer = io.BytesIO()
    with rasterio.open(
        buffer,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=len(band_names_out),
        dtype=str(stack.dtype),
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(stack)
        for i, name in enumerate(band_names_out, 1):
            dst.set_band_description(i, name)

    buffer.seek(0)
    return buffer.read()
