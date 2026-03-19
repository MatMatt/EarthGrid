"""Ingest COG/GeoTIFF files into EarthGrid — spatial tiling.

Each spatial tile contains ALL bands at that position, producing one chunk
per tile.  This enables:
- All bands at one location from a single node (NDVI = one fetch)
- Spatial parallelism across nodes
- Natural alignment with COG internal tiling
"""
from __future__ import annotations
import hashlib
import logging
import math
import os
import re
import subprocess
import tempfile
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

logger = logging.getLogger(__name__)

# Default chunk size: 512x512 pixels per band
DEFAULT_TILE_SIZE = 512

# Sentinel-2 band order (for band name detection)
S2_BANDS = ["B01", "B02", "B03", "B04", "B05", "B06", "B07",
            "B08", "B8A", "B09", "B11", "B12", "SCL"]


def _detect_band_names(file_path: Path, n_bands: int, item_id: str | None = None) -> list[str]:
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
    # Sentinel-1 polarization from filename or item_id
    if n_bands == 1:
        for src in [fname, (item_id or '').upper()]:
            if '_VV' in src or src.endswith('VV'):
                return ['VV']
            if '_VH' in src or src.endswith('VH'):
                return ['VH']
            if '_HH' in src or src.endswith('HH'):
                return ['HH']
            if '_HV' in src or src.endswith('HV'):
                return ['HV']
    # Generic
    return [f"B{i+1:02d}" for i in range(n_bands)]


def _needs_gdalwarp(file_path: Path) -> bool:
    """Check if a GeoTIFF has GCPs instead of a proper geotransform.

    S1 GRD files use GCPs for georeferencing — they need gdalwarp
    to create a properly georeferenced raster before chunking.
    """
    with rasterio.open(file_path) as src:
        has_gcps = len(src.gcps[0]) > 0
        # Identity transform = no real geotransform (just pixel coords)
        t = src.transform
        identity = (t.a == 1.0 and t.e == -1.0 and t.b == 0.0
                    and t.d == 0.0 and t.c == 0.0)
        return has_gcps and identity


def _gdalwarp_to_geotiff(file_path: Path) -> Path:
    """Warp a GCP-based GeoTIFF to a properly georeferenced COG.

    Returns path to the warped file (caller must clean up).
    """
    suffix = file_path.suffix or ".tiff"
    warped = Path(tempfile.mktemp(suffix=f"_warped{suffix}"))
    cmd = [
        "gdalwarp",
        "-t_srs", "EPSG:4326",
        "-co", "COMPRESS=LZW",
        "-co", "TILED=YES",
        str(file_path),
        str(warped),
    ]
    logger.info("Auto-warping GCP-based raster: %s", file_path.name)
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        warped.unlink(missing_ok=True)
        raise RuntimeError(
            f"gdalwarp failed: {result.stderr.strip()}"
        )
    return warped


def ingest_cog(
    file_path: Path,
    chunk_store: ChunkStore,
    catalog: Catalog,
    collection_id: str = "default",
    item_id: str | None = None,
    tile_size: int = DEFAULT_TILE_SIZE,
    extra_properties: dict | None = None,
) -> STACItem:
    """Ingest a COG/GeoTIFF: split into spatial tiles (all bands per tile).

    Chunk layout: each tile is a (n_bands, tile_h, tile_w) numpy array.
    chunk_hashes = ["sha1", "sha2", ...] — one per spatial tile, row-major order.

    Automatically detects GCP-based files (e.g. Sentinel-1 GRD) and
    runs gdalwarp to create a properly georeferenced raster before chunking.

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

    # Auto-warp GCP-based rasters (e.g. S1 GRD)
    warped_path = None
    if _needs_gdalwarp(file_path):
        warped_path = _gdalwarp_to_geotiff(file_path)
        ingest_source = warped_path
    else:
        ingest_source = file_path

    try:
        return _do_ingest(ingest_source, chunk_store, catalog,
                          collection_id, item_id, tile_size,
                          original_name=file_path.name,
                          extra_properties=extra_properties)
    finally:
        if warped_path:
            warped_path.unlink(missing_ok=True)


def _do_ingest(
    file_path: Path,
    chunk_store: ChunkStore,
    catalog: Catalog,
    collection_id: str,
    item_id: str,
    tile_size: int,
    original_name: str | None = None,
    extra_properties: dict | None = None,
) -> STACItem:
    """Core ingest logic — file must be properly georeferenced."""
    with rasterio.open(file_path) as src:
        bounds = src.bounds
        crs = str(src.crs)
        # Reproject bbox to WGS84 (STAC requires EPSG:4326)
        if src.crs and not src.crs.is_geographic:
            from rasterio.warp import transform_bounds
            w, s, e, n = transform_bounds(src.crs, "EPSG:4326",
                                          bounds.left, bounds.bottom,
                                          bounds.right, bounds.top)
            bbox = [round(w, 6), round(s, 6), round(e, 6), round(n, 6)]
        else:
            bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
        width, height = src.width, src.height
        native_transform = list(src.transform)[:6]  # [a, b, c, d, e, f] affine params
        n_bands = src.count
        dtype = str(src.dtypes[0])

        band_names = _detect_band_names(file_path, n_bands, item_id)

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

        # Spatial tiling: one chunk per tile, ALL bands together
        chunk_hashes: list[str] = []

        for row_i in range(n_rows):
            for col_i in range(n_cols):
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)

                window = Window(x_off, y_off, w, h)
                # Read ALL bands at this spatial position
                # Result shape: (n_bands, h, w)
                data = src.read(window=window)
                raw = data.tobytes()
                sha = chunk_store.put(raw)
                chunk_hashes.append(sha)

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

    # Parse acquisition date from item_id (e.g. S2C_33UUB_20250306_0_L2A_B04)
    acq_date = None
    if item_id:
        m = re.search(r'_(\d{8})_', item_id)
        if m:
            try:
                acq_date = datetime.strptime(m.group(1), "%Y%m%d").replace(
                    tzinfo=timezone.utc).isoformat()
            except ValueError:
                pass
    now = datetime.now(timezone.utc).isoformat()
    properties = {
        "datetime": acq_date or now,
        "earthgrid:ingested": now,
        "earthgrid:crs": crs,
        "earthgrid:width": width,
        "earthgrid:height": height,
        "earthgrid:bands": n_bands,
        "earthgrid:band_names": band_names,
        "earthgrid:dtype": dtype,
        "earthgrid:tile_size": tile_size,
        "earthgrid:tile_cols": n_cols,
        "earthgrid:tile_rows": n_rows,
        "earthgrid:source_file": original_name or file_path.name,
        "earthgrid:chunk_format": "spatial-tile",
        "earthgrid:transform": native_transform,
    }

    # Merge extra properties (e.g. processing_baseline from upstream)
    if extra_properties:
        properties.update(extra_properties)

    assets = {
        "data": {
            "href": "/chunks",
            "type": "application/octet-stream",
            "title": "Spatial-tiled raster data (all bands per tile)",
            "earthgrid:chunk_count": len(chunk_hashes),
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
