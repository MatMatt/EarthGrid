"""Ingest COG/GeoTIFF files into EarthGrid — chunk, hash, catalog."""
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

# Default chunk size: 512x512 pixels
DEFAULT_TILE_SIZE = 512


def ingest_cog(
    file_path: Path,
    chunk_store: ChunkStore,
    catalog: Catalog,
    collection_id: str = "default",
    item_id: str | None = None,
    tile_size: int = DEFAULT_TILE_SIZE,
) -> STACItem:
    """Ingest a COG/GeoTIFF: split into tiles, hash, store, catalog.

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

        # Ensure collection exists
        col = catalog.get_collection(collection_id)
        if not col:
            catalog.add_collection(STACCollection(
                id=collection_id,
                title=collection_id,
                description=f"Collection: {collection_id}",
            ))

        # Split into tiles and store
        chunk_hashes = []
        n_cols = math.ceil(width / tile_size)
        n_rows = math.ceil(height / tile_size)

        for row_i in range(n_rows):
            for col_i in range(n_cols):
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)

                window = Window(x_off, y_off, w, h)
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

    now = datetime.now(timezone.utc).isoformat()
    properties = {
        "datetime": now,
        "earthgrid:crs": crs,
        "earthgrid:width": width,
        "earthgrid:height": height,
        "earthgrid:bands": n_bands,
        "earthgrid:dtype": dtype,
        "earthgrid:tile_size": tile_size,
        "earthgrid:tile_cols": n_cols,
        "earthgrid:tile_rows": n_rows,
        "earthgrid:source_file": file_path.name,
    }

    assets = {
        "data": {
            "href": f"/chunks",
            "type": "application/octet-stream",
            "title": "Chunked raster data",
            "earthgrid:chunk_count": len(chunk_hashes),
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
