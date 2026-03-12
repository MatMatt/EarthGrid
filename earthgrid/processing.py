"""EarthGrid Processing Layer — compute where the data lives.

Operates on STAC items stored locally: reads chunks, applies operations,
stores results as new STAC items in the catalog.
"""
import math
import numpy as np
from datetime import datetime, timezone
from dataclasses import dataclass

from .chunk_store import ChunkStore
from .catalog import Catalog, STACItem, STACCollection


# ---------------------------------------------------------------------------
# Operation Registry
# ---------------------------------------------------------------------------

@dataclass
class OpResult:
    """Result of a processing operation."""
    data: np.ndarray          # (bands, height, width)
    band_names: list[str]     # output band names
    description: str          # human-readable description


def _get_band(bands: dict[str, np.ndarray], names: list[str]) -> np.ndarray:
    """Find a band by trying multiple names."""
    for n in names:
        if n in bands:
            return bands[n].astype(np.float32)
    raise KeyError(f"Need one of {names}, have {list(bands.keys())}")


def op_ndvi(bands: dict[str, np.ndarray], item: STACItem) -> OpResult:
    """Normalized Difference Vegetation Index: (NIR - Red) / (NIR + Red)"""
    nir = _get_band(bands, ["B08", "B8A", "nir", "NIR", "B05"])
    red = _get_band(bands, ["B04", "red", "RED", "B03"])
    with np.errstate(divide="ignore", invalid="ignore"):
        ndvi = np.where((nir + red) == 0, 0, (nir - red) / (nir + red))
    return OpResult(
        data=ndvi[np.newaxis, :, :],
        band_names=["ndvi"],
        description="NDVI — Normalized Difference Vegetation Index",
    )


def op_ndwi(bands: dict[str, np.ndarray], item: STACItem) -> OpResult:
    """Normalized Difference Water Index: (Green - NIR) / (Green + NIR)"""
    green = _get_band(bands, ["B03", "green", "GREEN", "B02"])
    nir = _get_band(bands, ["B08", "B8A", "nir", "NIR", "B05"])
    with np.errstate(divide="ignore", invalid="ignore"):
        ndwi = np.where((green + nir) == 0, 0, (green - nir) / (green + nir))
    return OpResult(
        data=ndwi[np.newaxis, :, :],
        band_names=["ndwi"],
        description="NDWI — Normalized Difference Water Index",
    )


def op_ndsi(bands: dict[str, np.ndarray], item: STACItem) -> OpResult:
    """Normalized Difference Snow Index: (Green - SWIR) / (Green + SWIR)"""
    green = _get_band(bands, ["B03", "green", "GREEN"])
    swir = _get_band(bands, ["B11", "swir16", "SWIR1", "B06"])
    with np.errstate(divide="ignore", invalid="ignore"):
        ndsi = np.where((green + swir) == 0, 0, (green - swir) / (green + swir))
    return OpResult(
        data=ndsi[np.newaxis, :, :],
        band_names=["ndsi"],
        description="NDSI — Normalized Difference Snow Index",
    )


def op_evi(bands: dict[str, np.ndarray], item: STACItem) -> OpResult:
    """Enhanced Vegetation Index: 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)"""
    nir = _get_band(bands, ["B08", "B8A", "nir"])
    red = _get_band(bands, ["B04", "red"])
    blue = _get_band(bands, ["B02", "blue"])
    with np.errstate(divide="ignore", invalid="ignore"):
        denom = nir + 6 * red - 7.5 * blue + 1
        evi = np.where(denom == 0, 0, 2.5 * (nir - red) / denom)
        evi = np.clip(evi, -1, 1)
    return OpResult(
        data=evi[np.newaxis, :, :],
        band_names=["evi"],
        description="EVI — Enhanced Vegetation Index",
    )


def op_cloud_mask(bands: dict[str, np.ndarray], item: STACItem) -> OpResult:
    """Cloud mask from SCL: 1=clear, 0=cloud (classes 8,9,10)"""
    scl = _get_band(bands, ["SCL", "scl", "scene_classification"])
    mask = ~np.isin(scl.astype(int), [8, 9, 10])
    return OpResult(
        data=mask.astype(np.float32)[np.newaxis, :, :],
        band_names=["cloud_mask"],
        description="Cloud mask from SCL (1=clear, 0=cloud)",
    )


def op_true_color(bands: dict[str, np.ndarray], item: STACItem) -> OpResult:
    """True color composite (Red, Green, Blue)"""
    red = _get_band(bands, ["B04", "red", "RED"])
    green = _get_band(bands, ["B03", "green", "GREEN"])
    blue = _get_band(bands, ["B02", "blue", "BLUE"])
    rgb = np.stack([red, green, blue], axis=0)
    return OpResult(
        data=rgb,
        band_names=["red", "green", "blue"],
        description="True color composite (RGB)",
    )


def op_band_math(bands: dict[str, np.ndarray], item: STACItem,
                 expression: str = "") -> OpResult:
    """Custom band math expression. Bands available as variables."""
    if not expression:
        raise ValueError("band_math requires 'expression' parameter")
    ns = {k: v.astype(np.float32) for k, v in bands.items()}
    ns["np"] = np
    with np.errstate(divide="ignore", invalid="ignore"):
        result = eval(expression, {"__builtins__": {}}, ns)  # noqa: S307
    if isinstance(result, (int, float)):
        raise ValueError("Expression must return an array, not a scalar")
    result = np.nan_to_num(result, nan=0, posinf=0, neginf=0)
    if result.ndim == 2:
        result = result[np.newaxis, :, :]
    return OpResult(
        data=result,
        band_names=["result"],
        description=f"Band math: {expression}",
    )


OPERATIONS: dict[str, callable] = {
    "ndvi": op_ndvi,
    "ndwi": op_ndwi,
    "ndsi": op_ndsi,
    "evi": op_evi,
    "cloud_mask": op_cloud_mask,
    "true_color": op_true_color,
    "band_math": op_band_math,
}


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class Processor:
    """Process STAC items using registered operations."""

    def __init__(self, chunk_store: ChunkStore, catalog: Catalog):
        self.chunk_store = chunk_store
        self.catalog = catalog

    def list_operations(self) -> list[dict]:
        ops = []
        for name, fn in OPERATIONS.items():
            doc = (fn.__doc__ or "").strip().split("\n")[0]
            ops.append({"name": name, "description": doc})
        return ops

    def reconstruct_bands(self, item: STACItem) -> dict[str, np.ndarray]:
        """Reconstruct per-band 2D arrays from stored chunks."""
        props = item.properties
        width = props["earthgrid:width"]
        height = props["earthgrid:height"]
        n_bands = props["earthgrid:bands"]
        dtype = np.dtype(props["earthgrid:dtype"])
        tile_size = props["earthgrid:tile_size"]
        tile_cols = props["earthgrid:tile_cols"]
        tile_rows = props["earthgrid:tile_rows"]

        full = np.zeros((n_bands, height, width), dtype=dtype)
        chunk_idx = 0
        for row_i in range(tile_rows):
            for col_i in range(tile_cols):
                if chunk_idx >= len(item.chunk_hashes):
                    break
                sha = item.chunk_hashes[chunk_idx]
                raw = self.chunk_store.get(sha)
                if raw is None:
                    chunk_idx += 1
                    continue
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)
                tile = np.frombuffer(raw, dtype=dtype).reshape(n_bands, h, w)
                full[:, y_off:y_off + h, x_off:x_off + w] = tile
                chunk_idx += 1

        source_file = props.get("earthgrid:source_file", "")
        band_names = self._guess_band_names(source_file, n_bands)
        return {name: full[i] for i, name in enumerate(band_names)}

    def reconstruct_multi(self, item_ids: list[str]) -> dict[str, np.ndarray]:
        """Reconstruct bands from multiple items (one band per item)."""
        merged = {}
        for item_id in item_ids:
            bands = self.reconstruct_bands(
                self.catalog.get_item(item_id)
            )
            merged.update(bands)
        return merged

    def _guess_band_names(self, source_file: str, n_bands: int) -> list[str]:
        fname = source_file.upper()
        s2_bands = ["B02", "B03", "B04", "B05", "B06", "B07",
                     "B08", "B8A", "B09", "B11", "B12", "SCL"]
        for band_id in s2_bands:
            if band_id in fname and n_bands == 1:
                return [band_id]
        if "TCI" in fname and n_bands == 3:
            return ["B04", "B03", "B02"]
        return [f"B{i+1:02d}" for i in range(n_bands)]

    def process(
        self,
        item_id: str | list[str],
        operation: str,
        output_collection: str | None = None,
        output_item_id: str | None = None,
        expression: str = "",
        tile_size: int = 512,
    ) -> STACItem:
        """Run an operation on one or more STAC items."""
        if operation not in OPERATIONS:
            raise ValueError(f"Unknown operation '{operation}'. Available: {list(OPERATIONS.keys())}")

        # Multi-item or single-item
        if isinstance(item_id, list):
            bands = self.reconstruct_multi(item_id)
            source_item = self.catalog.get_item(item_id[0])
            source_label = "+".join(item_id)
        else:
            source_item = self.catalog.get_item(item_id)
            if not source_item:
                raise ValueError(f"Item '{item_id}' not found")
            bands = self.reconstruct_bands(source_item)
            source_label = item_id

        # Run operation
        op_fn = OPERATIONS[operation]
        if operation == "band_math":
            result = op_fn(bands, source_item, expression=expression)
        else:
            result = op_fn(bands, source_item)

        # Store result chunks
        out_data = result.data
        n_bands, height, width = out_data.shape
        tile_cols = math.ceil(width / tile_size)
        tile_rows = math.ceil(height / tile_size)
        chunk_hashes = []
        for row_i in range(tile_rows):
            for col_i in range(tile_cols):
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)
                tile = out_data[:, y_off:y_off + h, x_off:x_off + w]
                sha = self.chunk_store.put(tile.tobytes())
                chunk_hashes.append(sha)

        if not output_collection:
            output_collection = f"{source_item.collection}_derived"
        if not output_item_id:
            output_item_id = f"{source_label}_{operation}"

        col = self.catalog.get_collection(output_collection)
        if not col:
            self.catalog.add_collection(STACCollection(
                id=output_collection,
                title=f"{output_collection} (derived)",
                description=f"Products derived from {source_item.collection}",
            ))

        now = datetime.now(timezone.utc).isoformat()
        out_item = STACItem(
            id=output_item_id,
            collection=output_collection,
            geometry=source_item.geometry,
            bbox=source_item.bbox,
            properties={
                "datetime": now,
                "earthgrid:crs": source_item.properties.get("earthgrid:crs", ""),
                "earthgrid:width": width,
                "earthgrid:height": height,
                "earthgrid:bands": n_bands,
                "earthgrid:dtype": str(out_data.dtype),
                "earthgrid:tile_size": tile_size,
                "earthgrid:tile_cols": tile_cols,
                "earthgrid:tile_rows": tile_rows,
                "earthgrid:operation": operation,
                "earthgrid:source_items": item_id if isinstance(item_id, list) else [item_id],
                "earthgrid:source_collection": source_item.collection,
                "earthgrid:band_names": result.band_names,
                "earthgrid:description": result.description,
            },
            assets={
                "data": {
                    "href": "/chunks",
                    "type": "application/octet-stream",
                    "title": result.description,
                    "earthgrid:chunk_count": len(chunk_hashes),
                }
            },
            chunk_hashes=chunk_hashes,
        )
        self.catalog.add_item(out_item)
        return out_item
