"""EarthGrid Processing Layer — compute where the data lives.

Band-aware: operations declare which bands they need, and only those
bands are read from the chunk store.  This is critical for band-level
chunking — NDVI reads B04+B08 chunks, not all 13 bands.
"""
from __future__ import annotations
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


@dataclass
class OpSpec:
    """Operation specification with required bands."""
    fn: callable
    required_bands: list[list[str]] | None  # None = all bands needed
    description: str


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


# Operations with their required bands (for selective loading)
OPERATIONS: dict[str, OpSpec] = {
    "ndvi": OpSpec(op_ndvi, [["B08", "B8A"], ["B04"]], "NDVI"),
    "ndwi": OpSpec(op_ndwi, [["B03"], ["B08", "B8A"]], "NDWI"),
    "ndsi": OpSpec(op_ndsi, [["B03"], ["B11"]], "NDSI"),
    "evi":  OpSpec(op_evi,  [["B08", "B8A"], ["B04"], ["B02"]], "EVI"),
    "cloud_mask": OpSpec(op_cloud_mask, [["SCL"]], "Cloud Mask"),
    "true_color": OpSpec(op_true_color, [["B04"], ["B03"], ["B02"]], "True Color"),
    "band_math":  OpSpec(op_band_math, None, "Band Math"),  # needs all bands
}


# ---------------------------------------------------------------------------
# Processor
# ---------------------------------------------------------------------------

class Processor:
    """Process STAC items using registered operations.

    Band-aware: only reads the chunks needed for each operation.
    """

    def __init__(self, chunk_store: ChunkStore, catalog: Catalog):
        self.chunk_store = chunk_store
        self.catalog = catalog

    def list_operations(self) -> list[dict]:
        ops = []
        for name, spec in OPERATIONS.items():
            doc = (spec.fn.__doc__ or "").strip().split("\n")[0]
            required = None
            if spec.required_bands:
                required = [group[0] for group in spec.required_bands]
            ops.append({
                "name": name,
                "description": doc,
                "required_bands": required,
            })
        return ops

    def _resolve_needed_bands(self, operation: str, available_bands: list[str]) -> list[str] | None:
        """Determine which bands an operation needs from the available set.

        Returns list of band names to load, or None for 'all bands'.
        """
        spec = OPERATIONS.get(operation)
        if not spec or spec.required_bands is None:
            return None  # load all

        needed = []
        for band_group in spec.required_bands:
            # Find first available band from the group (e.g. ["B08", "B8A"])
            found = False
            for candidate in band_group:
                if candidate in available_bands:
                    needed.append(candidate)
                    found = True
                    break
            if not found:
                # Try case-insensitive
                for candidate in band_group:
                    for avail in available_bands:
                        if candidate.upper() == avail.upper():
                            needed.append(avail)
                            found = True
                            break
                    if found:
                        break
        return needed if needed else None

    def reconstruct_bands(self, item: STACItem, bands: list[str] | None = None) -> dict[str, np.ndarray]:
        """Reconstruct per-band 2D arrays, optionally band-selective."""
        from .reconstruct import reconstruct_bands as _reconstruct
        return _reconstruct(
            item_id=item.id,
            collection_id=item.collection,
            catalog=self.catalog,
            chunk_store=self.chunk_store,
            bands=bands,
        )

    def process(
        self,
        item_id: str | list[str],
        operation: str,
        output_collection: str | None = None,
        output_item_id: str | None = None,
        expression: str = "",
        tile_size: int = 512,
    ) -> OpResult:
        """Run an operation on one or more STAC items.

        Band-selective: only loads the chunks needed for the operation.
        E.g., NDVI only reads B04 and B08 chunks — not all 13 bands.
        """
        if operation not in OPERATIONS:
            raise ValueError(f"Unknown operation '{operation}'. Available: {list(OPERATIONS.keys())}")

        spec = OPERATIONS[operation]

        # Multi-item or single-item
        if isinstance(item_id, list):
            bands = {}
            for iid in item_id:
                source_item = self.catalog.get_item(iid)
                if not source_item:
                    raise ValueError(f"Item '{iid}' not found")
                available = source_item.properties.get("earthgrid:band_names", [])
                needed = self._resolve_needed_bands(operation, available)
                item_bands = self.reconstruct_bands(source_item, bands=needed)
                bands.update(item_bands)
            source_item = self.catalog.get_item(item_id[0])
        else:
            source_item = self.catalog.get_item(item_id)
            if not source_item:
                raise ValueError(f"Item '{item_id}' not found")
            available = source_item.properties.get("earthgrid:band_names", [])
            needed = self._resolve_needed_bands(operation, available)
            bands = self.reconstruct_bands(source_item, bands=needed)

        # Run operation
        if operation == "band_math":
            result = spec.fn(bands, source_item, expression=expression)
        else:
            result = spec.fn(bands, source_item)

        return result
