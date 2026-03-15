"""EarthGrid openEO Gateway — openEO API v1.2.0 compatible backend.

Accepts openEO process graphs, resolves data requirements to EarthGrid chunks,
triggers auto-download for missing data, and executes supported operations.

Implements the minimum openEO API surface needed for the official Python + R
clients to work (load_collection → ndvi → download).

Root-level routes (/collections, /processes, /result, /jobs, etc.) are
registered via `root_router` and mounted at "/" in main.py.
Legacy /openeo/* routes are kept via `router` for backward compatibility.
"""
from __future__ import annotations
import asyncio
import io
import logging
import time
import uuid
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Depends, Request, Header
from fastapi.responses import Response, JSONResponse
from pydantic import BaseModel

logger = logging.getLogger("earthgrid.openeo")

# Legacy router (prefix /openeo) — kept for backward compatibility
router = APIRouter(prefix="/openeo", tags=["openeo-legacy"])

# New root-level router — openEO API v1.2.0

# --- Auth: API key for processing endpoints ---
async def _require_api_key(
    authorization: str | None = Header(None),
    x_api_key: str | None = Header(None, alias="X-API-Key"),
):
    """Require API key for processing endpoints.

    Accepts both the legacy single API key (EARTHGRID_API_KEY) and
    per-user keys from the network-wide user registry.
    """
    from .config import settings as _s
    # Extract token from either header
    token = x_api_key
    if not token and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]
    if not token:
        if not _s.api_key:
            return  # No auth configured at all = open access
        raise HTTPException(403, "Missing API key. Use X-API-Key header or Bearer token.")
    # Check legacy single key first
    if _s.api_key and token == _s.api_key:
        return
    # Check per-user keys
    gw = _get_gateway()
    if gw and gw.user_auth:
        user = gw.user_auth.validate_key(token)
        if user:
            return  # Valid user key
    raise HTTPException(403, "Invalid API key. Use X-API-Key header or Bearer token.")


root_router = APIRouter(tags=["openeo"])

API_VERSION = "1.2.0"
BACKEND_VERSION = "0.3.0"

# ---------------------------------------------------------------------------
# openEO API v1.2.0 — Process catalogue
# ---------------------------------------------------------------------------

PROCESS_CATALOGUE = [
    {
        "id": "load_collection",
        "summary": "Load a collection from the current back-end by its id.",
        "description": "Loads a collection from the current back-end by its id and returns it as a processable data cube.",
        "parameters": [
            {
                "name": "id",
                "description": "The collection id.",
                "schema": {"type": "string"},
            },
            {
                "name": "spatial_extent",
                "description": "Limits the data to load from the collection to the specified bounding box.",
                "optional": True,
                "default": None,
                "schema": [
                    {
                        "type": "object",
                        "subtype": "bounding-box",
                        "properties": {
                            "west": {"type": "number"},
                            "south": {"type": "number"},
                            "east": {"type": "number"},
                            "north": {"type": "number"},
                            "crs": {"type": "string", "default": "EPSG:4326"},
                        },
                        "required": ["west", "south", "east", "north"],
                    },
                    {"type": "null"},
                ],
            },
            {
                "name": "temporal_extent",
                "description": "Limits the data to load from the collection to the specified time interval.",
                "optional": True,
                "default": None,
                "schema": [
                    {
                        "type": "array",
                        "subtype": "temporal-interval",
                        "minItems": 2,
                        "maxItems": 2,
                        "items": [
                            {"type": ["string", "null"], "subtype": "date-time", "format": "date-time"},
                            {"type": ["string", "null"], "subtype": "date-time", "format": "date-time"},
                        ],
                    },
                    {"type": "null"},
                ],
            },
            {
                "name": "bands",
                "description": "Only adds the specified bands into the data cube so that bands that don't match the list of band names are dropped from the data cube.",
                "optional": True,
                "default": None,
                "schema": [
                    {"type": "array", "items": {"type": "string"}},
                    {"type": "null"},
                ],
            },
        ],
        "returns": {
            "description": "A data cube for further processing.",
            "schema": {"type": "object", "subtype": "raster-cube"},
        },
        "links": [],
    },
    {
        "id": "ndvi",
        "summary": "Normalized Difference Vegetation Index",
        "description": "Computes the Normalized Difference Vegetation Index (NDVI). The NDVI is computed as (nir - red) / (nir + red).",
        "parameters": [
            {
                "name": "data",
                "description": "A raster data cube with two bands that have the common names `red` and `nir` assigned.",
                "schema": {"type": "object", "subtype": "raster-cube"},
            },
            {
                "name": "nir",
                "description": "The name of the NIR band.",
                "optional": True,
                "default": "nir",
                "schema": [{"type": "string"}, {"type": "null"}],
            },
            {
                "name": "red",
                "description": "The name of the red band.",
                "optional": True,
                "default": "red",
                "schema": [{"type": "string"}, {"type": "null"}],
            },
            {
                "name": "target_band",
                "description": "By default, the dimension of type `bands` is dropped. Set this to add the NDVI as a new band with the specified name.",
                "optional": True,
                "default": None,
                "schema": [{"type": "string"}, {"type": "null"}],
            },
        ],
        "returns": {
            "description": "A raster data cube with the newly computed NDVI.",
            "schema": {"type": "object", "subtype": "raster-cube"},
        },
        "links": [],
    },
    {
        "id": "save_result",
        "summary": "Save processed data to storage",
        "description": "Saves processed data to the local user workspace / the job results.",
        "parameters": [
            {
                "name": "data",
                "description": "The data to save.",
                "schema": {"type": "object", "subtype": "raster-cube"},
            },
            {
                "name": "format",
                "description": "The file format to save to. Allowed values: GTiff, GeoTIFF, netCDF, JSON.",
                "schema": {"type": "string"},
            },
            {
                "name": "options",
                "description": "The file format parameters.",
                "optional": True,
                "default": {},
                "schema": {"type": "object"},
            },
        ],
        "returns": {
            "description": "false if saving failed, true otherwise.",
            "schema": {"type": "boolean"},
        },
        "links": [],
    },
    {
        "id": "filter_spatial",
        "summary": "Spatial filter for a raster data cube",
        "description": "Limits the data cube to the specified bounding box.",
        "parameters": [
            {"name": "data", "description": "The raster data cube.", "schema": {"type": "object"}},
            {"name": "geometries", "description": "The geometries to filter with.", "schema": {"type": "object"}},
        ],
        "returns": {"description": "A raster data cube restricted to the bounding box.", "schema": {"type": "object"}},
        "links": [],
    },
    {
        "id": "filter_temporal",
        "summary": "Temporal filter for a raster data cube",
        "description": "Limits the data cube to the specified interval of dates and/or times.",
        "parameters": [
            {"name": "data", "description": "The raster data cube.", "schema": {"type": "object"}},
            {"name": "extent", "description": "Left-closed temporal interval.", "schema": {"type": "array"}},
            {"name": "dimension", "description": "The name of the temporal dimension to filter on.", "optional": True, "schema": {"type": "string"}},
        ],
        "returns": {"description": "A raster data cube restricted to the specified temporal extent.", "schema": {"type": "object"}},
        "links": [],
    },
    {
        "id": "filter_bands",
        "summary": "Filter the bands by name",
        "description": "Filters the bands in the data cube so that bands that don't match any of the criteria are dropped from the data cube.",
        "parameters": [
            {"name": "data", "description": "The raster data cube.", "schema": {"type": "object"}},
            {"name": "bands", "description": "A list of band names.", "optional": True, "schema": {"type": "array"}},
        ],
        "returns": {"description": "A raster data cube with the same bands as specified.", "schema": {"type": "object"}},
        "links": [],
    },
]

PROCESS_IDS = {p["id"] for p in PROCESS_CATALOGUE}


# ---------------------------------------------------------------------------
# Pydantic models
# ---------------------------------------------------------------------------

class ProcessGraph(BaseModel):
    """An openEO process graph."""
    process_graph: dict[str, Any]
    title: str = ""
    description: str = ""
    budget: float = 0


class SyncRequest(BaseModel):
    """openEO POST /result request body."""
    process: dict[str, Any]   # contains process_graph key


class JobRequest(BaseModel):
    """openEO POST /jobs request body."""
    process: dict[str, Any]
    title: str = ""
    description: str = ""


class DataRequirement(BaseModel):
    collection_id: str
    spatial_extent: dict = {}
    temporal_extent: list = []
    bands: list[str] = []


class JobResult(BaseModel):
    job_id: str
    status: str
    progress: float = 0.0
    result: dict = {}
    errors: list[str] = []
    data_requirements: list[dict] = []
    chunks_local: int = 0
    chunks_fetched: int = 0
    chunks_downloaded: int = 0


# ---------------------------------------------------------------------------
# openEO Gateway
# ---------------------------------------------------------------------------

class OpenEOGateway:
    """Parse and execute openEO process graphs against EarthGrid data."""

    def __init__(self, catalog=None, chunk_store=None,
                 source_user_manager=None, stats_engine=None, user_auth=None,
                 bandwidth_manager=None):
        self.catalog = catalog
        self.chunk_store = chunk_store
        self.source_users = source_user_manager
        self.user_auth = user_auth
        self.stats = stats_engine
        self.bandwidth = bandwidth_manager
        self._jobs: dict[str, JobResult] = {}

    # ------------------------------------------------------------------
    # Process graph helpers
    # ------------------------------------------------------------------

    def parse_requirements(self, process_graph: dict) -> list[DataRequirement]:
        """Extract data requirements from a process graph."""
        requirements = []
        for node_id, node in process_graph.items():
            process_id = node.get("process_id", "")
            args = node.get("arguments", {})

            if process_id == "load_collection":
                req = DataRequirement(
                    collection_id=args.get("id", ""),
                    bands=args.get("bands") or [],
                )
                if args.get("spatial_extent"):
                    ext = args["spatial_extent"]
                    req.spatial_extent = {
                        "west":  ext.get("west",  ext.get("xmin", -180)),
                        "south": ext.get("south", ext.get("ymin", -90)),
                        "east":  ext.get("east",  ext.get("xmax",  180)),
                        "north": ext.get("north", ext.get("ymax",   90)),
                    }
                if args.get("temporal_extent"):
                    req.temporal_extent = args["temporal_extent"]
                requirements.append(req)

            elif process_id == "filter_spatial" and "extent" in args:
                ext = args["extent"]
                if requirements:
                    requirements[-1].spatial_extent = {
                        "west": ext.get("west", -180), "south": ext.get("south", -90),
                        "east": ext.get("east", 180),  "north": ext.get("north", 90),
                    }
            elif process_id == "filter_temporal" and "extent" in args:
                if requirements:
                    requirements[-1].temporal_extent = args["extent"]

        return requirements

    def _extract_operation(self, process_graph: dict) -> dict:
        """Extract the primary computation operation from the graph.

        Returns: {"operation": str, "red": str, "nir": str, "format": str}
        """
        info = {"operation": None, "red": "B04", "nir": "B08", "format": "GTiff"}
        for node_id, node in process_graph.items():
            pid = node.get("process_id", "")
            args = node.get("arguments", {})
            if pid == "ndvi":
                info["operation"] = "ndvi"
                if args.get("red"):
                    info["red"] = args["red"]
                if args.get("nir"):
                    info["nir"] = args["nir"]
            elif pid == "save_result":
                fmt = args.get("format", "GTiff")
                info["format"] = fmt
        return info

    # ------------------------------------------------------------------
    # Chunk resolution & acquisition (unchanged from original)
    # ------------------------------------------------------------------

    async def resolve_chunks(self, requirement: DataRequirement) -> dict:
        if not self.catalog:
            return {"local": [], "peer": [], "missing": [], "error": "No catalog"}

        bbox_list = None
        if requirement.spatial_extent:
            e = requirement.spatial_extent
            bbox_list = [e["west"], e["south"], e["east"], e["north"]]

        datetime_range = None
        if requirement.temporal_extent:
            t = requirement.temporal_extent
            def _normalise_end(dt: str) -> str:
                """Extend date-only end to end-of-day so full-timestamp items match."""
                if dt and len(dt) == 10 and dt != "..":
                    return dt + "T23:59:59"
                return dt
            if len(t) >= 2:
                datetime_range = f"{t[0]}/{_normalise_end(t[1])}"
            elif t:
                datetime_range = f"{t[0]}/.."

        items = self.catalog.search(
            collections=[requirement.collection_id] if requirement.collection_id else None,
            bbox=bbox_list,
            datetime_range=datetime_range,
        )

        local_chunks, missing_chunks = [], []
        for item in items:
            chunk_list = (item.chunk_hashes if hasattr(item, "chunk_hashes")
                          else item.get("chunks", []))
            item_id = item.id if hasattr(item, "id") else item.get("id", "")
            item_bbox = item.bbox if hasattr(item, "bbox") else item.get("bbox")
            for sha in (chunk_list if isinstance(chunk_list, list)
                         else sum(chunk_list.values(), [])):
                if self.chunk_store and self.chunk_store.has(sha):
                    local_chunks.append(sha)
                else:
                    missing_chunks.append({
                        "sha": sha, "collection": requirement.collection_id,
                        "item_id": item_id, "bbox": item_bbox,
                    })

        return {"local": local_chunks, "peer": [], "missing": missing_chunks,
                "items_found": len(items), "items": items}

    async def acquire_missing(self, missing_chunks: list[dict], nice_level: int = 0) -> dict:
        """Download missing chunks via source user pool."""
        if not self.source_users:
            return {"error": "No source user manager", "downloaded": 0, "failed": 0}

        items_needed: dict[str, dict] = {}
        for c in missing_chunks:
            iid = c.get("item_id", "")
            if iid not in items_needed:
                items_needed[iid] = {
                    "item_id": iid, "collection": c.get("collection", ""),
                    "bbox": c.get("bbox"), "bands": c.get("bands"),
                    "product_id": c.get("product_id", ""), "chunk_count": 0,
                }
            items_needed[iid]["chunk_count"] += 1

        if not items_needed:
            return {"downloaded": 0, "failed": 0, "errors": []}

        sample = next(iter(items_needed.values()))
        provider = "cdse"

        all_users = self.source_users.list_users_with_creds(provider=provider)
        if not all_users:
            creds = self.source_users.select_user(provider=provider)
            if creds:
                all_users = [creds]
            else:
                return {"error": f"No source users for {provider}", "downloaded": 0, "failed": 0}

        user_tasks = {i: [] for i in range(len(all_users))}
        for idx, item_info in enumerate(items_needed.values()):
            user_tasks[idx % len(all_users)].append(item_info)

        async def _worker(user_creds, items):
            from .cdse import CDSEClient
            from .ingest import ingest_cog
            import tempfile, rasterio
            downloaded = failed = 0
            errors = []
            client = CDSEClient(username=user_creds.get("username",""), password=user_creds.get("password",""))
            for info in items:
                try:
                    with tempfile.TemporaryDirectory() as tmpdir:
                        files = await client.download_product(product_id=info.get("product_id",""), output_dir=Path(tmpdir), bands=info.get("bands"))
                        for fpath in files:
                            if fpath.suffix.lower() == ".jp2":
                                tif = fpath.with_suffix(".tif")
                                with rasterio.open(fpath) as src:
                                    p = src.profile.copy(); p.update(driver="GTiff", compress="lzw", tiled=True)
                                    with rasterio.open(tif,"w",**p) as dst:
                                        dst.write(src.read())
                                fpath = tif
                            ingest_cog(fpath, self.chunk_store, self.catalog, info["collection"], f"{info['item_id']}_{fpath.stem}")
                            downloaded += 1
                except Exception as e:
                    errors.append(str(e)); failed += 1
            return {"downloaded": downloaded, "failed": failed, "errors": errors}

        tasks = [_worker(all_users[i], items) for i, items in user_tasks.items() if items]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        td, tf, te = 0, 0, []
        for r in results:
            if isinstance(r, Exception):
                te.append(str(r)); tf += 1
            else:
                td += r["downloaded"]; tf += r["failed"]; te.extend(r["errors"])
        return {"downloaded": td, "failed": tf, "errors": te}

    async def search_and_acquire(self, requirement: DataRequirement, nice_level: int = 0) -> dict:
        """Search external catalogs and download matching data (self-filling)."""
        if not self.source_users:
            return {"downloaded": 0, "error": "No source users"}

        from .cdse import CDSEClient, fetch_and_ingest

        all_users = self.source_users.list_users_with_creds(provider="cdse")
        if not all_users:
            creds = self.source_users.select_user(provider="cdse")
            if creds:
                all_users = [creds]
            else:
                return {"downloaded": 0, "error": "No CDSE source users"}

        collection_map = {
            "sentinel-2-l2a": ("SENTINEL-2", None),
            "sentinel-2-l1c": ("SENTINEL-2", None),
            "sentinel-1-grd": ("SENTINEL-1", None),
            "sentinel-3-lst": ("SENTINEL-3", None),
            "sentinel-3-olci": ("SENTINEL-3", None),
            "sentinel-5p":    ("SENTINEL-5P", None),
        }
        cdse_coll, product_type = collection_map.get(
            requirement.collection_id.lower(), ("SENTINEL-2", None))

        bbox = None
        if requirement.spatial_extent:
            e = requirement.spatial_extent
            bbox = [e.get("west",-180), e.get("south",-90), e.get("east",180), e.get("north",90)]

        t = requirement.temporal_extent
        start_date = t[0] if t else None
        end_date   = t[1] if len(t) > 1 else None

        client = CDSEClient(username=all_users[0].get("username",""), password=all_users[0].get("password",""))
        try:
            results = await fetch_and_ingest(
                cdse_client=client, chunk_store=self.chunk_store, catalog=self.catalog,
                collection=cdse_coll, product_type=product_type, bbox=bbox,
                start_date=start_date, end_date=end_date, bands=requirement.bands or None,
                cloud_cover=None, limit=5, earthgrid_collection=requirement.collection_id,
            )
            for r in results:
                if r.get("item_id"):
                    self.source_users.record_success(all_users[0]["user_id"], collection=requirement.collection_id, item_id=r["item_id"])
            ingested = sum(1 for r in results if r.get("item_id"))
            total_chunks = sum(r.get("chunks", 0) for r in results if r.get("item_id"))
            return {"downloaded": ingested, "chunks": total_chunks, "items": results,
                    "errors": [r["error"] for r in results if "error" in r]}
        except Exception as e:
            self.source_users.record_failure(all_users[0]["user_id"], error_msg=str(e), collection=requirement.collection_id)
            logger.error(f"Search+acquire failed: {e}")
            return {"downloaded": 0, "error": str(e)}

    # ------------------------------------------------------------------
    # Core execution: compute raster and return GeoTIFF bytes
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_band_from_item_id(item_id: str) -> str | None:
        """Extract a band name embedded in a SAFE/item ID string.

        EarthGrid ingest creates item IDs like:
          S2C_..._B04_10m  →  "B04"
          S2C_..._B08      →  "B08"
          S2C_..._TCI      →  "TCI"

        Returns the band name (e.g. "B04") or None if not detectable.
        """
        import re
        # Match _(B\w+)_?(\d+m)?$ or _TCI at end of id
        m = re.search(r"_(B\w+?)(?:_\d+m)?$", item_id)
        if m:
            return m.group(1)
        m = re.search(r"_(TCI)(?:_\d+m)?$", item_id)
        if m:
            return m.group(1)
        return None

    async def execute_sync(self, process_graph: dict) -> bytes:
        """Execute process graph synchronously and return GeoTIFF bytes.

        This is the implementation behind POST /result.
        Steps:
          1. Parse data requirements (load_collection nodes)
          2. Resolve items from catalog (with CRS-aware spatial fallback)
          3. Reconstruct band arrays from chunks
          4. Apply operation (ndvi, etc.)
          5. Return GeoTIFF bytes
        """
        import re
        import numpy as np
        import rasterio
        from rasterio.transform import from_bounds

        # 1. Parse
        requirements = self.parse_requirements(process_graph)
        if not requirements:
            raise HTTPException(400, "No load_collection found in process graph")

        op_info = self._extract_operation(process_graph)
        req = requirements[0]
        collection_id = req.collection_id
        bands = req.bands or None

        op = op_info.get("operation")
        nir_name = op_info.get("nir", "B08")
        red_name = op_info.get("red", "B04")

        # Determine which bands we need to reconstruct
        if op == "ndvi":
            need_bands = list({nir_name, red_name} | set(bands or []))
        else:
            need_bands = bands  # None = all

        # 2. Find items in catalog
        # First try with full spatial/temporal filter
        resolved = await self.resolve_chunks(req)
        items = resolved.get("items", [])

        # Fallback: stored bbox may be in projected CRS (UTM) while query is WGS84.
        # In that case the bbox numbers don't compare correctly — fall back to
        # collection-only search (no spatial filter).
        if not items and self.catalog:
            logger.info(
                f"Spatial search returned 0 items for {collection_id} "
                f"(possible WGS84/UTM mismatch). Falling back to collection-only search."
            )
            # Temporal filter still applies; skip bbox.
            # Normalise end date: "2026-03-12" → "2026-03-12T23:59:59" so that
            # items stored with full timestamps on that day are included.
            def _normalise_dt(dt: str | None, is_end: bool = False) -> str | None:
                if not dt or dt == "..":
                    return dt
                if is_end and len(dt) == 10:  # date-only
                    return dt + "T23:59:59"
                return dt

            datetime_range = None
            t = req.temporal_extent
            if t and len(t) >= 2:
                start = _normalise_dt(t[0], is_end=False)
                end   = _normalise_dt(t[1], is_end=True)
                datetime_range = f"{start}/{end}"
            elif t:
                start = _normalise_dt(t[0], is_end=False)
                datetime_range = f"{start}/.."
            items = self.catalog.search(
                collections=[collection_id],
                bbox=None,
                datetime_range=datetime_range,
                limit=500,
            )
            logger.info(f"Collection-only fallback found {len(items)} items.")

        # Second fallback: ignore temporal extent too (get whatever is stored)
        if not items and self.catalog:
            logger.info(f"Temporal search returned 0 items. Falling back to all items in {collection_id}.")
            items = self.catalog.search(collections=[collection_id], limit=500)
            logger.info(f"Collection-all fallback found {len(items)} items.")

        # Still nothing? try self-fill from CDSE
        if not items:
            logger.info(f"No local data for {collection_id}, triggering self-fill")
            fill = await self.search_and_acquire(req)
            if fill.get("downloaded", 0) == 0:
                errors = fill.get("errors", [fill.get("error", "unknown")])
                raise HTTPException(
                    503,
                    f"No data available for collection '{collection_id}'. "
                    f"Self-fill errors: {errors}"
                )
            resolved = await self.resolve_chunks(req)
            items = resolved.get("items", [])
            if not items and self.catalog:
                items = self.catalog.search(collections=[collection_id], limit=500)

        if not items:
            raise HTTPException(404, f"No items found for collection '{collection_id}'")

        # 3. Filter items to only the bands we need (band name embedded in item ID)
        #    and reconstruct into a per-band dict.
        #
        # EarthGrid stores each band as a separate item with the band name at the
        # end of the item ID (e.g. "...._B04", "..._B08_10m").  We detect the
        # band name from the item ID and map it to the correct output band.

        from .reconstruct import reconstruct_bands

        all_band_data: dict[str, np.ndarray] = {}
        reference_item = None

        def _item_band(item) -> str | None:
            """Return the band name for an item, from properties or item ID."""
            props = item.properties if hasattr(item, "properties") else {}
            # Try explicit band_names property first
            band_names = props.get("earthgrid:band_names")
            if band_names and len(band_names) == 1:
                return band_names[0]
            # Fall back to extracting from item ID
            return self._extract_band_from_item_id(item.id)

        # Group items by their detected band name.
        # For each needed band, pick the highest-resolution item.
        band_items: dict[str, list] = {}
        for item in items:
            bname = _item_band(item)
            if bname:
                band_items.setdefault(bname, []).append(item)

        logger.info(f"Detected bands in catalog: {list(band_items.keys())}")

        def _pick_best_item(item_list):
            """Pick highest-resolution item (smallest tile size / largest width)."""
            if len(item_list) == 1:
                return item_list[0]
            def _width(it):
                return it.properties.get("earthgrid:width", 0)
            return max(item_list, key=_width)

        # Reconstruct each needed band from its dedicated item
        for bname, item_list in band_items.items():
            # Skip if we don't need this band
            if need_bands and bname not in need_bands:
                continue
            item = _pick_best_item(item_list)
            try:
                # Each item is a single-band file; reconstruct_bands returns
                # {"B01": array} (generic name) — rename to the actual band name.
                bd = reconstruct_bands(
                    item_id=item.id,
                    collection_id=collection_id,
                    catalog=self.catalog,
                    chunk_store=self.chunk_store,
                    bands=None,  # load all (it's just one band)
                )
                if bd:
                    # bd keys are generic ("B01") — replace with actual band name
                    arr = next(iter(bd.values()))
                    all_band_data[bname] = arr
                    if reference_item is None:
                        reference_item = item
                    logger.info(f"Reconstructed band {bname} from {item.id}: shape={arr.shape}")
            except Exception as e:
                logger.warning(f"Could not reconstruct {item.id} ({bname}): {e}")

        # If band-name detection failed (no band in item ID), try generic reconstruct
        # on a multi-band item using user-specified band names.
        if not all_band_data:
            logger.info("Band-from-ID detection yielded nothing; trying generic multi-band reconstruct")
            for item in items[:10]:  # limit to first 10 items
                try:
                    bd = reconstruct_bands(
                        item_id=item.id,
                        collection_id=collection_id,
                        catalog=self.catalog,
                        chunk_store=self.chunk_store,
                        bands=need_bands,
                    )
                    if bd:
                        all_band_data.update(bd)
                        if reference_item is None:
                            reference_item = item
                except Exception as e:
                    logger.warning(f"Generic reconstruct failed for {item.id}: {e}")

        if not all_band_data:
            available_bands = list(band_items.keys())
            raise HTTPException(
                422,
                f"Failed to reconstruct band data. "
                f"Needed bands: {need_bands}. "
                f"Available in catalog: {available_bands}. "
                f"Items checked: {len(items)}."
            )

        logger.info(f"Reconstructed bands: {list(all_band_data.keys())}")

        # 4. Apply operation
        def _find_band(target: str, data: dict) -> np.ndarray:
            """Flexible band lookup with fallback aliases."""
            if target in data:
                return data[target].astype(np.float32)
            for k in data:
                if k.upper() == target.upper():
                    return data[k].astype(np.float32)
            aliases = {"B08": ["B8A", "nir", "NIR"], "B04": ["red", "RED"]}
            for fb in aliases.get(target, []):
                if fb in data:
                    return data[fb].astype(np.float32)
            raise KeyError(f"Band '{target}' not found; available: {list(data.keys())}")

        if op == "ndvi":
            try:
                nir_arr = _find_band(nir_name, all_band_data)
                red_arr = _find_band(red_name, all_band_data)
            except KeyError as e:
                raise HTTPException(422, str(e))

            # Align shapes if they differ (e.g. different resolutions ingested)
            if nir_arr.shape != red_arr.shape:
                # Resize to match the larger array using nearest-neighbor
                target_shape = (
                    max(nir_arr.shape[0], red_arr.shape[0]),
                    max(nir_arr.shape[1], red_arr.shape[1]),
                )
                from PIL import Image as _PILImage
                def _resize(arr, shape):
                    img = _PILImage.fromarray(arr)
                    return np.array(img.resize((shape[1], shape[0]), _PILImage.NEAREST))
                if nir_arr.shape != target_shape:
                    nir_arr = _resize(nir_arr, target_shape)
                if red_arr.shape != target_shape:
                    red_arr = _resize(red_arr, target_shape)

            with np.errstate(divide="ignore", invalid="ignore"):
                ndvi = np.where(
                    (nir_arr + red_arr) == 0, np.float32(0),
                    (nir_arr - red_arr) / (nir_arr + red_arr)
                )
            ndvi = np.nan_to_num(ndvi.astype(np.float32), nan=0.0, posinf=1.0, neginf=-1.0)
            output_data = ndvi[np.newaxis, :, :]
            output_band_names = ["ndvi"]
            output_dtype = "float32"
        else:
            band_names_out = list(all_band_data.keys())
            output_data = np.stack([all_band_data[b] for b in band_names_out], axis=0)
            output_band_names = band_names_out
            output_dtype = str(output_data.dtype)

        # 5. Write GeoTIFF to memory buffer
        props = reference_item.properties
        width  = output_data.shape[2]
        height = output_data.shape[1]
        crs    = props.get("earthgrid:crs", "EPSG:4326")
        bbox   = reference_item.bbox  # [west, south, east, north] in native CRS

        from rasterio.transform import from_bounds
        transform = from_bounds(bbox[0], bbox[1], bbox[2], bbox[3], width, height)

        buf = io.BytesIO()
        with rasterio.open(
            buf, "w",
            driver="GTiff",
            height=height,
            width=width,
            count=output_data.shape[0],
            dtype=output_dtype,
            crs=crs,
            transform=transform,
            compress="lzw",
        ) as dst:
            dst.write(output_data)
            for i, name in enumerate(output_band_names, 1):
                dst.set_band_description(i, name)

        buf.seek(0)
        result_bytes = buf.read()
        logger.info(
            f"execute_sync: {collection_id} → op={op or 'passthrough'} "
            f"→ GeoTIFF {width}x{height}x{output_data.shape[0]} ({output_dtype}) "
            f"= {len(result_bytes):,} bytes"
        )
        return result_bytes

    # ------------------------------------------------------------------
    # Async job execution (wraps execute_sync, stores result)
    # ------------------------------------------------------------------

    async def execute(self, graph: ProcessGraph, nice_level: int = 0) -> JobResult:
        """Execute an openEO process graph (async job path)."""
        job_id = str(uuid.uuid4())[:8]
        result = JobResult(job_id=job_id, status="running")
        self._jobs[job_id] = result

        try:
            requirements = self.parse_requirements(graph.process_graph)
            result.data_requirements = [r.model_dump() for r in requirements]

            if not requirements:
                result.status = "error"
                result.errors.append("No load_collection found in process graph")
                return result

            total_local = 0
            total_missing = []

            for req in requirements:
                resolved = await self.resolve_chunks(req)
                total_local += len(resolved["local"])
                total_missing.extend(resolved["missing"])

                if resolved.get("items_found", 0) == 0:
                    logger.info(f"Job {job_id}: no local data for {req.collection_id}, self-filling")
                    fill = await self.search_and_acquire(req, nice_level=nice_level)
                    if fill.get("downloaded", 0) > 0:
                        r2 = await self.resolve_chunks(req)
                        total_local += len(r2["local"])
                        result.chunks_downloaded = fill.get("chunks", 0)
                    if fill.get("errors"):
                        result.errors.extend(fill["errors"])

                if self.stats:
                    self.stats.record_collection_access(
                        req.collection_id, access_type="openeo",
                        bbox=str(req.spatial_extent), time_range=str(req.temporal_extent)
                    )

            result.chunks_local = total_local
            result.progress = 30.0

            if total_missing:
                dl = await self.acquire_missing(total_missing, nice_level=nice_level)
                result.chunks_downloaded += dl["downloaded"]
                result.chunks_fetched = dl["downloaded"]
                if dl["errors"]:
                    result.errors.extend(dl["errors"])

            result.progress = 60.0

            # Actual computation
            try:
                geotiff_bytes = await self.execute_sync(graph.process_graph)
                result.result = {
                    "message": "Computed successfully",
                    "bytes": len(geotiff_bytes),
                    "format": "GTiff",
                }
                # Store bytes for later retrieval via /jobs/{id}/results
                self._jobs[job_id + "_data"] = geotiff_bytes
            except HTTPException as e:
                result.errors.append(e.detail)

            result.progress = 100.0
            result.status = "finished" if not result.errors else "error"

        except Exception as e:
            result.status = "error"
            result.errors.append(str(e))
            logger.error(f"Job {job_id} failed: {e}")

        return result

    def get_job(self, job_id: str) -> Optional[JobResult]:
        return self._jobs.get(job_id)

    def get_job_data(self, job_id: str) -> Optional[bytes]:
        return self._jobs.get(job_id + "_data")

    async def _download_element84_cog(self, cog_url: str, item_id: str, collection: str) -> dict:
        """Download a public COG from Element84/AWS and ingest into grid."""
        import httpx, tempfile
        from .ingest import ingest_cog

        with tempfile.TemporaryDirectory(prefix="earthgrid_e84_") as tmpdir:
            tmpdir = Path(tmpdir)
            fname = cog_url.split("/")[-1]
            out_path = tmpdir / fname
            async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
                logger.info(f"Downloading COG: {cog_url}")
                async with client.stream("GET", cog_url) as resp:
                    resp.raise_for_status()
                    with open(out_path, "wb") as f:
                        async for chunk in resp.aiter_bytes(chunk_size=65536):
                            f.write(chunk)
            item = ingest_cog(
                file_path=out_path, chunk_store=self.chunk_store,
                catalog=self.catalog, collection_id=collection, item_id=item_id,
            )
            logger.info(f"Ingested {item_id} from Element84: {len(item.chunk_hashes)} chunks")
            return {"item_id": item.id, "chunks": len(item.chunk_hashes)}


# ---------------------------------------------------------------------------
# Module-level gateway singleton (wired up in main.py)
# ---------------------------------------------------------------------------

_gateway: Optional[OpenEOGateway] = None


def set_gateway(gw: OpenEOGateway):
    global _gateway
    _gateway = gw


def _get_gateway() -> OpenEOGateway:
    if not _gateway:
        raise HTTPException(503, "openEO gateway not initialized")
    return _gateway


# ---------------------------------------------------------------------------
# Helper: build openEO capabilities response
# ---------------------------------------------------------------------------

def _capabilities(base_url: str = "") -> dict:
    return {
        "api_version": API_VERSION,
        "backend_version": BACKEND_VERSION,
        "stac_version": "1.0.0",
        "id": "earthgrid",
        "title": "EarthGrid openEO Backend",
        "description": "Distributed satellite data storage and openEO-compatible processing.",
        "production": False,
        "endpoints": [
            {"path": "/",                       "methods": ["GET"]},
            {"path": "/.well-known/openeo",     "methods": ["GET"]},
            {"path": "/credentials/basic",      "methods": ["GET"]},
            {"path": "/me",                     "methods": ["GET"]},
            {"path": "/collections",            "methods": ["GET"]},
            {"path": "/collections/{collection_id}", "methods": ["GET"]},
            {"path": "/processes",              "methods": ["GET"]},
            {"path": "/result",                 "methods": ["POST"]},
            {"path": "/jobs",                   "methods": ["GET", "POST"]},
            {"path": "/jobs/{job_id}",          "methods": ["GET", "DELETE"]},
            {"path": "/jobs/{job_id}/results",  "methods": ["GET"]},
            {"path": "/jobs/{job_id}/logs",     "methods": ["GET"]},
        ],
        "links": [
            {"rel": "self",        "href": f"{base_url}/"},
            {"rel": "conformance", "href": "https://openeo.net/openeo-api-spec/"},
            {"rel": "version-history", "href": f"{base_url}/.well-known/openeo"},
        ],
        "billing": None,
        "file_formats": {
            "output": [
                {
                    "name": "GTiff",
                    "title": "GeoTIFF",
                    "gis_data_types": ["raster"],
                    "parameters": {},
                    "links": [],
                }
            ]
        },
    }


# ---------------------------------------------------------------------------
# Root-level openEO API routes (v1.2.0)
# ---------------------------------------------------------------------------

@root_router.get("/openeo-capabilities")
def openeo_root(request: Request):
    """GET /openeo-capabilities — openEO capabilities (same as /, merged into node_info)."""
    base = str(request.base_url).rstrip("/")
    return _capabilities(base)


@root_router.get("/.well-known/openeo")
def openeo_well_known(request: Request):
    """GET /.well-known/openeo — version discovery."""
    base = str(request.base_url).rstrip("/")
    return {
        "versions": [
            {
                "url": base,
                "api_version": API_VERSION,
                "production": False,
            }
        ]
    }


@root_router.get("/credentials/basic")
def openeo_credentials_basic(authorization: str | None = Header(None)):
    """GET /credentials/basic — validate Basic auth, return bearer token.

    openEO clients send Basic auth (username:password where password=api_key).
    Returns the API key as bearer token for subsequent requests.
    """
    from .config import settings as _cs
    import base64 as _b64
    if authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "basic":
            try:
                decoded = _b64.b64decode(parts[1]).decode()
                _, password = decoded.split(":", 1)
                # Check legacy key
                if _cs.api_key and password == _cs.api_key:
                    return {"access_token": password, "token_type": "Bearer"}
                # Check per-user key
                gw = _get_gateway()
                if gw and gw.user_auth:
                    user = gw.user_auth.validate_key(password)
                    if user:
                        return {"access_token": password, "token_type": "Bearer"}
                raise HTTPException(401, "Invalid credentials")
            except (ValueError, Exception) as e:
                if isinstance(e, HTTPException):
                    raise
    # Fallback: return legacy key or open access token
    token = _cs.api_key or "earthgrid-open-access"
    return {"access_token": token, "token_type": "Bearer"}


@root_router.get("/me")
def openeo_me(authorization: str | None = Header(None),
              x_api_key: str | None = Header(None, alias="X-API-Key")):
    """GET /me — current user info based on bearer token."""
    token = x_api_key
    if not token and authorization:
        parts = authorization.split()
        if len(parts) == 2 and parts[0].lower() == "bearer":
            token = parts[1]
    if token:
        gw = _get_gateway()
        if gw and gw.user_auth:
            user = gw.user_auth.validate_key(token)
            if user:
                return {
                    "user_id": user["user_id"],
                    "name": user["username"],
                    "roles": [user["role"]],
                    "links": [],
                }
    return {
        "user_id": "anonymous",
        "name": "Anonymous User",
        "links": [],
    }


@root_router.get("/collections")
def openeo_collections_root():
    """GET /collections — list available collections."""
    gw = _get_gateway()
    if not gw.catalog:
        return {"collections": [], "links": []}
    collections = gw.catalog.list_collections()
    return {
        "collections": [
            {
                "id": c.id,
                "title": c.title or c.id,
                "description": c.description or "",
                "extent": c.extent,
                "license": "EUPL-1.2",
                "links": [],
            }
            for c in collections
        ],
        "links": [],
    }


@root_router.get("/collections/{collection_id}")
def openeo_collection_detail(collection_id: str):
    """GET /collections/{id} — single collection detail."""
    gw = _get_gateway()
    if not gw.catalog:
        raise HTTPException(503, "Catalog not available")
    col = gw.catalog.get_collection(collection_id)
    if not col:
        raise HTTPException(404, f"Collection '{collection_id}' not found")

    # Determine band info per collection
    BAND_INFO = {
        "sentinel-2-l2a": {
            "bands": ["B01","B02","B03","B04","B05","B06","B07","B08","B8A","B09","B11","B12","SCL"],
            "common_names": ["coastal","blue","green","red","rededge","rededge","rededge","nir","nir08","nir09","swir16","swir22","scl"],
        },
        "sentinel-2-l1c": {
            "bands": ["B01","B02","B03","B04","B05","B06","B07","B08","B8A","B09","B10","B11","B12"],
            "common_names": ["coastal","blue","green","red","rededge","rededge","rededge","nir","nir08","nir09","cirrus","swir16","swir22"],
        },
        "sentinel-1-grd": {
            "bands": ["VV","VH"],
            "common_names": [],
        },
    }
    bi = BAND_INFO.get(collection_id.lower(), {"bands": [], "common_names": []})
    bands_stac = []
    for i, b in enumerate(bi["bands"]):
        entry = {"name": b, "aliases": []}
        cn_list = bi.get("common_names", [])
        if i < len(cn_list) and cn_list[i]:
            entry["common_name"] = cn_list[i]
        bands_stac.append(entry)

    return {
        "type": "Collection",
        "id": col.id,
        "stac_version": "1.0.0",
        "title": col.title or col.id,
        "description": col.description or "",
        "license": "EUPL-1.2",
        "extent": col.extent,
        "summaries": {
            "eo:bands": bands_stac,
        },
        "links": [],
        "cube:dimensions": {
            "x": {"type": "spatial", "axis": "x", "reference_system": "EPSG:4326"},
            "y": {"type": "spatial", "axis": "y", "reference_system": "EPSG:4326"},
            "t": {"type": "temporal"},
            "bands": {"type": "bands", "values": bi["bands"]},
        },
    }


@root_router.get("/processes")
def openeo_processes_root():
    """GET /processes — list supported openEO processes."""
    return {"processes": PROCESS_CATALOGUE, "links": []}


@root_router.post("/result")
async def openeo_result(request: Request, _auth=Depends(_require_api_key)):
    """POST /result — synchronous process graph execution.

    This is the critical endpoint called by conn.download() in the Python
    client. It executes the process graph immediately and returns a GeoTIFF.
    """
    gw = _get_gateway()

    body = await request.json()

    # The Python client sends {"process": {"process_graph": {...}}}
    # but older clients may send {"process_graph": {...}} directly
    if "process" in body:
        process_graph = body["process"].get("process_graph", {})
    elif "process_graph" in body:
        process_graph = body["process_graph"]
    else:
        raise HTTPException(400, "Request body must contain 'process' with 'process_graph'")

    if not process_graph:
        raise HTTPException(400, "Empty process_graph")

    try:
        geotiff_bytes = await gw.execute_sync(process_graph)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"POST /result failed: {e}", exc_info=True)
        raise HTTPException(500, f"Execution failed: {e}")

    return Response(
        content=geotiff_bytes,
        media_type="image/tiff",
        headers={
            "Content-Disposition": 'attachment; filename="result.tif"',
            "Content-Type": "image/tiff",
        },
    )


@root_router.post("/jobs")
async def openeo_create_job(request: Request, _auth=Depends(_require_api_key)):
    """POST /jobs — create a batch job."""
    gw = _get_gateway()
    body = await request.json()

    if "process" in body:
        pg = body["process"].get("process_graph", {})
    elif "process_graph" in body:
        pg = body["process_graph"]
    else:
        raise HTTPException(400, "Missing process_graph")

    graph = ProcessGraph(
        process_graph=pg,
        title=body.get("title", ""),
        description=body.get("description", ""),
    )
    job_result = await gw.execute(graph)
    job_id = job_result.job_id

    return JSONResponse(
        status_code=201,
        content={
            "job_id": job_id,
            "id": job_id,
            "status": job_result.status,
            "created": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        },
        headers={"Location": f"/jobs/{job_id}", "OpenEO-Identifier": job_id},
    )


@root_router.get("/jobs")
def openeo_list_jobs():
    """GET /jobs — list all jobs."""
    gw = _get_gateway()
    jobs = []
    for jid, jr in gw._jobs.items():
        if jid.endswith("_data"):
            continue
        jobs.append({
            "id": jr.job_id,
            "status": jr.status,
            "created": "unknown",
        })
    return {"jobs": jobs, "links": []}


@root_router.get("/jobs/{job_id}")
def openeo_job_status_root(job_id: str):
    """GET /jobs/{id} — job status."""
    gw = _get_gateway()
    job = gw.get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job '{job_id}' not found")
    return {
        "id": job.job_id,
        "status": job.status,
        "progress": job.progress,
        "created": "unknown",
        "updated": "unknown",
        "links": [
            {"rel": "self", "href": f"/jobs/{job_id}"},
            {"rel": "results", "href": f"/jobs/{job_id}/results"},
        ],
    }


@root_router.delete("/jobs/{job_id}")
def openeo_delete_job(job_id: str):
    """DELETE /jobs/{id} — delete a job."""
    gw = _get_gateway()
    if job_id not in gw._jobs:
        raise HTTPException(404, f"Job '{job_id}' not found")
    gw._jobs.pop(job_id, None)
    gw._jobs.pop(job_id + "_data", None)
    return Response(status_code=204)


@root_router.get("/jobs/{job_id}/results")
def openeo_job_results(job_id: str):
    """GET /jobs/{id}/results — download job result GeoTIFF."""
    gw = _get_gateway()
    job = gw.get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job '{job_id}' not found")
    if job.status not in ("finished", "completed"):
        raise HTTPException(
            400,
            f"Job is not finished yet (status: {job.status}). "
            "Use GET /jobs/{job_id} to check progress."
        )
    data = gw.get_job_data(job_id)
    if data is None:
        raise HTTPException(404, "Job result data not found (may have expired)")
    return Response(
        content=data,
        media_type="image/tiff",
        headers={"Content-Disposition": f'attachment; filename="{job_id}.tif"'},
    )


@root_router.get("/jobs/{job_id}/logs")
def openeo_job_logs(job_id: str):
    """GET /jobs/{id}/logs — job log entries."""
    gw = _get_gateway()
    job = gw.get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job '{job_id}' not found")
    logs = []
    for msg in job.errors:
        logs.append({"id": "1", "level": "error", "message": msg})
    return {"logs": logs, "links": []}


# ---------------------------------------------------------------------------
# Legacy /openeo/* routes (backward compatibility)
# ---------------------------------------------------------------------------

@router.get("/collections")
def openeo_collections():
    """List available collections (legacy /openeo/collections)."""
    return openeo_collections_root()


@router.get("/collections/{collection_id}")
def openeo_collection_detail_legacy(collection_id: str):
    return openeo_collection_detail(collection_id)


@router.get("/processes")
def openeo_processes():
    """List supported openEO processes (legacy)."""
    return openeo_processes_root()


@router.post("/process")
async def openeo_process(graph: ProcessGraph, request: Request):
    """Submit an openEO process graph for execution (legacy)."""
    gw = _get_gateway()
    nice = int(request.headers.get("X-Nice-Level", "0"))
    nice = max(-10, min(19, nice))
    result = await gw.execute(graph, nice_level=nice)
    return result.model_dump()


@router.get("/jobs/{job_id}")
def openeo_job_status(job_id: str):
    """Get job status (legacy)."""
    return openeo_job_status_root(job_id)


@router.post("/validate")
async def openeo_validate(graph: ProcessGraph):
    """Validate a process graph without executing (dry run)."""
    gw = _get_gateway()
    requirements = gw.parse_requirements(graph.process_graph)
    resolved = []
    for req in requirements:
        chunks = await gw.resolve_chunks(req)
        resolved.append({
            "collection": req.collection_id,
            "spatial_extent": req.spatial_extent,
            "temporal_extent": req.temporal_extent,
            "bands": req.bands,
            "local_chunks": len(chunks["local"]),
            "missing_chunks": len(chunks["missing"]),
            "needs_download": len(chunks["missing"]) > 0,
        })
    return {
        "valid": len(requirements) > 0,
        "requirements": resolved,
        "supported_processes": list(PROCESS_IDS),
    }
