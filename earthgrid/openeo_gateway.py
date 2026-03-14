"""EarthGrid openEO Gateway — Process graph parsing and execution.

Accepts openEO process graphs, resolves data requirements to EarthGrid chunks,
triggers auto-download for missing data, and executes supported operations.

This is NOT a full openEO backend — it supports the subset needed for common
Earth observation workflows.
"""
from __future__ import annotations
import asyncio
import logging
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Depends, Request
from pydantic import BaseModel

logger = logging.getLogger("earthgrid.openeo")

router = APIRouter(prefix="/openeo", tags=["openeo"])


# --- Models ---

class ProcessGraph(BaseModel):
    """An openEO process graph."""
    process_graph: dict[str, Any]
    title: str = ""
    description: str = ""
    budget: float = 0  # not used yet, placeholder


class DataRequirement(BaseModel):
    """Resolved data requirement from a process graph."""
    collection_id: str
    spatial_extent: dict = {}    # {"west": ..., "south": ..., "east": ..., "north": ...}
    temporal_extent: list = []   # ["2024-01-01", "2024-12-31"]
    bands: list[str] = []


class JobResult(BaseModel):
    """Result of processing a graph."""
    job_id: str
    status: str  # "queued", "running", "completed", "error"
    progress: float = 0.0  # 0-100
    result: dict = {}
    errors: list[str] = []
    data_requirements: list[dict] = []
    chunks_local: int = 0
    chunks_fetched: int = 0
    chunks_downloaded: int = 0


# --- Supported Processes ---

SUPPORTED_PROCESSES = {
    "load_collection": {
        "id": "load_collection",
        "summary": "Load data from a collection.",
        "parameters": [
            {"name": "id", "description": "Collection identifier", "schema": {"type": "string"}},
            {"name": "spatial_extent", "description": "Bounding box", "optional": True},
            {"name": "temporal_extent", "description": "Time range [start, end]", "optional": True},
            {"name": "bands", "description": "Band names to load", "optional": True},
        ],
    },
    "filter_spatial": {
        "id": "filter_spatial",
        "summary": "Filter by spatial extent.",
        "parameters": [
            {"name": "data", "description": "Input data"},
            {"name": "extent", "description": "Bounding box or GeoJSON"},
        ],
    },
    "filter_temporal": {
        "id": "filter_temporal",
        "summary": "Filter by time range.",
        "parameters": [
            {"name": "data", "description": "Input data"},
            {"name": "extent", "description": "[start, end] ISO dates"},
        ],
    },
    "ndvi": {
        "id": "ndvi",
        "summary": "Calculate NDVI from red and NIR bands.",
        "parameters": [
            {"name": "data", "description": "Input data with red and nir bands"},
            {"name": "red", "description": "Red band name", "optional": True},
            {"name": "nir", "description": "NIR band name", "optional": True},
        ],
    },
    "save_result": {
        "id": "save_result",
        "summary": "Save processing result.",
        "parameters": [
            {"name": "data", "description": "Input data"},
            {"name": "format", "description": "Output format (GTiff, PNG, JSON)"},
        ],
    },
}


class OpenEOGateway:
    """Parse and execute openEO process graphs against EarthGrid data."""

    def __init__(self, catalog=None, chunk_store=None,
                 source_user_manager=None, stats_engine=None,
                 bandwidth_manager=None):
        self.catalog = catalog
        self.chunk_store = chunk_store
        self.source_users = source_user_manager
        self.stats = stats_engine
        self.bandwidth = bandwidth_manager
        self._jobs: dict[str, JobResult] = {}

    def parse_requirements(self, process_graph: dict) -> list[DataRequirement]:
        """Extract data requirements from a process graph.
        
        Walks the graph to find all load_collection nodes and extracts
        collection IDs, spatial/temporal extents, and band selections.
        """
        requirements = []

        for node_id, node in process_graph.items():
            process_id = node.get("process_id", "")
            args = node.get("arguments", {})

            if process_id == "load_collection":
                req = DataRequirement(
                    collection_id=args.get("id", ""),
                    bands=args.get("bands", []),
                )
                # Spatial extent
                if "spatial_extent" in args and args["spatial_extent"]:
                    ext = args["spatial_extent"]
                    req.spatial_extent = {
                        "west": ext.get("west", ext.get("xmin", -180)),
                        "south": ext.get("south", ext.get("ymin", -90)),
                        "east": ext.get("east", ext.get("xmax", 180)),
                        "north": ext.get("north", ext.get("ymax", 90)),
                    }
                # Temporal extent
                if "temporal_extent" in args and args["temporal_extent"]:
                    req.temporal_extent = args["temporal_extent"]

                requirements.append(req)

            # Also check filter nodes for extent refinement
            elif process_id == "filter_spatial" and "extent" in args:
                ext = args["extent"]
                # Apply to the most recent requirement
                if requirements:
                    requirements[-1].spatial_extent = {
                        "west": ext.get("west", -180),
                        "south": ext.get("south", -90),
                        "east": ext.get("east", 180),
                        "north": ext.get("north", 90),
                    }
            elif process_id == "filter_temporal" and "extent" in args:
                if requirements:
                    requirements[-1].temporal_extent = args["extent"]

        return requirements

    async def resolve_chunks(self, requirement: DataRequirement) -> dict:
        """Resolve a data requirement to chunks.
        
        Returns: {"local": [...], "peer": [...], "missing": [...]}
        """
        if not self.catalog:
            return {"local": [], "peer": [], "missing": [], "error": "No catalog available"}

        # Search catalog for matching items
        bbox = None
        if requirement.spatial_extent:
            e = requirement.spatial_extent
            bbox = f"{e['west']},{e['south']},{e['east']},{e['north']}"

        time_start = requirement.temporal_extent[0] if requirement.temporal_extent else None
        time_end = requirement.temporal_extent[1] if len(requirement.temporal_extent) > 1 else None

        # Build datetime_range string for catalog
        datetime_range = None
        if time_start and time_end:
            datetime_range = f"{time_start}/{time_end}"
        elif time_start:
            datetime_range = f"{time_start}/.."
        elif time_end:
            datetime_range = f"../{time_end}"

        # bbox must be list of floats for catalog search
        bbox_list = None
        if bbox:
            bbox_list = [float(x) for x in bbox.split(",")]

        items = self.catalog.search(
            collections=[requirement.collection_id] if requirement.collection_id else None,
            bbox=bbox_list,
            datetime_range=datetime_range,
        )
        # Note: catalog stores native CRS bbox - WGS84 bbox may not match.
        # If 0 items found, execute() triggers search_and_acquire (self-fill).

        local_chunks = []
        missing_chunks = []

        for item in items:
            # Check which chunks exist locally
            chunk_list = item.chunk_hashes if hasattr(item, 'chunk_hashes') else (item.get("chunks") or [])
            item_id = item.id if hasattr(item, 'id') else item.get("id", "")
            item_bbox = item.bbox if hasattr(item, 'bbox') else item.get("bbox")
            for chunk_sha in chunk_list:
                if self.chunk_store and self.chunk_store.has(chunk_sha):
                    local_chunks.append(chunk_sha)
                else:
                    missing_chunks.append({
                        "sha": chunk_sha,
                        "collection": requirement.collection_id,
                        "item_id": item_id,
                        "bbox": item_bbox,
                    })

        return {
            "local": local_chunks,
            "peer": [],  # TODO: federation lookup
            "missing": missing_chunks,
            "items_found": len(items),
        }

    async def acquire_missing(self, missing_chunks: list[dict],
                              nice_level: int = 0) -> dict:
        """Download missing data via source users — parallel across user pool.

        Groups missing chunks by item_id, distributes items round-robin
        across available source users, downloads in parallel with
        asyncio.gather() (max concurrency = number of source users).
        """
        if not self.source_users:
            return {"error": "No source user manager configured",
                    "downloaded": 0, "failed": 0}

        # Group missing chunks by item_id (download whole items, not individual chunks)
        items_needed: dict[str, dict] = {}
        for chunk_info in missing_chunks:
            item_id = chunk_info.get("item_id", "")
            if item_id not in items_needed:
                items_needed[item_id] = {
                    "item_id": item_id,
                    "collection": chunk_info.get("collection", ""),
                    "bbox": chunk_info.get("bbox"),
                    "bands": chunk_info.get("bands"),
                    "product_id": chunk_info.get("product_id", ""),
                    "chunk_count": 0,
                }
            items_needed[item_id]["chunk_count"] += 1

        if not items_needed:
            return {"downloaded": 0, "failed": 0, "errors": []}

        # Determine provider
        sample_collection = next(iter(items_needed.values()))["collection"]
        provider = "cdse"
        if "element84" in sample_collection.lower():
            provider = "element84"
        elif "cmems" in sample_collection.lower():
            provider = "cmems"

        # Get ALL available source users for this provider
        all_users = self.source_users.list_users_with_creds(provider=provider)
        if not all_users:
            # Fallback: try select_user for single user
            creds = self.source_users.select_user(provider=provider)
            if creds:
                all_users = [creds]
            else:
                return {"error": f"No source users available for {provider}",
                        "downloaded": 0, "failed": 0}

        logger.info(f"Parallel download: {len(items_needed)} items across "
                     f"{len(all_users)} source users ({provider})")

        # Distribute items round-robin across source users
        user_tasks: dict[int, list[dict]] = {i: [] for i in range(len(all_users))}
        for idx, item_info in enumerate(items_needed.values()):
            user_idx = idx % len(all_users)
            user_tasks[user_idx].append(item_info)

        # Worker coroutine: one per source user
        async def _download_worker(user_creds: dict, items: list[dict]) -> dict:
            worker_downloaded = 0
            worker_failed = 0
            worker_errors = []

            from .cdse import CDSEClient
            from .ingest import ingest_cog
            import tempfile
            from pathlib import Path

            cdse_client = CDSEClient(
                username=user_creds.get("username", ""),
                password=user_creds.get("password", ""),
            )

            for item_info in items:
                item_id = item_info["item_id"]
                collection = item_info["collection"]
                stream_id = None

                try:
                    if self.bandwidth:
                        stream_id = f"dl-{user_creds.get('name','?')}-{item_id[:20]}"
                        self.bandwidth.register_stream(stream_id, nice_level=nice_level)

                    product_id = item_info.get("product_id", "")

                    if not product_id:
                        # Search CDSE for this item
                        search_collection = "SENTINEL-2"
                        product_type = "S2MSI2A"
                        if "sentinel-1" in collection.lower():
                            search_collection = "SENTINEL-1"
                            product_type = "GRD"
                        elif "sentinel-3" in collection.lower():
                            search_collection = "SENTINEL-3"
                            product_type = "SL_2_LST___"

                        bbox = item_info.get("bbox")
                        products = await cdse_client.search(
                            collection=search_collection,
                            product_type=product_type,
                            bbox=bbox,
                            limit=1,
                        )
                        if products:
                            product_id = products[0]["id"]
                        else:
                            raise ValueError(f"Product not found on CDSE: {item_id}")

                    # Download + ingest
                    with tempfile.TemporaryDirectory(prefix="earthgrid_dl_") as tmpdir:
                        tmpdir = Path(tmpdir)
                        bands = item_info.get("bands")
                        files = await cdse_client.download_product(
                            product_id=product_id,
                            output_dir=tmpdir,
                            bands=bands,
                        )

                        ingested = 0
                        for fpath in files:
                            if fpath.suffix.lower() == ".jp2":
                                import rasterio
                                tif_path = fpath.with_suffix(".tif")
                                with rasterio.open(fpath) as src:
                                    profile = src.profile.copy()
                                    profile.update(driver="GTiff", compress="lzw", tiled=True)
                                    with rasterio.open(tif_path, "w", **profile) as dst:
                                        for bi in range(1, src.count + 1):
                                            dst.write(src.read(bi), bi)
                                fpath = tif_path
                                logger.info(f"Converted JP2: {tif_path.name}")

                            band_item_id = f"{item_id}_{fpath.stem}"
                            item = ingest_cog(
                                file_path=fpath,
                                chunk_store=self.chunk_store,
                                catalog=self.catalog,
                                collection_id=collection,
                                item_id=band_item_id,
                            )
                            ingested += 1
                            logger.info(f"[{user_creds.get('name','?')}] "
                                        f"Ingested {band_item_id}: "
                                        f"{len(item.chunk_hashes)} chunks")

                    self.source_users.record_success(
                        user_creds["user_id"], collection=collection, item_id=item_id
                    )
                    if self.stats:
                        self.stats.record_collection_access(
                            collection, access_type="download"
                        )
                        self.stats.record_download(
                            origin="source",
                            provider=user_creds.get("provider", "cdse"),
                            collection_id=collection,
                            item_id=item_id,
                            bytes_transferred=sum(
                                self.chunk_store.chunk_size(h)
                                for h in (item.chunk_hashes if item else [])
                            ),
                        )
                    worker_downloaded += ingested
                    logger.info(f"[{user_creds.get('name','?')}] "
                                f"{item_id}: {ingested} bands ingested")

                except Exception as e:
                    self.source_users.record_failure(
                        user_creds["user_id"], error_msg=str(e),
                        collection=collection, item_id=item_id
                    )
                    worker_errors.append(f"{item_id}: {e}")
                    worker_failed += 1
                    logger.error(f"[{user_creds.get('name','?')}] "
                                 f"Failed {item_id}: {e}")

                finally:
                    if self.bandwidth and stream_id:
                        self.bandwidth.unregister_stream(stream_id)

            return {"downloaded": worker_downloaded,
                    "failed": worker_failed,
                    "errors": worker_errors}

        # Launch all workers in parallel
        tasks = []
        for user_idx, items in user_tasks.items():
            if items:  # skip empty assignments
                tasks.append(_download_worker(all_users[user_idx], items))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        total_downloaded = 0
        total_failed = 0
        all_errors = []

        for r in results:
            if isinstance(r, Exception):
                all_errors.append(str(r))
                total_failed += 1
            else:
                total_downloaded += r["downloaded"]
                total_failed += r["failed"]
                all_errors.extend(r["errors"])

        logger.info(f"Parallel download complete: {total_downloaded} ingested, "
                     f"{total_failed} failed, {len(all_users)} workers")

        return {"downloaded": total_downloaded, "failed": total_failed,
                "errors": all_errors, "workers": len(tasks)}


    async def _download_element84_cog(self, cog_url: str, item_id: str,
                                      collection: str) -> dict:
        """Download a public COG from Element84/AWS and ingest into grid."""
        import httpx
        import tempfile
        from pathlib import Path
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
                file_path=out_path,
                chunk_store=self.chunk_store,
                catalog=self.catalog,
                collection_id=collection,
                item_id=item_id,
            )
            logger.info(f"Ingested {item_id} from Element84: {len(item.chunk_hashes)} chunks")
            return {"item_id": item.id, "chunks": len(item.chunk_hashes)}


    async def search_and_acquire(self, requirement: DataRequirement,
                                  nice_level: int = 0) -> dict:
        """Search external catalogs (CDSE/Element84) and download matching data.
        
        Called when resolve_chunks finds 0 local items — the 'self-filling' path.
        Searches CDSE OData for matching products, downloads via source user pool,
        ingests into the grid, returns ingest summary.
        """
        if not self.source_users:
            return {"downloaded": 0, "error": "No source users configured"}

        from .cdse import CDSEClient, fetch_and_ingest

        # Get a source user for search
        all_users = self.source_users.list_users_with_creds(provider="cdse")
        if not all_users:
            creds = self.source_users.select_user(provider="cdse")
            if creds:
                all_users = [creds]
            else:
                return {"downloaded": 0, "error": "No CDSE source users"}

        # Determine CDSE collection + product type from EarthGrid collection ID
        collection_map = {
            "sentinel-2-l2a": ("SENTINEL-2", "S2MSI2A"),
            "sentinel-2-l1c": ("SENTINEL-2", "S2MSI1C"),
            "sentinel-1-grd": ("SENTINEL-1", "GRD"),
            "sentinel-3-lst": ("SENTINEL-3", "SL_2_LST___"),
            "sentinel-3-olci": ("SENTINEL-3", "OL_2_LFR___"),
            "sentinel-5p": ("SENTINEL-5P", "L2__NO2___"),
        }

        cdse_coll, product_type = collection_map.get(
            requirement.collection_id.lower(),
            ("SENTINEL-2", "S2MSI2A")  # default
        )
        # Skip product_type filter - CDSE collection filter is sufficient
        product_type = None

        # Build bbox from spatial extent
        bbox = None
        if requirement.spatial_extent:
            e = requirement.spatial_extent
            bbox = [e.get("west", -180), e.get("south", -90),
                    e.get("east", 180), e.get("north", 90)]

        # Build date range
        start_date = requirement.temporal_extent[0] if requirement.temporal_extent else None
        end_date = requirement.temporal_extent[1] if len(requirement.temporal_extent) > 1 else None

        bands = requirement.bands or None

        # Distribute across source users for parallel download
        total_results = []

        async def _worker(user_creds, limit=5):
            client = CDSEClient(
                username=user_creds.get("username", ""),
                password=user_creds.get("password", ""),
            )
            try:
                results = await fetch_and_ingest(
                    cdse_client=client,
                    chunk_store=self.chunk_store,
                    catalog=self.catalog,
                    collection=cdse_coll,
                    product_type=product_type,
                    bbox=bbox,
                    start_date=start_date,
                    end_date=end_date,
                    bands=bands,
                    cloud_cover=None,
                    limit=limit,
                    earthgrid_collection=requirement.collection_id,
                )
                for r in results:
                    if r.get("item_id"):
                        self.source_users.record_success(
                            user_creds["user_id"],
                            collection=requirement.collection_id,
                            item_id=r["item_id"],
                        )
                return results
            except Exception as e:
                self.source_users.record_failure(
                    user_creds["user_id"], error_msg=str(e),
                    collection=requirement.collection_id,
                )
                logger.error(f"[{user_creds.get('name','?')}] Search+acquire failed: {e}")
                return [{"error": str(e)}]

        # For now use first user to search, then distribute downloads
        # Future: each user searches a different time slice
        results = await _worker(all_users[0], limit=5)

        ingested = sum(1 for r in results if r.get("item_id"))
        total_chunks = sum(r.get("chunks", 0) for r in results if r.get("item_id"))

        logger.info(f"Self-filling: {ingested} items ingested, {total_chunks} chunks")

        if self.stats:
            self.stats.record_collection_access(
                requirement.collection_id, access_type="self-fill"
            )

        return {
            "downloaded": ingested,
            "chunks": total_chunks,
            "items": results,
            "errors": [r["error"] for r in results if "error" in r],
        }

    async def execute(self, graph: ProcessGraph,
                      nice_level: int = 0) -> JobResult:
        """Execute an openEO process graph.
        
        1. Parse data requirements
        2. Resolve to chunks (local, peer, missing)
        3. Download missing via source users
        4. Execute the process graph
        5. Return result
        """
        import uuid
        job_id = str(uuid.uuid4())[:8]
        result = JobResult(job_id=job_id, status="running")
        self._jobs[job_id] = result

        try:
            # Step 1: Parse requirements
            requirements = self.parse_requirements(graph.process_graph)
            result.data_requirements = [r.model_dump() for r in requirements]

            if not requirements:
                result.status = "error"
                result.errors.append("No load_collection found in process graph")
                return result

            # Step 2: Resolve chunks
            total_local = 0
            total_missing = []
            for req in requirements:
                resolved = await self.resolve_chunks(req)
                total_local += len(resolved["local"])
                total_missing.extend(resolved["missing"])

                # Self-filling: if no usable local chunks, search CDSE and download
                if len(resolved["local"]) == 0:
                    logger.info(f"Job {job_id}: No local data for {req.collection_id}, "
                                f"triggering self-fill from CDSE")
                    fill_result = await self.search_and_acquire(req, nice_level=nice_level)
                    if fill_result.get("downloaded", 0) > 0:
                        # Re-resolve after download
                        resolved2 = await self.resolve_chunks(req)
                        total_local += len(resolved2["local"])
                        result.chunks_downloaded = fill_result.get("chunks", 0)
                    if fill_result.get("errors"):
                        result.errors.extend(fill_result["errors"])

                if self.stats:
                    self.stats.record_collection_access(
                        req.collection_id, access_type="openeo",
                        bbox=str(req.spatial_extent),
                        time_range=str(req.temporal_extent)
                    )

            result.chunks_local = total_local
            result.progress = 30.0

            # Step 3: Download missing chunks (for partially available items)
            if total_missing:
                logger.info(f"Job {job_id}: {len(total_missing)} chunks missing, "
                            f"triggering download")
                dl_result = await self.acquire_missing(total_missing, nice_level=nice_level)
                result.chunks_downloaded += dl_result["downloaded"]
                result.chunks_fetched = dl_result["downloaded"]
                if dl_result["errors"]:
                    result.errors.extend(dl_result["errors"])
            result.progress = 60.0

            # Step 4: Execute process graph
            # TODO: Actual raster processing (GDAL/rasterio)
            # For now, return the resolution summary
            result.progress = 100.0
            result.status = "completed"
            result.result = {
                "message": "Process graph parsed and data resolved",
                "requirements": [r.model_dump() for r in requirements],
                "chunks_available": total_local + result.chunks_downloaded,
                "chunks_missing": len(total_missing) - result.chunks_downloaded,
            }

        except Exception as e:
            result.status = "error"
            result.errors.append(str(e))
            logger.error(f"Job {job_id} failed: {e}")

        return result

    def get_job(self, job_id: str) -> Optional[JobResult]:
        """Get job status by ID."""
        return self._jobs.get(job_id)


# --- FastAPI Routes ---

# These will be wired up in main.py with the actual gateway instance
_gateway: Optional[OpenEOGateway] = None


def set_gateway(gw: OpenEOGateway):
    global _gateway
    _gateway = gw


def _get_gateway():
    if not _gateway:
        raise HTTPException(503, "openEO gateway not initialized")
    return _gateway


@router.get("/collections")
def openeo_collections():
    """List available collections (STAC-compatible)."""
    gw = _get_gateway()
    if not gw.catalog:
        return {"collections": []}
    collections = gw.catalog.list_collections()
    return {
        "collections": [
            {"id": c.id, "title": c.title, "description": c.description}
            for c in collections
        ]
    }


@router.get("/processes")
def openeo_processes():
    """List supported openEO processes."""
    return {"processes": list(SUPPORTED_PROCESSES.values())}


@router.post("/process")
async def openeo_process(graph: ProcessGraph, request: Request):
    """Submit an openEO process graph for execution."""
    gw = _get_gateway()

    # Determine nice level from header (default 0)
    nice = int(request.headers.get("X-Nice-Level", "0"))
    nice = max(-10, min(19, nice))

    result = await gw.execute(graph, nice_level=nice)
    return result.model_dump()


@router.get("/jobs/{job_id}")
def openeo_job_status(job_id: str):
    """Get job status."""
    gw = _get_gateway()
    job = gw.get_job(job_id)
    if not job:
        raise HTTPException(404, f"Job {job_id} not found")
    return job.model_dump()


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
        "supported_processes": list(SUPPORTED_PROCESSES.keys()),
    }
