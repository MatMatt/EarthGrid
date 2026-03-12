"""EarthGrid Node — FastAPI application."""
import asyncio
import shutil
from pathlib import Path

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Query
from fastapi.responses import Response

from . import __version__
from .config import settings
from .chunk_store import ChunkStore
from .catalog import Catalog
from .federation import Federation
from .ingest import ingest_cog
from .processing import Processor

app = FastAPI(
    title="EarthGrid Node",
    version=__version__,
    description="Distributed satellite data storage and access",
)

# Initialize components
chunk_store = ChunkStore(settings.store_path, limit_gb=settings.storage_limit_gb)
catalog = Catalog(settings.catalog_path)
federation = Federation(settings.peers)
processor = Processor(chunk_store, catalog)


# --- Beacon Registration ---

async def _register_with_beacon():
    """Register this node with the configured beacon."""
    if not settings.beacon_url:
        return
    try:
        summary = catalog.summary()
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{settings.beacon_url.rstrip('/')}/register",
                params={
                    "node_id": settings.node_id,
                    "node_name": settings.node_name,
                    "url": settings.public_url or f"http://{settings.host}:{settings.port}",
                    "collections": ",".join(summary["collections"]),
                    "item_count": summary["item_count"],
                    "chunk_count": chunk_store.chunk_count,
                    "chunks_bytes": chunk_store.total_bytes,
                },
            )
    except Exception:
        pass  # beacon offline — retry on next heartbeat


async def _beacon_heartbeat_loop():
    """Send periodic heartbeats to the beacon."""
    if not settings.beacon_url:
        return
    while True:
        await asyncio.sleep(60)  # every 60s
        try:
            summary = catalog.summary()
            async with httpx.AsyncClient(timeout=10) as client:
                await client.post(
                    f"{settings.beacon_url.rstrip('/')}/heartbeat",
                    params={
                        "node_id": settings.node_id,
                        "collections": ",".join(summary["collections"]),
                        "item_count": summary["item_count"],
                        "chunk_count": chunk_store.chunk_count,
                        "chunks_bytes": chunk_store.total_bytes,
                    },
                )
        except Exception:
            pass


@app.on_event("startup")
async def startup():
    await _register_with_beacon()
    asyncio.create_task(_beacon_heartbeat_loop())

    # Mount beacon if enabled
    if settings.also_beacon:
        from .beacon import beacon_router, registry, _beacon_sync_loop
        app.include_router(beacon_router)
        if settings.beacon_peers:
            for url in settings.beacon_peers:
                await registry.add_peer_beacon(url)
            asyncio.create_task(_beacon_sync_loop())


# --- Node Info ---

@app.get("/")
def node_info():
    """Node identity and status."""
    summary = catalog.summary()
    return {
        "name": "EarthGrid",
        "version": __version__,
        "node_id": settings.node_id,
        "node_name": settings.node_name,
        "chunks": chunk_store.chunk_count,
        "chunks_bytes": chunk_store.total_bytes,
        "storage_limit_gb": settings.storage_limit_gb,
        "storage_used_pct": round(chunk_store.total_bytes / (settings.storage_limit_gb * 1024**3) * 100, 1) if settings.storage_limit_gb > 0 else 0,
        "item_count": summary["item_count"],
        "collections": summary["collections"],
        "peers": len(federation.peers),
        "beacon": settings.also_beacon,
    }


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stats")
def node_stats():
    """Detailed node statistics — storage, access, uptime."""
    summary = catalog.summary()
    cs = chunk_store.stats
    return {
        "node_id": settings.node_id,
        "node_name": settings.node_name,
        "version": __version__,
        "uptime_hours": cs["uptime_hours"],
        "storage": {
            "used_bytes": cs["storage_used_bytes"],
            "used_gb": round(cs["storage_used_bytes"] / 1024**3, 2),
            "limit_gb": settings.storage_limit_gb,
            "used_pct": round(cs["storage_used_bytes"] / (settings.storage_limit_gb * 1024**3) * 100, 1) if settings.storage_limit_gb > 0 else 0,
            "chunk_count": cs["chunk_count"],
        },
        "catalog": {
            "collections": summary["collections"],
            "item_count": summary["item_count"],
        },
        "activity": {
            "chunks_served": cs["chunks_served"],
            "bytes_served": cs["bytes_served"],
            "bytes_served_gb": round(cs["bytes_served"] / 1024**3, 2),
            "chunks_ingested": cs["chunks_stored"],
            "bytes_ingested": cs["bytes_ingested"],
            "requests_total": cs["requests_total"],
            "requests_today": cs["requests_today"],
        },
        "peers": len(federation.peers),
    }


# --- Chunk Store ---

@app.get("/chunks/{sha}")
def get_chunk(sha: str):
    """Download a chunk by its SHA-256 hash."""
    data = chunk_store.get(sha)
    if data is None:
        raise HTTPException(404, "Chunk not found")
    return Response(content=data, media_type="application/octet-stream")


@app.get("/chunks")
def list_chunks(limit: int = Query(100, le=10000)):
    """List stored chunk hashes."""
    chunks = chunk_store.list_chunks()
    return {"count": len(chunks), "hashes": chunks[:limit]}


# --- Ingest ---

@app.post("/ingest")
async def ingest_file(
    file: UploadFile = File(...),
    collection: str = Query("default"),
    item_id: str = Query(None),
):
    """Upload and ingest a COG/GeoTIFF file."""
    # Save uploaded file temporarily
    tmp_path = Path(f"/tmp/earthgrid_ingest_{file.filename}")
    try:
        with open(tmp_path, "wb") as f:
            content = await file.read()
            f.write(content)

        item = ingest_cog(
            file_path=tmp_path,
            chunk_store=chunk_store,
            catalog=catalog,
            collection_id=collection,
            item_id=item_id,
        )

        return {
            "status": "ingested",
            "item_id": item.id,
            "collection": item.collection,
            "chunks": len(item.chunk_hashes),
            "bbox": item.bbox,
        }
    finally:
        tmp_path.unlink(missing_ok=True)


# --- STAC Catalog ---

@app.get("/stac/collections")
def stac_collections():
    """List STAC collections."""
    collections = catalog.list_collections()
    return {
        "collections": [c.to_stac() for c in collections],
    }


@app.get("/stac/collections/{collection_id}")
def stac_collection(collection_id: str):
    """Get a single STAC collection."""
    col = catalog.get_collection(collection_id)
    if not col:
        raise HTTPException(404, "Collection not found")
    return col.to_stac()


@app.get("/stac/collections/{collection_id}/items")
def stac_collection_items(collection_id: str, limit: int = Query(100, le=1000)):
    """List items in a collection."""
    items = catalog.search(collections=[collection_id], limit=limit)
    return {
        "type": "FeatureCollection",
        "features": [i.to_stac() for i in items],
    }


@app.get("/stac/search")
def stac_search(
    collections: str = Query(None, description="Comma-separated collection IDs"),
    bbox: str = Query(None, description="west,south,east,north"),
    datetime: str = Query(None, description="RFC 3339 datetime or range (start/end)"),
    limit: int = Query(100, le=1000),
):
    """STAC item search with spatial and temporal filters."""
    col_list = collections.split(",") if collections else None
    bbox_list = [float(x) for x in bbox.split(",")] if bbox else None

    items = catalog.search(
        collections=col_list,
        bbox=bbox_list,
        datetime_range=datetime,
        limit=limit,
    )

    return {
        "type": "FeatureCollection",
        "numberMatched": len(items),
        "numberReturned": len(items),
        "features": [i.to_stac() for i in items],
    }


# --- Download / File Access ---

@app.get("/download/{collection_id}/{item_id}")
def download_file(collection_id: str, item_id: str):
    """Download a reconstructed GeoTIFF from stored chunks."""
    try:
        from .reconstruct import reconstruct_geotiff
    except ImportError:
        raise HTTPException(501, "Reconstruction requires rasterio (pip install earthgrid[geo])")

    try:
        data = reconstruct_geotiff(item_id, collection_id, catalog, chunk_store)
    except FileNotFoundError:
        raise HTTPException(404, f"Item {item_id} not found in {collection_id}")

    return Response(
        content=data,
        media_type="image/tiff",
        headers={
            "Content-Disposition": f'attachment; filename="{item_id}.tif"',
        },
    )


# --- Federation ---

@app.get("/peers")
def list_peers():
    """List known peers."""
    return {
        "peers": [
            {
                "url": p.url,
                "node_id": p.node_id,
                "node_name": p.node_name,
                "alive": p.alive,
                "collections": p.collections,
                "item_count": p.item_count,
            }
            for p in federation.list_peers()
        ]
    }


@app.post("/peers")
def register_peer(url: str = Query(...), node_id: str = Query(""), node_name: str = Query("")):
    """Register a new peer."""
    peer = federation.add_peer(url, node_id, node_name)
    return {"status": "registered", "url": peer.url}


@app.post("/federation/sync")
async def federation_sync():
    """Sync with all known peers."""
    synced = await federation.sync_all()
    return {
        "synced": len(synced),
        "peers": [{"url": p.url, "node_id": p.node_id, "alive": p.alive} for p in synced],
    }


@app.get("/federation/search")
async def federation_search(
    collections: str = Query(None),
    bbox: str = Query(None),
    datetime: str = Query(None),
    limit: int = Query(100, le=1000),
):
    """Federated search across all known peers."""
    col_list = collections.split(",") if collections else None
    bbox_list = [float(x) for x in bbox.split(",")] if bbox else None

    results = await federation.federated_search(
        collections=col_list,
        bbox=bbox_list,
        datetime_range=datetime,
        limit=limit,
    )

    return {
        "type": "FeatureCollection",
        "numberMatched": len(results),
        "numberReturned": len(results),
        "features": results,
        "context": {"source": "federation"},
    }


# --- Processing ---

@app.get("/process/operations")
def list_operations():
    """List available processing operations."""
    return {"operations": processor.list_operations()}


@app.post("/process")
def process_item(
    item_id: str = Query(None, description="Source STAC item ID (single item)"),
    items: str = Query(None, description="Comma-separated item IDs (multi-item, e.g. B04,B08)"),
    operation: str = Query(..., description="Operation: ndvi, ndwi, ndsi, evi, cloud_mask, true_color, band_math"),
    output_collection: str = Query(None),
    output_item_id: str = Query(None),
    expression: str = Query("", description="Band math expression"),
):
    """Process STAC item(s) with a built-in operation."""
    if items:
        ids = [i.strip() for i in items.split(",")]
    elif item_id:
        ids = item_id
    else:
        raise HTTPException(400, "Provide item_id or items parameter")

    try:
        result_item = processor.process(
            item_id=ids,
            operation=operation,
            output_collection=output_collection,
            output_item_id=output_item_id,
            expression=expression,
        )
        return {
            "status": "processed",
            "operation": operation,
            "source": ids,
            "result_item": result_item.id,
            "result_collection": result_item.collection,
            "chunks": len(result_item.chunk_hashes),
            "bands": result_item.properties.get("earthgrid:band_names", []),
            "description": result_item.properties.get("earthgrid:description", ""),
        }
    except (ValueError, KeyError) as e:
        raise HTTPException(400, str(e))
