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

app = FastAPI(
    title="EarthGrid Node",
    version=__version__,
    description="Distributed satellite data storage and access",
)

# Initialize components
chunk_store = ChunkStore(settings.store_path)
catalog = Catalog(settings.catalog_path)
federation = Federation(settings.peers)


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
        "item_count": summary["item_count"],
        "collections": summary["collections"],
        "peers": len(federation.peers),
    }


@app.get("/health")
def health():
    return {"status": "ok"}


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
