"""EarthGrid Node — FastAPI application."""
from __future__ import annotations
import asyncio
import shutil
from pathlib import Path

import httpx
from fastapi import FastAPI, UploadFile, File, HTTPException, Query, Depends, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import APIKeyHeader
import logging
import time
import json as json_module
from fastapi.responses import Response

from . import __version__
from .config import settings
from .chunk_store import ChunkStore
from .catalog import Catalog
from .federation import Federation
from .ingest import ingest_cog
from .processing import Processor
from .replication import Replicator
from .stats import StatsEngine
from .source_users import SourceUserManager
from .bandwidth import BandwidthManager
from .ratelimit import RateLimitMiddleware
from .openeo_gateway import router as openeo_router, root_router, OpenEOGateway, set_gateway, _capabilities, API_VERSION, BACKEND_VERSION
from .user_auth import UserAuth

app = FastAPI(
    title="EarthGrid Node",
    version=__version__,
    description="Distributed satellite data storage and access",
)



app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=['GET', 'POST', 'OPTIONS'],
    allow_headers=['*'],
)

# Built-in rate limiting — protects node without external config
app.add_middleware(RateLimitMiddleware, requests_per_minute=120, burst=20)

# --- Security ---
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_audit_log_path = settings.store_path.parent / "audit.jsonl"

def _audit(action: str, detail: str = "", ip: str = "", success: bool = True):
    """Append to audit log."""
    try:
        entry = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "action": action,
            "detail": detail,
            "ip": ip,
            "ok": success,
        }
        with open(_audit_log_path, "a") as f:
            f.write(json_module.dumps(entry) + "\n")
    except Exception:
        pass

def _require_write_auth(request: Request, x_api_key: str = Depends(_api_key_header)):
    """Require API key for write operations."""
    if not settings.api_key:
        return  # no key configured = open (backward compatible)
    if x_api_key != settings.api_key:
        _audit("auth_fail", "write", ip=request.client.host if request.client else "", success=False)
        raise HTTPException(401, "Invalid or missing API key")

def _require_admin_auth(request: Request, x_api_key: str = Depends(_api_key_header)):
    """Require admin key for destructive operations."""
    if not settings.admin_key:
        if not settings.api_key:
            return  # no keys configured = open
        # If api_key set but no admin_key, block destructive ops entirely
        raise HTTPException(403, "Destructive operations disabled (no admin key configured)")
    if x_api_key != settings.admin_key:
        _audit("auth_fail", "admin", ip=request.client.host if request.client else "", success=False)
        raise HTTPException(401, "Invalid or missing admin API key")


# Ensure data directories exist (native installs may not have /data)
for _p in [settings.store_path, Path(settings.catalog_path).parent,
           Path(settings.stats_db).parent, Path(settings.source_users_db).parent]:
    _p.mkdir(parents=True, exist_ok=True)

# Initialize components
chunk_store = ChunkStore(settings.store_path, limit_gb=settings.storage_limit_gb)
catalog = Catalog(settings.catalog_path)
federation = Federation(settings.peers)
processor = Processor(chunk_store, catalog)
replicator = Replicator(chunk_store, catalog)

# New architecture components
stats_engine = StatsEngine(Path(settings.stats_db))

# User authentication (network-wide)
user_auth = UserAuth(Path(settings.users_db))
_bootstrap = user_auth.ensure_admin(settings.node_name)
if _bootstrap:
    logging.getLogger("earthgrid").warning(
        f"Admin API key: {_bootstrap['api_key']} -- save this!")
source_user_mgr = SourceUserManager(
    Path(settings.source_users_db),
    encryption_key=settings.source_key,
)
bandwidth_mgr = BandwidthManager(
    max_mbps=settings.bw_limit_mbps,
    schedule=settings.bw_schedule_dict,
)
openeo_gw = OpenEOGateway(
    catalog=catalog,
    chunk_store=chunk_store,
    source_user_manager=source_user_mgr,
    stats_engine=stats_engine,
    user_auth=user_auth,
    bandwidth_manager=bandwidth_mgr,
)
set_gateway(openeo_gw)

# Include openEO routers


# --- Federation Key Exchange (auto-auth) ---
@app.post("/federation/exchange-key")
def federation_exchange_key(
    request: Request,
    body: dict = None,
):
    """Exchange API keys between nodes for automatic mutual authentication.

    When a peer node calls this, it sends its node_name, node_id, and api_key.
    We register it as a user and return our own key so the peer can do the same.
    No admin key required — nodes authenticate each other directly.
    """
    if not body or "node_name" not in body or "api_key" not in body:
        raise HTTPException(400, "Missing node_name or api_key")
    peer_name = body["node_name"]
    peer_id = body.get("node_id", "")
    peer_key = body["api_key"]
    # Register peer as a user (idempotent — skip if username exists)
    try:
        user_auth.create_user(
            username=f"node:{peer_name}",
            role="member",
            node_origin=peer_name,
        )
    except ValueError:
        pass  # already registered
    # Update the key if it changed (peer may have rotated)
    import sqlite3
    with sqlite3.connect(user_auth.db_path) as conn:
        conn.execute(
            "UPDATE users SET api_key = ?, updated_at = ? WHERE username = ?",
            (peer_key, __import__('time').time(), f"node:{peer_name}")
        )
    _audit("node_key_exchange", f"peer={peer_name} node_id={peer_id}",
           ip=request.client.host if request.client else "")
    # Return our key so the peer registers us too
    return {
        "node_name": settings.node_name,
        "node_id": settings.node_id if hasattr(settings, 'node_id') else "",
        "api_key": settings.api_key,
    }


# --- User Admin Endpoints ---
@app.post("/admin/users")
def admin_create_user(
    request: Request,
    body: dict = None,
    _: None = Depends(_require_admin_auth),
):
    """Create a new EarthGrid user. Requires admin key."""
    if not body or "username" not in body:
        raise HTTPException(400, "Missing 'username' in request body")
    try:
        user = user_auth.create_user(
            username=body["username"],
            role=body.get("role", "member"),
            node_origin=settings.node_name,
        )
        _audit("user_create", f"username={body['username']}",
               ip=request.client.host if request.client else "")
        return user
    except ValueError as e:
        raise HTTPException(409, str(e))


@app.get("/admin/users")
def admin_list_users(
    request: Request,
    _: None = Depends(_require_admin_auth),
):
    """List all EarthGrid users. Requires admin key."""
    return {"users": user_auth.list_users(include_inactive=True)}


@app.delete("/admin/users/{user_id}")
def admin_delete_user(
    user_id: str,
    request: Request,
    _: None = Depends(_require_admin_auth),
):
    """Deactivate an EarthGrid user. Requires admin key."""
    if user_auth.delete_user(user_id):
        _audit("user_delete", f"user_id={user_id}",
               ip=request.client.host if request.client else "")
        return {"status": "deactivated", "user_id": user_id}
    raise HTTPException(404, "User not found")


# --- Federation User Sync ---
@app.get("/federation/users")
def federation_export_users(
    request: Request,
    _: None = Depends(_require_write_auth),
):
    """Export user list for federation sync. Requires API key."""
    return {"users": user_auth.export_users()}


@app.post("/federation/users")
def federation_import_users(
    request: Request,
    body: dict = None,
    _: None = Depends(_require_write_auth),
):
    """Import users from another node. Requires API key."""
    if not body or "users" not in body:
        raise HTTPException(400, "Missing 'users' in request body")
    result = user_auth.import_users(body["users"])
    _audit("user_sync", f"added={result['added']} updated={result['updated']}",
           ip=request.client.host if request.client else "")
    return result

app.include_router(openeo_router)   # legacy /openeo/* routes
app.include_router(root_router)     # openEO API v1.2.0 root-level routes


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
                    "can_source": source_user_mgr.list_users() != [],
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
        # Self-register this node with its own beacon registry
        try:
            summary = catalog.summary()
            await registry.register(
                node_id=settings.node_id,
                node_name=settings.node_name,
                url=settings.public_url or f"http://{settings.host}:{settings.port}",
                collections=summary["collections"],
                item_count=summary["item_count"],
                chunk_count=chunk_store.chunk_count,
                chunks_bytes=chunk_store.total_bytes,
                can_source=source_user_mgr.list_users() != [],
            )
        except Exception as e:
            logger.warning(f"Self-registration with local beacon failed: {e}")
        if settings.beacon_peers:
            for url in settings.beacon_peers:
                await registry.add_peer_beacon(url)
            asyncio.create_task(_beacon_sync_loop())


# --- Stats Middleware ---

@app.middleware("http")
async def stats_middleware(request: Request, call_next):
    """Track chunk access and bandwidth in stats engine."""
    response = await call_next(request)
    try:
        path = request.url.path
        # Track chunk downloads
        if path.startswith("/chunks/") and request.method == "GET" and response.status_code == 200:
            sha = path.split("/chunks/")[1]
            stats_engine.record_chunk_access(sha, access_type="read", node_id=settings.node_id)
        # Track STAC searches
        elif path.startswith("/stac/search") and response.status_code == 200:
            collections = request.query_params.get("collections", "")
            for c in collections.split(","):
                if c.strip():
                    stats_engine.record_collection_access(c.strip(), access_type="query")
        # Track user downloads (served by EarthGrid)
        elif path.startswith("/download/") and response.status_code == 200:
            parts = path.split("/download/")[1].split("/")
            if len(parts) >= 2:
                stats_engine.record_collection_access(parts[0], access_type="download")
                client_ip = request.headers.get("x-real-ip") or request.client.host or ""
                content_length = int(response.headers.get("content-length", 0))
                stats_engine.record_download(
                    origin="user", collection_id=parts[0],
                    item_id=parts[1] if len(parts) > 1 else "",
                    bytes_transferred=content_length, client_ip=client_ip)
    except Exception:
        pass
    return response


# --- Node Info ---

@app.get("/node-info")
def node_info_detail():
    """EarthGrid node identity and status (full detail endpoint)."""
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
        "total_area_km2": summary["total_area_km2"],
        "collections": summary["collections"],
        "peers": len(federation.peers),
        "redundancy_index": _redundancy_index(),
        "beacon": settings.also_beacon,
        "openeo": True,
        "bandwidth": bandwidth_mgr.status(),
        "max_download_volume_gb": settings.max_download_volume_gb,
    }


@app.get("/")
def node_info(request: Request):
    """Root endpoint — openEO capabilities merged with EarthGrid node info.

    The openEO Python/R clients call GET / to discover the api_version field.
    EarthGrid-specific clients can use /node-info for full node details.
    """
    base = str(request.base_url).rstrip("/")
    caps = _capabilities(base)
    # Embed EarthGrid-specific metadata (non-breaking additions to openEO response)
    summary = catalog.summary()
    caps["earthgrid"] = {
        "name": "EarthGrid",
        "version": __version__,
        "node_id": settings.node_id,
        "node_name": settings.node_name,
        "chunks": chunk_store.chunk_count,
        "chunks_bytes": chunk_store.total_bytes,
        "item_count": summary["item_count"],
        "collections": summary["collections"],
    }
    return caps


def _redundancy_index() -> float:
    """Average replication factor: total chunks across all nodes / unique chunks.

    1.0 = no redundancy, 2.0 = every chunk on 2 nodes, etc.
    """
    try:
        local_chunks = chunk_store.chunk_count
        if local_chunks == 0:
            return 0.0
        total = local_chunks
        for peer in federation.peers:
            if hasattr(peer, "chunk_count") and peer.chunk_count:
                total += peer.chunk_count
        # Unique chunks = local (beacon has all), so index = total / local
        return round(total / local_chunks, 2) if local_chunks > 0 else 1.0
    except Exception:
        return 1.0


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/stats/coverage")
def stats_coverage():
    """Spatial coverage in km² per sensor collection."""
    cov = catalog.coverage_by_collection()
    # Only original sensor data — derived collections are excluded
    sensors = {col: info for col, info in cov["collections"].items() if "_derived" not in col}
    return {
        "total_area_km2": sum(s["area_km2"] for s in sensors.values()),
        "sensors": sensors,
    }


@app.get("/stats/requests")
def stats_requests():
    """Total km² queried (based on search/access bbox queries)."""
    try:
        import sqlite3
        with sqlite3.connect(stats_engine.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT query_bbox FROM collection_access WHERE query_bbox != ''"
            ).fetchall()
        total_km2 = 0.0
        for r in rows:
            try:
                parts = [float(x) for x in r["query_bbox"].split(",")]
                if len(parts) == 4:
                    w, s, e, n = parts
                    # If values look like UTM meters (> 1000)
                    if abs(w) > 1000 or abs(e) > 1000:
                        total_km2 += abs((e - w) * (n - s)) / 1e6
                    else:
                        # WGS84 degrees — rough conversion
                        import math
                        lat_mid = math.radians((n + s) / 2)
                        km_per_deg_lon = 111.32 * math.cos(lat_mid)
                        km_per_deg_lat = 111.32
                        total_km2 += abs((e - w) * km_per_deg_lon * (n - s) * km_per_deg_lat)
            except (ValueError, ZeroDivisionError):
                continue
        return {
            "total_requests": len(rows),
            "total_km2_queried": round(total_km2),
        }
    except Exception:
        return {"total_requests": 0, "total_km2_queried": 0}


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
        "bandwidth": bandwidth_mgr.status(),
        "access_stats": stats_engine.overview(),
    }


# --- Stats API ---

@app.get("/stats/access")
def stats_access_overview():
    """Full access stats overview (top collections, chunk heatmap, replication advice)."""
    return stats_engine.overview()

@app.get("/stats/bandwidth")
def stats_bandwidth(hours: int = Query(24)):
    """Bandwidth usage summary."""
    return stats_engine.bandwidth_summary(period_hours=hours)

@app.get("/stats/replication")
def stats_replication_advice():
    """Replication factor advice based on access patterns."""
    return stats_engine.replication_advice()

@app.get("/stats/downloads")
def stats_downloads(days: int = Query(30)):
    """Download statistics: source (from CDSE/WEkEO) vs user (served by EarthGrid)."""
    return stats_engine.download_stats(period_hours=days * 24)


# --- Source Users API ---

# Source user management removed from API — credentials are local-only.
# Use CLI: earthgrid users add/list/remove


# --- Bandwidth API ---

@app.get("/bandwidth")
def bandwidth_status():
    """Current bandwidth allocation status."""
    return bandwidth_mgr.status()


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

@app.post("/ingest", dependencies=[Depends(_require_write_auth)])
async def ingest_file(
    file: UploadFile = File(...),
    collection: str = Query("default"),
    item_id: str = Query(None),
):
    """Upload and ingest a COG/GeoTIFF file."""
    # Check download volume limit
    if settings.max_download_volume_gb > 0:
        total_gb = chunk_store.total_bytes / (1024**3)
        if total_gb >= settings.max_download_volume_gb:
            raise HTTPException(507, f"Download volume limit reached ({settings.max_download_volume_gb} GB)")

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

        # Notify beacon to push new item to registered nodes
        asyncio.create_task(_notify_peers_new_item(item))

        _audit("ingest", f"{item.id} ({len(item.chunk_hashes)} chunks)")

        return {
            "status": "ingested",
            "item_id": item.id,
            "collection": item.collection,
            "chunks": len(item.chunk_hashes),
            "bbox": item.bbox,
        }
    finally:
        tmp_path.unlink(missing_ok=True)




async def _notify_peers_new_item(item):
    """Notify all registered beacon nodes about a new item so they can auto-sync."""
    if not settings.also_beacon:
        return
    try:
        from .beacon import registry
        nodes = list(registry.nodes.values())
        if not nodes:
            return
        import logging
        log = logging.getLogger("earthgrid")
        log.info(f"Notifying {len(nodes)} peers about new item: {item.id}")
        async with httpx.AsyncClient(timeout=10) as client:
            for node in nodes:
                if node.url and "0.0.0.0" not in node.url:
                    try:
                        await client.post(
                            f"{node.url.rstrip('/')}/sync-item",
                            params={
                                "source_url": f"http://{settings.host}:{settings.port}",
                                "item_id": item.id,
                                "collection": item.collection,
                            },
                        )
                        log.info(f"Notified {node.node_name} ({node.url})")
                    except Exception as e:
                        log.warning(f"Could not notify {node.node_name}: {e}")
    except Exception as e:
        import logging
        logging.getLogger("earthgrid").warning(f"Peer notification failed: {e}")


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


@app.get('/chunk-map/{collection_id}/{item_id}')
def chunk_map(
    collection_id: str,
    item_id: str,
    bands: str = Query(None, description='Comma-separated band names to include'),
):
    """Return chunk map for parallel multi-node download.

    Clients use this to fetch chunks from multiple nodes simultaneously.
    Returns chunk hashes with metadata needed for reassembly.
    """
    item = catalog.get_item(item_id)
    if not item:
        raise HTTPException(404, f'Item {item_id} not found')

    props = item.properties
    chunk_format = props.get('earthgrid:chunk_format', 'legacy')

    band_list = [b.strip() for b in bands.split(',')] if bands else None

    if chunk_format == 'band-level':
        # Band-level: chunk_hashes = {"B04": ["sha1", ...], "B08": [...]}
        all_hashes = item.chunk_hashes  # dict
        if band_list:
            selected = {b: h for b, h in all_hashes.items() if b in band_list}
        else:
            selected = all_hashes
        total = sum(len(h) for h in selected.values())
        chunks_response = selected
    elif chunk_format == 'spatial-tile':
        # Spatial tile: chunk_hashes = ["sha1", "sha2", ...] — all bands per tile
        total = len(item.chunk_hashes)
        chunks_response = item.chunk_hashes
    else:
        # Legacy: flat list
        total = len(item.chunk_hashes) if isinstance(item.chunk_hashes, list) else 0
        chunks_response = item.chunk_hashes

    return {
        'item_id': item_id,
        'collection': collection_id,
        'format': chunk_format,
        'tile_size': props.get('earthgrid:tile_size', 512),
        'tile_cols': props.get('earthgrid:tile_cols', 1),
        'tile_rows': props.get('earthgrid:tile_rows', 1),
        'width': props.get('earthgrid:width'),
        'height': props.get('earthgrid:height'),
        'dtype': props.get('earthgrid:dtype'),
        'crs': props.get('earthgrid:crs'),
        'bands': props.get('earthgrid:band_names', []),
        'total_chunks': total,
        'chunks': chunks_response,
        'node_url': settings.public_url or f'http://{settings.host}:{settings.port}',
    }


@app.get("/download/{collection_id}/{item_id}")
def download_file(
    collection_id: str,
    item_id: str,
    bands: str = Query(None, description="Comma-separated band names (e.g. B04,B08). Omit for all."),
):
    """Download a reconstructed GeoTIFF. Band-selective: request only the bands you need."""
    try:
        from .reconstruct import reconstruct_geotiff
    except ImportError:
        raise HTTPException(501, "Reconstruction requires rasterio (pip install earthgrid[geo])")

    band_list = [b.strip() for b in bands.split(",")] if bands else None

    try:
        data = reconstruct_geotiff(item_id, collection_id, catalog, chunk_store, bands=band_list)
    except FileNotFoundError:
        raise HTTPException(404, f"Item {item_id} not found in {collection_id}")

    # Track in stats
    stats_engine.record_collection_access(collection_id, access_type="download")

    suffix = f"_{'_'.join(band_list)}" if band_list else ""
    return Response(
        content=data,
        media_type="image/tiff",
        headers={
            "Content-Disposition": f'attachment; filename="{item_id}{suffix}.tif"',
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
    synced = await federation.sync_all(
        local_node_name=settings.node_name,
        local_node_id=getattr(settings, 'node_id', ''),
        local_api_key=settings.api_key,
        user_auth=user_auth,
    )
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



@app.post("/sync-item", dependencies=[Depends(_require_write_auth)])
async def sync_item_from_peer(
    source_url: str = Query(..., description="URL of the source node"),
    item_id: str = Query(...),
    collection: str = Query(""),
):
    """Receive notification about a new item and auto-sync it."""
    import logging
    log = logging.getLogger("earthgrid")
    log.info(f"Auto-sync triggered: {item_id} from {source_url}")

    try:
        # Fetch item manifest from source
        async with httpx.AsyncClient(timeout=30) as client:
            r = await client.get(f"{source_url.rstrip('/')}/stac/collections/{collection}/items/{item_id}")
            if r.status_code != 200:
                return {"status": "skipped", "reason": "item not found on source"}
            item_data = r.json()

        # Check if we already have this item
        existing = catalog.get_item(item_id)
        if existing:
            return {"status": "skipped", "reason": "already have this item"}

        # Sync chunks from source
        chunk_hashes = item_data.get("properties", {}).get("earthgrid:chunk_hashes", [])
        synced = 0
        for h in chunk_hashes:
            if not chunk_store.has_chunk(h):
                try:
                    r = await client.get(f"{source_url.rstrip('/')}/chunks/{h}")
                    if r.status_code == 200:
                        # Verify chunk integrity: SHA-256 hash must match
                        import hashlib
                        actual_hash = hashlib.sha256(r.content).hexdigest()
                        if actual_hash == h:
                            chunk_store.store_chunk(h, r.content)
                            synced += 1
                        else:
                            log.warning(f"INTEGRITY VIOLATION: chunk {h[:16]}... hash mismatch! Expected {h[:16]}, got {actual_hash[:16]}. Rejecting.")
                except Exception:
                    pass

        # Register item in local catalog
        catalog.register_from_stac(item_data)
        log.info(f"Auto-synced {item_id}: {synced}/{len(chunk_hashes)} chunks")
        return {"status": "synced", "item_id": item_id, "chunks_synced": synced, "chunks_total": len(chunk_hashes)}

    except Exception as e:
        log.error(f"Auto-sync failed for {item_id}: {e}")
        return {"status": "error", "error": str(e)}




@app.get("/verify/{item_id}")
def verify_item_integrity(item_id: str):
    """Verify all chunks of an item against their SHA-256 hashes."""
    import hashlib
    item = catalog.get_item(item_id)
    if not item:
        raise HTTPException(404, f"Item {item_id} not found")

    results = {"item_id": item_id, "total": 0, "valid": 0, "corrupted": 0, "missing": 0, "details": []}
    for h in item.chunk_hashes:
        results["total"] += 1
        data = chunk_store.get_chunk(h)
        if data is None:
            results["missing"] += 1
            results["details"].append({"hash": h[:16], "status": "missing"})
        else:
            actual = hashlib.sha256(data).hexdigest()
            if actual == h:
                results["valid"] += 1
            else:
                results["corrupted"] += 1
                results["details"].append({"hash": h[:16], "status": "corrupted", "expected": h[:16], "actual": actual[:16]})

    results["integrity"] = "OK" if results["corrupted"] == 0 and results["missing"] == 0 else "FAILED"
    return results




@app.get("/audit", dependencies=[Depends(_require_admin_auth)])
def get_audit_log(limit: int = Query(50, description="Number of recent entries")):
    """View audit log (admin only)."""
    if not _audit_log_path.exists():
        return {"entries": []}
    lines = _audit_log_path.read_text().strip().split("\n")
    entries = []
    for line in lines[-limit:]:
        try:
            entries.append(json_module.loads(line))
        except Exception:
            pass
    return {"entries": entries}


@app.get("/process/operations")
def list_operations():
    """List available processing operations."""
    return {"operations": processor.list_operations()}


@app.post("/process", dependencies=[Depends(_require_write_auth)])
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
        result = processor.process(
            item_id=ids,
            operation=operation,
            output_collection=output_collection,
            output_item_id=output_item_id,
            expression=expression,
        )
        # Processing results are ephemeral — returned directly, not stored in grid.
        # Only original sensor data belongs in the grid.
        return {
            "status": "processed",
            "operation": operation,
            "source": ids,
            "bands": result.band_names,
            "description": result.description,
            "shape": list(result.data.shape),
            "dtype": str(result.data.dtype),
        }
    except (ValueError, KeyError) as e:
        raise HTTPException(400, str(e))


# --- Replication ---

@app.get("/replicate/items")
def replicate_items(
    collection: str = Query(None, description="Filter by collection"),
    limit: int = Query(10000, le=100000),
):
    """Export items with chunk hashes for replication.

    This is what remote nodes call to sync catalog + chunk lists.
    """
    items = catalog.search(
        collections=[collection] if collection else None,
        limit=limit,
    )
    return {
        "node_id": settings.node_id,
        "node_name": settings.node_name,
        "items": [i.to_stac(include_chunks=True) for i in items],
    }


@app.post("/sync")
async def trigger_sync(
    peer_url: str = Query(..., description="Peer URL to sync from"),
    collections: str = Query(None, description="Comma-separated collection filter"),
    max_items: int = Query(0, description="Max items to sync (0=all)"),
    dry_run: bool = Query(False, description="Only report, don't download"),
):
    """Pull catalog and chunks from a remote peer."""
    col_list = [c.strip() for c in collections.split(",")] if collections else None
    result = await replicator.sync_from_peer(
        peer_url=peer_url,
        collections=col_list,
        max_items=max_items,
        dry_run=dry_run,
    )
    return result
