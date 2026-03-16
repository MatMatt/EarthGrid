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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("earthgrid")
import time
import json as json_module
from fastapi.responses import Response, HTMLResponse

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
from .node_identity import NodeIdentity
from .gamification import GamificationEngine
from .gamification_endpoints import router as gamification_router, set_engine as set_gamification_engine

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


def _is_local_ip(ip: str) -> bool:
    """Check if IP is localhost or Docker bridge."""
    return ip in ("127.0.0.1", "::1", "localhost") or ip.startswith("172.")


def _is_lan_ip(ip: str) -> bool:
    """Check if IP is localhost, Docker bridge, or private LAN."""
    if _is_local_ip(ip):
        return True
    return ip.startswith("192.168.") or ip.startswith("10.") or ip.startswith("172.") or ip.startswith("fd")


def _require_write_auth(request: Request, x_api_key: str = Depends(_api_key_header)):
    """Require API key for write operations. Localhost is always allowed."""
    if request.client and _is_local_ip(request.client.host):
        return  # node operator
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
gamification_engine = GamificationEngine(Path(settings.stats_db).parent / "gamification.db")
set_gamification_engine(gamification_engine)

# User authentication (network-wide)
user_auth = UserAuth(Path(settings.users_db))
# Node identity (Ed25519 keypair)
node_identity = NodeIdentity(Path(settings.identity_key_path))
logging.getLogger("earthgrid").info(f"Node public key: {node_identity.public_key_b64}")

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
    """Exchange API keys between nodes using Ed25519 signatures.

    Peer sends a signed payload (node_name, node_id, api_key, public_key,
    timestamp, signature). We verify the signature, register the peer,
    and return our own signed payload.

    No shared secrets needed — authenticity proven by cryptographic signature.
    Replay attacks prevented by timestamp check (5 min window).
    """
    if not body or "signature" not in body:
        raise HTTPException(400, "Missing signed payload. Ed25519 signature required.")
    # Verify the peer's signature
    if not NodeIdentity.verify_exchange(body):
        _audit("key_exchange_rejected", f"peer={body.get('node_name','?')} invalid_signature",
               ip=request.client.host if request.client else "", success=False)
        raise HTTPException(403, "Invalid signature — key exchange rejected")
    peer_name = body["node_name"]
    peer_id = body.get("node_id", "")
    peer_key = body["api_key"]
    peer_pubkey = body["public_key"]
    # Register peer as a user (idempotent)
    import sqlite3
    try:
        user_auth.create_user(
            username=f"node:{peer_name}",
            role="member",
            node_origin=peer_name,
        )
    except ValueError:
        pass  # already registered
    # Update key + store public key for future verification
    with sqlite3.connect(user_auth.db_path) as conn:
        conn.execute(
            "UPDATE users SET api_key = ?, updated_at = ? WHERE username = ?",
            (peer_key, __import__('time').time(), f"node:{peer_name}")
        )
    _audit("key_exchange_ok", f"peer={peer_name} pubkey={peer_pubkey[:16]}...",
           ip=request.client.host if request.client else "")
    # Return our signed payload
    return node_identity.sign_exchange(
        node_name=settings.node_name,
        node_id=getattr(settings, 'node_id', ''),
        api_key=settings.api_key,
    )


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
app.include_router(gamification_router)


# --- Beacon Registration ---

async def _register_with_beacon():
    """Register this node with the configured beacon (or local beacon if also_beacon)."""
    beacon = settings.beacon_url
    # If we ARE a beacon, register with ourselves locally
    if settings.also_beacon:
        beacon = f"http://localhost:{settings.port}"
    if not beacon:
        return
    try:
        summary = catalog.summary()
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{beacon.rstrip('/')}/register",
                params={
                    "node_id": settings.node_id,
                    "node_name": settings.node_name,
                    "url": settings.public_url or f"http://{settings.host}:{settings.port}",
                    "collections": ",".join(summary["collections"]),
                    "item_count": summary["item_count"],
                    "chunk_count": chunk_store.chunk_count,
                    "chunks_bytes": chunk_store.total_bytes,
                    "can_source": source_user_mgr.list_users() != [],
                    "preferred_collections": settings.preferred_collections,
                    "preferred_bbox": settings.preferred_bbox,
                    "replication_factor": settings.replication_factor,
                    "storage_limit_gb": settings.storage_limit_gb,
        "auto_update": settings.auto_update,
                },
            )
    except Exception as e:
        import logging
        logging.getLogger("earthgrid").warning(f"Failed to register with beacon: {e}")


async def _beacon_heartbeat_loop():
    """Send periodic heartbeats to the beacon and discover peers."""
    beacon = settings.beacon_url
    if settings.also_beacon:
        beacon = f"http://localhost:{settings.port}"
    if not beacon:
        return
    log = logging.getLogger("earthgrid")
    while True:
        await asyncio.sleep(60)  # every 60s
        try:
            summary = catalog.summary()
            async with httpx.AsyncClient(timeout=10) as client:
                # Get uptime
                _uptime_s = 0
                try:
                    with open("/proc/uptime") as _f:
                        _uptime_s = int(float(_f.read().split()[0]))
                except Exception:
                    pass

                # Get cached speed (measured periodically)
                _dl_mbps = getattr(app.state, '_download_mbps', 0.0)
                _ul_mbps = getattr(app.state, '_upload_mbps', 0.0)

                await client.post(
                    f"{beacon.rstrip('/')}/heartbeat",
                    params={
                        "node_id": settings.node_id,
                        "collections": ",".join(summary["collections"]),
                        "item_count": summary["item_count"],
                        "chunk_count": chunk_store.chunk_count,
                        "chunks_bytes": chunk_store.total_bytes,
                        "storage_limit_gb": settings.storage_limit_gb,
                        "uptime_seconds": _uptime_s,
                        "download_mbps": _dl_mbps,
                        "upload_mbps": _ul_mbps,
                    },
                )
                # Report items for replication tracking
                try:
                    item_ids = [item.id for item in catalog.search(limit=10000)]
                    if item_ids:
                        await client.post(
                            f"{settings.beacon_url.rstrip('/')}/replication/report",
                            json={"node_id": settings.node_id, "item_ids": item_ids},
                            timeout=10,
                        )
                except Exception:
                    pass  # non-critical

                # --- Auto Peer Discovery from Beacon ---
                try:
                    resp = await client.get(
                        f"{settings.beacon_url.rstrip('/')}/nodes",
                        timeout=10,
                    )
                    if resp.status_code == 200:
                        nodes = resp.json().get("nodes", [])
                        discovered = 0
                        for node in nodes:
                            peer_url = node.get("url", "")
                            peer_id = node.get("node_id", "")
                            peer_name = node.get("node_name", "")
                            # Skip self
                            if peer_id == settings.node_id or not peer_url:
                                continue
                            # Beacon can also be a data node — include as peer
                            # Add/update peer in federation
                            peer = federation.add_peer(
                                url=peer_url,
                                node_id=peer_id,
                                node_name=peer_name,
                            )
                            peer.collections = node.get("collections", [])
                            peer.item_count = node.get("item_count", 0)
                            discovered += 1
                        if discovered > 0:
                            log.info(f"Peer discovery: {discovered} peers from beacon")
                except Exception:
                    pass  # beacon may not support /nodes yet
        except Exception:
            pass


# --- Auto-replication ---
async def _auto_replication_loop():
    """Periodically sync catalog + chunks from peer nodes."""
    log = logging.getLogger("earthgrid")
    if not settings.beacon_url and not settings.also_beacon:
        log.info("No beacon configured, auto-replication disabled")
        return
    await asyncio.sleep(30)  # wait for beacon registration
    log.info("Auto-replication started (every 5 min)")
    while True:
        try:
            # For beacon+node combos, query ourselves locally
            if settings.also_beacon:
                beacon = f"http://localhost:{settings.port}"
            else:
                beacon = settings.beacon_url
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.get(f"{beacon.rstrip('/')}/nodes")
                if resp.status_code != 200:
                    log.warning(f"Auto-replication: beacon returned {resp.status_code}")
                    await asyncio.sleep(300)
                    continue
                nodes = resp.json().get("nodes", [])
                peers = [n for n in nodes
                         if n.get("node_id") != settings.node_id
                         and n.get("url") and n.get("alive")]
                if not peers:
                    log.info(f"Auto-replication: no peers found ({len(nodes)} nodes, none eligible)")
                    await asyncio.sleep(300)
                    continue
                for node in peers:
                    peer_url = node["url"]
                    # Check storage limit (stop at 90%)
                    if settings.storage_limit_gb > 0 and chunk_store.total_bytes >= (settings.storage_limit_gb * 1024**3 * 0.9):
                        log.info("Storage >90% full, skipping auto-replication")
                        break
                    try:
                        log.info(f"Auto-replication: syncing from {node.get('node_name','?')} ({peer_url})")
                        result = await replicator.sync_from_peer(
                            peer_url=peer_url,
                            max_items=50,
                        )
                        if result["items_synced"] > 0:
                            log.info(
                                f"Replicated from {node.get('node_name','?')}: "
                                f"{result['items_synced']} items, "
                                f"{result['chunks_downloaded']} chunks, "
                                f"{result['bytes_downloaded'] / 1024**2:.1f} MB")
                        elif result["errors"]:
                            log.warning(f"Replication errors from {node.get('node_name','?')}: {result['errors'][:3]}")
                        else:
                            log.info(f"Auto-replication from {node.get('node_name','?')}: already in sync")
                    except Exception as e:
                        log.warning(f"Replication from {peer_url} failed: {e}")
        except Exception as e:
            log.warning(f"Auto-replication cycle failed: {e}")
        await asyncio.sleep(300)  # every 5 minutes




async def _discover_peers_from_beacon():
    """Query beacon for other nodes and add them as peers."""
    if not settings.beacon_url:
        return
    log = logging.getLogger("earthgrid")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.get(
                f"{settings.beacon_url.rstrip('/')}/nodes",
                timeout=10,
            )
            if resp.status_code == 200:
                nodes = resp.json().get("nodes", [])
                discovered = 0
                for node in nodes:
                    peer_url = node.get("url", "")
                    peer_id = node.get("node_id", "")
                    peer_name = node.get("node_name", "")
                    if peer_id == settings.node_id or not peer_url:
                        continue
                    # Beacon may also be a data node — included as peer
                    peer = federation.add_peer(
                        url=peer_url, node_id=peer_id, node_name=peer_name,
                    )
                    peer.collections = node.get("collections", [])
                    peer.item_count = node.get("item_count", 0)
                    discovered += 1
                if discovered > 0:
                    log.info(f"Startup peer discovery: {discovered} peers from beacon")
    except Exception as e:
        log.debug(f"Peer discovery failed: {e}")


async def _speed_measure_loop():
    """Measure internet speed periodically (every 6h) using a lightweight test."""
    import time as _time
    log = logging.getLogger("earthgrid")
    await asyncio.sleep(30)  # wait for startup
    while True:
        try:
            # Lightweight speed test: download a ~10MB file and measure throughput
            test_url = "https://speed.hetzner.de/10MB.bin"
            start = _time.monotonic()
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
                resp = await client.get(test_url)
                elapsed = _time.monotonic() - start
                if resp.status_code == 200 and elapsed > 0:
                    size_mb = len(resp.content) / 1024 / 1024
                    dl_mbps = round((size_mb * 8) / elapsed, 1)
                    app.state._download_mbps = dl_mbps
                    log.info(f"Speed test: {dl_mbps} Mbps download ({size_mb:.1f} MB in {elapsed:.1f}s)")

                # Upload test: POST ~1MB to httpbin (or just estimate from download)
                # For now, estimate upload as ~30% of download (typical asymmetric)
                app.state._upload_mbps = round(dl_mbps * 0.3, 1)
        except Exception as e:
            log.debug(f"Speed test failed: {e}")
        await asyncio.sleep(6 * 3600)  # every 6 hours

@app.on_event("startup")
async def startup():
    # Auto-register self in gamification (all nodes participate by default)
    try:
        from .gamification_endpoints import engine
        engine.ensure_node_registered(settings.node_id, node_name=settings.node_name)
        engine.record_heartbeat(
            settings.node_id,
            peers_count=len(federation.peers),
            uptime_seconds=0,
            storage_pledged_gb=settings.storage_limit_gb,
        )
    except Exception as e:
        import logging
        logging.getLogger("earthgrid").debug(f"Gamification self-register: {e}")

    await _register_with_beacon()
    await _discover_peers_from_beacon()
    asyncio.create_task(_beacon_heartbeat_loop())
    asyncio.create_task(_speed_measure_loop())
    asyncio.create_task(_auto_replication_loop())

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
            _self_node = registry.nodes.get(settings.node_id)
            if _self_node:
                _self_node.storage_limit_gb = settings.storage_limit_gb
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


def _system_info() -> dict:
    """CPU, memory, uptime for dashboard."""
    import os
    info = {}
    try:
        load1, load5, load15 = os.getloadavg()
        info["load_avg"] = [round(load1, 2), round(load5, 2), round(load15, 2)]
        info["cpu_count"] = os.cpu_count() or 1
        info["cpu_pct"] = round(load1 / (os.cpu_count() or 1) * 100, 1)
    except Exception:
        info["load_avg"] = [0, 0, 0]
        info["cpu_count"] = os.cpu_count() or 1
        info["cpu_pct"] = 0
    try:
        with open("/proc/meminfo") as f:
            mem = {}
            for line in f:
                parts = line.split()
                if parts[0] in ("MemTotal:", "MemAvailable:"):
                    mem[parts[0].rstrip(":")] = int(parts[1]) * 1024
            info["mem_total"] = mem.get("MemTotal", 0)
            info["mem_available"] = mem.get("MemAvailable", 0)
            info["mem_used"] = info["mem_total"] - info["mem_available"]
            info["mem_pct"] = round(info["mem_used"] / info["mem_total"] * 100, 1) if info["mem_total"] > 0 else 0
    except Exception:
        pass
    try:
        with open("/proc/uptime") as f:
            info["uptime_seconds"] = int(float(f.read().split()[0]))
    except Exception:
        pass
    return info


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
        "collections": {c: {"items": catalog.item_count(c)} for c in summary["collections"]},
        "peers": len(federation.peers),
        "redundancy_index": _redundancy_index(),
        "beacon": settings.also_beacon,
        "openeo": True,
        "bandwidth": bandwidth_mgr.status(),
        "download_mbps": getattr(app.state, '_download_mbps', 0.0),
        "upload_mbps": getattr(app.state, '_upload_mbps', 0.0),
        "max_download_volume_gb": settings.max_download_volume_gb,
        "system": _system_info(),
        "auto_update": settings.auto_update,
    }




@app.post("/resize", dependencies=[Depends(_require_admin_auth)])
async def resize_storage(size_gb: float = Query(..., gt=0)):
    """Resize storage limit."""
    old = settings.storage_limit_gb
    settings.storage_limit_gb = size_gb
    chunk_store.limit_gb = size_gb
    # Update config.json if it exists
    config_file = Path.home() / ".earthgrid" / "config.json"
    if config_file.exists():
        import json as _json
        cfg = _json.loads(config_file.read_text())
        cfg["storage_limit_gb"] = size_gb
        config_file.write_text(_json.dumps(cfg, indent=2) + "\n")
    return {"old_gb": old, "new_gb": size_gb, "status": "resized"}

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
        "storage_limit_gb": settings.storage_limit_gb,
        "item_count": summary["item_count"],
        "total_area_km2": summary["total_area_km2"],
        "collections": summary["collections"],
        "peers": len(federation.peers),
        "redundancy_index": _redundancy_index(),
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



def _require_grid_auth(request: Request, x_api_key: str = Depends(_api_key_header)):
    """Allow fetch from admin key OR LAN/grid peers."""
    # Admin key always works
    if settings.admin_key and x_api_key == settings.admin_key:
        return
    # Check all possible client IPs (direct, behind proxy)
    candidate_ips = [request.client.host if request.client else ""]
    candidate_ips.append(request.headers.get("x-real-ip", ""))
    candidate_ips.append(request.headers.get("x-forwarded-for", "").split(",")[0].strip())
    if any(_is_lan_ip(ip) for ip in candidate_ips if ip):
        return
    # No keys configured = open
    if not settings.admin_key and not settings.api_key:
        return
    raise HTTPException(401, "Grid fetch requires admin key or LAN access")

@app.post("/fetch", dependencies=[Depends(_require_grid_auth)])
async def remote_fetch(
    bbox: str = Query(...),
    start: str = Query(None),
    end: str = Query(None),
    cloud: float = Query(30.0),
    bands: str = Query(None),
    limit: int = Query(5),
    source: str = Query("element84"),
    collection: str = Query("sentinel-2-l2a"),
):
    """Accept a fetch request from a remote node (grid delegation)."""
    from .element84 import fetch_and_ingest_element84
    
    bbox_list = [float(x) for x in bbox.split(",")]
    band_list = [b.strip() for b in bands.split(",")] if bands else None
    
    results = await fetch_and_ingest_element84(
        chunk_store=chunk_store,
        catalog=catalog,
        bbox=bbox_list,
        start_date=start,
        end_date=end,
        cloud_cover=cloud,
        bands=band_list,
        limit=limit,
        earthgrid_collection=collection,
        distribute=False,  # Never re-delegate from a delegated fetch
    )
    
    ingested = [r for r in results if r.get("item_id") and not r.get("skipped")]
    errors = [r for r in results if r.get("error")]
    return {
        "status": "ok",
        "ingested": len(ingested),
        "errors": len(errors),
        "details": results,
    }

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/ui", response_class=HTMLResponse)
async def node_ui(request: Request):
    """Serve the EarthGrid Node management UI."""
    # Restrict UI: localhost for regular nodes, LAN for beacons
    client_ip = request.client.host if request.client else ""
    if settings.also_beacon:
        if not _is_lan_ip(client_ip):
            raise HTTPException(403, "Node UI is only accessible from LAN")
    else:
        if not _is_local_ip(client_ip):
            raise HTTPException(403, "Node UI is only accessible from localhost")
    ui_path = Path(__file__).parent / "static" / "ui.html"
    if not ui_path.exists():
        return HTMLResponse("<h1>UI not found</h1>", status_code=404)
    return HTMLResponse(content=ui_path.read_text(encoding="utf-8"), headers={"Cache-Control": "no-cache, no-store, must-revalidate"})




@app.get("/dashboard", response_class=HTMLResponse)
async def public_dashboard():
    """Serve the public EarthGrid dashboard (no auth required)."""
    # Look for docs/index.html relative to project root
    for candidate in [
        Path(__file__).parent.parent / "docs" / "index.html",
        Path("/data") / "docs" / "index.html",
    ]:
        if candidate.exists():
            content = candidate.read_text(encoding="utf-8")
            # Inject API base meta tag so JS can find the API
            content = content.replace(
                '<head>',
                '<head>\n<meta name="earthgrid-api" content="">',
            )
            return HTMLResponse(content=content, headers={
                "Cache-Control": "public, max-age=300",
            })
    return HTMLResponse("<h1>Dashboard not found</h1>", status_code=404)


@app.get("/stats.json")
async def stats_json():
    """Serve generated stats.json (updated by cron, not committed to git)."""
    for candidate in [
        Path(__file__).parent.parent / "docs" / "stats.json",
        Path("/data") / "docs" / "stats.json",
    ]:
        if candidate.exists():
            import json
            data = json.loads(candidate.read_text(encoding="utf-8"))
            return data
    raise HTTPException(404, "stats.json not found")

@app.get("/peers.json")
async def peers_json():
    """Serve generated peers.json (updated by cron, not committed to git)."""
    for candidate in [
        Path(__file__).parent.parent / "docs" / "peers.json",
        Path("/data") / "docs" / "peers.json",
    ]:
        if candidate.exists():
            import json
            data = json.loads(candidate.read_text(encoding="utf-8"))
            return data
    raise HTTPException(404, "peers.json not found")

@app.get("/stats/coverage")
def stats_coverage():
    """Spatial coverage per sensor collection (network-wide if beacon)."""
    cov = catalog.coverage_by_collection()
    sensors = {col: info for col, info in cov["collections"].items() if "_derived" not in col}
    # If beacon, add items from other nodes
    if settings.also_beacon:
        try:
            from .beacon import registry
            for node in registry.get_alive_nodes():
                if node.node_id == settings.node_id:
                    continue
                for col in node.collections:
                    if col not in sensors:
                        sensors[col] = {"items": 0, "tiles": 0, "area_km2": 0}
                    sensors[col]["items"] += node.item_count
        except Exception:
            pass
    return {
        "total_area_km2": sum(s.get("area_km2", 0) for s in sensors.values()),
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






@app.get("/stats/ingest")
async def stats_ingest(period_days: int = 365):
    """Daily ingest history — data fetched from upstream sources."""
    return stats_engine.ingest_history(period_days=period_days)

@app.get("/stats/uptake")
async def stats_uptake(period_days: int = 30):
    """Anonymous uptake statistics for reporting.

    No user identification — only aggregate counts per collection,
    job type, and time period. Safe for EU Commission reporting.
    """
    return stats_engine.uptake_report(period_days=period_days)


@app.get("/stats/uptake/csv")
async def stats_uptake_csv(period_days: int = 30):
    """Download uptake stats as CSV for reporting."""
    from fastapi.responses import Response
    csv_data = stats_engine.uptake_csv(period_days=period_days)
    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="earthgrid_uptake_{period_days}d.csv"'},
    )

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



# --- Point Extraction ---

@app.get("/point/{collection_id}/{item_id}")
async def point_extract(
    collection_id: str,
    item_id: str,
    lon: float = Query(...),
    lat: float = Query(...),
):
    """Extract pixel value at a geographic point (lon/lat).
    
    Returns the raw pixel value at the nearest pixel to the given coordinate.
    Useful for building time series from multiple items.
    """
    import struct
    import math

    item = catalog.get_item(item_id)
    if not item:
        raise HTTPException(404, f"Item {item_id} not found")

    props = item.properties
    bbox = item.bbox  # [west, south, east, north]
    width = props.get("earthgrid:width", 0)
    height = props.get("earthgrid:height", 0)
    tile_size = props.get("earthgrid:tile_size", 512)
    tile_cols = props.get("earthgrid:tile_cols", 0)
    tile_rows = props.get("earthgrid:tile_rows", 0)
    crs = props.get("earthgrid:crs", "EPSG:4326")
    dtype = props.get("earthgrid:dtype", "uint16")
    n_bands = props.get("earthgrid:bands", 1)

    if not width or not height:
        raise HTTPException(400, "Item has no spatial dimensions")

    # Transform lon/lat to item CRS if needed
    if crs.startswith("EPSG:326"):
        # UTM zone from EPSG code (e.g., EPSG:32633 → zone 33N)
        zone = int(crs.split(":")[1]) - 32600
        # Simple WGS84 → UTM conversion
        lat_rad = math.radians(lat)
        lon_rad = math.radians(lon)
        lon0 = math.radians((zone - 1) * 6 - 180 + 3)
        
        a = 6378137.0
        f_ellps = 1 / 298.257223563
        e = math.sqrt(2 * f_ellps - f_ellps**2)
        e2 = e**2
        
        N = a / math.sqrt(1 - e2 * math.sin(lat_rad)**2)
        T = math.tan(lat_rad)**2
        C = (e2 / (1 - e2)) * math.cos(lat_rad)**2
        A_val = math.cos(lat_rad) * (lon_rad - lon0)
        
        M = a * ((1 - e2/4 - 3*e2**2/64 - 5*e2**3/256) * lat_rad
                 - (3*e2/8 + 3*e2**2/32 + 45*e2**3/1024) * math.sin(2*lat_rad)
                 + (15*e2**2/256 + 45*e2**3/1024) * math.sin(4*lat_rad)
                 - (35*e2**3/3072) * math.sin(6*lat_rad))
        
        x = 500000 + 0.9996 * N * (A_val + (1-T+C)*A_val**3/6 + (5-18*T+T**2+72*C-58*(e2/(1-e2)))*A_val**5/120)
        y = 0.9996 * (M + N * math.tan(lat_rad) * (A_val**2/2 + (5-T+9*C+4*C**2)*A_val**4/24 + (61-58*T+T**2+600*C-330*(e2/(1-e2)))*A_val**6/720))
        if lat < 0:
            y += 10000000
    elif crs == "EPSG:4326":
        x, y = lon, lat
    else:
        raise HTTPException(400, f"Unsupported CRS: {crs}")

    # Quick check: is point within WGS84 bbox?
    if lon < bbox[0] or lon > bbox[2] or lat < bbox[1] or lat > bbox[3]:
        raise HTTPException(400, f"Point ({lon}, {lat}) is outside item extent")

    # For UTM CRS: transform bbox corners to get extent in projection coords
    if crs.startswith("EPSG:326"):
        # Re-use the UTM conversion for bbox corners
        def _to_utm(ln, lt, zone):
            import math as m
            lat_r = m.radians(lt); lon_r = m.radians(ln)
            lon0 = m.radians((zone - 1) * 6 - 180 + 3)
            a = 6378137.0; f_e = 1/298.257223563; e2 = 2*f_e - f_e**2
            N = a / m.sqrt(1 - e2 * m.sin(lat_r)**2)
            T = m.tan(lat_r)**2; C = (e2/(1-e2)) * m.cos(lat_r)**2
            A_v = m.cos(lat_r) * (lon_r - lon0)
            M = a*((1-e2/4-3*e2**2/64-5*e2**3/256)*lat_r - (3*e2/8+3*e2**2/32+45*e2**3/1024)*m.sin(2*lat_r) + (15*e2**2/256+45*e2**3/1024)*m.sin(4*lat_r) - (35*e2**3/3072)*m.sin(6*lat_r))
            ex = 500000 + 0.9996*N*(A_v + (1-T+C)*A_v**3/6 + (5-18*T+T**2+72*C-58*(e2/(1-e2)))*A_v**5/120)
            ey = 0.9996*(M + N*m.tan(lat_r)*(A_v**2/2 + (5-T+9*C+4*C**2)*A_v**4/24 + (61-58*T+T**2+600*C-330*(e2/(1-e2)))*A_v**6/720))
            if lt < 0: ey += 10000000
            return ex, ey
        zone = int(crs.split(":")[1]) - 32600
        x_min, y_min = _to_utm(bbox[0], bbox[1], zone)
        x_max, y_max = _to_utm(bbox[2], bbox[3], zone)
    else:
        x_min, y_min = bbox[0], bbox[1]
        x_max, y_max = bbox[2], bbox[3]

    res_x = (x_max - x_min) / width
    res_y = (y_max - y_min) / height

    col = int((x - x_min) / res_x)
    row = int((y_max - y) / res_y)  # y-axis inverted (north=0)

    if col < 0 or col >= width or row < 0 or row >= height:
        raise HTTPException(400, f"Point ({lon}, {lat}) maps to pixel ({col},{row}) — outside raster {width}x{height}")

    # Find which tile contains this pixel
    tile_col = col // tile_size
    tile_row = row // tile_size
    tile_idx = tile_row * tile_cols + tile_col

    # Get the chunk
    hashes = item.chunk_hashes
    if isinstance(hashes, dict):
        # band-level — take first band
        first_band = list(hashes.values())[0]
        if tile_idx >= len(first_band):
            raise HTTPException(500, f"Tile index {tile_idx} out of range")
        sha = first_band[tile_idx]
    elif isinstance(hashes, list):
        if tile_idx >= len(hashes):
            raise HTTPException(500, f"Tile index {tile_idx} out of range")
        sha = hashes[tile_idx]
    else:
        raise HTTPException(500, "Unknown chunk_hashes format")

    chunk_data = chunk_store.get(sha)
    if chunk_data is None:
        raise HTTPException(404, f"Chunk {sha[:12]}... not found")

    # Pixel position within tile
    local_col = col % tile_size
    local_row = row % tile_size

    # Tile dimensions (handle edge tiles)
    tile_w = min(tile_size, width - tile_col * tile_size)
    tile_h = min(tile_size, height - tile_row * tile_size)

    # Decode pixel value based on dtype
    dtype_map = {
        "uint8": ("B", 1), "int8": ("b", 1),
        "uint16": ("H", 2), "int16": ("h", 2),
        "uint32": ("I", 4), "int32": ("i", 4),
        "float32": ("f", 4), "float64": ("d", 8),
    }

    fmt, bpp = dtype_map.get(dtype, ("H", 2))

    # Pixel offset: (band * tile_h * tile_w + row * tile_w + col) * bpp
    # For spatial tiles with n_bands interleaved per pixel:
    # offset = (local_row * tile_w * n_bands + local_col * n_bands) * bpp
    # For band-sequential (BSQ) tiles:
    pixel_offset = (local_row * tile_w + local_col) * bpp

    if pixel_offset + bpp > len(chunk_data):
        raise HTTPException(500, f"Pixel offset {pixel_offset} exceeds chunk size {len(chunk_data)}")

    value = struct.unpack(fmt, chunk_data[pixel_offset:pixel_offset + bpp])[0]

    return {
        "value": value,
        "lon": lon,
        "lat": lat,
        "pixel": [col, row],
        "tile": [tile_col, tile_row],
        "item_id": item_id,
        "collection": collection_id,
        "dtype": dtype,
        "crs": crs,
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
    stats_engine.record_uptake(
        collection_id=collection_id, job_type="download",
        bytes_out=len(data) if isinstance(data, bytes) else 0,
    )

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
        node_identity=node_identity,
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
