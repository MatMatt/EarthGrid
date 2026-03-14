"""Beacon Node — coordination layer for semi-decentralized EarthGrid network.

A beacon does NOT store data. It:
1. Maintains a registry of all connected data nodes
2. Routes queries to the right nodes (spatial/collection index)
3. Accepts WebSocket connections from nodes behind NAT
4. Provides node discovery for new nodes joining the network
5. Federates with other beacons — shares node registries
"""
from __future__ import annotations
import asyncio
import json
import logging
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import httpx
from fastapi import Request,  APIRouter, FastAPI, WebSocket, WebSocketDisconnect, Query, HTTPException
from fastapi.responses import JSONResponse

from . import __version__
from .config import settings

logger = logging.getLogger("earthgrid.beacon")


@dataclass
class RegisteredNode:
    """A data node registered with this beacon."""
    node_id: str
    node_name: str
    url: Optional[str]  # Direct URL (if publicly reachable)
    websocket: Optional[WebSocket] = field(default=None, repr=False)
    collections: list[str] = field(default_factory=list)
    item_count: int = 0
    chunk_count: int = 0
    chunks_bytes: int = 0
    can_source: bool = False  # can fetch from official sources (CDSE etc.)
    last_seen: float = 0.0
    bbox_index: list[list[float]] = field(default_factory=list)  # per-collection bboxes

    @property
    def alive(self) -> bool:
        return time.time() - self.last_seen < 300

    @property
    def reachable_via_ws(self) -> bool:
        return self.websocket is not None

    def to_dict(self) -> dict:
        return {
            "node_id": self.node_id,
            "node_name": self.node_name,
            "url": self.url,
            "collections": self.collections,
            "item_count": self.item_count,
            "chunk_count": self.chunk_count,
            "chunks_bytes": self.chunks_bytes,
            "can_source": self.can_source,
            "alive": self.alive,
            "reachable": self.url is not None or self.reachable_via_ws,
            "last_seen": self.last_seen,
        }


@dataclass
class PeerBeacon:
    """Another beacon we federate with."""
    url: str
    node_id: str = ""
    node_name: str = ""
    last_seen: float = 0.0
    nodes_count: int = 0

    @property
    def alive(self) -> bool:
        return time.time() - self.last_seen < 600  # 10 min


class BeaconRegistry:
    """Central registry of all data nodes + peer beacons.

    Node metadata is persisted in SQLite; WebSocket connections are in-memory only.
    """

    def __init__(self, db_path: Optional[Path] = None):
        # In-memory WebSocket connections (cannot be persisted)
        self._websockets: dict[str, WebSocket] = {}
        self._lock = asyncio.Lock()
        self._db_lock = threading.Lock()

        # SQLite persistence
        if db_path is None:
            db_path = Path(settings.beacon_db)
        self._db_path = Path(db_path)
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

        # In-memory caches (loaded from DB on first access)
        self._nodes_cache: dict[str, RegisteredNode] = {}
        self._peers_cache: dict[str, PeerBeacon] = {}
        self._load_from_db()

    def _init_db(self):
        """Create tables if they don't exist."""
        with self._db_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS nodes (
                        node_id      TEXT PRIMARY KEY,
                        node_name    TEXT,
                        url          TEXT,
                        collections  TEXT,
                        item_count   INTEGER DEFAULT 0,
                        chunk_count  INTEGER DEFAULT 0,
                        chunks_bytes INTEGER DEFAULT 0,
                        can_source   BOOLEAN DEFAULT 0,
                        last_seen    REAL DEFAULT 0
                    )
                """)
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS peer_beacons (
                        url          TEXT PRIMARY KEY,
                        node_id      TEXT DEFAULT '',
                        node_name    TEXT DEFAULT '',
                        last_seen    REAL DEFAULT 0,
                        nodes_count  INTEGER DEFAULT 0
                    )
                """)
                conn.commit()

    def _load_from_db(self):
        """Load persisted nodes and peer beacons into in-memory cache."""
        with self._db_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.row_factory = sqlite3.Row

                for row in conn.execute("SELECT * FROM nodes"):
                    try:
                        cols = json.loads(row["collections"] or "[]")
                    except (json.JSONDecodeError, TypeError):
                        cols = []
                    node = RegisteredNode(
                        node_id=row["node_id"],
                        node_name=row["node_name"] or "",
                        url=row["url"],
                        collections=cols,
                        item_count=row["item_count"] or 0,
                        chunk_count=row["chunk_count"] or 0,
                        chunks_bytes=row["chunks_bytes"] or 0,
                        can_source=bool(row["can_source"]),
                        last_seen=row["last_seen"] or 0.0,
                    )
                    self._nodes_cache[node.node_id] = node

                for row in conn.execute("SELECT * FROM peer_beacons"):
                    peer = PeerBeacon(
                        url=row["url"],
                        node_id=row["node_id"] or "",
                        node_name=row["node_name"] or "",
                        last_seen=row["last_seen"] or 0.0,
                        nodes_count=row["nodes_count"] or 0,
                    )
                    self._peers_cache[peer.url] = peer

        logger.info(
            f"BeaconRegistry loaded {len(self._nodes_cache)} nodes, "
            f"{len(self._peers_cache)} peer beacons from {self._db_path}"
        )

    def _db_upsert_node(self, node: RegisteredNode):
        """Persist a node to SQLite (INSERT OR REPLACE)."""
        with self._db_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO nodes
                       (node_id, node_name, url, collections, item_count,
                        chunk_count, chunks_bytes, can_source, last_seen)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        node.node_id,
                        node.node_name,
                        node.url,
                        json.dumps(node.collections),
                        node.item_count,
                        node.chunk_count,
                        node.chunks_bytes,
                        int(node.can_source),
                        node.last_seen,
                    ),
                )
                conn.commit()

    def _db_delete_node(self, node_id: str):
        """Remove a node from SQLite."""
        with self._db_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute("DELETE FROM nodes WHERE node_id = ?", (node_id,))
                conn.commit()

    def _db_upsert_peer(self, peer: PeerBeacon):
        """Persist a peer beacon to SQLite."""
        with self._db_lock:
            with sqlite3.connect(self._db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO peer_beacons
                       (url, node_id, node_name, last_seen, nodes_count)
                       VALUES (?, ?, ?, ?, ?)""",
                    (peer.url, peer.node_id, peer.node_name, peer.last_seen, peer.nodes_count),
                )
                conn.commit()

    # Expose dicts as properties for backward-compat with code that does registry.nodes / registry.peer_beacons
    @property
    def nodes(self) -> dict[str, RegisteredNode]:
        return self._nodes_cache

    @property
    def peer_beacons(self) -> dict[str, PeerBeacon]:
        return self._peers_cache

    async def register(
        self,
        node_id: str,
        node_name: str,
        url: Optional[str] = None,
        websocket: Optional[WebSocket] = None,
        collections: list[str] = None,
        item_count: int = 0,
        chunk_count: int = 0,
        chunks_bytes: int = 0,
        can_source: bool = False,
    ) -> RegisteredNode:
        async with self._lock:
            existing = self._nodes_cache.get(node_id)
            if existing:
                # Update in-memory
                if websocket:
                    self._websockets[node_id] = websocket
                    existing.websocket = websocket
                existing.last_seen = time.time()
                if url:
                    existing.url = url
                if collections is not None:
                    existing.collections = collections
                existing.item_count = item_count
                existing.chunk_count = chunk_count
                existing.chunks_bytes = chunks_bytes
                existing.can_source = can_source
                self._db_upsert_node(existing)
                return existing

            node = RegisteredNode(
                node_id=node_id,
                node_name=node_name,
                url=url,
                websocket=websocket,
                collections=collections or [],
                item_count=item_count,
                chunk_count=chunk_count,
                chunks_bytes=chunks_bytes,
                can_source=can_source,
                last_seen=time.time(),
            )
            self._nodes_cache[node_id] = node
            if websocket:
                self._websockets[node_id] = websocket
            self._db_upsert_node(node)
            return node

    async def unregister(self, node_id: str):
        async with self._lock:
            self._nodes_cache.pop(node_id, None)
            self._websockets.pop(node_id, None)
            self._db_delete_node(node_id)

    async def heartbeat(self, node_id: str, **updates):
        async with self._lock:
            node = self._nodes_cache.get(node_id)
            if node:
                node.last_seen = time.time()
                for k, v in updates.items():
                    if hasattr(node, k) and v is not None:
                        setattr(node, k, v)
                self._db_upsert_node(node)

    def get_alive_nodes(self) -> list[RegisteredNode]:
        cutoff = time.time() - 300
        return [n for n in self._nodes_cache.values() if n.last_seen > cutoff]

    def find_nodes_for_collection(self, collection: str) -> list[RegisteredNode]:
        return [n for n in self.get_alive_nodes() if collection in n.collections]

    def network_stats(self) -> dict:
        alive = self.get_alive_nodes()
        all_collections = set()
        total_items = 0
        total_chunks = 0
        total_bytes = 0
        for n in alive:
            all_collections.update(n.collections)
            total_items += n.item_count
            total_chunks += n.chunk_count
            total_bytes += n.chunks_bytes
        return {
            "nodes_alive": len(alive),
            "nodes_total": len(self._nodes_cache),
            "collections": sorted(all_collections),
            "total_items": total_items,
            "total_chunks": total_chunks,
            "total_bytes": total_bytes,
            "peer_beacons": len(self._peers_cache),
            "peer_beacons_alive": len([b for b in self._peers_cache.values() if b.alive]),
        }

    # --- Beacon Federation ---

    async def add_peer_beacon(self, url: str) -> PeerBeacon:
        """Register a peer beacon."""
        url = url.rstrip("/")
        async with self._lock:
            peer = self._peers_cache.get(url)
            if not peer:
                peer = PeerBeacon(url=url)
                self._peers_cache[url] = peer
                self._db_upsert_peer(peer)
            return peer

    async def sync_with_peer_beacon(self, url: str) -> dict:
        """Exchange node registries with a peer beacon."""
        url = url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Get peer beacon info
                info_resp = await client.get(f"{url}/")
                if info_resp.status_code == 200:
                    info = info_resp.json()
                    peer = self._peers_cache.get(url)
                    if peer:
                        peer.node_id = info.get("node_id", "")
                        peer.node_name = info.get("node_name", "")
                        peer.last_seen = time.time()
                        peer.nodes_count = info.get("nodes_alive", 0)
                        self._db_upsert_peer(peer)

                # Get their nodes
                resp = await client.get(f"{url}/nodes", params={"alive_only": True})
                if resp.status_code == 200:
                    remote_nodes = resp.json().get("nodes", [])
                    merged = 0
                    for rn in remote_nodes:
                        nid = rn.get("node_id", "")
                        if nid and nid not in self._nodes_cache:
                            await self.register(
                                node_id=nid,
                                node_name=rn.get("node_name", ""),
                                url=rn.get("url"),
                                collections=rn.get("collections", []),
                                item_count=rn.get("item_count", 0),
                                chunk_count=rn.get("chunk_count", 0),
                                chunks_bytes=rn.get("chunks_bytes", 0),
                            )
                            merged += 1
                        elif nid in self._nodes_cache:
                            await self.heartbeat(
                                nid,
                                collections=rn.get("collections"),
                                item_count=rn.get("item_count"),
                                chunk_count=rn.get("chunk_count"),
                                chunks_bytes=rn.get("chunks_bytes"),
                            )

                    # Send our nodes + our beacon peers to peer (gossip)
                    our_nodes = [n.to_dict() for n in self.get_alive_nodes()]
                    our_beacons = [b.url for b in self._peers_cache.values()]
                    await client.post(f"{url}/beacon/exchange", json={
                        "nodes": our_nodes,
                        "beacons": our_beacons,
                    })

                    # Learn their beacon peers (gossip propagation)
                    try:
                        peers_resp = await client.get(f"{url}/beacon/peers")
                        if peers_resp.status_code == 200:
                            their_beacons = peers_resp.json().get("beacons", [])
                            for tb in their_beacons:
                                tb_url = tb.get("url", "")
                                if tb_url and tb_url not in self._peers_cache:
                                    if settings.public_url and tb_url == settings.public_url:
                                        continue
                                    await self.add_peer_beacon(tb_url)
                                    logger.info(f"Discovered beacon via gossip: {tb_url}")
                    except Exception:
                        pass  # Gossip is best-effort

                    return {"url": url, "status": "synced", "merged": merged, "sent": len(our_nodes)}

        except Exception as e:
            logger.warning(f"Beacon sync failed with {url}: {e}")

        return {"url": url, "status": "failed", "merged": 0, "sent": 0}

    async def sync_all_beacons(self) -> list[dict]:
        """Sync with all known peer beacons."""
        tasks = [self.sync_with_peer_beacon(url) for url in list(self._peers_cache.keys())]
        return await asyncio.gather(*tasks)


# --- Beacon FastAPI App ---

beacon_app = FastAPI(
    title="EarthGrid Beacon",
    version=__version__,
    description="Coordination node for the EarthGrid network",
)

registry = BeaconRegistry(db_path=Path(settings.beacon_db))


_beacon_started = time.time()


@beacon_app.get("/")
def beacon_info():
    stats = registry.network_stats()
    return {
        "name": "EarthGrid Beacon",
        "version": __version__,
        "node_id": settings.node_id,
        "node_name": settings.node_name,
        "role": "beacon",
        "uptime_hours": round((time.time() - _beacon_started) / 3600, 1),
        **stats,
        "total_bytes_human": _human_bytes(stats["total_bytes"]),
    }


def _human_bytes(b: int) -> str:
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} EB"


@beacon_app.get("/health")
def health():
    return {"status": "ok", "role": "beacon"}


@beacon_app.on_event("startup")
async def beacon_startup():
    """Register peer beacons from config and start sync loop."""
    for url in settings.beacon_peers:
        await registry.add_peer_beacon(url)
    if registry.peer_beacons:
        await registry.sync_all_beacons()
        asyncio.create_task(_beacon_sync_loop())


async def _beacon_sync_loop():
    """Periodically sync with peer beacons."""
    while True:
        await asyncio.sleep(120)  # every 2 min
        try:
            await registry.sync_all_beacons()
        except Exception as e:
            logger.warning(f"Beacon sync loop error: {e}")


# --- Node Registration ---

@beacon_app.post("/register")
async def register_node(
    request: Request,
    node_id: str = Query(...),
    node_name: str = Query(""),
    url: str = Query(None, description="Public URL of the node (if reachable)"),
    collections: str = Query("", description="Comma-separated collection IDs"),
    item_count: int = Query(0),
    chunk_count: int = Query(0),
    chunks_bytes: int = Query(0),
    can_source: bool = Query(False, description="Node can fetch from official sources"),
):
    """Register a data node with this beacon."""
    # Auto-detect peer IP if url is 0.0.0.0 or missing
    if url and "0.0.0.0" in url:
        client_ip = request.headers.get("x-real-ip") or request.headers.get("x-forwarded-for", "").split(",")[0].strip() or request.client.host
        if client_ip:
            import re
            url = re.sub(r"0\.0\.0\.0", client_ip, url)
            import logging
            logging.getLogger("earthgrid").info(f"Auto-detected peer URL: {url} (from {client_ip})")
    col_list = [c.strip() for c in collections.split(",") if c.strip()] if collections else []
    node = await registry.register(
        node_id=node_id,
        node_name=node_name,
        url=url,
        collections=col_list,
        item_count=item_count,
        chunk_count=chunk_count,
        chunks_bytes=chunks_bytes,
        can_source=can_source,
    )
    return {"status": "registered", "node": node.to_dict()}


@beacon_app.post("/heartbeat")
async def node_heartbeat(
    node_id: str = Query(...),
    collections: str = Query(None),
    item_count: int = Query(None),
    chunk_count: int = Query(None),
    chunks_bytes: int = Query(None),
):
    """Heartbeat from a data node — keeps it alive in the registry."""
    updates = {}
    if collections is not None:
        updates["collections"] = [c.strip() for c in collections.split(",") if c.strip()]
    if item_count is not None:
        updates["item_count"] = item_count
    if chunk_count is not None:
        updates["chunk_count"] = chunk_count
    if chunks_bytes is not None:
        updates["chunks_bytes"] = chunks_bytes

    await registry.heartbeat(node_id, **updates)
    return {"status": "ok"}


@beacon_app.get("/nodes")
def list_nodes(alive_only: bool = Query(True)):
    """List all registered nodes."""
    nodes = registry.get_alive_nodes() if alive_only else list(registry.nodes.values())
    return {
        "count": len(nodes),
        "nodes": [n.to_dict() for n in nodes],
    }


@beacon_app.get("/nodes/{node_id}")
def get_node(node_id: str):
    node = registry.nodes.get(node_id)
    if not node:
        raise HTTPException(404, "Node not found")
    return node.to_dict()


# --- Beacon Federation ---

@beacon_app.post("/beacon/peer")
async def add_peer_beacon(url: str = Query(..., description="URL of peer beacon")):
    """Register a peer beacon for federation."""
    peer = await registry.add_peer_beacon(url)
    # Immediately sync
    result = await registry.sync_with_peer_beacon(url)
    return {"status": "added", "peer": url, "sync": result}


@beacon_app.get("/beacon/peers")
def list_peer_beacons():
    """List all known peer beacons."""
    return {
        "count": len(registry.peer_beacons),
        "beacons": [
            {
                "url": b.url,
                "node_id": b.node_id,
                "node_name": b.node_name,
                "alive": b.alive,
                "nodes_count": b.nodes_count,
            }
            for b in registry.peer_beacons.values()
        ],
    }


@beacon_app.post("/beacon/sync")
async def sync_beacons():
    """Sync node registries with all peer beacons."""
    results = await registry.sync_all_beacons()
    return {"synced": len(results), "results": results}


@beacon_app.post("/beacon/exchange")
async def exchange_nodes(data: dict):
    """Receive node list + beacon list from a peer beacon (called during sync)."""
    remote_nodes = data.get("nodes", [])
    remote_beacons = data.get("beacons", [])
    merged = 0
    for rn in remote_nodes:
        nid = rn.get("node_id", "")
        if nid and nid not in registry.nodes:
            await registry.register(
                node_id=nid,
                node_name=rn.get("node_name", ""),
                url=rn.get("url"),
                collections=rn.get("collections", []),
                item_count=rn.get("item_count", 0),
                chunk_count=rn.get("chunk_count", 0),
                chunks_bytes=rn.get("chunks_bytes", 0),
            )
            merged += 1

    # Gossip: learn new beacons from the peer
    beacons_learned = 0
    for b_url in remote_beacons:
        if b_url and b_url not in registry.peer_beacons:
            if not (settings.public_url and b_url == settings.public_url):
                await registry.add_peer_beacon(b_url)
                beacons_learned += 1

    return {"status": "ok", "merged": merged, "beacons_learned": beacons_learned}


# --- Routed Search (the key feature) ---

@beacon_app.get("/search")
async def routed_search(
    collections: str = Query(None),
    bbox: str = Query(None),
    datetime: str = Query(None),
    limit: int = Query(100, le=1000),
):
    """Smart routed search — beacon knows which nodes have which data."""
    col_list = collections.split(",") if collections else None
    bbox_list = [float(x) for x in bbox.split(",")] if bbox else None

    # Find relevant nodes
    if col_list:
        target_nodes = set()
        for col in col_list:
            for node in registry.find_nodes_for_collection(col):
                target_nodes.add(node)
        target_nodes = list(target_nodes)
    else:
        target_nodes = registry.get_alive_nodes()

    if not target_nodes:
        return {
            "type": "FeatureCollection",
            "numberMatched": 0,
            "numberReturned": 0,
            "features": [],
            "context": {"source": "beacon", "nodes_queried": 0},
        }

    # Fan out search to relevant nodes
    all_results = []

    async def query_node(node: RegisteredNode):
        if not node.url:
            return []  # TODO: route through WebSocket for NAT'd nodes
        try:
            params = {"limit": limit}
            if collections:
                params["collections"] = collections
            if bbox:
                params["bbox"] = bbox
            if datetime:
                params["datetime"] = datetime

            async with httpx.AsyncClient(timeout=15) as client:
                resp = await client.get(f"{node.url}/stac/search", params=params)
                if resp.status_code == 200:
                    data = resp.json()
                    features = data.get("features", [])
                    for f in features:
                        f["earthgrid:source_node"] = node.url
                        f["earthgrid:source_name"] = node.node_name
                    return features
        except Exception:
            pass
        return []

    tasks = [query_node(n) for n in target_nodes]
    results = await asyncio.gather(*tasks)
    for r in results:
        all_results.extend(r)

    # Also query peer beacons (federated search across beacons)
    async def query_peer_beacon(beacon_url: str):
        try:
            params = {"limit": limit}
            if collections:
                params["collections"] = collections
            if bbox:
                params["bbox"] = bbox
            if datetime:
                params["datetime"] = datetime

            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(f"{beacon_url}/search", params=params)
                if resp.status_code == 200:
                    return resp.json().get("features", [])
        except Exception:
            pass
        return []

    if registry.peer_beacons:
        peer_tasks = [query_peer_beacon(url) for url, b in registry.peer_beacons.items() if b.alive]
        peer_task_results = await asyncio.gather(*peer_tasks)
        for r in peer_task_results:
            all_results.extend(r)

    # Deduplicate
    seen = set()
    deduped = []
    for item in all_results:
        iid = item.get("id", "")
        if iid not in seen:
            seen.add(iid)
            deduped.append(item)

    local_nodes_queried = len(target_nodes)
    peer_beacons_queried = len([b for b in registry.peer_beacons.values() if b.alive])

    return {
        "type": "FeatureCollection",
        "numberMatched": len(deduped),
        "numberReturned": min(len(deduped), limit),
        "features": deduped[:limit],
        "context": {
            "source": "beacon",
            "nodes_queried": local_nodes_queried,
            "peer_beacons_queried": peer_beacons_queried,
            "nodes_with_results": sum(1 for r in results if r),
        },
    }


# --- WebSocket for NAT traversal ---

@beacon_app.websocket("/ws/{node_id}")
async def node_websocket(websocket: WebSocket, node_id: str):
    """WebSocket connection for nodes behind NAT."""
    await websocket.accept()
    node = None

    try:
        while True:
            msg = await websocket.receive_json()
            msg_type = msg.get("type", "")

            if msg_type == "register":
                node = await registry.register(
                    node_id=node_id,
                    node_name=msg.get("node_name", ""),
                    url=msg.get("url"),  # None if behind NAT
                    websocket=websocket,
                    collections=msg.get("collections", []),
                    item_count=msg.get("item_count", 0),
                    chunk_count=msg.get("chunk_count", 0),
                    chunks_bytes=msg.get("chunks_bytes", 0),
                )
                await websocket.send_json({"type": "registered", "node_id": node_id})

            elif msg_type == "heartbeat":
                await registry.heartbeat(
                    node_id,
                    collections=msg.get("collections"),
                    item_count=msg.get("item_count"),
                    chunk_count=msg.get("chunk_count"),
                    chunks_bytes=msg.get("chunks_bytes"),
                )
                await websocket.send_json({"type": "heartbeat_ack"})

            elif msg_type == "search_response":
                # Response to a routed search — handled by the search coroutine
                pass

    except WebSocketDisconnect:
        if node:
            node.websocket = None
            registry._websockets.pop(node_id, None)
    except Exception:
        if node:
            node.websocket = None
            registry._websockets.pop(node_id, None)


# --- Seed Endpoint ---

@beacon_app.get("/seed/nodes")
def seed_nodes():
    """List nodes that can serve as seed sources for new nodes."""
    nodes = registry.get_alive_nodes()
    return {
        "seed_nodes": [
            {
                "node_id": n.node_id,
                "node_name": n.node_name,
                "url": n.url,
                "collections": n.collections,
                "item_count": n.item_count,
                "chunks_bytes": n.chunks_bytes,
            }
            for n in nodes
            if n.url and n.item_count > 0
        ],
    }


# --- APIRouter for mounting into a Node app (--also-beacon) ---

beacon_router = APIRouter(tags=["beacon"])

beacon_router.add_api_route("/nodes", list_nodes, methods=["GET"])
beacon_router.add_api_route("/register", register_node, methods=["POST"])
beacon_router.add_api_route("/heartbeat", node_heartbeat, methods=["POST"])
beacon_router.add_api_route("/search", routed_search, methods=["GET"])
beacon_router.add_api_route("/beacon/peer", add_peer_beacon, methods=["POST"])
beacon_router.add_api_route("/beacon/peers", list_peer_beacons, methods=["GET"])
beacon_router.add_api_route("/beacon/sync", sync_beacons, methods=["POST"])
beacon_router.add_api_route("/beacon/exchange", exchange_nodes, methods=["POST"])
beacon_router.add_api_route("/seed/nodes", seed_nodes, methods=["GET"])
