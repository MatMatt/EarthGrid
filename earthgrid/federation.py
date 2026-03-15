"""Federation — peer discovery and federated search."""
from __future__ import annotations
import asyncio
import time
from dataclasses import dataclass, field

import httpx


@dataclass
class Peer:
    url: str
    node_id: str = ""
    node_name: str = ""
    last_seen: float = 0.0
    collections: list[str] = field(default_factory=list)
    item_count: int = 0

    @property
    def alive(self) -> bool:
        return time.time() - self.last_seen < 300  # 5 min


class Federation:
    """Manage peer connections and federated queries."""

    def __init__(self, initial_peers: list[str] | None = None):
        self.peers: dict[str, Peer] = {}
        for url in (initial_peers or []):
            self.peers[url] = Peer(url=url)

    def add_peer(self, url: str, node_id: str = "", node_name: str = "") -> Peer:
        url = url.rstrip("/")
        peer = Peer(url=url, node_id=node_id, node_name=node_name, last_seen=time.time())
        self.peers[url] = peer
        return peer

    def remove_peer(self, url: str):
        self.peers.pop(url.rstrip("/"), None)

    def list_peers(self) -> list[Peer]:
        return list(self.peers.values())

    async def sync_peer(self, url: str, local_node_name: str = "",
                       local_node_id: str = "", local_api_key: str = "",
                       user_auth=None, node_identity=None) -> Peer | None:
        """Fetch info from a peer, update registry, and exchange keys."""
        url = url.rstrip("/")
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(f"{url}/")
                if resp.status_code == 200:
                    info = resp.json()
                    peer = Peer(
                        url=url,
                        node_id=info.get("node_id", ""),
                        node_name=info.get("node_name", ""),
                        last_seen=time.time(),
                        collections=info.get("collections", []),
                        item_count=info.get("item_count", 0),
                    )
                    self.peers[url] = peer

                    # Auto key exchange with Ed25519 signatures
                    if local_api_key and user_auth and node_identity:
                        try:
                            signed_payload = node_identity.sign_exchange(
                                local_node_name, local_node_id, local_api_key,
                            )
                            kx = await client.post(
                                f"{url}/federation/exchange-key",
                                json=signed_payload,
                                timeout=10,
                            )
                            if kx.status_code == 200:
                                peer_resp = kx.json()
                                # Verify the peer's response signature
                                from .node_identity import NodeIdentity
                                if NodeIdentity.verify_exchange(peer_resp):
                                    peer_uname = f"node:{peer_resp.get('node_name', peer.node_name)}"
                                    peer_key = peer_resp.get("api_key", "")
                                    if peer_key:
                                        try:
                                            user_auth.create_user(
                                                username=peer_uname,
                                                role="member",
                                                node_origin=peer_resp.get("node_name", ""),
                                            )
                                        except ValueError:
                                            pass
                                        import sqlite3 as _sql3
                                        with _sql3.connect(user_auth.db_path) as _conn:
                                            _conn.execute(
                                                "UPDATE users SET api_key = ?, updated_at = ? WHERE username = ?",
                                                (peer_key, time.time(), peer_uname)
                                            )
                        except Exception:
                            pass  # key exchange failed — non-fatal

                    return peer
        except Exception:
            pass
        return None

    async def sync_all(self, local_node_name: str = "",
                       local_node_id: str = "", local_api_key: str = "",
                       user_auth=None, node_identity=None) -> list[Peer]:
        """Sync with all known peers (including key exchange)."""
        tasks = [
            self.sync_peer(url, local_node_name, local_node_id,
                           local_api_key, user_auth, node_identity)
            for url in list(self.peers.keys())
        ]
        results = await asyncio.gather(*tasks)
        return [p for p in results if p is not None]

    async def federated_search(
        self,
        collections: list[str] | None = None,
        bbox: list[float] | None = None,
        datetime_range: str | None = None,
        limit: int = 100,
    ) -> list[dict]:
        """Fan out search to all peers, merge results."""
        all_results = []

        async def search_peer(url: str):
            try:
                params = {"limit": limit}
                if collections:
                    params["collections"] = ",".join(collections)
                if bbox:
                    params["bbox"] = ",".join(str(b) for b in bbox)
                if datetime_range:
                    params["datetime"] = datetime_range

                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(f"{url}/stac/search", params=params)
                    if resp.status_code == 200:
                        data = resp.json()
                        features = data.get("features", [])
                        # Tag each result with source node
                        for f in features:
                            f["earthgrid:source_node"] = url
                        return features
            except Exception:
                pass
            return []

        tasks = [search_peer(url) for url in list(self.peers.keys())]
        results = await asyncio.gather(*tasks)
        for r in results:
            all_results.extend(r)

        # Deduplicate by item ID (same hash = same data)
        seen = set()
        deduped = []
        for item in all_results:
            iid = item.get("id", "")
            if iid not in seen:
                seen.add(iid)
                deduped.append(item)

        return deduped[:limit]

    async def sync_users(self, url: str, local_api_key: str = "",
                         user_auth=None) -> dict:
        """Sync user registry with a peer node.

        Exports our users to the peer and imports theirs.
        Requires API key for authenticated federation endpoint.
        """
        if not user_auth:
            return {"error": "No user_auth configured"}
        url = url.rstrip("/")
        headers = {}
        if local_api_key:
            headers["X-API-Key"] = local_api_key
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                # Push our users
                our_users = user_auth.export_users()
                await client.post(
                    f"{url}/federation/users",
                    json={"users": our_users},
                    headers=headers,
                )
                # Pull their users
                resp = await client.get(
                    f"{url}/federation/users",
                    headers=headers,
                )
                if resp.status_code == 200:
                    data = resp.json()
                    result = user_auth.import_users(data.get("users", []))
                    return result
        except Exception as e:
            return {"error": str(e)}
        return {"error": "Failed to sync users"}

    async def sync_all_users(self, local_api_key: str = "",
                             user_auth=None) -> list[dict]:
        """Sync users with all known peers."""
        results = []
        for url in list(self.peers.keys()):
            r = await self.sync_users(url, local_api_key, user_auth)
            results.append({"peer": url, **r})
        return results
