"""Federation — peer discovery and federated search."""
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

    async def sync_peer(self, url: str) -> Peer | None:
        """Fetch info from a peer and update our registry."""
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
                    return peer
        except Exception:
            pass
        return None

    async def sync_all(self) -> list[Peer]:
        """Sync with all known peers."""
        tasks = [self.sync_peer(url) for url in list(self.peers.keys())]
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
