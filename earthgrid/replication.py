"""EarthGrid Replication — sync catalog and chunks between nodes.

Pull-based: a node connects to a peer and fetches what it doesn't have.
"""
from __future__ import annotations
import asyncio
import logging
from pathlib import Path

import httpx

from .chunk_store import ChunkStore
from .catalog import Catalog, STACItem, STACCollection

logger = logging.getLogger("earthgrid.replication")

# Max concurrent chunk downloads
MAX_CONCURRENT = 10
# Chunk download timeout (seconds)
CHUNK_TIMEOUT = 30


class Replicator:
    """Replicate catalog and chunks from a remote peer."""

    def __init__(self, chunk_store: ChunkStore, catalog: Catalog):
        self.chunk_store = chunk_store
        self.catalog = catalog

    async def sync_from_peer(
        self,
        peer_url: str,
        collections: list[str] | None = None,
        max_items: int = 0,
        dry_run: bool = False,
    ) -> dict:
        """Pull catalog + chunks from a peer node.

        Args:
            peer_url: Base URL of the remote node
            collections: Only sync these collections (None = all)
            max_items: Limit items to sync (0 = all)
            dry_run: Only report what would be synced

        Returns:
            Summary dict with counts.
        """
        peer_url = peer_url.rstrip("/")
        stats = {
            "peer": peer_url,
            "collections_synced": 0,
            "items_synced": 0,
            "chunks_downloaded": 0,
            "chunks_skipped": 0,
            "bytes_downloaded": 0,
            "errors": [],
        }

        async with httpx.AsyncClient(timeout=30) as client:
            # 1. Fetch remote node info
            try:
                resp = await client.get(f"{peer_url}/")
                resp.raise_for_status()
                info = resp.json()
                logger.info(f"Connected to {info.get('node_name', '?')} "
                           f"({info.get('chunks', 0)} chunks, "
                           f"{info.get('item_count', 0)} items)")
            except Exception as e:
                stats["errors"].append(f"Cannot reach peer: {e}")
                return stats

            # 2. Fetch collections
            resp = await client.get(f"{peer_url}/stac/collections")
            resp.raise_for_status()
            remote_collections = resp.json().get("collections", [])

            for col_data in remote_collections:
                col_id = col_data["id"]
                if collections and col_id not in collections:
                    continue

                # Create collection locally if missing
                if not self.catalog.get_collection(col_id):
                    if not dry_run:
                        self.catalog.add_collection(STACCollection(
                            id=col_id,
                            title=col_data.get("title", col_id),
                            description=col_data.get("description", ""),
                        ))
                    stats["collections_synced"] += 1

                # 3. Fetch items — try /replicate/items first (includes chunks),
                #    fall back to /stac/collections/{id}/items
                remote_items = []
                try:
                    resp = await client.get(
                        f"{peer_url}/replicate/items",
                        params={"collection": col_id, "limit": max_items or 10000},
                    )
                    resp.raise_for_status()
                    remote_items = resp.json().get("items", [])
                except (httpx.HTTPStatusError, httpx.RequestError):
                    # Fallback: standard STAC endpoint
                    resp = await client.get(
                        f"{peer_url}/stac/collections/{col_id}/items",
                        params={"limit": max_items or 10000},
                    )
                    resp.raise_for_status()
                    features = resp.json().get("features", [])
                    # For STAC items, chunk_hashes might not be in the response.
                    # Try to get them from the item detail endpoint or properties.
                    for feat in features:
                        # Check if chunk_hashes are embedded
                        chunk_hashes = feat.get("earthgrid:chunk_hashes",
                                       feat.get("properties", {}).get("earthgrid:chunk_hashes", []))
                        feat["earthgrid:chunk_hashes"] = chunk_hashes
                        remote_items.append(feat)

                for item_data in remote_items:
                    if max_items and stats["items_synced"] >= max_items:
                        break

                    item_id = item_data["id"]
                    chunk_hashes = item_data.get("earthgrid:chunk_hashes", [])

                    # Skip if we already have this item with all chunks
                    existing = self.catalog.get_item(item_id)
                    if existing and all(self.chunk_store.has(h) for h in chunk_hashes):
                        logger.debug(f"Skip {item_id} — already complete")
                        stats["chunks_skipped"] += len(chunk_hashes)
                        continue

                    if dry_run:
                        missing = sum(1 for h in chunk_hashes if not self.chunk_store.has(h))
                        stats["items_synced"] += 1
                        stats["chunks_downloaded"] += missing
                        stats["chunks_skipped"] += len(chunk_hashes) - missing
                        continue

                    # 4. Download missing chunks (concurrent)
                    missing_hashes = [h for h in chunk_hashes if not self.chunk_store.has(h)]
                    if missing_hashes:
                        downloaded, bytes_dl, errors = await self._download_chunks(
                            client, peer_url, missing_hashes
                        )
                        stats["chunks_downloaded"] += downloaded
                        stats["bytes_downloaded"] += bytes_dl
                        stats["chunks_skipped"] += len(chunk_hashes) - len(missing_hashes)
                        stats["errors"].extend(errors)

                    # 5. Store item in catalog
                    stac_item = STACItem(
                        id=item_data["id"],
                        collection=item_data["collection"],
                        geometry=item_data.get("geometry", {}),
                        bbox=item_data.get("bbox", []),
                        properties=item_data.get("properties", {}),
                        assets=item_data.get("assets", {}),
                        chunk_hashes=chunk_hashes,
                    )
                    self.catalog.add_item(stac_item)
                    stats["items_synced"] += 1
                    logger.info(f"Synced {item_id}: {len(missing_hashes)} new chunks")

        return stats

    async def _download_chunks(
        self, client: httpx.AsyncClient, peer_url: str, hashes: list[str]
    ) -> tuple[int, int, list[str]]:
        """Download chunks concurrently."""
        sem = asyncio.Semaphore(MAX_CONCURRENT)
        downloaded = 0
        bytes_total = 0
        errors = []

        async def fetch_one(sha: str):
            nonlocal downloaded, bytes_total
            async with sem:
                try:
                    resp = await client.get(
                        f"{peer_url}/chunks/{sha}",
                        timeout=CHUNK_TIMEOUT,
                    )
                    if resp.status_code == 200:
                        data = resp.content
                        stored_sha = self.chunk_store.put(data)
                        if stored_sha == sha:
                            downloaded += 1
                            bytes_total += len(data)
                        else:
                            errors.append(f"Hash mismatch for {sha}")
                    else:
                        errors.append(f"Chunk {sha[:12]}...: HTTP {resp.status_code}")
                except Exception as e:
                    errors.append(f"Chunk {sha[:12]}...: {e}")

        tasks = [fetch_one(h) for h in hashes]
        await asyncio.gather(*tasks)
        return downloaded, bytes_total, errors
