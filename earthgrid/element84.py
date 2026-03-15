"""Element84 Earth Search STAC — search and download COGs."""
from __future__ import annotations
import asyncio
import logging
import tempfile
from pathlib import Path

import httpx
import rasterio
from rasterio.transform import from_bounds

logger = logging.getLogger("earthgrid.element84")

STAC_API = "https://earth-search.aws.element84.com/v1"

BAND_MAP_S2 = {
    "B02": "blue", "B03": "green", "B04": "red", "B08": "nir",
    "B05": "rededge1", "B06": "rededge2", "B07": "rededge3",
    "B8A": "nir08", "B09": "nir09", "B11": "swir16", "B12": "swir22",
    "B01": "coastal", "SCL": "scl",
}


async def search_element84(
    bbox: tuple[float, float, float, float],
    start_date: str | None = None,
    end_date: str | None = None,
    cloud_cover: float = 30.0,
    limit: int = 5,
    collection: str = "sentinel-2-l2a",
) -> list[dict]:
    """Search Element84 STAC for items."""
    max_per_page = 100
    target = limit if limit > 0 else 10000
    body = {
        "collections": [collection],
        "bbox": list(bbox),
        "limit": min(target, max_per_page),
        "query": {"eo:cloud_cover": {"lte": cloud_cover}},
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
    }
    if start_date or end_date:
        start = start_date or "2015-01-01"
        end = end_date or "2099-12-31"
        body["datetime"] = f"{start}T00:00:00Z/{end}T23:59:59Z"

    items = []
    async with httpx.AsyncClient(timeout=30) as client:
        while len(items) < target:
            body["limit"] = min(target - len(items), max_per_page)
            resp = await client.post(f"{STAC_API}/search", json=body)
            resp.raise_for_status()
            data = resp.json()
            page = data.get("features", [])
            if not page:
                break
            items.extend(page)
            next_link = None
            for link in data.get("links", []):
                if link.get("rel") == "next":
                    next_link = link.get("body") or link.get("href")
                    break
            if not next_link or len(page) < body["limit"]:
                break
            if isinstance(next_link, dict):
                body.update(next_link)
            else:
                break
    results = []
    for item in items:
        props = item.get("properties", {})
        results.append({
            "id": item["id"],
            "name": item["id"],
            "date": props.get("datetime", ""),
            "cloud_cover": props.get("eo:cloud_cover", -1),
            "bbox": item.get("bbox"),
            "assets": item.get("assets", {}),
            "geometry": item.get("geometry"),
        })
    return results


async def _get_grid_nodes(beacon_url: str, local_node_id: str = "") -> list[dict]:
    """Get all alive nodes from beacon with free space info."""
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            resp = await client.get(f"{beacon_url}/nodes")
            data = resp.json()
        except Exception as e:
            logger.warning(f"Cannot reach beacon: {e}")
            return []

    nodes = []
    for n in data.get("nodes", []):
        if not n.get("alive"):
            continue
        limit_gb = n.get("storage_limit_gb", 0)
        used_bytes = n.get("chunks_bytes", 0)
        if limit_gb > 0:
            free_gb = limit_gb - (used_bytes / 1024**3)
        else:
            free_gb = 999999  # unlimited
        nodes.append({
            "node_id": n.get("node_id", ""),
            "node_name": n.get("node_name", ""),
            "url": n.get("url", ""),
            "free_gb": max(free_gb, 0),
            "is_local": n.get("node_id") == local_node_id,
            "admin_key": n.get("admin_key", ""),
        })
    # Sort by free space descending
    nodes.sort(key=lambda n: n["free_gb"], reverse=True)
    return nodes


async def _delegate_item_to_node(
    node: dict,
    item: dict,
    bands: list[str],
    collection: str,
    cloud_cover: float,
) -> list[dict]:
    """Send a fetch request for a single item to a remote node."""
    bbox = item.get("bbox", [])
    dt = item.get("date", "")[:10]
    bbox_str = ",".join(str(x) for x in bbox) if bbox else ""

    params = {
        "bbox": bbox_str,
        "start": dt,
        "end": dt,
        "cloud": cloud_cover,
        "limit": 1,
        "source": "element84",
        "collection": collection,
    }
    if bands:
        params["bands"] = ",".join(bands)

    headers = {}
    if node.get("admin_key"):
        headers["Authorization"] = f"Bearer {node['admin_key']}"

    async with httpx.AsyncClient(timeout=300) as client:
        try:
            resp = await client.post(
                f"{node['url']}/fetch",
                params=params,
                headers=headers,
            )
            result = resp.json()
            ingested = result.get("ingested", 0)
            return result.get("details", [{"status": "delegated", "node": node["url"], "ingested": ingested}])
        except Exception as e:
            logger.error(f"Delegation to {node['url']} failed: {e}")
            return [{"error": f"Delegation to {node['node_name']} failed: {e}"}]


async def _ingest_item_locally(
    item: dict,
    bands: list[str],
    chunk_store,
    catalog,
    collection: str,
) -> list[dict]:
    """Download and ingest a single item locally."""
    from .ingest import ingest_cog

    results = []
    assets = item["assets"]

    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        for band_name in bands:
            asset_key = BAND_MAP_S2.get(band_name, band_name.lower())
            if asset_key not in assets:
                logger.warning(f"Band {band_name} ({asset_key}) not in assets for {item['id']}")
                results.append({"error": f"Band {band_name} not found", "product": item["id"]})
                continue

            item_id_full = f"{item['id']}_{band_name}"
            if catalog.get_item(item_id_full):
                print(f"    \u2714 {band_name} (already ingested)")
                results.append({"item_id": item_id_full, "band": band_name, "product": item["id"], "skipped": True})
                continue

            cog_url = assets[asset_key]["href"]
            print(f"    \u2b07 {band_name}...")

            try:
                resp = await client.get(cog_url)
                resp.raise_for_status()

                with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
                    tmp.write(resp.content)
                    tmp_path = Path(tmp.name)

                try:
                    item_obj = ingest_cog(
                        file_path=tmp_path,
                        chunk_store=chunk_store,
                        catalog=catalog,
                        collection_id=collection,
                        item_id=f"{item['id']}_{band_name}",
                    )
                    results.append({"item_id": item_obj.id, "band": band_name, "product": item["id"]})
                    logger.info(f"Ingested {band_name} from {item['id']}")
                finally:
                    tmp_path.unlink(missing_ok=True)
            except Exception as e:
                if "Storage limit" in str(e):
                    logger.warning(f"Storage full during {item['id']}/{band_name}")
                    results.append({"error": str(e), "product": item["id"], "band": band_name, "storage_full": True})
                    return results  # Stop — no point trying more bands
                logger.error(f"Failed {band_name} from {item['id']}: {e}")
                results.append({"error": str(e), "product": item["id"], "band": band_name})

    return results


async def fetch_and_ingest_element84(
    chunk_store,
    catalog,
    bbox: tuple[float, float, float, float],
    start_date: str | None = None,
    end_date: str | None = None,
    cloud_cover: float = 30.0,
    bands: list[str] | None = None,
    limit: int = 1,
    earthgrid_collection: str = "sentinel-2-l2a",
    distribute: bool = True,
    local_node_id: str = "",
) -> list[dict]:
    """Search Element84 and distribute ingestion across the grid.

    When distribute=True (default), items are spread across all available
    nodes proportional to their free storage. Local node gets its share
    too. When distribute=False, all items are ingested locally (old behavior).
    """
    from .ingest import ingest_cog

    items = await search_element84(
        bbox=bbox,
        start_date=start_date,
        end_date=end_date,
        cloud_cover=cloud_cover,
        limit=limit,
        collection=earthgrid_collection,
    )

    if not items:
        logger.info("No items found on Element84")
        return []

    target_bands = bands or ["B02", "B03", "B04", "B08", "SCL"]
    results = []

    # Try to get grid nodes for distribution
    nodes = []
    if distribute:
        from . import _FALLBACK_BEACON
        try:
            from .config import settings
            beacon_url = settings.beacon_url or _FALLBACK_BEACON
            node_id = local_node_id or settings.node_id
        except Exception:
            beacon_url = _FALLBACK_BEACON
            node_id = local_node_id

        nodes = await _get_grid_nodes(beacon_url, node_id)

    # Filter out full nodes (< 0.5 GB free)
    if nodes:
        full = [n for n in nodes if n["free_gb"] < 0.5]
        nodes = [n for n in nodes if n["free_gb"] >= 0.5]
        if full:
            print(f"  \u26a0\ufe0f  Skipping {len(full)} full node(s): {', '.join(n['node_name'] for n in full)}")

    # If we have multiple nodes, distribute items across them
    if len(nodes) > 1 and distribute:
        total_free = sum(n["free_gb"] for n in nodes)
        if total_free <= 0:
            total_free = len(nodes)  # equal split if all report 0

        print(f"\n\U0001f310 Distributing {len(items)} items across {len(nodes)} nodes:")
        for n in nodes:
            tag = " (local)" if n["is_local"] else ""
            print(f"  \u2022 {n['node_name']}{tag}: {n['free_gb']:.0f} GB free")

        # Assign items to nodes proportional to free space (round-robin with weights)
        assignments = {n["node_id"]: [] for n in nodes}
        node_cycle = []
        for n in nodes:
            weight = max(1, int((n["free_gb"] / total_free) * len(items)))
            node_cycle.extend([n["node_id"]] * weight)

        # Distribute items
        for i, item in enumerate(items):
            target_id = node_cycle[i % len(node_cycle)]
            assignments[target_id].append(item)

        # Process all nodes in parallel
        async def _process_node(node, node_items):
            """Process items for a single node (local or remote)."""
            node_results = []
            if node["is_local"]:
                print(f"\n\U0001f4e6 Local ({node['node_name']}): {len(node_items)} items")
                remaining = list(node_items)
                while remaining:
                    item = remaining.pop(0)
                    date_str = item["date"][:10] if item["date"] else "?"
                    cc = item["cloud_cover"]
                    print(f"  \U0001f4e6 {item['id']}  ({date_str}, {cc:.0f}% cloud)")
                    r = await _ingest_item_locally(item, target_bands, chunk_store, catalog, earthgrid_collection)
                    if any(x.get("storage_full") for x in r if isinstance(x, dict)):
                        # Re-delegate this + remaining items to remote nodes
                        overflow = [item] + remaining
                        remote_nodes = [n for n in nodes if not n["is_local"] and n["free_gb"] >= 0.5]
                        if remote_nodes:
                            print(f"  \u26a0\ufe0f  Local storage full — re-delegating {len(overflow)} items to {remote_nodes[0]['node_name']}")
                            for ov_item in overflow:
                                ov_r = await _delegate_item_to_node(remote_nodes[0], ov_item, target_bands, earthgrid_collection, cloud_cover)
                                node_results.extend(ov_r)
                        else:
                            print(f"  \u274c No remote nodes available for overflow!")
                            node_results.extend(r)
                        break
                    node_results.extend(r)
            else:
                print(f"\n\U0001f4e1 Remote ({node['node_name']}): {len(node_items)} items")
                tasks = []
                for item in node_items:
                    date_str = item["date"][:10] if item["date"] else "?"
                    cc = item["cloud_cover"]
                    print(f"  \U0001f4e1 {item['id']}  ({date_str}, {cc:.0f}% cloud) \u2192 {node['node_name']}")
                    tasks.append(_delegate_item_to_node(node, item, target_bands, earthgrid_collection, cloud_cover))
                # All items to this remote node in parallel
                batch_results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in batch_results:
                    if isinstance(r, Exception):
                        node_results.append({"error": str(r)})
                    else:
                        node_results.extend(r)
            return node_results

        # Launch all nodes in parallel (local + all remotes simultaneously)
        node_tasks = []
        for node in nodes:
            node_items = assignments[node["node_id"]]
            if node_items:
                node_tasks.append(_process_node(node, node_items))

        all_node_results = await asyncio.gather(*node_tasks, return_exceptions=True)
        for r in all_node_results:
            if isinstance(r, Exception):
                results.append({"error": str(r)})
            else:
                results.extend(r)
    else:
        # Single node or distribution disabled — local only
        if distribute and not nodes:
            print("  \u2139\ufe0f  No grid nodes found — fetching locally only")

        async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
            for item in items:
                date_str = item["date"][:10] if item["date"] else "?"
                cc = item["cloud_cover"]
                print(f"\n  \U0001f4e6 {item['id']}  ({date_str}, {cc:.0f}% cloud)")
                r = await _ingest_item_locally(item, target_bands, chunk_store, catalog, earthgrid_collection)
                results.extend(r)

    return results
