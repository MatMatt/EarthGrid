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


async def _beacon_inventory(beacon_url: str, bbox: tuple, collection: str,
                             start_date: str = None, end_date: str = None) -> set[str]:
    """Ask beacon for all item IDs it already has for this bbox/collection.
    Returns a set of item IDs that exist in the grid."""
    params = {"collections": collection, "limit": 10000}
    if bbox:
        params["bbox"] = ",".join(str(x) for x in bbox)
    if start_date or end_date:
        s = start_date or "2015-01-01"
        e = end_date or "2099-12-31"
        params["datetime"] = f"{s}T00:00:00Z/{e}T23:59:59Z"
    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(f"{beacon_url}/search", params=params)
            if resp.status_code == 200:
                data = resp.json()
                features = data.get("features", [])
                ids = set()
                for feat in features:
                    fid = feat.get("id", "")
                    ids.add(fid)
                    # Also add without band suffix for matching
                    # e.g. "S2A_32TPS_20240101_0_L2A_B02" -> "S2A_32TPS_20240101_0_L2A"
                    base = "_".join(fid.split("_")[:5]) if "_" in fid else fid
                    ids.add(base)
                return ids
    except Exception as e:
        logger.debug(f"Beacon inventory check failed: {e}")
    return set()


BAND_MAP_S2 = {
    "B02": "blue", "B03": "green", "B04": "red", "B08": "nir",
    "B05": "rededge1", "B06": "rededge2", "B07": "rededge3",
    "B8A": "nir08", "B09": "nir09", "B11": "swir16", "B12": "swir22",
    "B01": "coastal", "SCL": "scl",
}


async def _search_chunk(client, bbox, start, end, cloud_cover, max_per_page, collection, fields):
    """Search a single time chunk, handling pagination."""
    body = {
        "collections": [collection],
        "bbox": list(bbox),
        "limit": max_per_page,
        "query": {"eo:cloud_cover": {"lte": cloud_cover}},
        "fields": fields,
        "datetime": f"{start}T00:00:00Z/{end}T23:59:59Z",
    }
    items = []
    while True:
        resp = await client.post(f"{STAC_API}/search", json=body)
        if resp.status_code != 200:
            break
        data = resp.json()
        page = data.get("features", [])
        if not page:
            break
        items.extend(page)
        # Follow pagination
        next_link = None
        for link in data.get("links", []):
            if link.get("rel") == "next":
                next_link = link.get("body") or link.get("href")
                break
        if not next_link or len(page) < max_per_page:
            break
        if isinstance(next_link, dict):
            body.update(next_link)
        else:
            break
    return items


async def search_element84(
    bbox: tuple[float, float, float, float],
    start_date: str | None = None,
    end_date: str | None = None,
    cloud_cover: float = 30.0,
    limit: int = 5,
    collection: str = "sentinel-2-l2a",
) -> list[dict]:
    """Search Element84 STAC for items. Splits time range into parallel chunks."""
    import time as _time
    from datetime import datetime as _dt, timedelta as _td
    _t0 = _time.monotonic()
    max_per_page = 250
    target = limit if limit > 0 else 99999

    fields = {
        "include": ["id", "bbox", "geometry", "properties.datetime",
                     "properties.eo:cloud_cover", "properties.grid:code",
                     "properties.s2:mgrs_tile", "assets"],
        "exclude": ["links"]
    }

    s = start_date or "2015-01-01"
    e = end_date or _dt.now().strftime("%Y-%m-%d")

    # Split time range into ~6-month chunks for parallel search
    d_start = _dt.strptime(s, "%Y-%m-%d")
    d_end = _dt.strptime(e, "%Y-%m-%d")
    chunks = []
    chunk_start = d_start
    while chunk_start < d_end:
        chunk_end = min(chunk_start + _td(days=182), d_end)
        chunks.append((chunk_start.strftime("%Y-%m-%d"), chunk_end.strftime("%Y-%m-%d")))
        chunk_start = chunk_end + _td(days=1)

    n_chunks = len(chunks)
    print(f"  \U0001f50d Searching {n_chunks} time chunks in parallel...", end="", flush=True)

    async with httpx.AsyncClient(timeout=60) as client:
        tasks = [
            _search_chunk(client, bbox, cs, ce, cloud_cover, max_per_page, collection, fields)
            for cs, ce in chunks
        ]
        chunk_results = await asyncio.gather(*tasks, return_exceptions=True)

    items = []
    for r in chunk_results:
        if isinstance(r, list):
            items.extend(r)

    # Deduplicate by item ID (chunks might overlap at boundaries)
    seen = set()
    unique = []
    for item in items:
        iid = item.get("id", "")
        if iid not in seen:
            seen.add(iid)
            unique.append(item)
    items = unique[:target]

    _elapsed = _time.monotonic() - _t0
    print(f"\r  \U0001f50d {len(items)} items found ({_elapsed:.1f}s)        ")
    # Sort by date descending (client-side, since we removed server-side sort)
    items.sort(key=lambda x: x.get("properties", {}).get("datetime", ""), reverse=True)
    if items:
        print()  # newline after progress counter

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


async def _check_node_health(node: dict, timeout: float = 5) -> bool:
    """Quick health check — is the node reachable?"""
    async with httpx.AsyncClient(timeout=timeout) as client:
        try:
            resp = await client.get(f"{node['url']}/health")
            return resp.status_code == 200
        except Exception:
            return False


async def _delegate_batch_to_node(
    node: dict,
    items: list[dict],
    bands: list[str],
    collection: str,
    cloud_cover: float,
) -> list[dict]:
    """Send a single batch fetch request to a remote node covering all items.
    Uses the bbox union and full date range instead of per-item requests."""
    if not items:
        return []
    
    # Compute bounding box union
    all_bboxes = [it.get("bbox", []) for it in items if it.get("bbox")]
    if all_bboxes:
        min_lon = min(b[0] for b in all_bboxes)
        min_lat = min(b[1] for b in all_bboxes)
        max_lon = max(b[2] for b in all_bboxes)
        max_lat = max(b[3] for b in all_bboxes)
        bbox_str = f"{min_lon},{min_lat},{max_lon},{max_lat}"
    else:
        bbox_str = ""
    
    # Date range
    dates = sorted([it["date"][:10] for it in items if it.get("date")])
    start = dates[0] if dates else ""
    end = dates[-1] if dates else ""
    
    params = {
        "bbox": bbox_str,
        "start": start,
        "end": end,
        "cloud": cloud_cover,
        "limit": 0,  # no limit — fetch all
        "source": "element84",
        "collection": collection,
    }
    if bands:
        params["bands"] = ",".join(bands)
    
    headers = {}
    if node.get("admin_key"):
        headers["Authorization"] = f"Bearer {node['admin_key']}"
    
    try:
        async with httpx.AsyncClient(timeout=300) as client:
            resp = await client.post(
                f"{node['url']}/fetch",
                params=params,
                headers=headers,
            )
            result = resp.json()
            status = result.get("status", "")
            if status == "accepted":
                job_id = result.get("job_id", "?")
                return [{"status": "delegated_batch", "node": node["url"],
                         "job_id": job_id, "items_count": len(items),
                         "message": f"Batch fetch of {len(items)} items started on {node['node_name']}"}]
            ingested = result.get("ingested", 0)
            skipped = result.get("skipped", 0)
            return [{"status": "batch_done", "node": node["node_name"],
                     "ingested": ingested, "skipped": skipped,
                     "items_sent": len(items)}]
    except Exception as e:
        logger.error(f"Batch delegation to {node['node_name']} failed: {e}")
        return [{"error": str(e), "node": node["node_name"], "items_count": len(items)}]


async def _delegate_item_to_node(
    node: dict,
    item: dict,
    bands: list[str],
    collection: str,
    cloud_cover: float,
    retries: int = 2,
) -> list[dict]:
    """Send a fetch request to a remote node with retry logic."""
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

    last_error = None
    for attempt in range(retries + 1):
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{node['url']}/fetch",
                    params=params,
                    headers=headers,
                )
                result = resp.json()
                status = result.get("status", "")
                if status == "accepted":
                    job_id = result.get("job_id", "?")
                    return [{"status": "delegated", "node": node["url"], "job_id": job_id,
                             "message": f"Background fetch started on {node['node_name']} (job {job_id})"}]
                ingested = result.get("ingested", 0)
                return result.get("details", [{"status": "delegated", "node": node["url"], "ingested": ingested}])
        except Exception as e:
            last_error = e
            if attempt < retries:
                wait = 2 ** attempt  # 1s, 2s backoff
                logger.warning(f"Delegation to {node['node_name']} attempt {attempt+1} failed: {e} — retrying in {wait}s")
                await asyncio.sleep(wait)
            else:
                logger.error(f"Delegation to {node['node_name']} failed after {retries+1} attempts: {e}")

    return [{"error": f"Delegation to {node['node_name']} failed after {retries+1} attempts: {last_error}",
             "item_id": item.get("id", ""), "retry_failed": True}]


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

    target_bands = bands or ["B02", "B03", "B04", "B08", "SCL"]
    results = []

    # Try to get grid nodes for distribution
    nodes = []
    beacon_url = ""
    if distribute:
        from . import BOOTSTRAP_PEERS
        try:
            from .config import settings
            beacon_url = settings.beacon_url or BOOTSTRAP_PEERS[0]
            node_id = local_node_id or settings.node_id
        except Exception:
            beacon_url = BOOTSTRAP_PEERS[0]
            node_id = local_node_id

        nodes = await _get_grid_nodes(beacon_url, node_id)

    # ── Beacon-first: check what the grid already has ──
    grid_item_ids = set()
    if beacon_url:
        import time as _time
        _t0 = _time.monotonic()
        print("  🔍 Checking grid inventory...", end="", flush=True)
        grid_item_ids = await _beacon_inventory(beacon_url, bbox, earthgrid_collection,
                                                 start_date, end_date)
        _elapsed = _time.monotonic() - _t0
        if grid_item_ids:
            print(f" {len(grid_item_ids)} items already in grid ({_elapsed:.1f}s)")
        else:
            print(f" empty ({_elapsed:.1f}s)")

    # ── Search Element84 for available items ──
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

    # ── Filter: skip items already in the grid ──
    if grid_item_ids:
        before = len(items)
        items_new = [it for it in items if it["id"] not in grid_item_ids]
        skipped = before - len(items_new)
        if skipped:
            print(f"  ⏭ {skipped} items already in grid, {len(items_new)} new to fetch")
            # Report skipped items
            for it in items:
                if it["id"] in grid_item_ids:
                    results.append({"item_id": it["id"], "skipped": True,
                                    "reason": "already in grid"})
        items = items_new
        if not items:
            print("  ✅ All items already in the grid!")
            return results

    # Filter out full nodes (< 0.5 GB free)
    if nodes:
        full = [n for n in nodes if n["free_gb"] < 0.5]
        nodes = [n for n in nodes if n["free_gb"] >= 0.5]
        if full:
            print(f"  \u26a0\ufe0f  Skipping {len(full)} full node(s): {', '.join(n['node_name'] for n in full)}")

    # Check if local node was filtered out (full) but remote nodes exist
    local_available = any(n["is_local"] for n in nodes)
    remote_available = [n for n in nodes if not n["is_local"]]

    # If we have nodes to distribute to (even just 1 remote when local is full)
    if nodes and distribute and (len(nodes) > 1 or (not local_available and remote_available)):
        # If local node was removed (full), all items go to remote nodes
        if not local_available and remote_available:
            print(f"\n\U0001f4e1 Local storage full — delegating all {len(items)} items to remote nodes")
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
                # Health check before sending items
                healthy = await _check_node_health(node)
                if not healthy:
                    print(f"\n\u274c {node['node_name']} unreachable — redistributing {len(node_items)} items")
                    # Find alternative nodes
                    alt_nodes = [n for n in nodes if n["node_id"] != node["node_id"]
                                 and n["free_gb"] >= 0.5 and not n.get("_failed")]
                    if alt_nodes:
                        alt = alt_nodes[0]
                        print(f"  \u21b3 Redirecting to {alt['node_name']}")
                        for item in node_items:
                            r = await _delegate_item_to_node(alt, item, target_bands, earthgrid_collection, cloud_cover)
                            node_results.extend(r)
                    else:
                        # Try local as last resort
                        print(f"  \u21b3 No alternatives — trying local ingest")
                        for item in node_items:
                            try:
                                r = await _ingest_item_locally(item, target_bands, chunk_store, catalog, earthgrid_collection)
                                node_results.extend(r)
                            except Exception as e:
                                node_results.append({"error": str(e), "item_id": item.get("id", "")})
                    return node_results

                dates = sorted([it["date"][:10] for it in node_items if it.get("date")])
                print(f"\n\U0001f4e1 Remote ({node['node_name']}): {len(node_items)} items ({dates[0] if dates else '?'} to {dates[-1] if dates else '?'})")
                print(f"  Sending batch fetch request...", end="", flush=True)
                r = await _delegate_batch_to_node(node, node_items, target_bands, earthgrid_collection, cloud_cover)
                node_results.extend(r)
                if any(x.get("error") for x in r if isinstance(x, dict)):
                    print(f" \u274c failed")
                    # Fallback to per-item delegation
                    node["_failed"] = True
                    alt_nodes = [n for n in nodes if n["node_id"] != node["node_id"]
                                 and n["free_gb"] >= 0.5 and not n.get("_failed")]
                    if alt_nodes:
                        alt = alt_nodes[0]
                        print(f"  \u26a0\ufe0f  Retrying on {alt['node_name']}...")
                        r2 = await _delegate_batch_to_node(alt, node_items, target_bands, earthgrid_collection, cloud_cover)
                        node_results.extend(r2)
                else:
                    msg = r[0].get("message", "done") if r else "done"
                    print(f" \u2705 {msg}")
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

        for item in items:
            date_str = item["date"][:10] if item["date"] else "?"
            cc = item["cloud_cover"]
            print(f"\n  \U0001f4e6 {item['id']}  ({date_str}, {cc:.0f}% cloud)")
            try:
                r = await _ingest_item_locally(item, target_bands, chunk_store, catalog, earthgrid_collection)
                results.extend(r)
            except Exception as e:
                logger.error(f"Failed to ingest {item['id']}: {e}")
                results.append({"error": str(e), "item_id": item.get("id", "")})
                # Continue with next item instead of crashing
                continue

    return results
