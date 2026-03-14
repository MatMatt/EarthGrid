#!/usr/bin/env python3
"""Migrate EarthGrid from band-level/legacy chunking to spatial tiling.

Groups items that belong to the same scene (same MGRS tile + date),
reconstructs all bands, and re-chunks as spatial tiles.

Usage:
    python scripts/migrate_to_spatial.py --db /mnt/sda/earthgrid/catalog.db --store /mnt/sda/earthgrid/store [--dry-run]
"""
from __future__ import annotations
import argparse
import hashlib
import json
import math
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np


def parse_args():
    p = argparse.ArgumentParser(description="Migrate EarthGrid to spatial tiling")
    p.add_argument("--db", required=True, help="Path to catalog.db")
    p.add_argument("--store", required=True, help="Path to chunk store directory")
    p.add_argument("--tile-size", type=int, default=512, help="Tile size (default: 512)")
    p.add_argument("--dry-run", action="store_true", help="Report only, don't modify")
    return p.parse_args()


def chunk_path(store: Path, sha: str) -> Path:
    return store / sha[:2] / sha[2:4] / sha


def chunk_get(store: Path, sha: str) -> bytes | None:
    p = chunk_path(store, sha)
    return p.read_bytes() if p.exists() else None


def chunk_put(store: Path, data: bytes) -> str:
    sha = hashlib.sha256(data).hexdigest()
    p = chunk_path(store, sha)
    if not p.exists():
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(data)
    return sha


def chunk_delete(store: Path, sha: str) -> bool:
    p = chunk_path(store, sha)
    if p.exists():
        p.unlink()
        return True
    return False


def load_items(db_path: str) -> list[dict]:
    """Load all items from catalog."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM items").fetchall()
    items = []
    for r in rows:
        items.append({
            "id": r["id"],
            "collection": r["collection"],
            "geometry_json": r["geometry_json"],
            "bbox": [r["bbox_west"], r["bbox_south"], r["bbox_east"], r["bbox_north"]],
            "properties": json.loads(r["properties_json"]),
            "assets": json.loads(r["assets_json"]),
            "chunk_hashes": json.loads(r["chunk_hashes_json"]),
        })
    conn.close()
    return items


def extract_scene_key(item: dict) -> str | None:
    """Extract a grouping key: MGRS_tile + date.
    
    E.g., 'T32TPT_20240709' or 'T33UUB_20260311'.
    Returns None if we can't parse it.
    """
    item_id = item["id"]
    # Try to find MGRS tile pattern (T + 2digits + 3letters)
    mgrs_match = re.search(r'(T\d{2}[A-Z]{3})', item_id)
    # Try to find date pattern (8 digits)
    date_match = re.search(r'(\d{8})', item_id)
    
    if mgrs_match and date_match:
        return f"{mgrs_match.group(1)}_{date_match.group(1)}"
    return None


def reconstruct_band_from_item(item: dict, store: Path) -> dict[str, np.ndarray]:
    """Reconstruct band data from a single item."""
    props = item["properties"]
    width = props["earthgrid:width"]
    height = props["earthgrid:height"]
    dtype = np.dtype(props["earthgrid:dtype"])
    tile_size = props["earthgrid:tile_size"]
    tile_cols = props["earthgrid:tile_cols"]
    tile_rows = props["earthgrid:tile_rows"]
    n_bands = props["earthgrid:bands"]
    band_names = props.get("earthgrid:band_names", [f"B{i+1:02d}" for i in range(n_bands)])
    chunk_format = props.get("earthgrid:chunk_format", "legacy")
    hashes = item["chunk_hashes"]

    if chunk_format == "band-level" and isinstance(hashes, dict):
        # Band-level: hashes = {"B04": ["sha1", ...]}
        result = {}
        for band_name, band_hashes in hashes.items():
            band_data = np.zeros((height, width), dtype=dtype)
            for idx, sha in enumerate(band_hashes):
                row_i = idx // tile_cols
                col_i = idx % tile_cols
                raw = chunk_get(store, sha)
                if raw is None:
                    continue
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)
                tile = np.frombuffer(raw, dtype=dtype).reshape(h, w)
                band_data[y_off:y_off + h, x_off:x_off + w] = tile
            result[band_name] = band_data
        return result
    else:
        # Legacy or spatial-tile: hashes = ["sha1", ...]
        if isinstance(hashes, dict):
            hashes = list(hashes.values())[0] if hashes else []
        full = np.zeros((n_bands, height, width), dtype=dtype)
        for idx, sha in enumerate(hashes):
            row_i = idx // tile_cols
            col_i = idx % tile_cols
            raw = chunk_get(store, sha)
            if raw is None:
                continue
            x_off = col_i * tile_size
            y_off = row_i * tile_size
            w = min(tile_size, width - x_off)
            h = min(tile_size, height - y_off)
            tile = np.frombuffer(raw, dtype=dtype).reshape(n_bands, h, w)
            full[:, y_off:y_off + h, x_off:x_off + w] = tile
        return {name: full[i] for i, name in enumerate(band_names)}


def collect_old_chunks(items: list[dict]) -> set[str]:
    """Collect all chunk hashes from a list of items."""
    chunks = set()
    for item in items:
        hashes = item["chunk_hashes"]
        if isinstance(hashes, dict):
            for band_hashes in hashes.values():
                chunks.update(band_hashes)
        elif isinstance(hashes, list):
            chunks.update(hashes)
    return chunks


def migrate_group(
    items: list[dict],
    store: Path,
    db_path: str,
    tile_size: int,
    dry_run: bool,
) -> dict:
    """Migrate a group of items (same scene) to spatial tiling."""
    # Collect all bands from all items in the group
    all_bands: dict[str, np.ndarray] = {}
    ref_item = items[0]  # Use first item for spatial metadata
    ref_props = ref_item["properties"]

    for item in items:
        try:
            bands = reconstruct_band_from_item(item, store)
            all_bands.update(bands)
        except Exception as e:
            print(f"  WARNING: Could not reconstruct {item['id']}: {e}")

    if not all_bands:
        return {"status": "skip", "reason": "no data"}

    # All bands must have the same dimensions — use the first item's dims
    # (items from same scene at same resolution should match)
    width = ref_props["earthgrid:width"]
    height = ref_props["earthgrid:height"]
    dtype = np.dtype(ref_props["earthgrid:dtype"])
    
    # Check if all bands match dimensions
    mismatched = []
    for name, arr in all_bands.items():
        if arr.shape != (height, width):
            mismatched.append(f"{name}: {arr.shape} != ({height}, {width})")
    
    if mismatched:
        # Can't merge bands with different resolutions — migrate each item individually
        print(f"  Dimension mismatch: {mismatched}")
        print(f"  → Migrating each item individually")
        results = []
        for item in items:
            r = migrate_single_item(item, store, db_path, tile_size, dry_run)
            results.append(r)
        return {"status": "individual", "results": results}

    band_names = sorted(all_bands.keys())
    n_bands = len(band_names)

    # Stack all bands: (n_bands, height, width)
    stack = np.stack([all_bands[b] for b in band_names], axis=0)

    n_cols = math.ceil(width / tile_size)
    n_rows = math.ceil(height / tile_size)

    if dry_run:
        return {
            "status": "would_migrate",
            "bands": band_names,
            "tiles": n_rows * n_cols,
            "items_merged": len(items),
        }

    # Create spatial tile chunks
    new_hashes = []
    for row_i in range(n_rows):
        for col_i in range(n_cols):
            x_off = col_i * tile_size
            y_off = row_i * tile_size
            w = min(tile_size, width - x_off)
            h = min(tile_size, height - y_off)
            tile = stack[:, y_off:y_off + h, x_off:x_off + w]
            raw = tile.tobytes()
            sha = chunk_put(store, raw)
            new_hashes.append(sha)

    # Build new combined item ID
    scene_key = extract_scene_key(ref_item)
    new_item_id = scene_key if scene_key else ref_item["id"] + "_spatial"
    
    # Check if there are multiple resolutions → add resolution suffix
    if len(items) == 1:
        new_item_id = ref_item["id"]  # Keep original ID for single items

    new_properties = {
        "datetime": ref_props.get("datetime", ""),
        "earthgrid:crs": ref_props.get("earthgrid:crs", ""),
        "earthgrid:width": width,
        "earthgrid:height": height,
        "earthgrid:bands": n_bands,
        "earthgrid:band_names": band_names,
        "earthgrid:dtype": str(dtype),
        "earthgrid:tile_size": tile_size,
        "earthgrid:tile_cols": n_cols,
        "earthgrid:tile_rows": n_rows,
        "earthgrid:source_file": ref_props.get("earthgrid:source_file", ""),
        "earthgrid:chunk_format": "spatial-tile",
        "earthgrid:migrated_from": [i["id"] for i in items],
    }

    new_assets = {
        "data": {
            "href": "/chunks",
            "type": "application/octet-stream",
            "title": "Spatial-tiled raster data (all bands per tile)",
            "earthgrid:chunk_count": len(new_hashes),
            "earthgrid:bands_available": band_names,
        }
    }

    # Write to DB
    conn = sqlite3.connect(db_path)
    
    # Delete old items
    old_ids = [i["id"] for i in items]
    for old_id in old_ids:
        conn.execute("DELETE FROM items WHERE id = ?", (old_id,))
    
    # Insert new item
    bbox = ref_item["bbox"]
    conn.execute(
        """INSERT OR REPLACE INTO items
           (id, collection, geometry_json, bbox_west, bbox_south, bbox_east, bbox_north,
            datetime, properties_json, assets_json, chunk_hashes_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (new_item_id, ref_item["collection"], ref_item["geometry_json"],
         bbox[0], bbox[1], bbox[2], bbox[3],
         new_properties["datetime"],
         json.dumps(new_properties), json.dumps(new_assets),
         json.dumps(new_hashes)),
    )
    conn.commit()
    conn.close()

    return {
        "status": "migrated",
        "new_id": new_item_id,
        "bands": band_names,
        "tiles": len(new_hashes),
        "old_items": old_ids,
    }


def migrate_single_item(
    item: dict,
    store: Path,
    db_path: str,
    tile_size: int,
    dry_run: bool,
) -> dict:
    """Migrate a single item to spatial-tile format."""
    props = item["properties"]
    if props.get("earthgrid:chunk_format") == "spatial-tile":
        return {"status": "skip", "reason": "already spatial-tile"}

    try:
        bands = reconstruct_band_from_item(item, store)
    except Exception as e:
        return {"status": "error", "reason": str(e)}

    if not bands:
        return {"status": "skip", "reason": "no data"}

    width = props["earthgrid:width"]
    height = props["earthgrid:height"]
    dtype = np.dtype(props["earthgrid:dtype"])
    band_names = sorted(bands.keys())
    n_bands = len(band_names)
    n_cols = math.ceil(width / tile_size)
    n_rows = math.ceil(height / tile_size)

    if dry_run:
        return {"status": "would_migrate", "bands": band_names, "tiles": n_rows * n_cols}

    stack = np.stack([bands[b] for b in band_names], axis=0)

    new_hashes = []
    for row_i in range(n_rows):
        for col_i in range(n_cols):
            x_off = col_i * tile_size
            y_off = row_i * tile_size
            w = min(tile_size, width - x_off)
            h = min(tile_size, height - y_off)
            tile = stack[:, y_off:y_off + h, x_off:x_off + w]
            raw = tile.tobytes()
            sha = chunk_put(store, raw)
            new_hashes.append(sha)

    new_properties = dict(props)
    new_properties["earthgrid:chunk_format"] = "spatial-tile"
    new_properties["earthgrid:band_names"] = band_names
    new_properties["earthgrid:bands"] = n_bands
    new_properties["earthgrid:tile_size"] = tile_size
    new_properties["earthgrid:tile_cols"] = n_cols
    new_properties["earthgrid:tile_rows"] = n_rows

    new_assets = {
        "data": {
            "href": "/chunks",
            "type": "application/octet-stream",
            "title": "Spatial-tiled raster data (all bands per tile)",
            "earthgrid:chunk_count": len(new_hashes),
            "earthgrid:bands_available": band_names,
        }
    }

    conn = sqlite3.connect(db_path)
    bbox = item["bbox"]
    conn.execute(
        """UPDATE items SET
            properties_json = ?, assets_json = ?, chunk_hashes_json = ?
            WHERE id = ?""",
        (json.dumps(new_properties), json.dumps(new_assets),
         json.dumps(new_hashes), item["id"]),
    )
    conn.commit()
    conn.close()

    return {"status": "migrated", "id": item["id"], "bands": band_names, "tiles": len(new_hashes)}


def cleanup_orphaned_chunks(db_path: str, store: Path, dry_run: bool) -> dict:
    """Remove chunks that are no longer referenced by any item."""
    # Collect all referenced hashes
    conn = sqlite3.connect(db_path)
    rows = conn.execute("SELECT chunk_hashes_json FROM items").fetchall()
    conn.close()

    referenced = set()
    for (hashes_json,) in rows:
        hashes = json.loads(hashes_json)
        if isinstance(hashes, dict):
            for band_hashes in hashes.values():
                referenced.update(band_hashes)
        elif isinstance(hashes, list):
            referenced.update(hashes)

    # Find all chunks on disk
    on_disk = set()
    for p in store.rglob("*"):
        if p.is_file() and len(p.name) == 64:
            on_disk.add(p.name)

    orphaned = on_disk - referenced
    orphaned_bytes = 0

    if not dry_run:
        for sha in orphaned:
            p = chunk_path(store, sha)
            if p.exists():
                orphaned_bytes += p.stat().st_size
                p.unlink()
    else:
        for sha in orphaned:
            p = chunk_path(store, sha)
            if p.exists():
                orphaned_bytes += p.stat().st_size

    return {
        "orphaned_chunks": len(orphaned),
        "orphaned_bytes": orphaned_bytes,
        "orphaned_mb": round(orphaned_bytes / 1024 / 1024, 1),
        "cleaned": not dry_run,
    }


def main():
    args = parse_args()
    store = Path(args.store)
    db_path = args.db

    print(f"EarthGrid Migration: {'DRY RUN' if args.dry_run else 'LIVE'}")
    print(f"Database: {db_path}")
    print(f"Store: {store}")
    print(f"Tile size: {args.tile_size}")
    print()

    items = load_items(db_path)
    print(f"Total items: {len(items)}")

    # Separate already-migrated items
    to_migrate = [i for i in items if i["properties"].get("earthgrid:chunk_format") != "spatial-tile"]
    already_done = len(items) - len(to_migrate)
    if already_done:
        print(f"Already spatial-tile: {already_done}")
    print(f"To migrate: {len(to_migrate)}")
    print()

    if not to_migrate:
        print("Nothing to migrate!")
        return

    # Group by scene key (MGRS + date) and resolution
    groups: dict[str, list[dict]] = defaultdict(list)
    ungrouped = []

    for item in to_migrate:
        key = extract_scene_key(item)
        if key:
            # Also group by resolution (width x height)
            w = item["properties"].get("earthgrid:width", 0)
            h = item["properties"].get("earthgrid:height", 0)
            res_key = f"{key}_{w}x{h}"
            groups[res_key].append(item)
        else:
            ungrouped.append(item)

    print(f"Scene groups: {len(groups)}")
    print(f"Ungrouped items: {len(ungrouped)}")
    print()

    # Migrate groups
    for key, group_items in sorted(groups.items()):
        band_info = []
        for item in group_items:
            bn = item["properties"].get("earthgrid:band_names", [])
            band_info.extend(bn)
        print(f"Group {key}: {len(group_items)} items, bands: {sorted(set(band_info))}")
        
        result = migrate_group(group_items, store, db_path, args.tile_size, args.dry_run)
        print(f"  → {result}")
        print()

    # Migrate ungrouped
    for item in ungrouped:
        print(f"Single: {item['id']}")
        result = migrate_single_item(item, store, db_path, args.tile_size, args.dry_run)
        print(f"  → {result}")
        print()

    # Cleanup orphaned chunks
    print("Cleaning up orphaned chunks...")
    cleanup = cleanup_orphaned_chunks(db_path, store, args.dry_run)
    print(f"  Orphaned: {cleanup['orphaned_chunks']} chunks ({cleanup['orphaned_mb']} MB)")
    if cleanup["cleaned"]:
        print(f"  Deleted!")
    else:
        print(f"  (dry run — not deleted)")

    print("\nDone!")


if __name__ == "__main__":
    main()
