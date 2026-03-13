"""Content-Addressable Storage (CAS) — SHA-256 hash-based chunk storage."""
import hashlib
import json
import time
from pathlib import Path


class StorageLimitError(Exception):
    """Raised when storage limit would be exceeded."""
    pass


class ChunkStore:
    """Store and retrieve data chunks by their SHA-256 hash."""

    def __init__(self, store_path: Path, limit_gb: float = 0):
        self.store_path = store_path
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.limit_bytes = int(limit_gb * 1024 * 1024 * 1024) if limit_gb > 0 else 0

        # Stats tracking
        self._stats_file = store_path.parent / "stats.json" if store_path.parent.exists() else None
        self._stats = self._load_stats()

    def _load_stats(self) -> dict:
        if self._stats_file and self._stats_file.exists():
            try:
                return json.loads(self._stats_file.read_text())
            except Exception:
                pass
        return {
            "started": time.time(),
            "chunks_served": 0,
            "bytes_served": 0,
            "chunks_stored": 0,
            "bytes_ingested": 0,
            "requests_total": 0,
            "requests_today": 0,
            "today": time.strftime("%Y-%m-%d"),
            "daily_history": {},  # date → {served, bytes, requests}
        }

    def _save_stats(self):
        if self._stats_file:
            try:
                self._stats_file.write_text(json.dumps(self._stats, indent=2))
            except Exception:
                pass

    def _track_day(self):
        today = time.strftime("%Y-%m-%d")
        if self._stats.get("today") != today:
            self._stats["requests_today"] = 0
            self._stats["today"] = today

    def _track_serve(self, size: int):
        self._track_day()
        self._stats["chunks_served"] += 1
        self._stats["bytes_served"] += size
        self._stats["requests_total"] += 1
        self._stats["requests_today"] += 1
        today = self._stats["today"]
        day = self._stats["daily_history"].setdefault(today, {"served": 0, "bytes": 0, "requests": 0})
        day["served"] += 1
        day["bytes"] += size
        day["requests"] += 1
        # Keep last 90 days only
        if len(self._stats["daily_history"]) > 90:
            keys = sorted(self._stats["daily_history"].keys())
            for k in keys[:-90]:
                del self._stats["daily_history"][k]
        self._save_stats()

    def _track_store(self, size: int):
        self._stats["chunks_stored"] += 1
        self._stats["bytes_ingested"] += size
        self._save_stats()

    @property
    def stats(self) -> dict:
        self._track_day()
        uptime = time.time() - self._stats.get("started", time.time())
        return {
            "uptime_hours": round(uptime / 3600, 1),
            "chunks_served": self._stats["chunks_served"],
            "bytes_served": self._stats["bytes_served"],
            "chunks_stored": self._stats["chunks_stored"],
            "bytes_ingested": self._stats["bytes_ingested"],
            "requests_total": self._stats["requests_total"],
            "requests_today": self._stats["requests_today"],
            "storage_used_bytes": self.total_bytes,
            "chunk_count": self.chunk_count,
        }

    def _chunk_path(self, sha: str) -> Path:
        """Two-level directory: ab/cd/abcd1234..."""
        return self.store_path / sha[:2] / sha[2:4] / sha

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        """SHA-256 hash of raw bytes."""
        return hashlib.sha256(data).hexdigest()

    def put(self, data: bytes) -> str:
        """Store chunk, return its SHA-256 hash. Raises StorageLimitError if full."""
        sha = self.hash_bytes(data)
        path = self._chunk_path(sha)
        if not path.exists():
            if self.limit_bytes > 0 and (self.total_bytes + len(data)) > self.limit_bytes:
                raise StorageLimitError(
                    f"Storage limit reached ({self.limit_bytes / 1024**3:.1f} GB)"
                )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            self._track_store(len(data))
        return sha

    def get(self, sha: str) -> bytes | None:
        """Retrieve chunk by hash. Returns None if not found."""
        path = self._chunk_path(sha)
        if path.exists():
            data = path.read_bytes()
            self._track_serve(len(data))
            return data
        return None

    def has(self, sha: str) -> bool:
        """Check if chunk exists."""
        return self._chunk_path(sha).exists()

    def delete(self, sha: str) -> bool:
        """Delete chunk. Returns True if it existed."""
        path = self._chunk_path(sha)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_chunks(self) -> list[str]:
        """List all chunk hashes."""
        chunks = []
        for p in self.store_path.rglob("*"):
            if p.is_file() and len(p.name) == 64:
                chunks.append(p.name)
        return chunks

    @property
    def chunk_count(self) -> int:
        return len(self.list_chunks())

    @property
    def total_bytes(self) -> int:
        total = 0
        for p in self.store_path.rglob("*"):
            if p.is_file() and len(p.name) == 64:
                total += p.stat().st_size
        return total
"""Ingest COG/GeoTIFF files into EarthGrid — chunk, hash, catalog."""
import hashlib
import math
import os
from datetime import datetime, timezone
from pathlib import Path

try:
    import rasterio
    from rasterio.windows import Window
    HAS_RASTERIO = True
except ImportError:
    HAS_RASTERIO = False

from .chunk_store import ChunkStore
from .catalog import Catalog, STACItem, STACCollection

# Default chunk size: 512x512 pixels
DEFAULT_TILE_SIZE = 512


def ingest_cog(
    file_path: Path,
    chunk_store: ChunkStore,
    catalog: Catalog,
    collection_id: str = "default",
    item_id: str | None = None,
    tile_size: int = DEFAULT_TILE_SIZE,
) -> STACItem:
    """Ingest a COG/GeoTIFF: split into tiles, hash, store, catalog.

    Returns the created STAC item.
    """
    if not HAS_RASTERIO:
        raise ImportError(
            "Geospatial ingest requires rasterio. "
            "Install with: pip install earthgrid[geo]"
        )

    file_path = Path(file_path)
    if not item_id:
        item_id = file_path.stem

    with rasterio.open(file_path) as src:
        bounds = src.bounds
        bbox = [bounds.left, bounds.bottom, bounds.right, bounds.top]
        crs = str(src.crs)
        width, height = src.width, src.height
        n_bands = src.count
        dtype = str(src.dtypes[0])

        # Ensure collection exists
        col = catalog.get_collection(collection_id)
        if not col:
            catalog.add_collection(STACCollection(
                id=collection_id,
                title=collection_id,
                description=f"Collection: {collection_id}",
            ))

        # Split into tiles and store
        chunk_hashes = []
        n_cols = math.ceil(width / tile_size)
        n_rows = math.ceil(height / tile_size)

        for row_i in range(n_rows):
            for col_i in range(n_cols):
                x_off = col_i * tile_size
                y_off = row_i * tile_size
                w = min(tile_size, width - x_off)
                h = min(tile_size, height - y_off)

                window = Window(x_off, y_off, w, h)
                data = src.read(window=window)
                raw = data.tobytes()
                sha = chunk_store.put(raw)
                chunk_hashes.append(sha)

    # Build STAC item
    geometry = {
        "type": "Polygon",
        "coordinates": [[
            [bbox[0], bbox[1]],
            [bbox[2], bbox[1]],
            [bbox[2], bbox[3]],
            [bbox[0], bbox[3]],
            [bbox[0], bbox[1]],
        ]],
    }

    now = datetime.now(timezone.utc).isoformat()
    properties = {
        "datetime": now,
        "earthgrid:crs": crs,
        "earthgrid:width": width,
        "earthgrid:height": height,
        "earthgrid:bands": n_bands,
        "earthgrid:dtype": dtype,
        "earthgrid:tile_size": tile_size,
        "earthgrid:tile_cols": n_cols,
        "earthgrid:tile_rows": n_rows,
        "earthgrid:source_file": file_path.name,
    }

    assets = {
        "data": {
            "href": f"/chunks",
            "type": "application/octet-stream",
            "title": "Chunked raster data",
            "earthgrid:chunk_count": len(chunk_hashes),
        }
    }

    item = STACItem(
        id=item_id,
        collection=collection_id,
        geometry=geometry,
        bbox=bbox,
        properties=properties,
        assets=assets,
        chunk_hashes=chunk_hashes,
    )

    catalog.add_item(item)
    return item
"""Reconstruct files from chunks — reverse of ingest."""
import io
import math
from pathlib import Path

try:
    import numpy as np
    import rasterio
    from rasterio.transform import from_bounds
    HAS_RASTERIO = True
except ImportError:
    HAS_RASTERIO = False

from .chunk_store import ChunkStore
from .catalog import Catalog


def reconstruct_geotiff(
    item_id: str,
    collection_id: str,
    catalog: Catalog,
    chunk_store: ChunkStore,
) -> bytes:
    """Reconstruct a GeoTIFF from stored chunks.

    Returns the file as bytes (in-memory GeoTIFF).
    """
    if not HAS_RASTERIO:
        raise ImportError("Reconstruction requires rasterio. Install with: pip install earthgrid[geo]")

    # Get STAC item
    items = catalog.search(collections=[collection_id], limit=1000)
    item = None
    for i in items:
        if i.id == item_id:
            item = i
            break

    if not item:
        raise FileNotFoundError(f"Item {item_id} not found in {collection_id}")

    props = item.properties
    width = props["earthgrid:width"]
    height = props["earthgrid:height"]
    bands = props["earthgrid:bands"]
    dtype = props["earthgrid:dtype"]
    tile_size = props["earthgrid:tile_size"]
    tile_cols = props["earthgrid:tile_cols"]
    tile_rows = props["earthgrid:tile_rows"]
    crs = props.get("earthgrid:crs", "EPSG:4326")

    bbox = item.bbox  # [left, bottom, right, top]

    # Reconstruct raster from chunks
    full_data = np.zeros((bands, height, width), dtype=dtype)

    for idx, sha in enumerate(item.chunk_hashes):
        row_i = idx // tile_cols
        col_i = idx % tile_cols

        chunk_bytes = chunk_store.get(sha)
        if chunk_bytes is None:
            continue  # missing chunk — leave as zeros

        x_off = col_i * tile_size
        y_off = row_i * tile_size
        w = min(tile_size, width - x_off)
        h = min(tile_size, height - y_off)

        tile_data = np.frombuffer(chunk_bytes, dtype=dtype).reshape(bands, h, w)
        full_data[:, y_off:y_off + h, x_off:x_off + w] = tile_data

    # Write to in-memory GeoTIFF
    transform = from_bounds(bbox[0], bbox[1], bbox[2], bbox[3], width, height)

    buffer = io.BytesIO()
    with rasterio.open(
        buffer,
        "w",
        driver="GTiff",
        height=height,
        width=width,
        count=bands,
        dtype=dtype,
        crs=crs,
        transform=transform,
    ) as dst:
        dst.write(full_data)

    buffer.seek(0)
    return buffer.read()
"""SQLite-backed STAC catalog for a single node."""
import json
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path


@dataclass
class STACItem:
    id: str
    collection: str
    geometry: dict  # GeoJSON geometry
    bbox: list[float]  # [west, south, east, north]
    properties: dict  # must include "datetime"
    assets: dict  # name → {href, type, ...}
    chunk_hashes: list[str] = field(default_factory=list)

    def to_stac(self, base_url: str = "", include_chunks: bool = False) -> dict:
        d = {
            "type": "Feature",
            "stac_version": "1.0.0",
            "id": self.id,
            "collection": self.collection,
            "geometry": self.geometry,
            "bbox": self.bbox,
            "properties": self.properties,
            "assets": self.assets,
            "links": [],
        }
        if include_chunks:
            d["earthgrid:chunk_hashes"] = self.chunk_hashes
        return d


@dataclass
class STACCollection:
    id: str
    title: str
    description: str
    extent: dict = field(default_factory=lambda: {
        "spatial": {"bbox": [[-180, -90, 180, 90]]},
        "temporal": {"interval": [[None, None]]},
    })

    def to_stac(self) -> dict:
        return {
            "type": "Collection",
            "stac_version": "1.0.0",
            "id": self.id,
            "title": self.title,
            "description": self.description,
            "extent": self.extent,
            "links": [],
            "license": "EUPL-1.2",
        }


class Catalog:
    """SQLite-backed STAC catalog."""

    def __init__(self, db_path: Path):
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(str(db_path), check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS collections (
                id TEXT PRIMARY KEY,
                title TEXT,
                description TEXT,
                extent_json TEXT
            );
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                collection TEXT NOT NULL,
                geometry_json TEXT,
                bbox_west REAL, bbox_south REAL, bbox_east REAL, bbox_north REAL,
                datetime TEXT,
                properties_json TEXT,
                assets_json TEXT,
                chunk_hashes_json TEXT,
                created_at REAL DEFAULT (strftime('%s','now')),
                FOREIGN KEY (collection) REFERENCES collections(id)
            );
            CREATE INDEX IF NOT EXISTS idx_items_collection ON items(collection);
            CREATE INDEX IF NOT EXISTS idx_items_bbox ON items(bbox_west, bbox_south, bbox_east, bbox_north);
            CREATE INDEX IF NOT EXISTS idx_items_datetime ON items(datetime);
        """)

    # --- Collections ---

    def add_collection(self, col: STACCollection):
        self.db.execute(
            "INSERT OR REPLACE INTO collections (id, title, description, extent_json) VALUES (?, ?, ?, ?)",
            (col.id, col.title, col.description, json.dumps(col.extent)),
        )
        self.db.commit()

    def get_collection(self, col_id: str) -> STACCollection | None:
        row = self.db.execute("SELECT * FROM collections WHERE id = ?", (col_id,)).fetchone()
        if not row:
            return None
        return STACCollection(
            id=row["id"], title=row["title"], description=row["description"],
            extent=json.loads(row["extent_json"]),
        )

    def list_collections(self) -> list[STACCollection]:
        rows = self.db.execute("SELECT * FROM collections").fetchall()
        return [
            STACCollection(id=r["id"], title=r["title"], description=r["description"],
                           extent=json.loads(r["extent_json"]))
            for r in rows
        ]

    # --- Items ---

    def add_item(self, item: STACItem):
        dt = item.properties.get("datetime", "")
        self.db.execute(
            """INSERT OR REPLACE INTO items
               (id, collection, geometry_json, bbox_west, bbox_south, bbox_east, bbox_north,
                datetime, properties_json, assets_json, chunk_hashes_json)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (item.id, item.collection, json.dumps(item.geometry),
             item.bbox[0], item.bbox[1], item.bbox[2], item.bbox[3],
             dt, json.dumps(item.properties), json.dumps(item.assets),
             json.dumps(item.chunk_hashes)),
        )
        self.db.commit()

    def get_item(self, item_id: str) -> STACItem | None:
        row = self.db.execute("SELECT * FROM items WHERE id = ?", (item_id,)).fetchone()
        if not row:
            return None
        return self._row_to_item(row)

    def search(
        self,
        collections: list[str] | None = None,
        bbox: list[float] | None = None,
        datetime_range: str | None = None,
        limit: int = 100,
    ) -> list[STACItem]:
        """STAC-compatible search with bbox and datetime filtering."""
        query = "SELECT * FROM items WHERE 1=1"
        params: list = []

        if collections:
            placeholders = ",".join("?" * len(collections))
            query += f" AND collection IN ({placeholders})"
            params.extend(collections)

        if bbox and len(bbox) == 4:
            west, south, east, north = bbox
            query += " AND bbox_east >= ? AND bbox_west <= ? AND bbox_north >= ? AND bbox_south <= ?"
            params.extend([west, east, south, north])

        if datetime_range:
            parts = datetime_range.split("/")
            if len(parts) == 2:
                start, end = parts
                if start and start != "..":
                    query += " AND datetime >= ?"
                    params.append(start)
                if end and end != "..":
                    query += " AND datetime <= ?"
                    params.append(end)
            else:
                query += " AND datetime = ?"
                params.append(datetime_range)

        query += " ORDER BY datetime DESC LIMIT ?"
        params.append(limit)

        rows = self.db.execute(query, params).fetchall()
        return [self._row_to_item(r) for r in rows]

    def item_count(self) -> int:
        return self.db.execute("SELECT COUNT(*) FROM items").fetchone()[0]

    def summary(self) -> dict:
        """Quick summary for federation sync."""
        collections = self.list_collections()
        return {
            "collections": [c.id for c in collections],
            "item_count": self.item_count(),
        }

    def _row_to_item(self, row) -> STACItem:
        return STACItem(
            id=row["id"],
            collection=row["collection"],
            geometry=json.loads(row["geometry_json"]),
            bbox=[row["bbox_west"], row["bbox_south"], row["bbox_east"], row["bbox_north"]],
            properties=json.loads(row["properties_json"]),
            assets=json.loads(row["assets_json"]),
            chunk_hashes=json.loads(row["chunk_hashes_json"]),
        )
