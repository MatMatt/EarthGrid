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

    def to_stac(self, base_url: str = "") -> dict:
        return {
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
