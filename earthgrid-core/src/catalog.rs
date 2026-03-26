//! SQLite-backed STAC catalog for EarthGrid.
//!
//! Stores STAC Items and Collections with chunk hash references.

use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::error::Result;

/// A STAC Collection.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacCollection {
    pub id: String,
    pub title: String,
    pub description: String,
}

/// A STAC Item with chunk references.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacItem {
    pub id: String,
    pub collection: String,
    pub bbox: [f64; 4],
    pub properties: serde_json::Value,
    pub chunk_hashes: Vec<String>,
    pub created_at: f64,
    /// GeoJSON geometry (real tile footprint polygon)
    pub geometry: Option<serde_json::Value>,
}

/// Parsed datetime filter from STAC spec.
/// Supports: single datetime, open/closed ranges ("start/end", "../end", "start/..").
#[derive(Debug, Clone)]
pub struct DatetimeFilter {
    pub start: Option<String>, // inclusive lower bound (ISO 8601)
    pub end: Option<String>,   // inclusive upper bound (ISO 8601)
}

impl DatetimeFilter {
    /// Parse a STAC datetime parameter.
    /// Accepts: "2020-01-01T00:00:00Z" or "2020-01-01T00:00:00Z/2020-12-31T23:59:59Z"
    /// or "../2020-12-31" or "2020-01-01/.."
    pub fn parse(s: &str) -> Option<Self> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        if let Some((start, end)) = s.split_once('/') {
            let start = if start == ".." || start.is_empty() { None } else { Some(start.to_string()) };
            let end = if end == ".." || end.is_empty() { None } else { Some(end.to_string()) };
            Some(DatetimeFilter { start, end })
        } else {
            // Single datetime — treat as exact match by using same value for both bounds
            Some(DatetimeFilter {
                start: Some(s.to_string()),
                end: Some(s.to_string()),
            })
        }
    }
}

/// SQLite-backed STAC catalog.
pub struct Catalog {
    conn: Connection,
    /// MGRS tile reference polygons: tile_id -> [[lon,lat], ...]
    tile_grid: std::collections::HashMap<String, Vec<Vec<f64>>>,
}


/// A grid cell for spatial coverage
pub struct GridCell {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
    pub count: i64,
    pub collection: String,
}

/// An MGRS tile coverage entry with real tile geometry
pub struct MgrsTile {
    pub collection: String,
    pub tile_id: String,
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
    pub date_count: i64,
    pub item_count: i64,
    pub dates: Vec<String>,
    pub bands: Vec<String>,
    /// Real tile polygon coordinates [[lon,lat], ...] from STAC geometry
    pub polygon: Option<Vec<Vec<f64>>>,
}

impl Catalog {
    /// Open or create a catalog at the given path.
    pub fn new(db_path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        // WAL mode allows concurrent readers (Python + Rust)
        conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")?;
        let tile_grid = Self::load_tile_grid(db_path);
        let catalog = Self { conn, tile_grid };
        catalog.init_tables()?;
        Ok(catalog)
    }

    /// Create an in-memory catalog (for testing).
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let catalog = Self { conn, tile_grid: std::collections::HashMap::new() };
        catalog.init_tables()?;
        Ok(catalog)
    }

    /// Load S2 tile reference grid from s2_tile_grid.json (sibling of catalog.db or in data dir)
    fn load_tile_grid(db_path: &std::path::Path) -> std::collections::HashMap<String, Vec<Vec<f64>>> {
        // Try data dir (parent of catalog.db), then well-known paths
        let candidates = [
            db_path.parent().map(|p| p.join("s2_tile_grid.json")),
            db_path.parent().and_then(|p| p.parent()).map(|p| p.join("s2_tile_grid.json")),
        ];
        for candidate in candidates.iter().flatten() {
            if candidate.exists() {
                match std::fs::read_to_string(candidate) {
                    Ok(json_str) => {
                        match serde_json::from_str(&json_str) {
                            Ok(grid) => {
                                let grid: std::collections::HashMap<String, Vec<Vec<f64>>> = grid;
                                println!("🗺️  Loaded S2 tile grid: {} tiles from {}", grid.len(), candidate.display());
                                return grid;
                            }
                            Err(e) => eprintln!("⚠️  Failed to parse tile grid: {}", e),
                        }
                    }
                    Err(e) => eprintln!("⚠️  Failed to read tile grid: {}", e),
                }
            }
        }
        println!("ℹ️  No s2_tile_grid.json found, using STAC geometry fallback");
        std::collections::HashMap::new()
    }

    fn init_tables(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS collections (
                id TEXT PRIMARY KEY,
                title TEXT NOT NULL,
                description TEXT NOT NULL DEFAULT ''
            );
            CREATE TABLE IF NOT EXISTS items (
                id TEXT PRIMARY KEY,
                collection TEXT NOT NULL,
                bbox_west REAL NOT NULL,
                bbox_south REAL NOT NULL,
                bbox_east REAL NOT NULL,
                bbox_north REAL NOT NULL,
                properties_json TEXT NOT NULL DEFAULT '{}',
                chunk_hashes_json TEXT NOT NULL DEFAULT '[]',
                created_at REAL DEFAULT (strftime('%s','now')),
                FOREIGN KEY (collection) REFERENCES collections(id)
            );
            CREATE INDEX IF NOT EXISTS idx_items_collection ON items(collection);
            CREATE INDEX IF NOT EXISTS idx_items_bbox ON items(bbox_west, bbox_south, bbox_east, bbox_north);

            CREATE TABLE IF NOT EXISTS catalog_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT OR IGNORE INTO catalog_meta (key, value) VALUES ('catalog_version', '0');",
        )?;
        Ok(())
    }


    // --- Catalog Version ---

    /// Get the current catalog version (monotonically increasing counter).
    pub fn catalog_version(&self) -> Result<u64> {
        let v: String = self.conn.query_row(
            "SELECT value FROM catalog_meta WHERE key = 'catalog_version'",
            [],
            |row| row.get(0),
        )?;
        Ok(v.parse::<u64>().unwrap_or(0))
    }

    /// Increment catalog version and return the new value.
    pub fn increment_version(&self) -> Result<u64> {
        self.conn.execute(
            "UPDATE catalog_meta SET value = CAST(CAST(value AS INTEGER) + 1 AS TEXT) WHERE key = 'catalog_version'",
            [],
        )?;
        self.catalog_version()
    }

    /// Return items with created_at > since_ts (for change detection).
    pub fn changes_since(&self, since_ts: f64) -> Result<Vec<StacItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, collection, bbox_west, bbox_south, bbox_east, bbox_north, properties_json, chunk_hashes_json, created_at, geometry_json
             FROM items WHERE created_at > ?1 ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map(params![since_ts], |row| {
            let props_str: String = row.get(6)?;
            let hashes_str: String = row.get(7)?;
            Ok(StacItem {
                id: row.get(0)?,
                collection: row.get(1)?,
                bbox: [row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?],
                properties: serde_json::from_str(&props_str).unwrap_or_default(),
                chunk_hashes: serde_json::from_str(&hashes_str).unwrap_or_default(),
                created_at: row.get(8)?,
                geometry: row.get::<_, Option<String>>(9)
                    .ok().flatten()
                    .and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    // --- Collections ---

    /// Add a collection.
    pub fn add_collection(&self, col: &StacCollection) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO collections (id, title, description) VALUES (?1, ?2, ?3)",
            params![col.id, col.title, col.description],
        )?;
        Ok(())
    }

    /// Get a collection by ID.
    pub fn get_collection(&self, id: &str) -> Result<Option<StacCollection>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, title, description FROM collections WHERE id = ?1")?;
        let mut rows = stmt.query_map(params![id], |row| {
            Ok(StacCollection {
                id: row.get(0)?,
                title: row.get(1)?,
                description: row.get(2)?,
            })
        })?;
        match rows.next() {
            Some(Ok(col)) => Ok(Some(col)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// List all collections.
    pub fn list_collections(&self) -> Result<Vec<StacCollection>> {
        let mut stmt = self
            .conn
            .prepare("SELECT id, title, description FROM collections")?;
        let rows = stmt.query_map([], |row| {
            Ok(StacCollection {
                id: row.get(0)?,
                title: row.get(1)?,
                description: row.get(2)?,
            })
        })?;
        let mut cols = Vec::new();
        for row in rows {
            cols.push(row?);
        }
        Ok(cols)
    }

    // --- Items ---

    /// Add a STAC item.
    pub fn add_item(&self, item: &StacItem) -> Result<()> {
        // Ensure collection exists
        if self.get_collection(&item.collection)?.is_none() {
            self.add_collection(&StacCollection {
                id: item.collection.clone(),
                title: item.collection.clone(),
                description: String::new(),
            })?;
        }

        let hashes_json = serde_json::to_string(&item.chunk_hashes)?;
        let props_json = serde_json::to_string(&item.properties)?;

        let geom_json = item.geometry.as_ref()
            .map(|g| serde_json::to_string(g).unwrap_or_default());

        self.conn.execute(
            "INSERT OR REPLACE INTO items (id, collection, bbox_west, bbox_south, bbox_east, bbox_north, properties_json, chunk_hashes_json, created_at, geometry_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                item.id,
                item.collection,
                item.bbox[0],
                item.bbox[1],
                item.bbox[2],
                item.bbox[3],
                props_json,
                hashes_json,
                item.created_at,
                geom_json,
            ],
        )?;
        self.increment_version()?;
        Ok(())
    }

    /// Get an item by ID.
    pub fn get_item(&self, id: &str) -> Result<Option<StacItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, collection, bbox_west, bbox_south, bbox_east, bbox_north, properties_json, chunk_hashes_json, created_at, geometry_json
             FROM items WHERE id = ?1",
        )?;
        let mut rows = stmt.query_map(params![id], |row| {
            let props_str: String = row.get(6)?;
            let hashes_str: String = row.get(7)?;
            Ok(StacItem {
                id: row.get(0)?,
                collection: row.get(1)?,
                bbox: [row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?],
                properties: serde_json::from_str(&props_str).unwrap_or_default(),
                chunk_hashes: serde_json::from_str(&hashes_str).unwrap_or_default(),
                created_at: row.get(8)?,
                geometry: row.get::<_, Option<String>>(9)
                    .ok().flatten()
                    .and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?;
        match rows.next() {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Get a single item within a specific collection.
    pub fn get_collection_item(&self, collection: &str, item_id: &str) -> Result<Option<StacItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, collection, bbox_west, bbox_south, bbox_east, bbox_north, properties_json, chunk_hashes_json, created_at
             FROM items WHERE id = ?1 AND collection = ?2",
        )?;
        let mut rows = stmt.query_map(params![item_id, collection], |row| {
            let props_str: String = row.get(6)?;
            let hashes_str: String = row.get(7)?;
            Ok(StacItem {
                id: row.get(0)?,
                collection: row.get(1)?,
                bbox: [row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?],
                properties: serde_json::from_str(&props_str).unwrap_or_default(),
                chunk_hashes: serde_json::from_str(&hashes_str).unwrap_or_default(),
                created_at: row.get(8)?,
                geometry: row.get::<_, Option<String>>(9)
                    .ok().flatten()
                    .and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?;
        match rows.next() {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Build SQL WHERE clauses and params for search filters.
    /// Returns (where_clauses, params_vec).
    fn build_search_where(
        collection: Option<&str>,
        bbox: Option<[f64; 4]>,
        datetime: Option<&DatetimeFilter>,
    ) -> (String, Vec<Box<dyn rusqlite::types::ToSql>>) {
        let mut clauses = String::from(" WHERE 1=1");
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(col) = collection {
            clauses.push_str(" AND collection = ?");
            params_vec.push(Box::new(col.to_string()));
        }

        if let Some(b) = bbox {
            // Overlap: item.west < query.east AND item.east > query.west
            //          AND item.south < query.north AND item.north > query.south
            clauses.push_str(" AND bbox_west < ? AND bbox_east > ? AND bbox_south < ? AND bbox_north > ?");
            params_vec.push(Box::new(b[2])); // query east
            params_vec.push(Box::new(b[0])); // query west
            params_vec.push(Box::new(b[3])); // query north
            params_vec.push(Box::new(b[1])); // query south
        }

        if let Some(dt) = datetime {
            // Use json_extract to get datetime from properties_json
            if let Some(ref start) = dt.start {
                clauses.push_str(" AND json_extract(properties_json, '$.datetime') >= ?");
                params_vec.push(Box::new(start.clone()));
            }
            if let Some(ref end) = dt.end {
                clauses.push_str(" AND json_extract(properties_json, '$.datetime') <= ?");
                params_vec.push(Box::new(end.clone()));
            }
        }

        (clauses, params_vec)
    }

    /// Search items by collection, bounding box, datetime, with pagination.
    pub fn search(
        &self,
        collection: Option<&str>,
        bbox: Option<[f64; 4]>,
        datetime: Option<&DatetimeFilter>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<StacItem>> {
        let (where_clause, params_vec) = Self::build_search_where(collection, bbox, datetime);
        let sql = format!(
            "SELECT id, collection, bbox_west, bbox_south, bbox_east, bbox_north, properties_json, chunk_hashes_json, created_at FROM items{} ORDER BY json_extract(properties_json, '$.datetime') DESC, created_at DESC LIMIT {} OFFSET {}",
            where_clause, limit, offset
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
        let rows = stmt.query_map(param_refs.as_slice(), |row| {
            let props_str: String = row.get(6)?;
            let hashes_str: String = row.get(7)?;
            Ok(StacItem {
                id: row.get(0)?,
                collection: row.get(1)?,
                bbox: [row.get(2)?, row.get(3)?, row.get(4)?, row.get(5)?],
                properties: serde_json::from_str(&props_str).unwrap_or_default(),
                chunk_hashes: serde_json::from_str(&hashes_str).unwrap_or_default(),
                created_at: row.get(8)?,
                geometry: row.get::<_, Option<String>>(9)
                    .ok().flatten()
                    .and_then(|s| serde_json::from_str(&s).ok()),
            })
        })?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    /// Count items matching the given filters (for numberMatched in pagination).
    pub fn search_count(
        &self,
        collection: Option<&str>,
        bbox: Option<[f64; 4]>,
        datetime: Option<&DatetimeFilter>,
    ) -> Result<usize> {
        let (where_clause, params_vec) = Self::build_search_where(collection, bbox, datetime);
        let sql = format!("SELECT COUNT(*) FROM items{}", where_clause);

        let mut stmt = self.conn.prepare(&sql)?;
        let param_refs: Vec<&dyn rusqlite::types::ToSql> = params_vec.iter().map(|p| p.as_ref()).collect();
        let count: i64 = stmt.query_row(param_refs.as_slice(), |row| row.get(0))?;
        Ok(count as usize)
    }

    /// Count items total or per collection.
    pub fn item_count(&self, collection: Option<&str>) -> Result<usize> {
        let count = if let Some(col) = collection {
            let mut stmt = self
                .conn
                .prepare("SELECT COUNT(*) FROM items WHERE collection = ?1")?;
            let c: i64 = stmt.query_row(params![col], |row| row.get(0))?;
            c as usize
        } else {
            let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM items")?;
            let c: i64 = stmt.query_row([], |row| row.get(0))?;
            c as usize
        };
        Ok(count)
    }

    /// Aggregate items into grid cells of given degree resolution
    pub fn spatial_grid(&self, cell_deg: f64) -> Result<Vec<GridCell>> {
        let mut stmt = self.conn.prepare(
            "SELECT collection,                     CAST(CAST(bbox_west / ?1 AS INTEGER) AS REAL) * ?1 as cw,                     CAST(CAST(bbox_south / ?1 AS INTEGER) AS REAL) * ?1 as cs,                     COUNT(*) as cnt              FROM items              WHERE bbox_west IS NOT NULL              GROUP BY collection, cw, cs"
        )?;
        let rows = stmt.query_map(params![cell_deg], |row| {
            let col: String = row.get(0)?;
            let w: f64 = row.get(1)?;
            let s: f64 = row.get(2)?;
            let cnt: i64 = row.get(3)?;
            Ok(GridCell {
                west: w,
                south: s,
                east: w + cell_deg,
                north: s + cell_deg,
                count: cnt,
                collection: col,
            })
        })?;
        let cells: Vec<GridCell> = rows.filter_map(|r| r.ok()).collect();
        Ok(cells)
    }

    /// Aggregate items by MGRS tile ID (parsed from item ID), returning real tile bboxes.
    pub fn mgrs_coverage(&self) -> Result<Vec<MgrsTile>> {
        let mut stmt = self.conn.prepare(
            "SELECT collection,
                    SUBSTR(id, INSTR(id, '_') + 1, INSTR(SUBSTR(id, INSTR(id, '_') + 1), '_') - 1) as tile_id,
                    MIN(bbox_west) as w,
                    MIN(bbox_south) as s,
                    MAX(bbox_east) as e,
                    MAX(bbox_north) as n,
                    COUNT(DISTINCT SUBSTR(id, INSTR(SUBSTR(id, INSTR(id, '_') + 1), '_') + INSTR(id, '_') + 1, 8)) as date_count,
                    COUNT(*) as item_count,
                    GROUP_CONCAT(DISTINCT SUBSTR(id, INSTR(SUBSTR(id, INSTR(id, '_') + 1), '_') + INSTR(id, '_') + 1, 8)) as dates_csv
             FROM items
             WHERE bbox_west IS NOT NULL
             GROUP BY collection, tile_id"
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(MgrsTile {
                collection: row.get(0)?,
                tile_id: row.get(1)?,
                west: row.get(2)?,
                south: row.get(3)?,
                east: row.get(4)?,
                north: row.get(5)?,
                date_count: row.get(6)?,
                item_count: row.get(7)?,
                dates: {
                    let csv: String = row.get::<_, String>(8).unwrap_or_default();
                    let mut d: Vec<String> = csv.split(',').filter(|s| !s.is_empty() && s.len() == 8).map(|s| s.to_string()).collect();
                    d.sort();
                    d.dedup();
                    d
                },
                bands: Vec::new(), // populated separately
                polygon: None,
            })
        })?;
        let mut tiles: Vec<MgrsTile> = rows.filter_map(|r| r.ok()).collect();

        // Enrich tiles with real polygon geometry from STAC data
        for tile in &mut tiles {
            // Prefer non-rectangular geometry (ORDER BY LENGTH DESC picks trapezoids over rectangles)
            let geom_result: rusqlite::Result<Option<String>> = self.conn.query_row(
                "SELECT geometry_json FROM items
                 WHERE geometry_json IS NOT NULL
                   AND collection = ?1
                   AND id LIKE '%_' || ?2 || '_%'
                 ORDER BY LENGTH(geometry_json) DESC
                 LIMIT 1",
                params![tile.collection, tile.tile_id],
                |row| row.get(0),
            );
            if let Ok(Some(json_str)) = geom_result {
                if let Ok(geom) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    if let Some(coords) = geom.get("coordinates")
                        .and_then(|c| c.as_array())
                        .and_then(|rings| rings.first())
                        .and_then(|ring| ring.as_array())
                    {
                        let poly: Vec<Vec<f64>> = coords.iter()
                            .filter_map(|pt| pt.as_array().map(|a| {
                                a.iter().filter_map(|v| v.as_f64()).collect()
                            }))
                            .collect();
                        if poly.len() >= 4 {
                            tile.polygon = Some(poly);
                        }
                    }
                }
            }
        }

        Ok(tiles)
    }

    /// Delete an item by ID.
    pub fn delete_item(&self, id: &str) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM items WHERE id = ?1", params![id])?;
        if affected > 0 {
            self.increment_version()?;
        }
        Ok(affected > 0)
    }

    /// Delete all items in a collection, then delete the collection record.
    pub fn delete_collection(&self, collection_id: &str) -> Result<usize> {
        let items_deleted = self
            .conn
            .execute("DELETE FROM items WHERE collection = ?1", params![collection_id])?;
        self.conn
            .execute("DELETE FROM collections WHERE id = ?1", params![collection_id])?;
        if items_deleted > 0 {
            self.increment_version()?;
        }
        Ok(items_deleted)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn now_ts() -> f64 {
        Utc::now().timestamp() as f64
    }

    #[test]
    fn test_add_and_get_item() {
        let catalog = Catalog::in_memory().unwrap();
        let item = StacItem {
            id: "test-item".to_string(),
            collection: "sentinel-2".to_string(),
            bbox: [12.0, 55.0, 13.0, 56.0],
            properties: serde_json::json!({"datetime": "2026-03-11"}),
            chunk_hashes: vec!["abc123".to_string(), "def456".to_string()],
            created_at: now_ts(),
        };
        catalog.add_item(&item).unwrap();

        let retrieved = catalog.get_item("test-item").unwrap().unwrap();
        assert_eq!(retrieved.id, "test-item");
        assert_eq!(retrieved.chunk_hashes.len(), 2);
    }

    #[test]
    fn test_search_by_collection() {
        let catalog = Catalog::in_memory().unwrap();
        for i in 0..3 {
            catalog
                .add_item(&StacItem {
                    id: format!("item-{}", i),
                    collection: "s2".to_string(),
                    bbox: [0.0, 0.0, 1.0, 1.0],
                    properties: serde_json::json!({}),
                    chunk_hashes: vec![],
                    created_at: now_ts(),
                })
                .unwrap();
        }
        catalog
            .add_item(&StacItem {
                id: "other".to_string(),
                collection: "landsat".to_string(),
                bbox: [0.0, 0.0, 1.0, 1.0],
                properties: serde_json::json!({}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();

        let results = catalog.search(Some("s2"), None, None, 100, 0).unwrap();
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_search_by_bbox() {
        let catalog = Catalog::in_memory().unwrap();
        catalog
            .add_item(&StacItem {
                id: "copenhagen".to_string(),
                collection: "s2".to_string(),
                bbox: [12.0, 55.0, 13.0, 56.0],
                properties: serde_json::json!({}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();
        catalog
            .add_item(&StacItem {
                id: "tokyo".to_string(),
                collection: "s2".to_string(),
                bbox: [139.0, 35.0, 140.0, 36.0],
                properties: serde_json::json!({}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();

        // Search around Copenhagen
        let results = catalog.search(None, Some([11.0, 54.0, 14.0, 57.0]), None, 100, 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "copenhagen");
    }

    #[test]
    fn test_search_by_datetime() {
        let catalog = Catalog::in_memory().unwrap();
        catalog
            .add_item(&StacItem {
                id: "item-2020".to_string(),
                collection: "s2".to_string(),
                bbox: [0.0, 0.0, 1.0, 1.0],
                properties: serde_json::json!({"datetime": "2020-06-15T00:00:00Z"}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();
        catalog
            .add_item(&StacItem {
                id: "item-2021".to_string(),
                collection: "s2".to_string(),
                bbox: [0.0, 0.0, 1.0, 1.0],
                properties: serde_json::json!({"datetime": "2021-06-15T00:00:00Z"}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();

        let dt = DatetimeFilter::parse("2020-01-01T00:00:00Z/2020-12-31T23:59:59Z").unwrap();
        let results = catalog.search(None, None, Some(&dt), 100, 0).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "item-2020");
    }

    #[test]
    fn test_pagination() {
        let catalog = Catalog::in_memory().unwrap();
        for i in 0..5 {
            catalog
                .add_item(&StacItem {
                    id: format!("item-{}", i),
                    collection: "s2".to_string(),
                    bbox: [0.0, 0.0, 1.0, 1.0],
                    properties: serde_json::json!({}),
                    chunk_hashes: vec![],
                    created_at: i as f64,
                })
                .unwrap();
        }
        let page1 = catalog.search(None, None, None, 2, 0).unwrap();
        let page2 = catalog.search(None, None, None, 2, 2).unwrap();
        assert_eq!(page1.len(), 2);
        assert_eq!(page2.len(), 2);
        // Ensure no overlap
        let ids1: Vec<_> = page1.iter().map(|i| &i.id).collect();
        let ids2: Vec<_> = page2.iter().map(|i| &i.id).collect();
        for id in &ids2 {
            assert!(!ids1.contains(id));
        }
        let total = catalog.search_count(None, None, None).unwrap();
        assert_eq!(total, 5);
    }

    #[test]
    fn test_item_count() {
        let catalog = Catalog::in_memory().unwrap();
        assert_eq!(catalog.item_count(None).unwrap(), 0);
        catalog
            .add_item(&StacItem {
                id: "a".to_string(),
                collection: "c".to_string(),
                bbox: [0.0; 4],
                properties: serde_json::json!({}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();
        assert_eq!(catalog.item_count(None).unwrap(), 1);
    }

    #[test]
    fn test_catalog_version() {
        let catalog = Catalog::in_memory().unwrap();
        assert_eq!(catalog.catalog_version().unwrap(), 0);

        catalog.add_item(&StacItem {
            id: "v1".to_string(),
            collection: "c".to_string(),
            bbox: [0.0; 4],
            properties: serde_json::json!({}),
            chunk_hashes: vec![],
            created_at: now_ts(),
        }).unwrap();
        assert_eq!(catalog.catalog_version().unwrap(), 1);

        catalog.add_item(&StacItem {
            id: "v2".to_string(),
            collection: "c".to_string(),
            bbox: [0.0; 4],
            properties: serde_json::json!({}),
            chunk_hashes: vec![],
            created_at: now_ts(),
        }).unwrap();
        assert_eq!(catalog.catalog_version().unwrap(), 2);

        catalog.delete_item("v1").unwrap();
        assert_eq!(catalog.catalog_version().unwrap(), 3);
    }

    #[test]
    fn test_changes_since() {
        let catalog = Catalog::in_memory().unwrap();
        let t1 = now_ts();
        catalog.add_item(&StacItem {
            id: "old".to_string(),
            collection: "c".to_string(),
            bbox: [0.0; 4],
            properties: serde_json::json!({}),
            chunk_hashes: vec![],
            created_at: t1,
        }).unwrap();
        let t2 = t1 + 100.0;
        catalog.add_item(&StacItem {
            id: "new".to_string(),
            collection: "c".to_string(),
            bbox: [0.0; 4],
            properties: serde_json::json!({}),
            chunk_hashes: vec![],
            created_at: t2,
        }).unwrap();

        let changes = catalog.changes_since(t1).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].id, "new");
    }

    #[test]
    fn test_delete_item() {
        let catalog = Catalog::in_memory().unwrap();
        catalog
            .add_item(&StacItem {
                id: "del".to_string(),
                collection: "c".to_string(),
                bbox: [0.0; 4],
                properties: serde_json::json!({}),
                chunk_hashes: vec![],
                created_at: now_ts(),
            })
            .unwrap();
        assert!(catalog.delete_item("del").unwrap());
        assert!(catalog.get_item("del").unwrap().is_none());
    }
}
