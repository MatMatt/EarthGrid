//! SQLite-backed STAC catalog for EarthGrid.
//!
//! Stores STAC Items and Collections with chunk hash references.

use chrono::Utc;
use rusqlite::{params, Connection};
use serde::{Deserialize, Serialize};

use crate::error::{EarthGridError, Result};

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
    pub created_at: String,
}

/// SQLite-backed STAC catalog.
pub struct Catalog {
    conn: Connection,
}

impl Catalog {
    /// Open or create a catalog at the given path.
    pub fn new(db_path: &std::path::Path) -> Result<Self> {
        if let Some(parent) = db_path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(db_path)?;
        let catalog = Self { conn };
        catalog.init_tables()?;
        Ok(catalog)
    }

    /// Create an in-memory catalog (for testing).
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory()?;
        let catalog = Self { conn };
        catalog.init_tables()?;
        Ok(catalog)
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
                collection_id TEXT NOT NULL,
                bbox_west REAL NOT NULL,
                bbox_south REAL NOT NULL,
                bbox_east REAL NOT NULL,
                bbox_north REAL NOT NULL,
                properties TEXT NOT NULL DEFAULT '{}',
                chunk_hashes TEXT NOT NULL DEFAULT '[]',
                created_at TEXT NOT NULL,
                FOREIGN KEY (collection_id) REFERENCES collections(id)
            );
            CREATE INDEX IF NOT EXISTS idx_items_collection ON items(collection_id);
            CREATE INDEX IF NOT EXISTS idx_items_bbox ON items(bbox_west, bbox_south, bbox_east, bbox_north);",
        )?;
        Ok(())
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

        self.conn.execute(
            "INSERT OR REPLACE INTO items (id, collection_id, bbox_west, bbox_south, bbox_east, bbox_north, properties, chunk_hashes, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
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
            ],
        )?;
        Ok(())
    }

    /// Get an item by ID.
    pub fn get_item(&self, id: &str) -> Result<Option<StacItem>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, collection_id, bbox_west, bbox_south, bbox_east, bbox_north, properties, chunk_hashes, created_at
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
            })
        })?;
        match rows.next() {
            Some(Ok(item)) => Ok(Some(item)),
            Some(Err(e)) => Err(e.into()),
            None => Ok(None),
        }
    }

    /// Search items by collection and/or bounding box.
    pub fn search(
        &self,
        collection: Option<&str>,
        bbox: Option<[f64; 4]>,
        limit: usize,
    ) -> Result<Vec<StacItem>> {
        let mut sql = String::from(
            "SELECT id, collection_id, bbox_west, bbox_south, bbox_east, bbox_north, properties, chunk_hashes, created_at FROM items WHERE 1=1",
        );
        let mut params_vec: Vec<Box<dyn rusqlite::types::ToSql>> = Vec::new();

        if let Some(col) = collection {
            sql.push_str(" AND collection_id = ?");
            params_vec.push(Box::new(col.to_string()));
        }

        if let Some(b) = bbox {
            // Overlap check: item.west < query.east AND item.east > query.west
            //                 AND item.south < query.north AND item.north > query.south
            sql.push_str(" AND bbox_west < ? AND bbox_east > ? AND bbox_south < ? AND bbox_north > ?");
            params_vec.push(Box::new(b[2])); // query east
            params_vec.push(Box::new(b[0])); // query west
            params_vec.push(Box::new(b[3])); // query north
            params_vec.push(Box::new(b[1])); // query south
        }

        sql.push_str(&format!(" LIMIT {}", limit));

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
            })
        })?;

        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    /// Count items total or per collection.
    pub fn item_count(&self, collection: Option<&str>) -> Result<usize> {
        let (sql, count) = if let Some(col) = collection {
            let mut stmt = self
                .conn
                .prepare("SELECT COUNT(*) FROM items WHERE collection_id = ?1")?;
            let c: i64 = stmt.query_row(params![col], |row| row.get(0))?;
            (String::new(), c as usize)
        } else {
            let mut stmt = self.conn.prepare("SELECT COUNT(*) FROM items")?;
            let c: i64 = stmt.query_row([], |row| row.get(0))?;
            (String::new(), c as usize)
        };
        let _ = sql;
        Ok(count)
    }

    /// Delete an item by ID.
    pub fn delete_item(&self, id: &str) -> Result<bool> {
        let affected = self
            .conn
            .execute("DELETE FROM items WHERE id = ?1", params![id])?;
        Ok(affected > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn now_str() -> String {
        Utc::now().to_rfc3339()
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
            created_at: now_str(),
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
                    created_at: now_str(),
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
                created_at: now_str(),
            })
            .unwrap();

        let results = catalog.search(Some("s2"), None, 100).unwrap();
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
                created_at: now_str(),
            })
            .unwrap();
        catalog
            .add_item(&StacItem {
                id: "tokyo".to_string(),
                collection: "s2".to_string(),
                bbox: [139.0, 35.0, 140.0, 36.0],
                properties: serde_json::json!({}),
                chunk_hashes: vec![],
                created_at: now_str(),
            })
            .unwrap();

        // Search around Copenhagen
        let results = catalog.search(None, Some([11.0, 54.0, 14.0, 57.0]), 100).unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].id, "copenhagen");
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
                created_at: now_str(),
            })
            .unwrap();
        assert_eq!(catalog.item_count(None).unwrap(), 1);
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
                created_at: now_str(),
            })
            .unwrap();
        assert!(catalog.delete_item("del").unwrap());
        assert!(catalog.get_item("del").unwrap().is_none());
    }
}
