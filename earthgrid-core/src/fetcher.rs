//! Element84 STAC Fetcher — downloads Sentinel-2 COGs from earth-search.aws.element84.com
//! and ingests them into the local ChunkStore + Catalog.

use std::collections::{HashMap, HashSet};
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use chrono::{Datelike, NaiveDate};
use futures::future::join_all;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};
use tracing::{info, warn};

use crate::catalog::Catalog;
use crate::chunk_store::ChunkStore;
use crate::config::Settings;
use crate::eviction;
use crate::ingest;

const ELEMENT84_BASE: &str = "https://earth-search.aws.element84.com/v1";
const MAX_CONCURRENT_DOWNLOADS: usize = 4;

// ---------------------------------------------------------------------------
// Band mapping: STAC asset key → friendly band name
// ---------------------------------------------------------------------------

fn band_key_to_name(key: &str) -> Option<&'static str> {
    match key {
        "B01" | "coastal" => Some("B01"),
        "B02" | "blue" => Some("B02"),
        "B03" | "green" => Some("B03"),
        "B04" | "red" => Some("B04"),
        "B05" | "rededge1" => Some("B05"),
        "B06" | "rededge2" => Some("B06"),
        "B07" | "rededge3" => Some("B07"),
        "B08" | "nir" => Some("B08"),
        "B8A" | "nir08" => Some("B8A"),
        "B11" | "swir16" => Some("B11"),
        "B12" | "swir22" => Some("B12"),
        "SCL" | "scl" => Some("SCL"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

/// Real-time progress reporter for fetch operations.
/// Called from search and download phases — no channels needed.
#[derive(Clone)]
pub enum FetchPhase {
    Searching { chunks_done: usize, chunks_total: usize, found: usize },
    Downloading { scenes_done: usize, scenes_total: usize, bytes: u64, errors: usize },
}

pub type ProgressSink = Arc<dyn Fn(FetchPhase) + Send + Sync + 'static>;

/// A single result from a STAC search.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StacSearchResult {
    pub id: String,
    pub bbox: [f64; 4],
    pub datetime: String,
    pub cloud_cover: f64,
    /// band_name → COG URL
    pub assets: HashMap<String, String>,
    pub processing_baseline: Option<String>,
    /// GeoJSON geometry from STAC (real tile footprint polygon)
    pub geometry: Option<serde_json::Value>,
}

/// Aggregated result of a fetch+ingest run.
/// `items_*` counts refer to scenes (STAC items), not individual bands.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FetchResult {
    /// Number of scenes found in STAC search
    pub items_searched: usize,
    /// Number of scenes successfully downloaded (at least 1 band)
    pub items_downloaded: usize,
    /// Number of scenes skipped (already in catalog)
    pub items_skipped: usize,
    /// Total bytes downloaded across all bands
    pub bytes_downloaded: u64,
    pub errors: Vec<String>,
}

// ---------------------------------------------------------------------------
// Internal STAC API response shapes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct StacFeatureCollection {
    features: Vec<StacFeature>,
    links: Option<Vec<StacLink>>,
}

#[derive(Debug, Deserialize)]
struct StacFeature {
    id: String,
    bbox: Option<Vec<f64>>,
    geometry: Option<serde_json::Value>,
    properties: serde_json::Value,
    assets: Option<serde_json::Value>,
    #[allow(dead_code)]
    links: Option<Vec<StacLink>>,
}

#[derive(Debug, Deserialize, Clone)]
struct StacLink {
    rel: String,
    #[allow(dead_code)]
    href: String,
    #[serde(rename = "type")]
    #[allow(dead_code)]
    media_type: Option<String>,
    #[allow(dead_code)]
    method: Option<String>,
    body: Option<serde_json::Value>,
}

// ---------------------------------------------------------------------------
// Helper: parse a STAC feature into StacSearchResult
// ---------------------------------------------------------------------------

fn parse_feature(f: StacFeature) -> Option<StacSearchResult> {
    let bbox = f.bbox.as_ref().filter(|b| b.len() >= 4).map(|b| {
        [b[0], b[1], b[2], b[3]]
    })?;

    let props = &f.properties;
    let datetime = props
        .get("datetime")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_string();

    let cloud_cover = props
        .get("eo:cloud_cover")
        .and_then(|v| v.as_f64())
        .unwrap_or(100.0);

    let processing_baseline = props
        .get("s2:processing_baseline")
        .or_else(|| props.get("processing_baseline"))
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Extract asset URLs, mapping to canonical band names
    let mut assets: HashMap<String, String> = HashMap::new();
    if let Some(asset_map) = f.assets.as_ref().and_then(|v| v.as_object()) {
        for (key, val) in asset_map {
            if let Some(band_name) = band_key_to_name(key) {
                if let Some(href) = val.get("href").and_then(|h| h.as_str()) {
                    assets.insert(band_name.to_string(), href.to_string());
                }
            }
        }
    }

    Some(StacSearchResult {
        id: f.id,
        bbox,
        datetime,
        cloud_cover,
        assets,
        processing_baseline,
        geometry: f.geometry,
    })
}

// ---------------------------------------------------------------------------
// 1. STAC Search
// ---------------------------------------------------------------------------

/// Build the POST body for a STAC search request.
fn build_search_body(
    bbox: &[f64; 4],
    start_date: &str,
    end_date: &str,
    cloud_cover: f64,
    limit: usize,
    collection: &str,
) -> serde_json::Value {
    serde_json::json!({
        "collections": [collection],
        "bbox": [bbox[0], bbox[1], bbox[2], bbox[3]],
        "limit": limit,
        "datetime": format!("{}/{}",
            if start_date.contains('T') { start_date.to_string() } else { format!("{}T00:00:00Z", start_date) },
            if end_date.contains('T') { end_date.to_string() } else { format!("{}T23:59:59Z", end_date) }
        ),
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
        "query": {
            "eo:cloud_cover": { "lte": cloud_cover }
        },
        "fields": {
            "include": [
                "id", "bbox", "properties.datetime",
                "properties.eo:cloud_cover",
                "properties.s2:processing_baseline",
                "assets"
            ],
            "exclude": []
        }
    })
}

/// Search Element84 STAC for one time range. Follows `next` pagination links.
async fn search_one_range(
    client: &reqwest::Client,
    bbox: [f64; 4],
    start_date: &str,
    end_date: &str,
    cloud_cover: f64,
    limit: usize,
    collection: &str,
    progress: Option<Arc<dyn Fn(FetchPhase) + Send + Sync>>,
) -> (Vec<StacSearchResult>, Vec<String>) {
    let mut results = Vec::new();
    let mut errors = Vec::new();

    let search_url = format!("{}/search", ELEMENT84_BASE);
    let mut body = build_search_body(&bbox, start_date, end_date, cloud_cover, limit, collection);
    let mut page = 1usize;

    loop {
        // Emit search progress
        if let Some(ref cb) = progress {
            cb(FetchPhase::Searching {
                chunk: format!("{}-{}", &start_date[..4], &end_date[..4]),
                page,
                items_found: results.len(),
            });
        }
        page += 1;
        let resp = match client
            .post(&search_url)
            .json(&body)
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                errors.push(format!("STAC search request failed: {e}"));
                break;
            }
        };

        if !resp.status().is_success() {
            errors.push(format!("STAC search returned {}: {}", resp.status(), resp.text().await.unwrap_or_default()));
            break;
        }

        let fc: StacFeatureCollection = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                errors.push(format!("Failed to parse STAC response: {e}"));
                break;
            }
        };

        let fetched_count = fc.features.len();
        for f in fc.features {
            if let Some(r) = parse_feature(f) {
                results.push(r);
            }
        }

        // Follow `next` link if present
        let next = fc.links.as_deref().unwrap_or(&[]).iter().find(|l| l.rel == "next").cloned();
        match next {
            Some(link) => {
                // The `next` link may carry a new body (POST pagination)
                if let Some(next_body) = link.body {
                    body = next_body;
                } else {
                    // No body override — stop to avoid infinite loop
                    break;
                }
                if fetched_count == 0 {
                    break;
                }
            }
            None => break,
        }

        // Respect limit across pages
        if results.len() >= limit {
            results.truncate(limit);
            break;
        }
    }

    (results, errors)
}

/// Split a date range into yearly chunks. Returns (start, end) pairs as strings.
fn yearly_chunks(start_date: &str, end_date: &str) -> Vec<(String, String)> {
    let parse = |s: &str| -> Option<NaiveDate> {
        // Accept ISO 8601: "2020-01-01" or "2020-01-01T00:00:00Z"
        let s = s.split('T').next().unwrap_or(s);
        NaiveDate::parse_from_str(s, "%Y-%m-%d").ok()
    };

    let today = chrono::Utc::now().date_naive();

    let start = match parse(start_date) {
        Some(d) => d,
        None => NaiveDate::from_ymd_opt(2015, 7, 1).unwrap(), // Sentinel-2 launch
    };
    let end = match parse(end_date) {
        Some(d) => d,
        None => today,
    };

    if end <= start {
        return vec![(start_date.to_string(), end_date.to_string())];
    }

    let mut chunks = Vec::new();
    let mut current = start;

    while current <= end {
        let chunk_end = NaiveDate::from_ymd_opt(current.year(), 12, 31)
            .unwrap_or(end)
            .min(end);
        chunks.push((
            current.format("%Y-%m-%d").to_string(),
            chunk_end.format("%Y-%m-%d").to_string(),
        ));
        // Move to Jan 1 of next year
        match NaiveDate::from_ymd_opt(current.year() + 1, 1, 1) {
            Some(d) => current = d,
            None => break,
        }
        if current > end {
            break;
        }
    }

    chunks
}

/// Search Element84 STAC API. Splits large date ranges into yearly chunks and
/// searches in parallel. Returns deduplicated results.
pub async fn search_element84(
    bbox: [f64; 4],
    start_date: &str,
    end_date: &str,
    cloud_cover: f64,
    limit: usize,
    collection: &str,
) -> (Vec<StacSearchResult>, Vec<String>) {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .unwrap_or_default();

    let chunks = yearly_chunks(start_date, end_date);
    let per_chunk_limit = (limit + chunks.len() - 1) / chunks.len().max(1);

    // Search all chunks in parallel
    let futures: Vec<_> = chunks
        .iter()
        .map(|(s, e)| {
            let client = client.clone();
            let collection = collection.to_string();
            let s = s.clone();
            let e = e.clone();
            async move {
                search_one_range(&client, bbox, &s, &e, cloud_cover, per_chunk_limit, &collection, None).await
            }
        })
        .collect();

    let all = join_all(futures).await;

    let mut results = Vec::new();
    let mut errors = Vec::new();
    let mut seen_ids = HashSet::new();

    for (chunk_results, chunk_errors) in all {
        errors.extend(chunk_errors);
        for r in chunk_results {
            if seen_ids.insert(r.id.clone()) {
                results.push(r);
            }
        }
    }

    // Sort by datetime desc, truncate to limit
    results.sort_by(|a, b| b.datetime.cmp(&a.datetime));
    results.truncate(limit);

    (results, errors)
}

// ---------------------------------------------------------------------------
// 2. Download + Ingest Pipeline
// ---------------------------------------------------------------------------

/// Download a single URL to a temp file. Returns (path, bytes_downloaded).
async fn download_to_temp(
    client: &reqwest::Client,
    url: &str,
    temp_dir: &std::path::Path,
    filename: &str,
) -> std::result::Result<(PathBuf, u64), String> {
    let temp_path = temp_dir.join(filename);

    let resp = client
        .get(url)
        .send()
        .await
        .map_err(|e| format!("Download request failed for {url}: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("Download {url} returned {}", resp.status()));
    }

    let mut file = std::fs::File::create(&temp_path)
        .map_err(|e| format!("Failed to create temp file {}: {e}", temp_path.display()))?;

    let mut bytes_written: u64 = 0;
    let mut stream = resp;

    // Stream the response body
    while let Some(chunk) = stream.chunk().await.map_err(|e| format!("Stream error: {e}"))? {
        file.write_all(&chunk)
            .map_err(|e| format!("Write error: {e}"))?;
        bytes_written += chunk.len() as u64;
    }

    Ok((temp_path, bytes_written))
}

/// Ingest a single STAC search result item (one band file) into store + catalog.
/// Returns bytes ingested or an error string.
async fn ingest_item_band(
    store: Arc<Mutex<ChunkStore>>,
    catalog: Arc<Mutex<Catalog>>,
    item: &StacSearchResult,
    band: &str,
    url: &str,
    client: &reqwest::Client,
    temp_dir: &std::path::Path,
    collection: &str,
) -> std::result::Result<u64, String> {
    let file_id = uuid::Uuid::new_v4().to_string();
    let filename = format!("{}_{}_{}.tif", item.id, band, file_id);

    info!("Downloading {} band {} from {}", item.id, band, url);
    let (temp_path, bytes) = download_to_temp(client, url, temp_dir, &filename).await?;

    // Ingest into chunk store
    let mut stac_item = {
        let mut store_lock = store.lock().await;
        ingest::ingest_file(&temp_path, collection, ingest::DEFAULT_CHUNK_SIZE, &mut store_lock)
            .map_err(|e| format!("Ingest failed for {} {}: {}", item.id, band, e))?
    };

    // Override with STAC metadata: proper bbox, id, datetime, and geometry
    stac_item.bbox = item.bbox;
    stac_item.geometry = item.geometry.clone();
    stac_item.id = format!("{}_{}", item.id, band);
    if let Some(props) = stac_item.properties.as_object_mut() {
        props.insert("datetime".to_string(), serde_json::json!(item.datetime));
        props.insert("earthgrid:cloud_cover".to_string(), serde_json::json!(item.cloud_cover));
        props.insert("earthgrid:stac_id".to_string(), serde_json::json!(item.id));
        props.insert("earthgrid:band".to_string(), serde_json::json!(band));
    }

    // Add to catalog
    {
        let catalog_lock = catalog.lock().await;
        catalog_lock
            .add_item(&stac_item)
            .map_err(|e| format!("Catalog add failed for {} {}: {}", item.id, band, e))?;
    }

    // Clean up temp file
    let _ = std::fs::remove_file(&temp_path);

    Ok(bytes)
}

/// Fetch and ingest Sentinel-2 items from Element84 STAC.
///
/// - Searches for items matching the given parameters
/// - Skips items already in catalog (by checking item ID presence)
/// - Downloads requested bands concurrently (up to 4 at a time)
/// - Ingests each COG file via `ingest::ingest_file`
pub async fn fetch_and_ingest(
    store: Arc<Mutex<ChunkStore>>,
    catalog: Arc<Mutex<Catalog>>,
    bbox: [f64; 4],
    start_date: &str,
    end_date: &str,
    cloud_cover: f64,
    bands: &[String],
    limit: usize,
    collection: &str,
    tile_filter: Option<&str>,
) -> FetchResult {
    let (mut items, search_errors) = search_element84(bbox, start_date, end_date, cloud_cover, limit, collection).await;

    // Filter by tile name if provided (match "_TILE_" pattern in STAC item ID)
    if let Some(tile) = tile_filter {
        let pattern = format!("_{}_", tile.to_uppercase());
        let before = items.len();
        items.retain(|item| item.id.to_uppercase().contains(&pattern));
        if items.len() < before {
            info!("Tile filter '{}': kept {}/{} items", tile, items.len(), before);
        }
    }

    // --- Grid-wide dedup: skip items already on any node ---
    let beacon_url = std::env::var("EARTHGRID_BEACON_URL").ok().or_else(|| {
        let cfg_path = Settings::config_dir().join("config.json");
        std::fs::read_to_string(&cfg_path).ok()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
            .and_then(|v| v["beacon_url"].as_str().map(|s| s.to_string()))
    });
    if let Some(ref bu) = beacon_url {
        match beacon_inventory(bu, bbox, collection, start_date, end_date).await {
            Ok(grid_ids) => {
                let before = items.len();
                // Check by STAC item ID (scene-level, not band-level)
                items.retain(|item| !grid_ids.contains(&item.id));
                let skipped = before - items.len();
                if skipped > 0 {
                    info!("Grid dedup: skipped {}/{} items already on other nodes", skipped, before);
                }
            }
            Err(e) => {
                warn!("Beacon inventory check failed (continuing without dedup): {}", e);
            }
        }
    }

    let items_searched = items.len();

    let mut result = FetchResult {
        items_searched,
        errors: search_errors,
        ..Default::default()
    };

    if items.is_empty() {
        return result;
    }

    let client = Arc::new(
        reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .unwrap_or_default(),
    );

    let temp_dir = std::env::temp_dir();
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOADS));

    // Collect download tasks
    for item in &items {
        // Check if item already in catalog (dedup by STAC item ID)
        let already_present = {
            let cat = catalog.lock().await;
            cat.get_item(&item.id).ok().flatten().is_some()
        };

        if already_present {
            info!("Skipping {} — already in catalog", item.id);
            result.items_skipped += 1;
            continue;
        }

        // Check storage limit before downloading
        {
            let st = store.lock().await;
            let current_bytes = st.total_bytes() as f64;
            let config = Settings::load_or_default().unwrap_or_default();
            let limit_bytes = config.storage_limit_gb * 1_073_741_824.0;
            if current_bytes >= limit_bytes {
                drop(st);
                // Try eviction first
                let evict_result = {
                    let cat = catalog.lock().await;
                    let mut st = store.lock().await;
                    let beacon_db = Settings::config_dir().join("beacon.db");
                    let beacon_path = if beacon_db.exists() { Some(beacon_db.as_path()) } else { None };
                    eviction::evict(&*cat, &mut *st, config.storage_limit_gb, beacon_path)
                };
                match evict_result {
                    Ok(ref r) if r.bytes_freed > 0 => {
                        info!("Eviction freed {:.1} MB", r.bytes_freed as f64 / 1_048_576.0);
                    }
                    _ => {
                        // Still full after eviction — stop fetching
                        warn!("Storage full ({:.1} GB / {:.1} GB limit), stopping fetch",
                            current_bytes / 1_073_741_824.0, config.storage_limit_gb);
                        result.errors.push(format!(
                            "Storage limit reached ({:.1} GB). {} items not fetched.",
                            config.storage_limit_gb,
                            items_searched - result.items_downloaded - result.items_skipped
                        ));
                        break;
                    }
                }
            }
        }

        // Determine which bands to download
        let bands_to_download: Vec<(String, String)> = if bands.is_empty() {
            // Download all available bands
            item.assets
                .iter()
                .map(|(b, u)| (b.clone(), u.clone()))
                .collect()
        } else {
            bands
                .iter()
                .filter_map(|b| item.assets.get(b).map(|u| (b.clone(), u.clone())))
                .collect()
        };

        if bands_to_download.is_empty() {
            warn!("No matching bands for item {}", item.id);
            result.items_skipped += 1;
            continue;
        }

        // Download bands for this scene in parallel, then check storage before next scene
        let mut scene_tasks = Vec::new();
        let band_count = bands_to_download.len();
        info!("📥 Scene {}/{}: {} ({} bands)",
            result.items_downloaded + result.items_skipped + 1,
            items_searched,
            item.id, band_count);

        for (band, url) in bands_to_download {
            let store = store.clone();
            let catalog = catalog.clone();
            let client = client.clone();
            let semaphore = semaphore.clone();
            let temp_dir = temp_dir.clone();
            let item = item.clone();
            let collection = collection.to_string();

            scene_tasks.push(tokio::spawn(async move {
                let _permit = semaphore.acquire().await.ok()?;
                match ingest_item_band(
                    store,
                    catalog,
                    &item,
                    &band,
                    &url,
                    &client,
                    &temp_dir,
                    &collection,
                )
                .await
                {
                    Ok(bytes) => Some(bytes),
                    Err(e) => {
                        warn!("Error: {}", e);
                        None
                    }
                }
            }));
        }

        // Wait for this scene's bands to finish before moving to next
        let scene_results = join_all(scene_tasks).await;
        let mut scene_bytes: u64 = 0;
        let mut scene_bands_ok = 0;
        for task_result in scene_results {
            match task_result {
                Ok(Some(bytes)) => {
                    scene_bands_ok += 1;
                    scene_bytes += bytes;
                }
                Ok(None) => {
                    result.errors.push("Download/ingest failed (see logs)".to_string());
                }
                Err(e) => {
                    result.errors.push(format!("Task join error: {e}"));
                }
            }
        }
        if scene_bands_ok > 0 {
            result.items_downloaded += 1;
            result.bytes_downloaded += scene_bytes;
            info!("   ✅ {} — {}/{} bands, {:.1} MB",
                item.id, scene_bands_ok, band_count,
                scene_bytes as f64 / 1_048_576.0);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// 3. Beacon Inventory Check
// ---------------------------------------------------------------------------

/// Query a beacon node's `/search` endpoint to get the set of item IDs it holds.
/// Used to skip downloading items that are already present on the network.
pub async fn beacon_inventory(
    beacon_url: &str,
    bbox: [f64; 4],
    collection: &str,
    start_date: &str,
    end_date: &str,
) -> std::result::Result<HashSet<String>, String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()
        .map_err(|e| format!("Failed to build HTTP client: {e}"))?;

    let url = format!("{}/api/stac/search", beacon_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .query(&[
            ("bbox", format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3])),
            ("collections", collection.to_string()),
            ("datetime", format!("{}/{}",
                if start_date.contains('T') || start_date.is_empty() { start_date.to_string() } else { format!("{}T00:00:00Z", start_date) },
                if end_date.contains('T') || end_date.is_empty() { end_date.to_string() } else { format!("{}T23:59:59Z", end_date) }
            )),
            ("limit", "10000".to_string()),
        ])
        .send()
        .await
        .map_err(|e| format!("Beacon request to {url} failed: {e}"))?;

    if !resp.status().is_success() {
        return Err(format!("Beacon {url} returned {}", resp.status()));
    }

    let body: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("Failed to parse beacon response: {e}"))?;

    let mut ids = HashSet::new();
    if let Some(features) = body.get("features").and_then(|v| v.as_array()) {
        for feature in features {
            if let Some(id) = feature.get("id").and_then(|v| v.as_str()) {
                ids.insert(id.to_string());
            }
        }
    }
    // Also handle items array (non-STAC-FC responses)
    if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
        for item in items {
            if let Some(id) = item.get("id").and_then(|v| v.as_str()) {
                ids.insert(id.to_string());
            }
        }
    }

    Ok(ids)
}


// ---------------------------------------------------------------------------
// Distributed fetch — beacon distributes items across grid nodes
// ---------------------------------------------------------------------------

/// Node info for distribution decisions.
#[derive(Debug, Clone)]
struct GridNode {
    node_id: String,
    node_name: String,
    url: String,
    free_gb: f64,
    is_local: bool,
    #[allow(dead_code)]
    admin_key: String,
}

/// Query the beacon for alive nodes with free storage.
async fn get_grid_nodes(beacon_url: &str) -> Vec<GridNode> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let url = format!("{}/api/beacon/nodes?alive_only=true", beacon_url.trim_end_matches('/'));
    let resp = match client.get(&url).send().await {
        Ok(r) if r.status().is_success() => r,
        _ => {
            warn!("Cannot reach beacon at {}", url);
            return vec![];
        }
    };

    let data: serde_json::Value = match resp.json().await {
        Ok(d) => d,
        Err(_) => return vec![],
    };

    let mut nodes = vec![];
    if let Some(arr) = data.get("nodes").and_then(|v| v.as_array()) {
        for n in arr {
            let limit_gb = n.get("storage_limit_gb").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let used_bytes = n.get("chunks_bytes").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let free_gb = if limit_gb > 0.0 {
                limit_gb - (used_bytes / 1_073_741_824.0)
            } else {
                999999.0 // unlimited
            };
            let url = n.get("url").and_then(|v| v.as_str()).unwrap_or("").to_string();
            if url.is_empty() { continue; }

            nodes.push(GridNode {
                node_id: n.get("node_id").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                node_name: n.get("node_name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                url,
                free_gb: free_gb.max(0.0),
                is_local: false, // will be set by caller
                admin_key: String::new(),
            });
        }
    }
    // Sort by free space descending
    nodes.sort_by(|a, b| b.free_gb.total_cmp(&a.free_gb));
    nodes
}

/// Send a fetch request to a remote node.
async fn delegate_fetch_to_node(
    node: &GridNode,
    bbox: [f64; 4],
    start_date: &str,
    end_date: &str,
    cloud_cover: f64,
    bands: &[String],
    limit: usize,
    collection: &str,
    admin_key: &str,
) -> FetchResult {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(300))
        .build()
        .unwrap_or_default();

    let bbox_str = format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3]);
    let bands_str = bands.join(",");
    let url = format!(
        "{}/api/fetch?bbox={}&start_date={}&end_date={}&cloud_cover={}&bands={}&limit={}&collection={}&local_only=true",
        node.url.trim_end_matches('/'), bbox_str, start_date, end_date, cloud_cover, bands_str, limit, collection
    );

    info!("Delegating fetch to {} ({}): {} items max", node.node_name, node.url, limit);

    let resp = client.post(&url)
        .header("x-api-key", admin_key)
        .send()
        .await;

    match resp {
        Ok(r) if r.status().is_success() => {
            if let Ok(data) = r.json::<serde_json::Value>().await {
                FetchResult {
                    items_searched: data.get("items_searched").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                    items_downloaded: data.get("items_downloaded").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                    items_skipped: data.get("items_skipped").and_then(|v| v.as_u64()).unwrap_or(0) as usize,
                    bytes_downloaded: data.get("bytes_downloaded").and_then(|v| v.as_u64()).unwrap_or(0),
                    errors: data.get("errors").and_then(|v| v.as_array())
                        .map(|a| a.iter().filter_map(|e| e.as_str().map(String::from)).collect())
                        .unwrap_or_default(),
                }
            } else {
                FetchResult { items_searched: 0, items_downloaded: 0, items_skipped: 0, bytes_downloaded: 0,
                    errors: vec![format!("Bad response from {}", node.node_name)] }
            }
        }
        Ok(r) => {
            let status = r.status();
            let body = r.text().await.unwrap_or_default();
            warn!("Delegate to {} failed: {} {}", node.node_name, status, body);
            FetchResult { items_searched: 0, items_downloaded: 0, items_skipped: 0, bytes_downloaded: 0,
                errors: vec![format!("{}: {} {}", node.node_name, status, body)] }
        }
        Err(e) => {
            warn!("Delegate to {} error: {}", node.node_name, e);
            FetchResult { items_searched: 0, items_downloaded: 0, items_skipped: 0, bytes_downloaded: 0,
                errors: vec![format!("{}: {}", node.node_name, e)] }
        }
    }
}

/// Distributed fetch: search Element84 then spread items across grid nodes.
/// Each node gets a proportional share based on free storage.
pub async fn fetch_distributed(
    store: Arc<Mutex<ChunkStore>>,
    catalog: Arc<Mutex<Catalog>>,
    bbox: [f64; 4],
    start_date: &str,
    end_date: &str,
    cloud_cover: f64,
    bands: &[String],
    limit: usize,
    collection: &str,
    tile_filter: Option<&str>,
    beacon_url: &str,
    local_node_id: &str,
    admin_key: &str,
) -> FetchResult {
    // Get alive nodes from beacon
    let mut nodes = get_grid_nodes(beacon_url).await;
    if nodes.is_empty() {
        info!("No grid nodes found, falling back to local fetch");
        return fetch_and_ingest(store, catalog, bbox, start_date, end_date, cloud_cover, bands, limit, collection, tile_filter).await;
    }

    // Mark local node (match by node_id OR by localhost URL)
    for n in &mut nodes {
        if n.node_id == local_node_id
            || n.url.contains("127.0.0.1")
            || n.url.contains("localhost")
        {
            n.is_local = true;
        }
    }

    // Filter out full nodes (< 0.5 GB free)
    nodes.retain(|n| n.free_gb >= 0.5);
    if nodes.is_empty() {
        warn!("All nodes full, falling back to local fetch");
        return fetch_and_ingest(store, catalog, bbox, start_date, end_date, cloud_cover, bands, limit, collection, tile_filter).await;
    }

    let node_count = nodes.len();
    let total_free: f64 = nodes.iter().map(|n| n.free_gb).sum();

    // Calculate items per node (weighted by free space)
    let mut assignments: Vec<(GridNode, usize)> = vec![];
    let mut assigned = 0usize;
    for (i, node) in nodes.iter().enumerate() {
        let share = if total_free > 0.0 {
            ((node.free_gb / total_free) * limit as f64).round() as usize
        } else {
            limit / node_count
        };
        let share = if i == nodes.len() - 1 {
            limit.saturating_sub(assigned) // last node gets remainder
        } else {
            share.max(1).min(limit.saturating_sub(assigned))
        };
        if share > 0 {
            assignments.push((node.clone(), share));
            assigned += share;
        }
    }

    info!("Distributing fetch across {} nodes: {:?}",
        assignments.len(),
        assignments.iter().map(|(n, s)| format!("{}={}", n.node_name, s)).collect::<Vec<_>>());

    // Execute in parallel: local node uses fetch_and_ingest, remote nodes get /fetch
    let mut handles = vec![];
    for (node, share) in assignments {
        if node.is_local {
            let s = store.clone();
            let c = catalog.clone();
            let b = bands.to_vec();
            let coll = collection.to_string();
            let tf = tile_filter.map(|t| t.to_string());
            let sd = start_date.to_string();
            let ed = end_date.to_string();
            handles.push(tokio::spawn(async move {
                fetch_and_ingest(s, c, bbox, &sd, &ed, cloud_cover, &b, share, &coll, tf.as_deref()).await
            }));
        } else {
            let n = node.clone();
            let b = bands.to_vec();
            let coll = collection.to_string();
            let ak = admin_key.to_string();
            let sd = start_date.to_string();
            let ed = end_date.to_string();
            handles.push(tokio::spawn(async move {
                delegate_fetch_to_node(&n, bbox, &sd, &ed, cloud_cover, &b, share, &coll, &ak).await
            }));
        }
    }

    // Collect results
    let mut combined = FetchResult {
        items_searched: 0, items_downloaded: 0, items_skipped: 0, bytes_downloaded: 0, errors: vec![],
    };
    for handle in handles {
        match handle.await {
            Ok(r) => {
                combined.items_searched += r.items_searched;
                combined.items_downloaded += r.items_downloaded;
                combined.items_skipped += r.items_skipped;
                combined.bytes_downloaded += r.bytes_downloaded;
                combined.errors.extend(r.errors);
            }
            Err(e) => {
                combined.errors.push(format!("Task error: {}", e));
            }
        }
    }

    combined
}
