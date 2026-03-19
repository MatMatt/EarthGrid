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
use crate::ingest;

const ELEMENT84_BASE: &str = "https://earth-search.aws.element84.com/v1";
const MAX_CONCURRENT_DOWNLOADS: usize = 4;

// ---------------------------------------------------------------------------
// Band mapping: STAC asset key → friendly band name
// ---------------------------------------------------------------------------

fn band_key_to_name(key: &str) -> Option<&'static str> {
    match key {
        "B01" | "coastal" => Some("coastal"),
        "B02" | "blue" => Some("blue"),
        "B03" | "green" => Some("green"),
        "B04" | "red" => Some("red"),
        "B05" | "rededge1" => Some("rededge1"),
        "B06" | "rededge2" => Some("rededge2"),
        "B07" | "rededge3" => Some("rededge3"),
        "B08" | "nir" => Some("nir"),
        "B8A" | "nir08" => Some("nir08"),
        "B11" | "swir16" => Some("swir16"),
        "B12" | "swir22" => Some("swir22"),
        "SCL" | "scl" => Some("scl"),
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Data structures
// ---------------------------------------------------------------------------

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
}

/// Aggregated result of a fetch+ingest run.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FetchResult {
    pub items_searched: usize,
    pub items_downloaded: usize,
    pub items_skipped: usize,
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
        "datetime": format!("{}/{}", start_date, end_date),
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
) -> (Vec<StacSearchResult>, Vec<String>) {
    let mut results = Vec::new();
    let mut errors = Vec::new();

    let search_url = format!("{}/search", ELEMENT84_BASE);
    let mut body = build_search_body(&bbox, start_date, end_date, cloud_cover, limit, collection);

    loop {
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

    let start = match parse(start_date) {
        Some(d) => d,
        None => return vec![(start_date.to_string(), end_date.to_string())],
    };
    let end = match parse(end_date) {
        Some(d) => d,
        None => return vec![(start_date.to_string(), end_date.to_string())],
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
                search_one_range(&client, bbox, &s, &e, cloud_cover, per_chunk_limit, &collection).await
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
    let stac_item = {
        let mut store_lock = store.lock().await;
        ingest::ingest_file(&temp_path, collection, ingest::DEFAULT_CHUNK_SIZE, &mut store_lock)
            .map_err(|e| format!("Ingest failed for {} {}: {}", item.id, band, e))?
    };

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
) -> FetchResult {
    let (items, search_errors) = search_element84(bbox, start_date, end_date, cloud_cover, limit, collection).await;
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
    let mut tasks = Vec::new();

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

        for (band, url) in bands_to_download {
            let store = store.clone();
            let catalog = catalog.clone();
            let client = client.clone();
            let semaphore = semaphore.clone();
            let temp_dir = temp_dir.clone();
            let item = item.clone();
            let collection = collection.to_string();

            tasks.push(tokio::spawn(async move {
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
    }

    // Await all tasks
    let task_results = join_all(tasks).await;
    for task_result in task_results {
        match task_result {
            Ok(Some(bytes)) => {
                result.items_downloaded += 1;
                result.bytes_downloaded += bytes;
            }
            Ok(None) => {
                // Error already logged + warned
                result.errors.push("Download/ingest failed (see logs)".to_string());
            }
            Err(e) => {
                result.errors.push(format!("Task join error: {e}"));
            }
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

    let url = format!("{}/search", beacon_url.trim_end_matches('/'));

    let resp = client
        .get(&url)
        .query(&[
            ("bbox", format!("{},{},{},{}", bbox[0], bbox[1], bbox[2], bbox[3])),
            ("collections", collection.to_string()),
            ("datetime", format!("{}/{}", start_date, end_date)),
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
