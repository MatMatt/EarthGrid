//! openEO API v1.2.0 Gateway for EarthGrid.
//!
//! Implements the minimum openEO API surface for Python + R clients:
//! discovery, process catalogue, sync/async execution, job management.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{
    Json,
    extract::{ Path, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    routing::{get, post},
    Router,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

use crate::catalog::{Catalog, DatetimeFilter, StacItem};
use crate::chunk_store::ChunkStore;
use crate::processing;
use crate::server::AppState;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const API_VERSION: &str = "1.2.0";
const BACKEND_VERSION: &str = "0.3.0";

// ---------------------------------------------------------------------------
// Data models
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessGraph {
    pub process_graph: HashMap<String, ProcessNode>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessNode {
    pub process_id: String,
    pub arguments: serde_json::Value,
    #[serde(default)]
    pub result: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenEOBBox {
    pub west: f64,
    pub south: f64,
    pub east: f64,
    pub north: f64,
}

#[derive(Debug, Clone)]
pub struct DataRequirement {
    pub collection_id: String,
    pub spatial_extent: Option<OpenEOBBox>,
    pub temporal_extent: Option<Vec<String>>,
    pub bands: Option<Vec<String>>,
}

#[derive(Debug, Clone)]
pub struct OperationInfo {
    pub operation: Option<String>,
    pub red: String,
    pub nir: String,
    pub green: String,
    pub blue: String,
    pub scl: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResult {
    pub job_id: String,
    pub status: String,
    #[serde(skip_serializing)]
    pub data: Option<Vec<u8>>,
    pub errors: Vec<String>,
    pub created: f64,
    pub updated: f64,
}

pub type JobStore = Arc<Mutex<HashMap<String, JobResult>>>;

// ---------------------------------------------------------------------------
// Process graph parser
// ---------------------------------------------------------------------------

pub fn parse_requirements(graph: &ProcessGraph) -> Vec<DataRequirement> {
    let mut reqs = Vec::new();
    for (_key, node) in &graph.process_graph {
        if node.process_id == "load_collection" {
            let args = &node.arguments;

            let collection_id = args
                .get("id")
                .and_then(|v| v.as_str())
                .unwrap_or("sentinel-2-l2a")
                .to_string();

            let spatial_extent = args.get("spatial_extent").and_then(|se| {
                Some(OpenEOBBox {
                    west: se.get("west")?.as_f64()?,
                    south: se.get("south")?.as_f64()?,
                    east: se.get("east")?.as_f64()?,
                    north: se.get("north")?.as_f64()?,
                })
            });

            let temporal_extent = args.get("temporal_extent").and_then(|te| {
                te.as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
            });

            let bands = args.get("bands").and_then(|b| {
                b.as_array().map(|arr| {
                    arr.iter()
                        .filter_map(|v| v.as_str().map(String::from))
                        .collect()
                })
            });

            reqs.push(DataRequirement {
                collection_id,
                spatial_extent,
                temporal_extent,
                bands,
            });
        }
    }
    reqs
}

pub fn extract_operation(graph: &ProcessGraph) -> OperationInfo {
    let mut info = OperationInfo {
        operation: None,
        red: "B04".to_string(),
        nir: "B08".to_string(),
        green: "B03".to_string(),
        blue: "B02".to_string(),
        scl: "SCL".to_string(),
    };

    // HashMap iteration order is non-deterministic; sort for reproducible operation selection.
    let mut keys: Vec<&String> = graph.process_graph.keys().collect();
    keys.sort();
    for key in keys {
        let node = &graph.process_graph[key];
        match node.process_id.as_str() {
            "ndvi" => {
                info.operation = Some("ndvi".to_string());
                if let Some(red) = node.arguments.get("red").and_then(|v| v.as_str()) {
                    info.red = red.to_string();
                }
                if let Some(nir) = node.arguments.get("nir").and_then(|v| v.as_str()) {
                    info.nir = nir.to_string();
                }
            }
            "ndwi" => {
                info.operation = Some("ndwi".to_string());
                if let Some(green) = node.arguments.get("green").and_then(|v| v.as_str()) {
                    info.green = green.to_string();
                }
                if let Some(nir) = node.arguments.get("nir").and_then(|v| v.as_str()) {
                    info.nir = nir.to_string();
                }
            }
            "evi" => {
                info.operation = Some("evi".to_string());
                if let Some(blue) = node.arguments.get("blue").and_then(|v| v.as_str()) {
                    info.blue = blue.to_string();
                }
                if let Some(red) = node.arguments.get("red").and_then(|v| v.as_str()) {
                    info.red = red.to_string();
                }
                if let Some(nir) = node.arguments.get("nir").and_then(|v| v.as_str()) {
                    info.nir = nir.to_string();
                }
            }
            "cloud_mask" => {
                info.operation = Some("cloud_mask".to_string());
                if let Some(scl) = node.arguments.get("scl").and_then(|v| v.as_str()) {
                    info.scl = scl.to_string();
                }
            }
            "reduce_dimension" => {
                if let Some(reducer) = node.arguments.get("reducer") {
                    let s = reducer.to_string();
                    if s.contains("ndvi") {
                        info.operation = Some("ndvi".to_string());
                    } else if s.contains("ndwi") {
                        info.operation = Some("ndwi".to_string());
                    } else if s.contains("evi") {
                        info.operation = Some("evi".to_string());
                    }
                }
            }
            _ => {}
        }
    }

    info
}

/// Heuristic for spectral band dtype. Prefer float32 when length is divisible by 4.
fn detect_spectral_dtype(data: &[u8]) -> &'static str {
    if data.len() % 4 == 0 {
        "float32"
    } else if data.len() % 2 == 0 {
        "uint16"
    } else {
        // Invalid packing for both u16/f32; keep float32 as a conservative fallback.
        "float32"
    }
}

fn ensure_same_raster_len(op: &str, a: &[u8], b: &[u8]) -> Result<(), String> {
    if a.len() != b.len() {
        Err(format!(
            "{}: band raster size mismatch ({} vs {} bytes); check chunk alignment / same scene",
            op,
            a.len(),
            b.len()
        ))
    } else {
        Ok(())
    }
}

fn ensure_three_raster_len(op: &str, a: &[u8], b: &[u8], c: &[u8]) -> Result<(), String> {
    if a.len() != b.len() || b.len() != c.len() {
        Err(format!(
            "{}: band raster size mismatch ({} / {} / {} bytes)",
            op,
            a.len(),
            b.len(),
            c.len()
        ))
    } else {
        Ok(())
    }
}

fn truncate_pair<'a>(op: &str, a: &'a [u8], b: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), String> {
    let min_len = a.len().min(b.len());
    if min_len == 0 {
        return Err(format!("{op}: empty input buffer"));
    }
    if a.len() != b.len() {
        tracing::warn!(
            "{}: input size mismatch ({} vs {}), truncating both to {} bytes",
            op, a.len(), b.len(), min_len
        );
    }
    Ok((&a[..min_len], &b[..min_len]))
}

fn truncate_three<'a>(
    op: &str,
    a: &'a [u8],
    b: &'a [u8],
    c: &'a [u8],
) -> Result<(&'a [u8], &'a [u8], &'a [u8]), String> {
    let min_len = a.len().min(b.len()).min(c.len());
    if min_len == 0 {
        return Err(format!("{op}: empty input buffer"));
    }
    if a.len() != b.len() || b.len() != c.len() {
        tracing::warn!(
            "{}: input size mismatch ({} / {} / {}), truncating all to {} bytes",
            op, a.len(), b.len(), c.len(), min_len
        );
    }
    Ok((&a[..min_len], &b[..min_len], &c[..min_len]))
}

#[cfg(test)]
mod tests {
    use super::detect_spectral_dtype;

    #[test]
    fn detect_dtype_prefers_float32_when_ambiguous() {
        assert_eq!(detect_spectral_dtype(&vec![0u8; 8]), "float32");
    }

    #[test]
    fn detect_dtype_uses_uint16_for_non_float_even_lengths() {
        assert_eq!(detect_spectral_dtype(&vec![0u8; 6]), "uint16");
    }

    #[test]
    fn detect_dtype_handles_invalid_odd_lengths() {
        assert_eq!(detect_spectral_dtype(&vec![0u8; 7]), "float32");
    }
}

// ---------------------------------------------------------------------------
// Band extraction from item ID
// ---------------------------------------------------------------------------

fn extract_band_from_item_id(item_id: &str) -> Option<String> {
    // Match: _B02, _B08, _B8A, _SCL, _TCI etc. at end of ID
    let patterns = [
        "B01", "B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A",
        "B09", "B10", "B11", "B12", "SCL", "TCI", "AOT", "WVP",
        "VV", "VH", "HH", "HV",
    ];
    for pat in &patterns {
        if item_id.ends_with(pat) || item_id.contains(&format!("_{}_", pat)) {
            return Some(pat.to_string());
        }
    }
    // Try regex-like: last _XXX segment
    let parts: Vec<&str> = item_id.rsplitn(2, '_').collect();
    if parts.len() == 2 {
        let last = parts[0];
        // Strip resolution suffix like "10m"
        let band = last.trim_end_matches(|c: char| c.is_ascii_digit() || c == 'm');
        if !band.is_empty() && band.len() <= 3 {
            return Some(band.to_string());
        }
    }
    None
}

#[allow(dead_code)]
fn extract_date_from_item(item: &StacItem) -> String {
    // Try properties.datetime first
    if let Some(dt) = item.properties.get("datetime").and_then(|v| v.as_str()) {
        if dt != "None" && !dt.is_empty() {
            return dt[..10.min(dt.len())].to_string();
        }
    }
    // Extract from ID: S2A_33UUB_20250629_...
    let parts: Vec<&str> = item.id.split('_').collect();
    for p in &parts {
        if p.len() == 8 && p.chars().all(|c| c.is_ascii_digit()) {
            return format!("{}-{}-{}", &p[..4], &p[4..6], &p[6..8]);
        }
    }
    "unknown".to_string()
}

// ---------------------------------------------------------------------------
// Synchronous execution engine
// ---------------------------------------------------------------------------

pub async fn execute_sync(
    graph: &ProcessGraph,
    catalog: &Arc<Mutex<Catalog>>,
    store: &Arc<Mutex<ChunkStore>>,
) -> Result<Vec<u8>, String> {
    let reqs = parse_requirements(graph);
    if reqs.is_empty() {
        return Err("No load_collection found in process graph".to_string());
    }

    let op = extract_operation(graph);
    let req = &reqs[0];

    // Build search parameters
    let bbox = req.spatial_extent.as_ref().map(|b| (b.west, b.south, b.east, b.north));
    let datetime_filter = req.temporal_extent.as_ref().and_then(|te| {
        if te.len() >= 2 {
            let start = &te[0];
            let end = &te[1];
            Some(format!("{}/{}", start, end))
        } else if te.len() == 1 {
            Some(te[0].clone())
        } else {
            None
        }
    });

    let dt_filter = datetime_filter.as_deref().and_then(DatetimeFilter::parse);

    let bbox_arr = bbox.map(|(w, s, e, n)| [w, s, e, n]);

    // Search catalog
    let cat = catalog.lock().await;
    let items = cat.search(
        Some(&req.collection_id),
        bbox_arr,
        dt_filter.as_ref(),
        500,
        0,
    ).map_err(|e| format!("Catalog search error: {}", e))?;
    drop(cat);

    if items.is_empty() {
        return Err(format!(
            "No items found for collection '{}' with given spatial/temporal extent",
            req.collection_id
        ));
    }

    let needed_bands: Vec<String> = match op.operation.as_deref() {
        Some("ndvi") => vec![op.red.clone(), op.nir.clone()],
        Some("ndwi") => vec![op.green.clone(), op.nir.clone()],
        Some("evi") => vec![op.blue.clone(), op.red.clone(), op.nir.clone()],
        Some("cloud_mask") => vec![op.scl.clone()],
        _ => req.bands.clone().unwrap_or_default(),
    };

    // Check which bands are available locally
    let mut available_bands_initial: Vec<String> = Vec::new();
    for item in &items {
        if let Some(band) = extract_band_from_item_id(&item.id) {
            if !available_bands_initial.contains(&band) {
                available_bands_initial.push(band);
            }
        }
    }

    let missing_bands: Vec<String> = needed_bands.iter()
        .filter(|b| !available_bands_initial.contains(b))
        .cloned()
        .collect();

    // Auto-fetch missing bands from Element84
    let items = if !missing_bands.is_empty() && bbox.is_some() {
        let (w, s, e, n) = bbox.unwrap();
        let datetime = req.temporal_extent.as_ref().map(|te| {
            if te.len() >= 2 { (te[0].as_str(), te[1].as_str()) } else { (te[0].as_str(), te[0].as_str()) }
        });
        let (start, end) = datetime.unwrap_or(("2020-01-01", "2030-01-01"));

        tracing::info!("Auto-fetching missing bands {:?} from Element84 (have: {:?})", missing_bands, available_bands_initial);
        let fetch_bands = if needed_bands.is_empty() { missing_bands.clone() } else { needed_bands.clone() };
        let fetch_result = crate::fetcher::fetch_and_ingest(
            store.clone(),
            catalog.clone(),
            [w, s, e, n],
            start,
            end,
            50.0,
            &fetch_bands,
            10,
            &req.collection_id,
            None,  // no tile filter
        ).await;

        tracing::info!("Auto-fetch done: {} downloaded, {} skipped, {} errors",
            fetch_result.items_downloaded, fetch_result.items_skipped, fetch_result.errors.len());

        // Re-search catalog after fetch
        let cat = catalog.lock().await;
        let refreshed = cat.search(
            Some(&req.collection_id),
            bbox_arr,
            dt_filter.as_ref(),
            500,
            0,
        ).map_err(|e| format!("Catalog re-search error: {}", e))?;
        drop(cat);
        refreshed
    } else {
        items
    };

    // Group items by band
    let mut band_items: HashMap<String, Vec<&StacItem>> = HashMap::new();
    for item in &items {
        if let Some(band) = extract_band_from_item_id(&item.id) {
            band_items.entry(band).or_default().push(item);
        }
    }

    let avail: Vec<&String> = band_items.keys().collect();

    let load_band = |band: &str, store_locked: &mut ChunkStore| -> Result<Vec<u8>, String> {
        let items = band_items.get(band).ok_or_else(|| {
            format!("Band '{}' not found. Needed: {:?}. Available: {:?}. Items checked: {}.",
                band, needed_bands, avail, items.len())
        })?;
        // When multiple candidates map to the same logical band (e.g. nir vs nir08),
        // prefer the densest chunk set as a proxy for highest resolution.
        let item = items
            .iter()
            .max_by_key(|it| it.chunk_hashes.len())
            .copied()
            .ok_or_else(|| format!("No items for band '{}'", band))?;
        let mut data = Vec::new();
        for hash in &item.chunk_hashes {
            if let Ok(Some(chunk)) = store_locked.get(hash) {
                data.extend_from_slice(&chunk);
            }
        }
        if data.is_empty() {
            return Err(format!("Could not load chunk data for band '{}'", band));
        }
        Ok(data)
    };

    match op.operation.as_deref() {
        Some("ndvi") => {
            let mut store = store.lock().await;
            let red_data = load_band(&op.red, &mut store)?;
            let nir_data = load_band(&op.nir, &mut store)?;
            let (red_data, nir_data) = truncate_pair("NDVI", &red_data, &nir_data)?;
            let dtype = detect_spectral_dtype(red_data);
            Ok(processing::compute_ndvi(red_data, nir_data, dtype))
        }
        Some("ndwi") => {
            let mut store = store.lock().await;
            let green_data = load_band(&op.green, &mut store)?;
            let nir_data = load_band(&op.nir, &mut store)?;
            let (green_data, nir_data) = truncate_pair("NDWI", &green_data, &nir_data)?;
            let dtype = detect_spectral_dtype(green_data);
            Ok(processing::compute_ndwi(green_data, nir_data, dtype))
        }
        Some("evi") => {
            let mut store = store.lock().await;
            let blue_data = load_band(&op.blue, &mut store)?;
            let red_data = load_band(&op.red, &mut store)?;
            let nir_data = load_band(&op.nir, &mut store)?;
            let (blue_data, red_data, nir_data) = truncate_three("EVI", &blue_data, &red_data, &nir_data)?;
            let dtype = detect_spectral_dtype(red_data);
            Ok(processing::compute_evi(blue_data, red_data, nir_data, dtype))
        }
        Some("cloud_mask") => {
            let mut store = store.lock().await;
            let scl_data = load_band(&op.scl, &mut store)?;
            Ok(processing::cloud_mask(&scl_data))
        }
        _ => {
            let first = &items[0];
            let mut store = store.lock().await;
            let mut data = Vec::new();
            for hash in &first.chunk_hashes {
                if let Ok(Some(chunk)) = store.get(hash) {
                    data.extend_from_slice(&chunk);
                }
            }
            if data.is_empty() {
                return Err("No chunk data found".to_string());
            }
            Ok(data)
        }
    }
}

// ---------------------------------------------------------------------------
// Process catalogue
// ---------------------------------------------------------------------------

fn process_catalogue() -> serde_json::Value {
    serde_json::json!({
        "processes": [
            {
                "id": "load_collection",
                "summary": "Load a collection from the current back-end by its id.",
                "description": "Loads a collection from the current back-end by its id and returns it as a processable data cube.",
                "parameters": [
                    {"name": "id", "description": "The collection id.", "schema": {"type": "string"}},
                    {"name": "spatial_extent", "description": "Bounding box.", "schema": {"type": "object"}},
                    {"name": "temporal_extent", "description": "Temporal filter.", "schema": {"type": "array"}},
                    {"name": "bands", "description": "Band names.", "schema": {"type": "array"}}
                ],
                "returns": {"description": "A data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "save_result",
                "summary": "Save processed data.",
                "description": "Saves processed data to the given file format.",
                "parameters": [
                    {"name": "data", "description": "Data to save.", "schema": {"type": "object"}},
                    {"name": "format", "description": "Output format.", "schema": {"type": "string"}}
                ],
                "returns": {"description": "false", "schema": {"type": "boolean"}}
            },
            {
                "id": "ndvi",
                "summary": "Compute NDVI.",
                "description": "Normalized Difference Vegetation Index: (NIR - RED) / (NIR + RED)",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"},
                    {"name": "red", "description": "Red band name.", "schema": {"type": "string"}, "default": "B04"}
                ],
                "returns": {"description": "NDVI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "ndwi",
                "summary": "Compute NDWI.",
                "description": "Normalized Difference Water Index: (Green - NIR) / (Green + NIR)",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "green", "description": "Green band name.", "schema": {"type": "string"}, "default": "B03"},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"}
                ],
                "returns": {"description": "NDWI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "evi",
                "summary": "Compute EVI.",
                "description": "Enhanced Vegetation Index: 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1)",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "blue", "description": "Blue band name.", "schema": {"type": "string"}, "default": "B02"},
                    {"name": "red", "description": "Red band name.", "schema": {"type": "string"}, "default": "B04"},
                    {"name": "nir", "description": "NIR band name.", "schema": {"type": "string"}, "default": "B08"}
                ],
                "returns": {"description": "EVI data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "cloud_mask",
                "summary": "Compute cloud mask from SCL.",
                "description": "Binary cloud mask from Scene Classification Layer. Cloud pixels (SCL 8/9/10) → 0, clear → 1.",
                "parameters": [
                    {"name": "data", "description": "Input data cube.", "schema": {"type": "object"}},
                    {"name": "scl", "description": "SCL band name.", "schema": {"type": "string"}, "default": "SCL"}
                ],
                "returns": {"description": "Cloud mask data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "reduce_dimension",
                "summary": "Reduce a dimension.",
                "description": "Applies a reducer to a data cube dimension.",
                "parameters": [
                    {"name": "data", "description": "Data cube.", "schema": {"type": "object"}},
                    {"name": "reducer", "description": "Reducer process.", "schema": {"type": "object"}},
                    {"name": "dimension", "description": "Dimension name.", "schema": {"type": "string"}}
                ],
                "returns": {"description": "Reduced data cube.", "schema": {"type": "object"}}
            },
            {
                "id": "apply",
                "summary": "Apply a process to each value.",
                "description": "Applies a process to each value in the data cube.",
                "parameters": [
                    {"name": "data", "description": "Data cube.", "schema": {"type": "object"}},
                    {"name": "process", "description": "Process to apply.", "schema": {"type": "object"}}
                ],
                "returns": {"description": "Processed data cube.", "schema": {"type": "object"}}
            }
        ],
        "links": []
    })
}

// ---------------------------------------------------------------------------
// Capabilities
// ---------------------------------------------------------------------------

fn capabilities(base_url: &str) -> serde_json::Value {
    serde_json::json!({
        "api_version": API_VERSION,
        "backend_version": BACKEND_VERSION,
        "stac_version": "1.0.0",
        "id": "earthgrid",
        "title": "EarthGrid openEO Backend",
        "description": "Distributed satellite data storage and openEO-compatible processing.",
        "production": false,
        "endpoints": [
            {"path": "/", "methods": ["GET"]},
            {"path": "/.well-known/openeo", "methods": ["GET"]},
            {"path": "/credentials/basic", "methods": ["GET"]},
            {"path": "/me", "methods": ["GET"]},
            {"path": "/collections", "methods": ["GET"]},
            {"path": "/collections/{collection_id}", "methods": ["GET"]},
            {"path": "/processes", "methods": ["GET"]},
            {"path": "/result", "methods": ["POST"]},
            {"path": "/jobs", "methods": ["GET", "POST"]},
            {"path": "/jobs/{job_id}", "methods": ["GET", "DELETE"]},
            {"path": "/jobs/{job_id}/results", "methods": ["GET"]},
            {"path": "/jobs/{job_id}/logs", "methods": ["GET"]}
        ],
        "links": [
            {"rel": "self", "href": format!("{}/", base_url)},
            {"rel": "conformance", "href": "https://openeo.net/openeo-api-spec/"},
            {"rel": "version-history", "href": format!("{}/.well-known/openeo", base_url)}
        ],
        "billing": null,
        "file_formats": {
            "output": [{
                "name": "GTiff",
                "title": "GeoTIFF",
                "gis_data_types": ["raster"],
                "parameters": {},
                "links": []
            }]
        }
    })
}

// ---------------------------------------------------------------------------
// Extended state for openEO (includes job store)
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct OpenEOState {
    pub app: AppState,
    pub jobs: JobStore,
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn openeo_capabilities() -> Json<serde_json::Value> {
    Json(capabilities(""))
}

async fn well_known_openeo() -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "versions": [{
            "url": "/",
            "api_version": API_VERSION,
            "production": false
        }]
    }))
}

async fn credentials_basic(headers: HeaderMap, State(state): State<OpenEOState>) -> impl IntoResponse {
    // Extract Basic auth, return bearer token
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        let parts: Vec<&str> = auth.splitn(2, ' ').collect();
        if parts.len() == 2 && parts[0].eq_ignore_ascii_case("basic") {
            // Decode base64
            if let Ok(decoded) = base64_decode(parts[1]) {
                if let Some((_user, pass)) = decoded.split_once(':') {
                    // Check if password matches API key
                    if state.app.auth.check_write(Some(pass)).is_ok() {
                        return (
                            StatusCode::OK,
                            Json(serde_json::json!({
                                "access_token": pass,
                                "token_type": "bearer"
                            })),
                        );
                    }
                }
            }
        }
    }
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"code": "AuthenticationRequired", "message": "Invalid credentials"})),
    )
}

fn base64_decode(input: &str) -> Result<String, String> {
    // Simple base64 decode without external crate
    use std::collections::HashMap;
    let chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut table = HashMap::new();
    for (i, c) in chars.chars().enumerate() {
        table.insert(c, i as u8);
    }

    let input = input.trim_end_matches('=');
    let mut bytes = Vec::new();
    let mut buf = 0u32;
    let mut bits = 0;

    for c in input.chars() {
        if let Some(&val) = table.get(&c) {
            buf = (buf << 6) | val as u32;
            bits += 6;
            if bits >= 8 {
                bits -= 8;
                bytes.push((buf >> bits) as u8);
                buf &= (1 << bits) - 1;
            }
        }
    }

    String::from_utf8(bytes).map_err(|e| e.to_string())
}

async fn me_handler(headers: HeaderMap, State(state): State<OpenEOState>) -> impl IntoResponse {
    // Check bearer token
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if let Some(t) = token {
        if state.app.auth.check_write(Some(t)).is_ok() {
            return (
                StatusCode::OK,
                Json(serde_json::json!({
                    "user_id": "earthgrid-user",
                    "name": "EarthGrid User",
                    "budget": null,
                    "links": []
                })),
            );
        }
    }
    if !state.app.auth.is_enabled() {
        return (
            StatusCode::OK,
            Json(serde_json::json!({
                "user_id": "earthgrid-user",
                "name": "EarthGrid User (open mode)",
                "budget": null,
                "links": []
            })),
        );
    }
    (
        StatusCode::FORBIDDEN,
        Json(serde_json::json!({"code": "AuthenticationRequired", "message": "Not authenticated"})),
    )
}

async fn openeo_collections(State(state): State<OpenEOState>) -> impl IntoResponse {
    let cat = state.app.catalog.lock().await;
    match cat.list_collections() {
        Ok(collections) => {
            let cols: Vec<serde_json::Value> = collections
                .iter()
                .map(|c| {
                    serde_json::json!({
                        "stac_version": "1.0.0",
                        "id": c.id,
                        "description": c.description,
                        "license": "proprietary",
                        "extent": {
                            "spatial": {"bbox": [[-180, -90, 180, 90]]},
                            "temporal": {"interval": [[null, null]]}
                        },
                        "links": []
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({"collections": cols, "links": []})))
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{}", e)})),
        ),
    }
}

async fn openeo_collection(
    Path(id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let cat = state.app.catalog.lock().await;
    match cat.get_collection(&id) {
        Ok(Some(c)) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "stac_version": "1.0.0",
                "id": c.id,
                "description": c.description,
                "license": "proprietary",
                "extent": {
                    "spatial": {"bbox": [[-180, -90, 180, 90]]},
                    "temporal": {"interval": [[null, null]]}
                },
                "links": []
            })),
        ),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "CollectionNotFound", "message": format!("Collection '{}' not found", id)})),
        ),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"error": format!("{}", e)})),
        ),
    }
}

async fn openeo_processes() -> Json<serde_json::Value> {
    Json(process_catalogue())
}

async fn openeo_result(
    headers: HeaderMap,
    State(state): State<OpenEOState>,
    Json(graph): Json<ProcessGraph>,
) -> impl IntoResponse {
    // Auth check
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if state.app.auth.check_write(token).is_err() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"code": "AuthenticationRequired", "message": "API key required"}))
                .into_response(),
        );
    }

    match execute_sync(&graph, &state.app.catalog, &state.app.store).await {
        Ok(data) => {
            let headers = [
                ("content-type", "application/octet-stream"),
                ("content-disposition", "attachment; filename=\"result.tif\""),
            ];
            (StatusCode::OK, (headers, data).into_response())
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "ProcessingError", "message": e})).into_response(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Jobs API
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct CreateJobRequest {
    pub process: ProcessGraph,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
}

fn now_epoch() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

async fn create_job(
    headers: HeaderMap,
    State(state): State<OpenEOState>,
    Json(req): Json<CreateJobRequest>,
) -> impl IntoResponse {
    let token = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.strip_prefix("Bearer "))
        .or_else(|| headers.get("x-api-key").and_then(|v| v.to_str().ok()));

    if state.app.auth.check_write(token).is_err() {
        return (
            StatusCode::FORBIDDEN,
            Json(serde_json::json!({"code": "AuthenticationRequired", "message": "API key required"})),
        );
    }

    let job_id = uuid::Uuid::new_v4().to_string();
    let now = now_epoch();

    let job = JobResult {
        job_id: job_id.clone(),
        status: "created".to_string(),
        data: None,
        errors: Vec::new(),
        created: now,
        updated: now,
    };

    // Store job
    state.jobs.lock().await.insert(job_id.clone(), job);

    // Spawn async execution
    let jobs = state.jobs.clone();
    let catalog = state.app.catalog.clone();
    let store = state.app.store.clone();
    let graph = req.process;
    let jid = job_id.clone();

    tokio::spawn(async move {
        // Update to running
        if let Some(j) = jobs.lock().await.get_mut(&jid) {
            j.status = "running".to_string();
            j.updated = now_epoch();
        }

        match execute_sync(&graph, &catalog, &store).await {
            Ok(data) => {
                if let Some(j) = jobs.lock().await.get_mut(&jid) {
                    j.status = "finished".to_string();
                    j.data = Some(data);
                    j.updated = now_epoch();
                }
            }
            Err(e) => {
                if let Some(j) = jobs.lock().await.get_mut(&jid) {
                    j.status = "error".to_string();
                    j.errors.push(e);
                    j.updated = now_epoch();
                }
            }
        }
    });

    (
        StatusCode::CREATED,
        Json(serde_json::json!({
            "id": job_id,
            "status": "created",
            "created": now
        })),
    )
}

async fn list_jobs(State(state): State<OpenEOState>) -> Json<serde_json::Value> {
    let jobs = state.jobs.lock().await;
    let job_list: Vec<serde_json::Value> = jobs
        .values()
        .map(|j| {
            serde_json::json!({
                "id": j.job_id,
                "status": j.status,
                "created": j.created,
                "updated": j.updated
            })
        })
        .collect();
    Json(serde_json::json!({"jobs": job_list, "links": []}))
}

async fn get_job(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().await;
    match jobs.get(&job_id) {
        Some(j) => (
            StatusCode::OK,
            Json(serde_json::json!({
                "id": j.job_id,
                "status": j.status,
                "created": j.created,
                "updated": j.updated,
                "errors": j.errors
            })),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "JobNotFound", "message": format!("Job '{}' not found", job_id)})),
        ),
    }
}

async fn get_job_results(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().await;
    match jobs.get(&job_id) {
        Some(j) if j.status == "finished" => {
            if let Some(data) = &j.data {
                let headers = [
                    ("content-type", "application/octet-stream"),
                    ("content-disposition", "attachment; filename=\"result.tif\""),
                ];
                (StatusCode::OK, (headers, data.clone()).into_response())
            } else {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(serde_json::json!({"error": "Job finished but no data"})).into_response(),
                )
            }
        }
        Some(j) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "JobNotFinished",
                "message": format!("Job status: {}", j.status)
            }))
            .into_response(),
        ),
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "JobNotFound", "message": format!("Job '{}' not found", job_id)}))
                .into_response(),
        ),
    }
}

async fn get_job_logs(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let jobs = state.jobs.lock().await;
    match jobs.get(&job_id) {
        Some(j) => {
            let logs: Vec<serde_json::Value> = j
                .errors
                .iter()
                .enumerate()
                .map(|(i, e)| {
                    serde_json::json!({
                        "id": format!("{}", i),
                        "level": if j.status == "error" { "error" } else { "info" },
                        "message": e
                    })
                })
                .collect();
            (StatusCode::OK, Json(serde_json::json!({"logs": logs, "links": []})))
        }
        None => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({"code": "JobNotFound", "message": format!("Job '{}' not found", job_id)})),
        ),
    }
}

async fn delete_job(
    Path(job_id): Path<String>,
    State(state): State<OpenEOState>,
) -> impl IntoResponse {
    let mut jobs = state.jobs.lock().await;
    if jobs.remove(&job_id).is_some() {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::NOT_FOUND
    }
}

/// POST /validate — validate an openEO process graph.
///
/// Returns a list of validation errors (empty array = valid).
async fn validate_process_graph(
    State(_state): State<OpenEOState>,
    Json(body): Json<serde_json::Value>,
) -> impl IntoResponse {
    // Extract the process graph from the request body.
    // Accept both {"process_graph": {...}} and {"process": {"process_graph": {...}}}
    let pg = body.get("process_graph")
        .or_else(|| body.get("process").and_then(|p| p.get("process_graph")));

    let mut errors: Vec<serde_json::Value> = Vec::new();

    match pg {
        None => {
            errors.push(serde_json::json!({
                "code": "ProcessGraphMissing",
                "message": "No process_graph found in request body."
            }));
        }
        Some(pg_val) => {
            if let Some(nodes) = pg_val.as_object() {
                // Validate each node: check process_id is known
                let known_processes = [
                    "load_collection", "save_result", "ndvi", "reduce_dimension",
                    "apply", "normalized_difference", "array_element",
                    "multiply", "add", "subtract", "divide",
                    "filter_temporal", "filter_bbox", "filter_bands",
                ];
                for (node_id, node) in nodes {
                    let process_id = node.get("process_id").and_then(|v| v.as_str()).unwrap_or("");
                    if process_id.is_empty() {
                        errors.push(serde_json::json!({
                            "code": "ProcessIdMissing",
                            "message": format!("Node '{}' has no process_id.", node_id)
                        }));
                    } else if !known_processes.contains(&process_id) {
                        // Unknown process — warn but don't fail (backend may support more)
                        errors.push(serde_json::json!({
                            "code": "ProcessUnsupported",
                            "message": format!("Process '{}' is not supported by this backend.", process_id),
                            "level": "warning"
                        }));
                    }
                }
            } else {
                errors.push(serde_json::json!({
                    "code": "ProcessGraphInvalid",
                    "message": "process_graph must be an object of process nodes."
                }));
            }
        }
    }

    (StatusCode::OK, Json(serde_json::json!({"errors": errors})))
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

pub fn openeo_router(state: OpenEOState) -> Router {
    Router::new()
        .route("/", get(openeo_capabilities))
        .route("/.well-known/openeo", get(well_known_openeo))
        .route("/credentials/basic", get(credentials_basic))
        .route("/me", get(me_handler))
        .route("/collections", get(openeo_collections))
        .route("/collections/{id}", get(openeo_collection))
        .route("/processes", get(openeo_processes))
        .route("/result", post(openeo_result))
        .route("/jobs", get(list_jobs).post(create_job))
        .route("/jobs/{job_id}", get(get_job).delete(delete_job))
        .route("/jobs/{job_id}/results", get(get_job_results))
        .route("/jobs/{job_id}/logs", get(get_job_logs))
        .route("/validate", post(validate_process_graph))
        .with_state(state)
}
