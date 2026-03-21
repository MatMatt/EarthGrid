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
use gdal::raster::{Buffer, RasterCreationOptions};
use gdal::spatial_ref::SpatialRef;
use gdal::DriverManager;
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

/// Extract the output format requested by a `save_result` node (e.g. "GTiff").
pub fn extract_output_format(graph: &ProcessGraph) -> Option<String> {
    for node in graph.process_graph.values() {
        if node.process_id == "save_result" {
            if let Some(fmt) = node.arguments.get("format").and_then(|v| v.as_str()) {
                return Some(fmt.to_string());
            }
        }
    }
    None
}

/// Metadata carried alongside raw pixel output so callers can build a GeoTIFF.
#[derive(Debug, Clone)]
pub struct RasterMeta {
    pub width: usize,
    pub height: usize,
    pub crs: String,
    pub transform: [f64; 6],
    pub dtype: String,
}

/// Resolve a user-facing format string to a canonical key.
pub fn canonical_format(fmt: &str) -> &str {
    match fmt.to_ascii_lowercase().as_str() {
        "gtiff" | "geotiff" | "tiff" | "tif" => "GTiff",
        "netcdf" | "nc" => "netCDF",
        "geojson" | "json" => "GeoJSON",
        "geoparquet" | "parquet" => "GeoParquet",
        _ => "GTiff",
    }
}

/// Wrap raw result bytes into the requested output format.
/// Currently only GTiff is implemented; GeoJSON and GeoParquet are stubbed
/// for when openEO vector process support is added.
pub fn wrap_output(pixels: &[u8], meta: &RasterMeta, format: &str) -> Result<(Vec<u8>, &'static str), String> {
    match canonical_format(format) {
        "GTiff" => {
            let bytes = wrap_geotiff(pixels, meta)?;
            Ok((bytes, "image/tiff"))
        }
        "netCDF" => {
            let bytes = wrap_netcdf(pixels, meta)?;
            Ok((bytes, "application/x-netcdf"))
        }
        "GeoJSON" => Err(
            "GeoJSON output is not yet implemented. \
             It will be supported when openEO vector processes are added."
                .to_string(),
        ),
        "GeoParquet" => Err(
            "GeoParquet output is not yet implemented. \
             It will be supported when openEO vector processes are added."
                .to_string(),
        ),
        _ => Err(format!("Unsupported output format: {format}")),
    }
}

/// Package raw f32 pixel bytes into a proper GeoTIFF using GDAL `/vsimem`.
pub fn wrap_geotiff(pixels: &[u8], meta: &RasterMeta) -> Result<Vec<u8>, String> {
    let pixel_count = (meta.width * meta.height) as usize;
    let expected = pixel_count * 4; // f32
    if pixels.len() < expected {
        return Err(format!(
            "wrap_geotiff: buffer too small ({} bytes, need {} for {}x{} f32)",
            pixels.len(), expected, meta.width, meta.height
        ));
    }
    let floats: Vec<f32> = pixels[..expected]
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();

    let vsi_path = format!("/vsimem/earthgrid_wrap_{}.tif", uuid::Uuid::new_v4());
    let driver = DriverManager::get_driver_by_name("GTiff")
        .map_err(|e| format!("GDAL GTiff driver: {e}"))?;
    let w = meta.width;
    let h = meta.height;
    let mut ds = driver
        .create_with_band_type_with_options::<f32, _>(
            &vsi_path, w, h, 1,
            &RasterCreationOptions::from_iter(["TILED=YES", "COMPRESS=LZW"]),
        )
        .map_err(|e| format!("GDAL create: {e}"))?;

    ds.set_geo_transform(&meta.transform)
        .map_err(|e| format!("GDAL set geotransform: {e}"))?;
    if let Ok(srs) = SpatialRef::from_definition(&meta.crs) {
        let _ = ds.set_spatial_ref(&srs);
    }

    let mut band = ds.rasterband(1).map_err(|e| format!("GDAL rasterband: {e}"))?;
    let mut buf = Buffer::new((w, h), floats);
    band.write((0, 0), (w, h), &mut buf)
        .map_err(|e| format!("GDAL write: {e}"))?;
    drop(band);
    drop(ds);

    let bytes = gdal::vsi::get_vsi_mem_file_bytes_owned(&vsi_path)
        .map_err(|e| format!("GDAL vsimem read: {e}"))?;
    let _ = gdal::vsi::unlink_mem_file(&vsi_path);
    Ok(bytes)
}

/// Package raw f32 pixel bytes into a netCDF-4 file via a temp file
/// (GDAL's netCDF driver does not support `/vsimem`).
pub fn wrap_netcdf(pixels: &[u8], meta: &RasterMeta) -> Result<Vec<u8>, String> {
    let pixel_count = meta.width * meta.height;
    let expected = pixel_count * 4;
    if pixels.len() < expected {
        return Err(format!(
            "wrap_netcdf: buffer too small ({} bytes, need {} for {}x{} f32)",
            pixels.len(), expected, meta.width, meta.height
        ));
    }
    let floats: Vec<f32> = pixels[..expected]
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();

    let tmp_path = std::env::temp_dir().join(format!("earthgrid_wrap_{}.nc", uuid::Uuid::new_v4()));
    // GDAL's C API is happier with POSIX-style paths on Windows (conda builds).
    let tmp_str = tmp_path.to_string_lossy().replace('\\', "/");

    let driver = DriverManager::get_driver_by_name("netCDF")
        .map_err(|e| format!("GDAL netCDF driver: {e}"))?;
    let w = meta.width;
    let h = meta.height;
    let opts_nc4 =
        RasterCreationOptions::from_iter(["FORMAT=NC4", "COMPRESS=DEFLATE"]);
    let mut ds = driver
        .create_with_band_type_with_options::<f32, _>(&tmp_str, w, h, 1, &opts_nc4)
        .or_else(|e| {
            // Windows conda / some builds reject NC4+DEFLATE; classic netCDF still works.
            driver
                .create_with_band_type::<f32, _>(&tmp_str, w, h, 1)
                .map_err(|e2| format!("GDAL netCDF create: {e}; fallback: {e2}"))
        })?;

    // netCDF driver on some platforms (notably Windows conda) rejects georeferencing;
    // pixels still write correctly — do not fail the whole export.
    let _ = ds.set_geo_transform(&meta.transform);
    if let Ok(srs) = SpatialRef::from_definition(&meta.crs) {
        let _ = ds.set_spatial_ref(&srs);
    }

    let mut band = ds.rasterband(1).map_err(|e| format!("GDAL rasterband: {e}"))?;
    let mut buf = Buffer::new((w, h), floats);
    band.write((0, 0), (w, h), &mut buf)
        .map_err(|e| format!("GDAL write: {e}"))?;
    drop(band);
    drop(ds);

    let bytes = std::fs::read(&tmp_path)
        .map_err(|e| format!("read netCDF tmp: {e}"))?;
    let _ = std::fs::remove_file(&tmp_path);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use gdal::Dataset;
    use gdal::DatasetOptions;
    use gdal::GdalOpenFlags;
    use gdal::Metadata;
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};

    // --- detect_spectral_dtype ---

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

    // --- canonical_format ---

    #[test]
    fn canonical_format_gtiff_aliases() {
        assert_eq!(canonical_format("GTiff"), "GTiff");
        assert_eq!(canonical_format("geotiff"), "GTiff");
        assert_eq!(canonical_format("GeoTIFF"), "GTiff");
        assert_eq!(canonical_format("tiff"), "GTiff");
        assert_eq!(canonical_format("tif"), "GTiff");
        assert_eq!(canonical_format("TIFF"), "GTiff");
    }

    #[test]
    fn canonical_format_netcdf_aliases() {
        assert_eq!(canonical_format("netCDF"), "netCDF");
        assert_eq!(canonical_format("NetCDF"), "netCDF");
        assert_eq!(canonical_format("NETCDF"), "netCDF");
        assert_eq!(canonical_format("nc"), "netCDF");
        assert_eq!(canonical_format("NC"), "netCDF");
    }

    #[test]
    fn canonical_format_vector_aliases() {
        assert_eq!(canonical_format("GeoJSON"), "GeoJSON");
        assert_eq!(canonical_format("geojson"), "GeoJSON");
        assert_eq!(canonical_format("json"), "GeoJSON");
        assert_eq!(canonical_format("GeoParquet"), "GeoParquet");
        assert_eq!(canonical_format("geoparquet"), "GeoParquet");
        assert_eq!(canonical_format("parquet"), "GeoParquet");
    }

    #[test]
    fn canonical_format_unknown_defaults_to_gtiff() {
        assert_eq!(canonical_format("png"), "GTiff");
        assert_eq!(canonical_format(""), "GTiff");
    }

    // --- resolve_band_alias ---

    #[test]
    fn band_alias_code_to_common() {
        assert_eq!(resolve_band_alias("B04"), "red");
        assert_eq!(resolve_band_alias("B08"), "nir");
        assert_eq!(resolve_band_alias("B03"), "green");
        assert_eq!(resolve_band_alias("B02"), "blue");
        assert_eq!(resolve_band_alias("B8A"), "nir08");
        assert_eq!(resolve_band_alias("SCL"), "scl");
    }

    #[test]
    fn band_alias_common_to_code() {
        assert_eq!(resolve_band_alias("red"), "B04");
        assert_eq!(resolve_band_alias("nir"), "B08");
        assert_eq!(resolve_band_alias("green"), "B03");
        assert_eq!(resolve_band_alias("blue"), "B02");
        assert_eq!(resolve_band_alias("scl"), "SCL");
    }

    #[test]
    fn band_alias_unknown_passes_through() {
        assert_eq!(resolve_band_alias("VV"), "VV");
        assert_eq!(resolve_band_alias("custom"), "custom");
    }

    // --- extract_output_format ---

    #[test]
    fn extract_format_from_save_result() {
        let mut nodes = HashMap::new();
        nodes.insert("load1".to_string(), ProcessNode {
            process_id: "load_collection".to_string(),
            arguments: serde_json::json!({"id": "sentinel-2-l2a"}),
            result: Some(false),
        });
        nodes.insert("save1".to_string(), ProcessNode {
            process_id: "save_result".to_string(),
            arguments: serde_json::json!({"format": "netCDF"}),
            result: Some(true),
        });
        let graph = ProcessGraph { process_graph: nodes };
        assert_eq!(extract_output_format(&graph), Some("netCDF".to_string()));
    }

    #[test]
    fn extract_format_missing_returns_none() {
        let mut nodes = HashMap::new();
        nodes.insert("load1".to_string(), ProcessNode {
            process_id: "load_collection".to_string(),
            arguments: serde_json::json!({"id": "s2"}),
            result: Some(true),
        });
        let graph = ProcessGraph { process_graph: nodes };
        assert_eq!(extract_output_format(&graph), None);
    }

    // --- extract_operation ---

    #[test]
    fn extract_ndvi_operation() {
        let mut nodes = HashMap::new();
        nodes.insert("ndvi1".to_string(), ProcessNode {
            process_id: "ndvi".to_string(),
            arguments: serde_json::json!({"red": "B04", "nir": "B08"}),
            result: Some(true),
        });
        let graph = ProcessGraph { process_graph: nodes };
        let op = extract_operation(&graph);
        assert_eq!(op.operation, Some("ndvi".to_string()));
        assert_eq!(op.red, "B04");
        assert_eq!(op.nir, "B08");
    }

    #[test]
    fn extract_ndwi_operation() {
        let mut nodes = HashMap::new();
        nodes.insert("ndwi1".to_string(), ProcessNode {
            process_id: "ndwi".to_string(),
            arguments: serde_json::json!({"green": "B03", "nir": "B08"}),
            result: Some(true),
        });
        let graph = ProcessGraph { process_graph: nodes };
        let op = extract_operation(&graph);
        assert_eq!(op.operation, Some("ndwi".to_string()));
        assert_eq!(op.green, "B03");
    }

    #[test]
    fn extract_evi_operation() {
        let mut nodes = HashMap::new();
        nodes.insert("evi1".to_string(), ProcessNode {
            process_id: "evi".to_string(),
            arguments: serde_json::json!({"blue": "B02", "red": "B04", "nir": "B08"}),
            result: Some(true),
        });
        let graph = ProcessGraph { process_graph: nodes };
        let op = extract_operation(&graph);
        assert_eq!(op.operation, Some("evi".to_string()));
        assert_eq!(op.blue, "B02");
    }

    #[test]
    fn extract_defaults_when_no_operation() {
        let mut nodes = HashMap::new();
        nodes.insert("load1".to_string(), ProcessNode {
            process_id: "load_collection".to_string(),
            arguments: serde_json::json!({"id": "s2"}),
            result: Some(true),
        });
        let graph = ProcessGraph { process_graph: nodes };
        let op = extract_operation(&graph);
        assert_eq!(op.operation, None);
        assert_eq!(op.red, "B04");
        assert_eq!(op.nir, "B08");
    }

    // --- wrap_geotiff ---

    #[test]
    fn wrap_geotiff_produces_valid_tiff() {
        let meta = RasterMeta {
            width: 4, height: 4, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 4.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let pixels: Vec<u8> = (0..16).map(|i| i as f32)
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let tiff = wrap_geotiff(&pixels, &meta).expect("wrap_geotiff failed");
        assert!(tiff.len() > 100, "TIFF too small: {} bytes", tiff.len());
        // TIFF magic: II* (little-endian)
        assert_eq!(&tiff[..2], b"II", "not a TIFF header");
        assert_eq!(tiff[2], 42, "not a TIFF magic number");
    }

    #[test]
    fn wrap_geotiff_rejects_short_buffer() {
        let meta = RasterMeta {
            width: 4, height: 4, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 4.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let short = vec![0u8; 10];
        assert!(wrap_geotiff(&short, &meta).is_err());
    }

    // --- wrap_netcdf ---

    fn netcdf_open_options() -> DatasetOptions<'static> {
        DatasetOptions {
            open_flags: GdalOpenFlags::GDAL_OF_READONLY | GdalOpenFlags::GDAL_OF_RASTER,
            allowed_drivers: Some(&["netCDF"]),
            ..Default::default()
        }
    }

    fn try_open_netcdf_path(p: &Path) -> Option<Dataset> {
        Dataset::open_ex(p, netcdf_open_options())
            .ok()
            .or_else(|| Dataset::open(p).ok())
    }

    /// Open a netCDF written by GDAL: root may be a container with zero raster size; the
    /// actual 2D grid is often in `SUBDATASET_*_NAME` (typical on Windows netCDF driver).
    fn open_netcdf_raster_dataset(path: &Path) -> Dataset {
        let posix = path.to_string_lossy().replace('\\', "/");
        let mut attempts: Vec<PathBuf> = Vec::new();
        if cfg!(windows) {
            attempts.push(PathBuf::from(&posix));
        }
        attempts.push(path.to_path_buf());

        for p in attempts {
            let Some(ds) = try_open_netcdf_path(&p) else {
                continue;
            };
            let (w, h) = ds.raster_size();
            if w > 0 && h > 0 {
                return ds;
            }
            if let Some(items) = ds.metadata_domain("SUBDATASETS") {
                for item in items {
                    let Some((key, val)) = item.split_once('=') else {
                        continue;
                    };
                    if !key.ends_with("_NAME") && !key.ends_with("_name") {
                        continue;
                    }
                    // GDAL subdataset strings embed paths; normalize slashes for Windows.
                    let val = val.trim().replace('\\', "/");
                    if let Some(sub) = try_open_netcdf_path(Path::new(&val)) {
                        let (sw, sh) = sub.raster_size();
                        if sw > 0 && sh > 0 {
                            return sub;
                        }
                    }
                }
            }
        }
        panic!(
            "GDAL could not open netCDF as raster (tried path and SUBDATASETS): {}",
            path.display()
        );
    }

    fn assert_netcdf_bytes_readable_by_gdal(nc: &[u8], width: usize, height: usize) {
        assert!(nc.len() > 20, "netCDF output too small: {} bytes", nc.len());
        let tmp_path = std::env::temp_dir().join(format!("earthgrid_verify_nc_{}.nc", uuid::Uuid::new_v4()));
        std::fs::write(&tmp_path, nc).expect("write verify netCDF");
        let ds = open_netcdf_raster_dataset(&tmp_path);
        let (w, h) = ds.raster_size();
        assert_eq!(w as usize, width, "raster width");
        assert_eq!(h as usize, height, "raster height");
        drop(ds);
        let _ = std::fs::remove_file(&tmp_path);
    }

    #[test]
    fn wrap_netcdf_produces_valid_file() {
        let meta = RasterMeta {
            width: 4, height: 4, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 4.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let pixels: Vec<u8> = (0..16).map(|i| i as f32)
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let nc = wrap_netcdf(&pixels, &meta).expect("wrap_netcdf failed");
        assert_netcdf_bytes_readable_by_gdal(&nc, 4, 4);
    }

    #[test]
    fn wrap_netcdf_rejects_short_buffer() {
        let meta = RasterMeta {
            width: 4, height: 4, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 4.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let short = vec![0u8; 10];
        assert!(wrap_netcdf(&short, &meta).is_err());
    }

    // --- wrap_output dispatch ---

    #[test]
    fn wrap_output_dispatches_gtiff() {
        let meta = RasterMeta {
            width: 2, height: 2, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let pixels: Vec<u8> = [0.1f32, 0.2, 0.3, 0.4]
            .iter().flat_map(|v| v.to_le_bytes()).collect();
        let (bytes, ct) = wrap_output(&pixels, &meta, "GTiff").unwrap();
        assert_eq!(ct, "image/tiff");
        assert_eq!(&bytes[..2], b"II");
    }

    #[test]
    fn wrap_output_dispatches_netcdf() {
        let meta = RasterMeta {
            width: 2, height: 2, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let pixels: Vec<u8> = [0.1f32, 0.2, 0.3, 0.4]
            .iter().flat_map(|v| v.to_le_bytes()).collect();
        let (bytes, ct) = wrap_output(&pixels, &meta, "netCDF").unwrap();
        assert_eq!(ct, "application/x-netcdf");
        assert_netcdf_bytes_readable_by_gdal(&bytes, 2, 2);
    }

    #[test]
    fn wrap_output_rejects_geojson() {
        let meta = RasterMeta {
            width: 2, height: 2, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let pixels = vec![0u8; 16];
        let err = wrap_output(&pixels, &meta, "GeoJSON").unwrap_err();
        assert!(err.contains("not yet implemented"), "unexpected error: {err}");
    }

    #[test]
    fn wrap_output_rejects_geoparquet() {
        let meta = RasterMeta {
            width: 2, height: 2, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
            dtype: "float32".to_string(),
        };
        let pixels = vec![0u8; 16];
        let err = wrap_output(&pixels, &meta, "GeoParquet").unwrap_err();
        assert!(err.contains("not yet implemented"), "unexpected error: {err}");
    }

    // --- extract_band_from_item_id ---

    #[test]
    fn extract_band_from_sentinel2_ids() {
        assert_eq!(extract_band_from_item_id("S2A_32UMC_20250702_0_L2A_B04"), Some("B04".to_string()));
        assert_eq!(extract_band_from_item_id("S2A_32UMC_20250702_0_L2A_B08"), Some("B08".to_string()));
        assert_eq!(extract_band_from_item_id("S2A_32UMC_20250702_0_L2A_SCL"), Some("SCL".to_string()));
        assert_eq!(extract_band_from_item_id("S2A_32UMC_20250702_0_L2A_B8A"), Some("B8A".to_string()));
    }

    #[test]
    fn extract_band_from_common_name_suffix() {
        let result = extract_band_from_item_id("S2A_32UMC_20250702_0_L2A_red");
        assert!(result.is_some());
    }

    // --- truncate_pair / truncate_three ---

    #[test]
    fn truncate_pair_equal_lengths() {
        let a = vec![1u8, 2, 3, 4];
        let b = vec![5u8, 6, 7, 8];
        let (ra, rb) = truncate_pair("test", &a, &b).unwrap();
        assert_eq!(ra.len(), 4);
        assert_eq!(rb.len(), 4);
    }

    #[test]
    fn truncate_pair_different_lengths() {
        let a = vec![1u8, 2, 3, 4, 5, 6];
        let b = vec![7u8, 8, 9, 10];
        let (ra, rb) = truncate_pair("test", &a, &b).unwrap();
        assert_eq!(ra.len(), 4);
        assert_eq!(rb.len(), 4);
    }

    #[test]
    fn truncate_pair_rejects_empty() {
        let a = vec![];
        let b = vec![1u8];
        assert!(truncate_pair("test", &a, &b).is_err());
    }

    #[test]
    fn truncate_three_aligns_to_shortest() {
        let a = vec![1u8; 10];
        let b = vec![2u8; 6];
        let c = vec![3u8; 8];
        let (ra, rb, rc) = truncate_three("test", &a, &b, &c).unwrap();
        assert_eq!(ra.len(), 6);
        assert_eq!(rb.len(), 6);
        assert_eq!(rc.len(), 6);
    }
}

// ---------------------------------------------------------------------------
// Band extraction from item ID
// ---------------------------------------------------------------------------

/// Sentinel-2 band code ↔ common name mapping (bidirectional).
fn resolve_band_alias(band: &str) -> &str {
    match band {
        "B01" => "coastal", "coastal" => "B01",
        "B02" => "blue",    "blue"    => "B02",
        "B03" => "green",   "green"   => "B03",
        "B04" => "red",     "red"     => "B04",
        "B05" => "rededge1","rededge1"=> "B05",
        "B06" => "rededge2","rededge2"=> "B06",
        "B07" => "rededge3","rededge3"=> "B07",
        "B08" => "nir",     "nir"     => "B08",
        "B8A" => "nir08",   "nir08"   => "B8A",
        "B09" => "nir09",   "nir09"   => "B09",
        "B11" => "swir16",  "swir16"  => "B11",
        "B12" => "swir22",  "swir22"  => "B12",
        "SCL" => "scl",     "scl"     => "SCL",
        _ => band,
    }
}

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
) -> Result<(Vec<u8>, Option<RasterMeta>), String> {
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

    fn is_tiff(data: &[u8]) -> bool {
        data.len() >= 4
            && ((data[0] == b'I' && data[1] == b'I' && data[2] == 42 && data[3] == 0)
                || (data[0] == b'M' && data[1] == b'M' && data[2] == 0 && data[3] == 42))
    }

    /// Read a TIFF blob via GDAL → (f32 pixels, RasterMeta).
    fn decode_tiff(raw: &[u8]) -> Result<(Vec<u8>, RasterMeta), String> {
        let tmp = std::env::temp_dir().join(format!("eg_band_{}.tif", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, raw).map_err(|e| format!("write tmp tiff: {e}"))?;
        let ds = gdal::Dataset::open(&tmp).map_err(|e| format!("GDAL open tmp: {e}"))?;
        let (w, h) = ds.raster_size();
        let gt = ds.geo_transform().map_err(|e| format!("GDAL geotransform: {e}"))?;
        let crs = ds.spatial_ref().ok()
            .map(|s| format!("EPSG:{}", s.auth_code().unwrap_or(4326)))
            .unwrap_or_else(|| "EPSG:4326".to_string());
        let band = ds.rasterband(1).map_err(|e| format!("GDAL band 1: {e}"))?;
        let buf = band.read_as::<f32>((0, 0), (w, h), (w, h), None)
            .map_err(|e| format!("GDAL read: {e}"))?;
        let pixels: Vec<u8> = buf.data().iter().flat_map(|v| v.to_le_bytes()).collect();
        drop(band);
        drop(ds);
        let _ = std::fs::remove_file(&tmp);
        let meta = RasterMeta {
            width: w, height: h, crs,
            transform: [gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]],
            dtype: "float32".to_string(),
        };
        Ok((pixels, meta))
    }

    fn meta_from_item(item: &StacItem) -> Option<RasterMeta> {
        let p = &item.properties;
        let width = p.get("earthgrid:width")?.as_u64()? as usize;
        let height = p.get("earthgrid:height")?.as_u64()? as usize;
        let crs = p.get("earthgrid:crs")?.as_str()?.to_string();
        let dtype = p.get("earthgrid:dtype").and_then(|v| v.as_str()).unwrap_or("float32").to_string();
        let tf = p.get("earthgrid:transform")?.as_array()?;
        if tf.len() < 6 { return None; }
        let transform = [
            tf[0].as_f64()?, tf[1].as_f64()?, tf[2].as_f64()?,
            tf[3].as_f64()?, tf[4].as_f64()?, tf[5].as_f64()?,
        ];
        Some(RasterMeta { width, height, crs, transform, dtype })
    }

    // Load a band: reassemble chunks, decode TIFF if applicable, return (f32 pixels, meta).
    let load_band = |band: &str, store_locked: &mut ChunkStore| -> Result<(Vec<u8>, Option<RasterMeta>), String> {
        let resolved = band_items.get(band)
            .or_else(|| band_items.get(resolve_band_alias(band)));
        let items = resolved.ok_or_else(|| {
            format!("Band '{}' not found. Needed: {:?}. Available: {:?}. Items checked: {}.",
                band, needed_bands, avail, items.len())
        })?;
        let item = items
            .iter()
            .max_by_key(|it| it.chunk_hashes.len())
            .copied()
            .ok_or_else(|| format!("No items for band '{}'", band))?;
        let mut raw = Vec::new();
        for hash in &item.chunk_hashes {
            if let Ok(Some(chunk)) = store_locked.get(hash) {
                raw.extend_from_slice(&chunk);
            }
        }
        if raw.is_empty() {
            return Err(format!("Could not load chunk data for band '{}'", band));
        }
        if is_tiff(&raw) {
            let (pixels, meta) = decode_tiff(&raw)?;
            Ok((pixels, Some(meta)))
        } else {
            let meta = meta_from_item(item);
            Ok((raw, meta))
        }
    };

    match op.operation.as_deref() {
        Some("ndvi") => {
            let mut store = store.lock().await;
            let (red_data, meta) = load_band(&op.red, &mut store)?;
            let (nir_data, _) = load_band(&op.nir, &mut store)?;
            let (red_data, nir_data) = truncate_pair("NDVI", &red_data, &nir_data)?;
            let dtype = detect_spectral_dtype(red_data);
            Ok((processing::compute_ndvi(red_data, nir_data, dtype), meta))
        }
        Some("ndwi") => {
            let mut store = store.lock().await;
            let (green_data, meta) = load_band(&op.green, &mut store)?;
            let (nir_data, _) = load_band(&op.nir, &mut store)?;
            let (green_data, nir_data) = truncate_pair("NDWI", &green_data, &nir_data)?;
            let dtype = detect_spectral_dtype(green_data);
            Ok((processing::compute_ndwi(green_data, nir_data, dtype), meta))
        }
        Some("evi") => {
            let mut store = store.lock().await;
            let (blue_data, _) = load_band(&op.blue, &mut store)?;
            let (red_data, meta) = load_band(&op.red, &mut store)?;
            let (nir_data, _) = load_band(&op.nir, &mut store)?;
            let (blue_data, red_data, nir_data) = truncate_three("EVI", &blue_data, &red_data, &nir_data)?;
            let dtype = detect_spectral_dtype(red_data);
            Ok((processing::compute_evi(blue_data, red_data, nir_data, dtype), meta))
        }
        Some("cloud_mask") => {
            let mut store = store.lock().await;
            let (scl_data, meta) = load_band(&op.scl, &mut store)?;
            Ok((processing::cloud_mask(&scl_data), meta))
        }
        _ => {
            let first = &items[0];
            let mut store = store.lock().await;
            let mut raw = Vec::new();
            for hash in &first.chunk_hashes {
                if let Ok(Some(chunk)) = store.get(hash) {
                    raw.extend_from_slice(&chunk);
                }
            }
            if raw.is_empty() {
                return Err("No chunk data found".to_string());
            }
            let meta = meta_from_item(first);
            Ok((raw, meta))
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
            Json(serde_json::json!({"code": "AuthenticationRequired", "message": "API key required"})),
        ).into_response();
    }

    match execute_sync(&graph, &state.app.catalog, &state.app.store).await {
        Ok((data, meta)) => {
            let fmt = extract_output_format(&graph).unwrap_or_else(|| "GTiff".to_string());
            if let Some(ref m) = meta {
                match wrap_output(&data, m, &fmt) {
                    Ok((output, ct)) => (
                        StatusCode::OK,
                        [(axum::http::header::CONTENT_TYPE, ct)],
                        output,
                    ).into_response(),
                    Err(e) => (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({"code": "FormatError", "message": e})),
                    ).into_response(),
                }
            } else {
                (
                    StatusCode::OK,
                    [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                    data,
                ).into_response()
            }
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"code": "ProcessingError", "message": e})),
        ).into_response(),
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
            Ok((data, _meta)) => {
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
