use chrono::Datelike;
use gdal::Dataset;
use gdal::DatasetOptions;
use gdal::DriverManager;
use gdal::GdalOpenFlags;
use gdal::Metadata;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use super::graph::{
    detect_spectral_dtype, extract_aggregate_temporal_period, extract_band_from_item_id,
    extract_operation, extract_output_format, extract_resample_spatial, extract_ymd_from_item_id,
    resolve_band_alias, truncate_pair, truncate_three,
};
use super::output::{
    canonical_format, decode_geotiff_bsq_f32, wrap_geotiff, wrap_netcdf, wrap_output,
};
use super::types::{ProcessGraph, ProcessNode, RasterMeta};
use super::geoprocess;

fn gdal_netcdf_driver_available() -> bool {
    DriverManager::get_driver_by_name("netCDF").is_ok()
}

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

#[test]
fn extract_aggregate_temporal_period_reads_period_and_reducer() {
    let mut nodes = HashMap::new();
    nodes.insert("agg1".to_string(), ProcessNode {
        process_id: "aggregate_temporal_period".to_string(),
        arguments: serde_json::json!({
            "period": "month",
            "reducer": {"process_graph": {"m": {"process_id": "mean", "result": true}}}
        }),
        result: Some(false),
    });
    let graph = ProcessGraph { process_graph: nodes };
    let cfg = extract_aggregate_temporal_period(&graph).expect("cfg");
    assert_eq!(cfg.period, "month");
        assert_eq!(cfg.reducer, geoprocess::ReducerKind::Mean);
}

#[test]
fn extract_resample_spatial_epsg_and_tr() {
    let mut nodes = HashMap::new();
    nodes.insert("rs1".to_string(), ProcessNode {
        process_id: "resample_spatial".to_string(),
        arguments: serde_json::json!({
            "resolution": [30.0, 30.0],
            "projection": 3857,
            "method": "bilinear"
        }),
        result: Some(false),
    });
    let graph = ProcessGraph { process_graph: nodes };
    let cfg = extract_resample_spatial(&graph).expect("cfg");
    assert!(cfg.change_resolution);
    assert_eq!(cfg.res_x, 30.0);
    assert_eq!(cfg.epsg, Some(3857));
    assert_eq!(cfg.method, "bilinear");
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
        band_count: 1,
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
        band_count: 1,
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
    if gdal_netcdf_driver_available() {
        let meta = RasterMeta {
            width: 4, height: 4, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 4.0, 0.0, -1.0],
            dtype: "float32".to_string(),
            band_count: 1,
        };
        let pixels: Vec<u8> = (0..16).map(|i| i as f32)
            .flat_map(|v| v.to_le_bytes())
            .collect();
        let nc = wrap_netcdf(&pixels, &meta).expect("wrap_netcdf failed");
        assert_netcdf_bytes_readable_by_gdal(&nc, 4, 4);
    }
}

#[test]
fn wrap_netcdf_rejects_short_buffer() {
    let meta = RasterMeta {
        width: 4, height: 4, crs: "EPSG:4326".to_string(),
        transform: [0.0, 1.0, 0.0, 4.0, 0.0, -1.0],
        dtype: "float32".to_string(),
        band_count: 1,
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
        band_count: 1,
    };
    let pixels: Vec<u8> = [0.1f32, 0.2, 0.3, 0.4]
        .iter().flat_map(|v| v.to_le_bytes()).collect();
    let (bytes, ct) = wrap_output(&pixels, &meta, "GTiff").unwrap();
    assert_eq!(ct, "image/tiff");
    assert_eq!(&bytes[..2], b"II");
}

#[test]
fn wrap_output_dispatches_netcdf() {
    if gdal_netcdf_driver_available() {
        let meta = RasterMeta {
            width: 2, height: 2, crs: "EPSG:4326".to_string(),
            transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
            dtype: "float32".to_string(),
            band_count: 1,
        };
        let pixels: Vec<u8> = [0.1f32, 0.2, 0.3, 0.4]
            .iter().flat_map(|v| v.to_le_bytes()).collect();
        let (bytes, ct) = wrap_output(&pixels, &meta, "netCDF").unwrap();
        assert_eq!(ct, "application/x-netcdf");
        assert_netcdf_bytes_readable_by_gdal(&bytes, 2, 2);
    }
}

#[test]
fn wrap_output_rejects_geojson() {
    let meta = RasterMeta {
        width: 2, height: 2, crs: "EPSG:4326".to_string(),
        transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
        dtype: "float32".to_string(),
        band_count: 1,
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
        band_count: 1,
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

#[test]
fn extract_ymd_from_sentinel2_id() {
    let d = extract_ymd_from_item_id("S2A_32UMC_20250702_0_L2A_B04").expect("date");
    assert_eq!(d.year(), 2025);
    assert_eq!(d.month(), 7);
    assert_eq!(d.day(), 2);
}

#[test]
fn wrap_geotiff_two_bands_roundtrip_decode() {
    let meta = RasterMeta {
        width: 2,
        height: 2,
        crs: "EPSG:4326".to_string(),
        transform: [0.0, 1.0, 0.0, 2.0, 0.0, -1.0],
        dtype: "float32".to_string(),
        band_count: 2,
    };
    let b1: Vec<u8> = (0..4)
        .flat_map(|i| (i as f32).to_le_bytes())
        .collect();
    let b2: Vec<u8> = (10..14)
        .flat_map(|i| (i as f32).to_le_bytes())
        .collect();
    let mut pixels = b1;
    pixels.extend(b2);
    let tiff = wrap_geotiff(&pixels, &meta).expect("wrap");
    let (out, m2) = decode_geotiff_bsq_f32(&tiff).expect("decode");
    assert_eq!(m2.band_count, 2);
    assert_eq!(out.len(), pixels.len());
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
