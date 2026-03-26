//! Ingest GeoTIFF/COG files into EarthGrid — spatial tiling via GDAL.
//!
//! Downloads or local files → read with GDAL → spatial tile → chunk → store.
//! Metadata (CRS, bbox, transform, bands) extracted from the raster.

use std::fs;
use std::io::Read;
use std::path::Path;

use sha2::{Digest, Sha256};

use crate::catalog::StacItem;
use crate::chunk_store::ChunkStore;
use crate::error::Result;

/// Default spatial tile size (pixels per side).
pub const DEFAULT_TILE_SIZE: usize = 512;

/// Default chunk size for non-raster files: 4 MB.
pub const DEFAULT_CHUNK_SIZE: usize = 4 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Raster ingest via GDAL (spatial tiling)
// ---------------------------------------------------------------------------

/// Ingest a GeoTIFF/COG: read with GDAL, split into spatial tiles.
///
/// Each tile contains all bands at that spatial position.
/// Chunk layout: (n_bands, tile_h, tile_w) as raw bytes, row-major.
///
/// Automatically reprojects bbox to WGS84 for STAC compliance.
pub fn ingest_raster(
    path: &Path,
    collection: &str,
    item_id: Option<&str>,
    tile_size: usize,
    store: &mut ChunkStore,
) -> Result<StacItem> {
    let tile_size = if tile_size == 0 { DEFAULT_TILE_SIZE } else { tile_size };

    let ds = gdal::Dataset::open(path)
        .map_err(|e| crate::error::EarthGridError::Other(
            format!("GDAL open '{}': {}", path.display(), e),
        ))?;

    let (width, height) = ds.raster_size();
    let n_bands = ds.raster_count();
    let geotransform = ds.geo_transform()
        .map_err(|e| crate::error::EarthGridError::Other(
            format!("GDAL geotransform: {}", e),
        ))?;

    // Get CRS
    let _crs_str = ds.spatial_ref()
        .ok()
        .and_then(|srs| srs.to_proj4().ok())
        .unwrap_or_else(|| "EPSG:4326".to_string());

    let epsg = ds.spatial_ref()
        .ok()
        .and_then(|srs| srs.auth_code().ok())
        .unwrap_or(4326);

    let crs = format!("EPSG:{}", epsg);

    // Compute native bounds
    let x_min = geotransform[0];
    let y_max = geotransform[3];
    let x_max = x_min + geotransform[1] * width as f64;
    let y_min = y_max + geotransform[5] * height as f64;

    // Reproject bbox to WGS84 if needed
    let bbox = if epsg != 4326 && epsg != 0 {
        reproject_bbox(x_min, y_min, x_max, y_max, epsg)
            .unwrap_or([x_min, y_min, x_max, y_max])
    } else {
        [x_min, y_min, x_max, y_max]
    };

    // Detect dtype from first band
    let dtype_str = {
        let rb = ds.rasterband(1)
            .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL band 1: {}", e)))?;
        match rb.band_type() {
            gdal::raster::GdalDataType::UInt8 => "uint8",
            gdal::raster::GdalDataType::UInt16 => "uint16",
            gdal::raster::GdalDataType::Int16 => "int16",
            gdal::raster::GdalDataType::UInt32 => "uint32",
            gdal::raster::GdalDataType::Int32 => "int32",
            gdal::raster::GdalDataType::Float32 => "float32",
            gdal::raster::GdalDataType::Float64 => "float64",
            _ => "uint16",
        }
        .to_string()
    };

    let bpp = dtype_size(&dtype_str);

    // Detect band names from item_id
    let stem = item_id.unwrap_or_else(|| {
        path.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("unknown")
    });
    let band_names = detect_band_names(stem, n_bands);

    // Spatial tiling
    let n_cols = (width + tile_size - 1) / tile_size;
    let n_rows = (height + tile_size - 1) / tile_size;

    let mut chunk_hashes: Vec<String> = Vec::new();

    for row_i in 0..n_rows {
        for col_i in 0..n_cols {
            let x_off = col_i * tile_size;
            let y_off = row_i * tile_size;
            let w = tile_size.min(width - x_off);
            let h = tile_size.min(height - y_off);
            let tile_pixels = w * h;

            // Read all bands at this spatial position
            let mut tile_data = Vec::with_capacity(n_bands * tile_pixels * bpp);

            for band_idx in 1..=n_bands {
                let rb = ds.rasterband(band_idx)
                    .map_err(|e| crate::error::EarthGridError::Other(
                        format!("GDAL band {}: {}", band_idx, e),
                    ))?;

                // Read as raw bytes depending on dtype
                let band_bytes = read_band_window(&rb, x_off, y_off, w, h, &dtype_str)?;
                tile_data.extend_from_slice(&band_bytes);
            }

            let sha = store.put(&tile_data)?;
            chunk_hashes.push(sha);
        }
    }

    // Parse acquisition date from item_id
    let acq_date = parse_date_from_id(stem);

    let now = chrono::Utc::now();
    let now_str = now.to_rfc3339();
    let now_ts = now.timestamp() as f64;

    let item = StacItem {
        id: stem.to_string(),
        collection: collection.to_string(),
        bbox,
        geometry: None,
        properties: serde_json::json!({
            "datetime": acq_date.as_deref().unwrap_or(&now_str),
            "earthgrid:ingested": now_str,
            "earthgrid:crs": crs,
            "earthgrid:width": width,
            "earthgrid:height": height,
            "earthgrid:bands": n_bands,
            "earthgrid:band_names": band_names,
            "earthgrid:dtype": dtype_str,
            "earthgrid:tile_size": tile_size,
            "earthgrid:tile_cols": n_cols,
            "earthgrid:tile_rows": n_rows,
            "earthgrid:source_file": path.file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
            "earthgrid:chunk_format": "spatial-tile",
            "earthgrid:transform": [
                geotransform[0], geotransform[1], geotransform[2],
                geotransform[3], geotransform[4], geotransform[5],
            ],
        }),
        chunk_hashes,
        created_at: now_ts,
    };

    Ok(item)
}

// ---------------------------------------------------------------------------
// Band reading helpers
// ---------------------------------------------------------------------------

fn read_band_window(
    rb: &gdal::raster::RasterBand,
    x_off: usize,
    y_off: usize,
    width: usize,
    height: usize,
    dtype: &str,
) -> Result<Vec<u8>> {
    let offset = (x_off as isize, y_off as isize);
    let size = (width, height);

    match dtype {
        "uint8" | "u8" => {
            let buf = rb.read_as::<u8>(offset, size, size, None)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL read: {}", e)))?;
            Ok(buf.data().to_vec())
        }
        "uint16" | "u16" => {
            let buf = rb.read_as::<u16>(offset, size, size, None)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL read: {}", e)))?;
            Ok(buf.data().iter().flat_map(|v| v.to_le_bytes()).collect())
        }
        "int16" | "i16" => {
            let buf = rb.read_as::<i16>(offset, size, size, None)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL read: {}", e)))?;
            Ok(buf.data().iter().flat_map(|v| v.to_le_bytes()).collect())
        }
        "float32" | "f32" => {
            let buf = rb.read_as::<f32>(offset, size, size, None)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL read: {}", e)))?;
            Ok(buf.data().iter().flat_map(|v| v.to_le_bytes()).collect())
        }
        "float64" | "f64" => {
            let buf = rb.read_as::<f64>(offset, size, size, None)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL read: {}", e)))?;
            Ok(buf.data().iter().flat_map(|v| v.to_le_bytes()).collect())
        }
        _ => {
            // Default to uint16
            let buf = rb.read_as::<u16>(offset, size, size, None)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL read: {}", e)))?;
            Ok(buf.data().iter().flat_map(|v| v.to_le_bytes()).collect())
        }
    }
}

// ---------------------------------------------------------------------------
// Simple file ingest (non-raster, raw chunking)
// ---------------------------------------------------------------------------

/// Ingest a non-raster file by splitting into fixed-size chunks.
pub fn ingest_file(
    path: &Path,
    collection: &str,
    chunk_size: usize,
    store: &mut ChunkStore,
) -> Result<StacItem> {
    let chunk_size = if chunk_size == 0 { DEFAULT_CHUNK_SIZE } else { chunk_size };

    let metadata = fs::metadata(path)?;
    let file_size = metadata.len();
    let filename = path
        .file_name()
        .map(|n| n.to_string_lossy().to_string())
        .unwrap_or_else(|| "unknown".to_string());

    let mut file = fs::File::open(path)?;
    let mut chunk_hashes: Vec<String> = Vec::new();
    let mut file_hasher = Sha256::new();
    let mut buf = vec![0u8; chunk_size];

    loop {
        let mut bytes_read = 0;
        while bytes_read < chunk_size {
            match file.read(&mut buf[bytes_read..]) {
                Ok(0) => break,
                Ok(n) => bytes_read += n,
                Err(e) => return Err(e.into()),
            }
        }
        if bytes_read == 0 {
            break;
        }
        let chunk_data = &buf[..bytes_read];
        file_hasher.update(chunk_data);
        let hash = store.put(chunk_data)?;
        chunk_hashes.push(hash);
    }

    let file_hash = hex::encode(file_hasher.finalize());
    let short_hash = &file_hash[..12];
    let stem = path
        .file_stem()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".to_string());
    let item_id = format!("{}_{}", stem, short_hash);
    let now_str = chrono::Utc::now().to_rfc3339();
    let now_ts = chrono::Utc::now().timestamp() as f64;

    Ok(StacItem {
        id: item_id,
        collection: collection.to_string(),
        bbox: [0.0, 0.0, 0.0, 0.0],
        geometry: None,
        properties: serde_json::json!({
            "earthgrid:filename": filename,
            "earthgrid:file_size": file_size,
            "earthgrid:file_hash": file_hash,
            "earthgrid:chunk_count": chunk_hashes.len(),
            "earthgrid:chunk_size": chunk_size,
            "datetime": &now_str,
        }),
        chunk_hashes,
        created_at: now_ts,
    })
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn reproject_bbox(x_min: f64, y_min: f64, x_max: f64, y_max: f64, src_epsg: i32) -> Option<[f64; 4]> {
    use gdal::spatial_ref::{CoordTransform, SpatialRef};

    let src = SpatialRef::from_epsg(src_epsg as u32).ok()?;
    let dst = SpatialRef::from_epsg(4326).ok()?;
    let ct = CoordTransform::new(&src, &dst).ok()?;

    // Transform corners
    let mut xs = [x_min, x_max, x_min, x_max];
    let mut ys = [y_min, y_min, y_max, y_max];
    let mut zs = [0.0f64; 4];
    ct.transform_coords(&mut xs, &mut ys, &mut zs).ok()?;

    let w = xs.iter().cloned().reduce(f64::min)?;
    let e = xs.iter().cloned().reduce(f64::max)?;
    let s = ys.iter().cloned().reduce(f64::min)?;
    let n = ys.iter().cloned().reduce(f64::max)?;

    Some([
        (w * 1e6).round() / 1e6,
        (s * 1e6).round() / 1e6,
        (e * 1e6).round() / 1e6,
        (n * 1e6).round() / 1e6,
    ])
}

fn detect_band_names(item_id: &str, n_bands: usize) -> Vec<String> {
    let upper = item_id.to_uppercase();
    let s2_bands = ["B02", "B03", "B04", "B05", "B06", "B07",
                    "B08", "B8A", "B09", "B11", "B12", "SCL", "TCI"];

    for band in &s2_bands {
        if upper.contains(band) && n_bands == 1 {
            return vec![band.to_string()];
        }
    }
    if upper.contains("TCI") && n_bands == 3 {
        return vec!["B04".to_string(), "B03".to_string(), "B02".to_string()];
    }
    (0..n_bands).map(|i| format!("B{:02}", i + 1)).collect()
}

fn parse_date_from_id(item_id: &str) -> Option<String> {
    // Find 8-digit date in item ID: S2A_33UUB_20250629_...
    for part in item_id.split('_') {
        if part.len() == 8 && part.chars().all(|c| c.is_ascii_digit()) {
            let y = &part[..4];
            let m = &part[4..6];
            let d = &part[6..8];
            return Some(format!("{}-{}-{}T00:00:00Z", y, m, d));
        }
    }
    None
}

fn dtype_size(dtype: &str) -> usize {
    match dtype {
        "uint8" | "u8" => 1,
        "uint16" | "u16" | "int16" | "i16" => 2,
        "uint32" | "u32" | "int32" | "i32" | "float32" | "f32" => 4,
        "float64" | "f64" => 8,
        _ => 2,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn setup() -> (ChunkStore, TempDir) {
        let dir = TempDir::new().unwrap();
        let store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();
        (store, dir)
    }

    #[test]
    fn test_ingest_small_file() {
        let (mut store, dir) = setup();
        let test_file = dir.path().join("test.tif");
        fs::write(&test_file, b"Hello EarthGrid!").unwrap();
        let item = ingest_file(&test_file, "test-collection", DEFAULT_CHUNK_SIZE, &mut store).unwrap();
        assert!(item.id.starts_with("test_"));
        assert_eq!(item.collection, "test-collection");
        assert_eq!(item.chunk_hashes.len(), 1);
    }

    #[test]
    fn test_detect_band_names() {
        assert_eq!(detect_band_names("S2A_33UUB_20250629_0_L2A_B04", 1), vec!["B04"]);
        assert_eq!(detect_band_names("S2A_33UUB_20250629_0_L2A_TCI", 3),
                   vec!["B04", "B03", "B02"]);
        assert_eq!(detect_band_names("random_file", 4),
                   vec!["B01", "B02", "B03", "B04"]);
    }

    #[test]
    fn test_parse_date_from_id() {
        assert_eq!(parse_date_from_id("S2A_33UUB_20250629_0_L2A_B04"),
                   Some("2025-06-29T00:00:00Z".to_string()));
        assert_eq!(parse_date_from_id("random_file"), None);
    }
}
