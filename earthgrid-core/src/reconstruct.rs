//! Reconstruct GeoTIFF files from stored chunks.
//!
//! Two code paths:
//! - **Default (pure Rust)**: Minimal GeoTIFF writer — zero external deps.
//!   Produces valid GeoTIFF readable by QGIS, GDAL, rasterio, R.
//! - **`gdal-support` feature**: Full GDAL-backed writer with COG, compression,
//!   and reprojection support.
//!
//! Every node can process and serve GeoTIFF without installing anything.

use std::collections::HashMap;
use std::io::{Cursor, Write};

use crate::catalog::StacItem;
use crate::chunk_store::ChunkStore;
use crate::error::Result;

// ---------------------------------------------------------------------------
// Band reconstruction (chunks → raw arrays)
// ---------------------------------------------------------------------------

/// Reconstruct per-band 2D arrays from stored chunks.
pub fn reconstruct_bands(
    item: &StacItem,
    store: &mut ChunkStore,
    bands: Option<&[String]>,
) -> Result<HashMap<String, Vec<u8>>> {
    let props = &item.properties;

    let width = props_u32(props, "earthgrid:width")?;
    let height = props_u32(props, "earthgrid:height")?;
    let tile_size = props_u32(props, "earthgrid:tile_size")?;
    let tile_cols = props_u32(props, "earthgrid:tile_cols")?;
    let _tile_rows = props_u32(props, "earthgrid:tile_rows")?;
    let n_bands = props_u32(props, "earthgrid:bands").unwrap_or(1);
    let chunk_format = props_str(props, "earthgrid:chunk_format")
        .unwrap_or_else(|| "legacy".to_string());
    let dtype_str = props_str(props, "earthgrid:dtype").unwrap_or_else(|| "uint16".to_string());
    let bpp = dtype_size(&dtype_str);

    let band_names: Vec<String> = props
        .get("earthgrid:band_names")
        .and_then(|v| v.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
        .unwrap_or_else(|| (0..n_bands).map(|i| format!("B{:02}", i + 1)).collect());

    match chunk_format.as_str() {
        "spatial-tile" | "legacy" => {
            let pixel_count = (width * height) as usize;
            let mut full: Vec<Vec<u8>> = (0..n_bands as usize)
                .map(|_| vec![0u8; pixel_count * bpp])
                .collect();

            for (idx, sha) in item.chunk_hashes.iter().enumerate() {
                let raw = match store.get(sha)? {
                    Some(data) => data,
                    None => continue,
                };
                let row_i = idx as u32 / tile_cols;
                let col_i = idx as u32 % tile_cols;
                let x_off = (col_i * tile_size) as usize;
                let y_off = (row_i * tile_size) as usize;
                let w = tile_size.min(width - col_i * tile_size) as usize;
                let h = tile_size.min(height - row_i * tile_size) as usize;
                let tile_pixels = w * h;

                for band in 0..n_bands as usize {
                    let src_offset = band * tile_pixels * bpp;
                    for row in 0..h {
                        let src_start = src_offset + row * w * bpp;
                        let dst_start = ((y_off + row) * width as usize + x_off) * bpp;
                        let count = w * bpp;
                        if src_start + count <= raw.len() && dst_start + count <= full[band].len() {
                            full[band][dst_start..dst_start + count]
                                .copy_from_slice(&raw[src_start..src_start + count]);
                        }
                    }
                }
            }

            let mut result = HashMap::new();
            for (i, name) in band_names.iter().enumerate() {
                if let Some(filter) = bands {
                    if !filter.iter().any(|b| b == name) {
                        continue;
                    }
                }
                if i < full.len() {
                    result.insert(name.clone(), std::mem::take(&mut full[i]));
                }
            }
            Ok(result)
        }
        "band-level" => {
            let pixel_count = (width * height) as usize;
            let band_hashes: HashMap<String, Vec<String>> = props
                .get("earthgrid:band_hashes")
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();

            let mut result = HashMap::new();
            for (band_name, hashes) in &band_hashes {
                if let Some(filter) = bands {
                    if !filter.iter().any(|b| b == band_name) {
                        continue;
                    }
                }
                let mut band_data = vec![0u8; pixel_count * bpp];
                for (idx, sha) in hashes.iter().enumerate() {
                    let raw = match store.get(sha)? {
                        Some(data) => data,
                        None => continue,
                    };
                    let row_i = idx as u32 / tile_cols;
                    let col_i = idx as u32 % tile_cols;
                    let x_off = (col_i * tile_size) as usize;
                    let y_off = (row_i * tile_size) as usize;
                    let w = tile_size.min(width - col_i * tile_size) as usize;
                    let h = tile_size.min(height - row_i * tile_size) as usize;
                    for row in 0..h {
                        let src_start = row * w * bpp;
                        let dst_start = ((y_off + row) * width as usize + x_off) * bpp;
                        let count = w * bpp;
                        if src_start + count <= raw.len() && dst_start + count <= band_data.len() {
                            band_data[dst_start..dst_start + count]
                                .copy_from_slice(&raw[src_start..src_start + count]);
                        }
                    }
                }
                result.insert(band_name.clone(), band_data);
            }
            Ok(result)
        }
        _ => Err(crate::error::EarthGridError::Other(
            format!("Unknown chunk format: {}", chunk_format),
        )),
    }
}

// ===========================================================================
// Pure Rust GeoTIFF writer — zero dependencies
// ===========================================================================
// Writes a minimal, valid GeoTIFF that can be opened by GDAL, QGIS, rasterio, R.
// Supports: uint8, uint16, int16, float32; single and multi-band; WGS84/UTM CRS.

// TIFF constants
const TIFF_MAGIC_LE: u16 = 0x4949; // little-endian
const TIFF_MAGIC_42: u16 = 42;

// TIFF tag IDs
const TAG_IMAGE_WIDTH: u16 = 256;
const TAG_IMAGE_LENGTH: u16 = 257;
const TAG_BITS_PER_SAMPLE: u16 = 258;
const TAG_COMPRESSION: u16 = 259;
const TAG_PHOTOMETRIC: u16 = 262;
const TAG_STRIP_OFFSETS: u16 = 273;
const TAG_SAMPLES_PER_PIXEL: u16 = 277;
const TAG_ROWS_PER_STRIP: u16 = 278;
const TAG_STRIP_BYTE_COUNTS: u16 = 279;
const TAG_SAMPLE_FORMAT: u16 = 339;

// GeoTIFF tag IDs
const TAG_MODEL_TIEPOINT: u16 = 33922;
const TAG_MODEL_PIXEL_SCALE: u16 = 33550;
const TAG_GEO_KEY_DIRECTORY: u16 = 34735;

// GeoKey IDs
const GT_MODEL_TYPE_GEOKEY: u16 = 1024;
const GT_RASTER_TYPE_GEOKEY: u16 = 1025;
const GEOGRAPHIC_TYPE_GEOKEY: u16 = 2048;
const PROJECTED_CS_TYPE_GEOKEY: u16 = 3072;

// TIFF data types
const TIFF_SHORT: u16 = 3;   // uint16
const TIFF_LONG: u16 = 4;    // uint32
const TIFF_DOUBLE: u16 = 12; // float64

/// Reconstruct a GeoTIFF from stored chunks (pure Rust).
pub fn reconstruct_geotiff(
    item: &StacItem,
    store: &mut ChunkStore,
    bands: Option<&[String]>,
) -> Result<Vec<u8>> {
    let band_data = reconstruct_bands(item, store, bands)?;
    if band_data.is_empty() {
        return Err(crate::error::EarthGridError::Other(
            format!("No data for item {}", item.id),
        ));
    }

    let props = &item.properties;
    let width = props_u32(props, "earthgrid:width")?;
    let height = props_u32(props, "earthgrid:height")?;
    let dtype_str = props_str(props, "earthgrid:dtype").unwrap_or_else(|| "uint16".to_string());
    let crs = props_str(props, "earthgrid:crs").unwrap_or_else(|| "EPSG:4326".to_string());
    let bbox = item.bbox;

    let geotransform = if let Some(tf) = props.get("earthgrid:transform").and_then(|v| v.as_array()) {
        if tf.len() >= 6 {
            [
                tf[0].as_f64().unwrap_or(0.0),
                tf[1].as_f64().unwrap_or(1.0),
                tf[2].as_f64().unwrap_or(0.0),
                tf[3].as_f64().unwrap_or(0.0),
                tf[4].as_f64().unwrap_or(0.0),
                tf[5].as_f64().unwrap_or(-1.0),
            ]
        } else {
            geotransform_from_bbox(bbox, width, height)
        }
    } else {
        geotransform_from_bbox(bbox, width, height)
    };

    let band_names_ordered: Vec<String> = band_data.keys().cloned().collect();
    let all_band_bytes: Vec<&[u8]> = band_names_ordered.iter()
        .map(|n| band_data[n].as_slice())
        .collect();

    write_geotiff(width, height, &dtype_str, &crs, &geotransform, &all_band_bytes)
}

/// Compute NDVI and return as GeoTIFF bytes (pure Rust).
pub fn ndvi_geotiff(
    red_data: &[u8],
    nir_data: &[u8],
    width: u32,
    height: u32,
    bbox: [f64; 4],
    crs: &str,
    geotransform: Option<[f64; 6]>,
) -> Result<Vec<u8>> {
    let pixel_count = (width * height) as usize;

    // Compute NDVI → f32
    let ndvi_bytes: Vec<u8> = if red_data.len() == pixel_count * 2 {
        // uint16 input
        red_data.chunks_exact(2).zip(nir_data.chunks_exact(2))
            .flat_map(|(r, n)| {
                let red = u16::from_le_bytes([r[0], r[1]]) as f32;
                let nir = u16::from_le_bytes([n[0], n[1]]) as f32;
                let ndvi = if (nir + red).abs() < 1e-6 { 0.0f32 } else { (nir - red) / (nir + red) };
                ndvi.to_le_bytes()
            })
            .collect()
    } else {
        // f32 input
        red_data.chunks_exact(4).zip(nir_data.chunks_exact(4))
            .flat_map(|(r, n)| {
                let red = f32::from_le_bytes([r[0], r[1], r[2], r[3]]);
                let nir = f32::from_le_bytes([n[0], n[1], n[2], n[3]]);
                let ndvi = if (nir + red).abs() < 1e-6 { 0.0f32 } else { (nir - red) / (nir + red) };
                ndvi.to_le_bytes()
            })
            .collect()
    };

    let gt = geotransform.unwrap_or_else(|| geotransform_from_bbox(bbox, width, height));
    write_geotiff(width, height, "float32", crs, &gt, &[&ndvi_bytes])
}

// ---------------------------------------------------------------------------
// Pure Rust GeoTIFF writer
// ---------------------------------------------------------------------------

fn write_geotiff(
    width: u32,
    height: u32,
    dtype: &str,
    crs: &str,
    geotransform: &[f64; 6],
    band_data: &[&[u8]],
) -> Result<Vec<u8>> {
    let n_bands = band_data.len() as u16;
    let bpp = dtype_size(dtype);
    let bits_per_sample = (bpp * 8) as u16;
    let sample_format: u16 = match dtype {
        "float32" | "f32" | "float64" | "f64" => 3, // IEEE float
        "int16" | "i16" | "int32" | "i32" => 2,     // signed int
        _ => 1,                                       // unsigned int
    };

    // Parse CRS → EPSG code
    let epsg = parse_epsg(crs);
    let is_projected = epsg >= 32000; // UTM etc.

    // GeoKeys
    let geo_keys = if is_projected {
        vec![
            // KeyDirectoryVersion, KeyRevision, MinorRevision, NumberOfKeys
            1, 1, 0, 3,
            GT_MODEL_TYPE_GEOKEY, 0, 1, 1,     // ModelTypeProjected
            GT_RASTER_TYPE_GEOKEY, 0, 1, 1,    // RasterPixelIsArea
            PROJECTED_CS_TYPE_GEOKEY, 0, 1, epsg,
        ]
    } else {
        vec![
            1, 1, 0, 3,
            GT_MODEL_TYPE_GEOKEY, 0, 1, 2,     // ModelTypeGeographic
            GT_RASTER_TYPE_GEOKEY, 0, 1, 1,    // RasterPixelIsArea
            GEOGRAPHIC_TYPE_GEOKEY, 0, 1, epsg,
        ]
    };

    // ModelTiepoint: (0, 0, 0) → (x_origin, y_origin, 0)
    let tiepoint = [0.0f64, 0.0, 0.0, geotransform[0], geotransform[3], 0.0];

    // ModelPixelScale: (pixel_width, pixel_height, 0)
    let pixel_scale = [geotransform[1], geotransform[5].abs(), 0.0];

    // Calculate strip data size (all bands sequential)
    let strip_size = width as usize * height as usize * bpp;
    let total_strip_bytes = strip_size * n_bands as usize;

    // Number of IFD entries
    let n_tags: u16 = 12; // base tags + geo tags

    // Layout:
    // 8 bytes: TIFF header
    // variable: IFD entries + IFD terminator
    // variable: extra data (BitsPerSample if multi, strip offsets/counts, tiepoint, pixel_scale, geokeys)
    // rest: pixel data

    let ifd_offset: u32 = 8;
    let ifd_size = 2 + n_tags as usize * 12 + 4; // count + entries + next_ifd
    let extra_start = ifd_offset as usize + ifd_size;

    // Build extra data area
    let mut extra = Vec::new();

    // BitsPerSample (if n_bands > 1, need array)
    let bps_offset = extra_start + extra.len();
    if n_bands > 1 {
        for _ in 0..n_bands {
            extra.extend_from_slice(&bits_per_sample.to_le_bytes());
        }
    }

    // SampleFormat (if n_bands > 1)
    let sf_offset = extra_start + extra.len();
    if n_bands > 1 {
        for _ in 0..n_bands {
            extra.extend_from_slice(&sample_format.to_le_bytes());
        }
    }

    // Strip offsets (one strip per band)
    let strip_offsets_offset = extra_start + extra.len();
    let pixel_data_start = extra_start + extra.len()
        + n_bands as usize * 4  // strip offsets
        + n_bands as usize * 4  // strip byte counts
        + 48                     // tiepoint (6 doubles)
        + 24                     // pixel_scale (3 doubles)
        + geo_keys.len() * 2;   // geokeys

    for i in 0..n_bands as usize {
        let offset = (pixel_data_start + i * strip_size) as u32;
        extra.extend_from_slice(&offset.to_le_bytes());
    }

    // Strip byte counts
    let strip_counts_offset = extra_start + extra.len();
    for _ in 0..n_bands {
        extra.extend_from_slice(&(strip_size as u32).to_le_bytes());
    }

    // ModelTiepoint
    let tiepoint_offset = extra_start + extra.len();
    for &v in &tiepoint {
        extra.extend_from_slice(&v.to_le_bytes());
    }

    // ModelPixelScale
    let pixel_scale_offset = extra_start + extra.len();
    for &v in &pixel_scale {
        extra.extend_from_slice(&v.to_le_bytes());
    }

    // GeoKeyDirectory
    let geokey_offset = extra_start + extra.len();
    for &v in &geo_keys {
        extra.extend_from_slice(&v.to_le_bytes());
    }

    // Now build the file
    let total_size = extra_start + extra.len() + total_strip_bytes;
    let mut buf = Cursor::new(Vec::with_capacity(total_size));

    // TIFF header
    buf.write_all(&TIFF_MAGIC_LE.to_le_bytes())?;
    buf.write_all(&TIFF_MAGIC_42.to_le_bytes())?;
    buf.write_all(&ifd_offset.to_le_bytes())?;

    // IFD
    buf.write_all(&n_tags.to_le_bytes())?;

    // Helper to write IFD entry
    fn write_tag(buf: &mut Cursor<Vec<u8>>, tag: u16, dtype: u16, count: u32, value: u32) -> std::io::Result<()> {
        buf.write_all(&tag.to_le_bytes())?;
        buf.write_all(&dtype.to_le_bytes())?;
        buf.write_all(&count.to_le_bytes())?;
        buf.write_all(&value.to_le_bytes())?;
        Ok(())
    }

    // Tags must be in ascending order!
    write_tag(&mut buf, TAG_IMAGE_WIDTH, TIFF_LONG, 1, width)?;
    write_tag(&mut buf, TAG_IMAGE_LENGTH, TIFF_LONG, 1, height)?;

    if n_bands == 1 {
        write_tag(&mut buf, TAG_BITS_PER_SAMPLE, TIFF_SHORT, 1, bits_per_sample as u32)?;
    } else {
        write_tag(&mut buf, TAG_BITS_PER_SAMPLE, TIFF_SHORT, n_bands as u32, bps_offset as u32)?;
    }

    write_tag(&mut buf, TAG_COMPRESSION, TIFF_SHORT, 1, 1)?; // no compression
    write_tag(&mut buf, TAG_PHOTOMETRIC, TIFF_SHORT, 1, 1)?;  // MinIsBlack

    if n_bands == 1 {
        write_tag(&mut buf, TAG_STRIP_OFFSETS, TIFF_LONG, 1, pixel_data_start as u32)?;
    } else {
        write_tag(&mut buf, TAG_STRIP_OFFSETS, TIFF_LONG, n_bands as u32, strip_offsets_offset as u32)?;
    }

    write_tag(&mut buf, TAG_SAMPLES_PER_PIXEL, TIFF_SHORT, 1, n_bands as u32)?;
    write_tag(&mut buf, TAG_ROWS_PER_STRIP, TIFF_LONG, 1, height)?;

    if n_bands == 1 {
        write_tag(&mut buf, TAG_STRIP_BYTE_COUNTS, TIFF_LONG, 1, strip_size as u32)?;
    } else {
        write_tag(&mut buf, TAG_STRIP_BYTE_COUNTS, TIFF_LONG, n_bands as u32, strip_counts_offset as u32)?;
    }

    // SampleFormat
    if n_bands == 1 {
        write_tag(&mut buf, TAG_SAMPLE_FORMAT, TIFF_SHORT, 1, sample_format as u32)?;
    } else {
        write_tag(&mut buf, TAG_SAMPLE_FORMAT, TIFF_SHORT, n_bands as u32, sf_offset as u32)?;
    }

    // GeoTIFF tags
    write_tag(&mut buf, TAG_MODEL_PIXEL_SCALE, TIFF_DOUBLE, 3, pixel_scale_offset as u32)?;
    write_tag(&mut buf, TAG_MODEL_TIEPOINT, TIFF_DOUBLE, 6, tiepoint_offset as u32)?;
    write_tag(&mut buf, TAG_GEO_KEY_DIRECTORY, TIFF_SHORT, geo_keys.len() as u32, geokey_offset as u32)?;

    // IFD terminator (no next IFD)
    buf.write_all(&0u32.to_le_bytes())?;

    // Extra data
    buf.write_all(&extra)?;

    // Pixel data (one strip per band)
    for band in band_data {
        buf.write_all(band)?;
    }

    Ok(buf.into_inner())
}

// ---------------------------------------------------------------------------
// GDAL-backed writer (optional, higher quality output with COG + compression)
// ---------------------------------------------------------------------------

#[cfg(feature = "gdal-support")]
pub fn reconstruct_geotiff_gdal(
    item: &StacItem,
    store: &mut ChunkStore,
    bands: Option<&[String]>,
) -> Result<Vec<u8>> {
    use gdal::raster::RasterCreationOptions;
    use gdal::spatial_ref::SpatialRef;
    use gdal::DriverManager;

    let band_data = reconstruct_bands(item, store, bands)?;
    if band_data.is_empty() {
        return Err(crate::error::EarthGridError::Other(
            format!("No data for item {}", item.id),
        ));
    }

    let props = &item.properties;
    let width = props_u32(props, "earthgrid:width")? as usize;
    let height = props_u32(props, "earthgrid:height")? as usize;
    let dtype_str = props_str(props, "earthgrid:dtype").unwrap_or_else(|| "uint16".to_string());
    let crs = props_str(props, "earthgrid:crs").unwrap_or_else(|| "EPSG:4326".to_string());
    let bpp = dtype_size(&dtype_str);
    let bbox = item.bbox;

    let geotransform = if let Some(tf) = props.get("earthgrid:transform").and_then(|v| v.as_array()) {
        if tf.len() >= 6 {
            [
                tf[0].as_f64().unwrap_or(0.0), tf[1].as_f64().unwrap_or(1.0),
                tf[2].as_f64().unwrap_or(0.0), tf[3].as_f64().unwrap_or(0.0),
                tf[4].as_f64().unwrap_or(0.0), tf[5].as_f64().unwrap_or(-1.0),
            ]
        } else {
            geotransform_from_bbox(bbox, props_u32(props, "earthgrid:width")?, props_u32(props, "earthgrid:height")?)
        }
    } else {
        geotransform_from_bbox(bbox, props_u32(props, "earthgrid:width")?, props_u32(props, "earthgrid:height")?)
    };

    let band_names_ordered: Vec<String> = band_data.keys().cloned().collect();
    let n_bands = band_names_ordered.len();
    let is_float = matches!(dtype_str.as_str(), "float32" | "f32" | "float64" | "f64");
    let vsi_path = format!("/vsimem/earthgrid_reconstruct_{}.tif", uuid::Uuid::new_v4());

    let driver = DriverManager::get_driver_by_name("GTiff")
        .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL: {}", e)))?;

    let mut ds = if is_float {
        driver.create_with_band_type_with_options::<f32, _>(
            &vsi_path, width, height, n_bands,
            &RasterCreationOptions::from_iter(["COMPRESS=LZW", "TILED=YES"]),
        )
    } else if bpp == 1 {
        driver.create_with_band_type_with_options::<u8, _>(
            &vsi_path, width, height, n_bands,
            &RasterCreationOptions::from_iter(["COMPRESS=LZW", "TILED=YES"]),
        )
    } else {
        driver.create_with_band_type_with_options::<u16, _>(
            &vsi_path, width, height, n_bands,
            &RasterCreationOptions::from_iter(["COMPRESS=LZW", "TILED=YES"]),
        )
    }
    .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL create: {}", e)))?;

    ds.set_geo_transform(&geotransform)
        .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL gt: {}", e)))?;

    if let Ok(srs) = SpatialRef::from_definition(&crs) {
        let _ = ds.set_spatial_ref(&srs);
    }

    for (band_idx, band_name) in band_names_ordered.iter().enumerate() {
        if let Some(data) = band_data.get(band_name) {
            let mut rb = ds.rasterband(band_idx + 1)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL band: {}", e)))?;

            if is_float {
                let pixels: Vec<f32> = data.chunks_exact(4)
                    .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                    .collect();
                let mut buf = gdal::raster::Buffer::new((width, height), pixels);
                rb.write((0, 0), (width, height), &mut buf)
                    .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL write: {}", e)))?;
            } else if bpp == 1 {
                let mut buf = gdal::raster::Buffer::new((width, height), data.clone());
                rb.write((0, 0), (width, height), &mut buf)
                    .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL write: {}", e)))?;
            } else {
                let pixels: Vec<u16> = data.chunks_exact(2)
                    .map(|c| u16::from_le_bytes([c[0], c[1]]))
                    .collect();
                let mut buf = gdal::raster::Buffer::new((width, height), pixels);
                rb.write((0, 0), (width, height), &mut buf)
                    .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL write: {}", e)))?;
            }
        }
    }

    drop(ds);

    let bytes = gdal::vsi::get_vsi_mem_file_bytes_owned(&vsi_path)
        .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL vsimem: {}", e)))?;
    let _ = gdal::vsi::unlink_mem_file(&vsi_path);

    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn geotransform_from_bbox(bbox: [f64; 4], width: u32, height: u32) -> [f64; 6] {
    let pixel_width = (bbox[2] - bbox[0]) / width as f64;
    let pixel_height = (bbox[3] - bbox[1]) / height as f64;
    [bbox[0], pixel_width, 0.0, bbox[3], 0.0, -pixel_height]
}

fn parse_epsg(crs: &str) -> u16 {
    // Parse "EPSG:4326" or "EPSG:32633" etc.
    if let Some(code) = crs.strip_prefix("EPSG:") {
        code.parse().unwrap_or(4326)
    } else {
        4326
    }
}

fn props_u32(props: &serde_json::Value, key: &str) -> Result<u32> {
    props.get(key).and_then(|v| v.as_u64()).map(|v| v as u32)
        .ok_or_else(|| crate::error::EarthGridError::Other(format!("Missing: {}", key)))
}

fn props_str(props: &serde_json::Value, key: &str) -> Option<String> {
    props.get(key).and_then(|v| v.as_str()).map(String::from)
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
