//! Reconstruct Cloud-Optimized GeoTIFF (COG) from stored chunks via GDAL.
//!
//! Every node produces the same format: COG with LZW compression + tiling.
//! Requires libgdal on the system (standard for the geo community).

use std::collections::HashMap;

use gdal::raster::RasterCreationOptions;
use gdal::spatial_ref::SpatialRef;
use gdal::DriverManager;

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

// ---------------------------------------------------------------------------
// COG output via GDAL
// ---------------------------------------------------------------------------

/// Reconstruct a Cloud-Optimized GeoTIFF from stored chunks.
///
/// Output: COG with LZW compression, 256×256 tiles, overviews.
/// Readable by QGIS, GDAL, rasterio, R terra/stars, any STAC client.
pub fn reconstruct_cog(
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
    let width = props_u32(props, "earthgrid:width")? as usize;
    let height = props_u32(props, "earthgrid:height")? as usize;
    let dtype_str = props_str(props, "earthgrid:dtype").unwrap_or_else(|| "uint16".to_string());
    let crs = props_str(props, "earthgrid:crs").unwrap_or_else(|| "EPSG:4326".to_string());
    let bbox = item.bbox;
    let bpp = dtype_size(&dtype_str);
    let is_float = matches!(dtype_str.as_str(), "float32" | "f32" | "float64" | "f64");

    let geotransform = extract_geotransform(props, bbox, width as u32, height as u32);

    let band_names_ordered: Vec<String> = band_data.keys().cloned().collect();
    let n_bands = band_names_ordered.len();

    write_cog(
        width, height, n_bands, bpp, is_float,
        &crs, &geotransform,
        &band_names_ordered, &band_data,
    )
}

/// Compute NDVI and return as COG.
pub fn ndvi_cog(
    red_data: &[u8],
    nir_data: &[u8],
    width: u32,
    height: u32,
    bbox: [f64; 4],
    crs: &str,
    geotransform: Option<[f64; 6]>,
) -> Result<Vec<u8>> {
    let pixel_count = (width * height) as usize;

    let ndvi: Vec<f32> = if red_data.len() == pixel_count * 2 {
        red_data.chunks_exact(2).zip(nir_data.chunks_exact(2))
            .map(|(r, n)| {
                let red = u16::from_le_bytes([r[0], r[1]]) as f32;
                let nir = u16::from_le_bytes([n[0], n[1]]) as f32;
                if (nir + red).abs() < 1e-6 { 0.0 } else { (nir - red) / (nir + red) }
            })
            .collect()
    } else {
        red_data.chunks_exact(4).zip(nir_data.chunks_exact(4))
            .map(|(r, n)| {
                let red = f32::from_le_bytes([r[0], r[1], r[2], r[3]]);
                let nir = f32::from_le_bytes([n[0], n[1], n[2], n[3]]);
                if (nir + red).abs() < 1e-6 { 0.0 } else { (nir - red) / (nir + red) }
            })
            .collect()
    };

    let ndvi_bytes: Vec<u8> = ndvi.iter().flat_map(|v| v.to_le_bytes()).collect();

    let gt = geotransform.unwrap_or_else(|| geotransform_from_bbox(bbox, width, height));

    let mut band_data = HashMap::new();
    band_data.insert("NDVI".to_string(), ndvi_bytes);

    write_cog(
        width as usize, height as usize, 1, 4, true,
        crs, &gt,
        &["NDVI".to_string()], &band_data,
    )
}

// ---------------------------------------------------------------------------
// COG writer (GDAL /vsimem)
// ---------------------------------------------------------------------------

fn write_cog(
    width: usize,
    height: usize,
    n_bands: usize,
    bpp: usize,
    is_float: bool,
    crs: &str,
    geotransform: &[f64; 6],
    band_names: &[String],
    band_data: &HashMap<String, Vec<u8>>,
) -> Result<Vec<u8>> {
    let vsi_path = format!("/vsimem/earthgrid_cog_{}.tif", uuid::Uuid::new_v4());

    // COG creation options: tiled, LZW compressed, with overviews
    let cog_options = [
        "COMPRESS=LZW",
        "TILED=YES",
        "BLOCKXSIZE=256",
        "BLOCKYSIZE=256",
        "PREDICTOR=2",      // horizontal differencing for better compression
    ];

    let driver = DriverManager::get_driver_by_name("GTiff")
        .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL GTiff driver: {}", e)))?;

    let mut ds = if is_float {
        driver.create_with_band_type_with_options::<f32, _>(
            &vsi_path, width, height, n_bands,
            &RasterCreationOptions::from_iter(cog_options),
        )
    } else if bpp == 1 {
        driver.create_with_band_type_with_options::<u8, _>(
            &vsi_path, width, height, n_bands,
            &RasterCreationOptions::from_iter(cog_options),
        )
    } else {
        driver.create_with_band_type_with_options::<u16, _>(
            &vsi_path, width, height, n_bands,
            &RasterCreationOptions::from_iter(cog_options),
        )
    }
    .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL create: {}", e)))?;

    ds.set_geo_transform(geotransform)
        .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL geotransform: {}", e)))?;

    if let Ok(srs) = SpatialRef::from_definition(crs) {
        let _ = ds.set_spatial_ref(&srs);
    }

    for (band_idx, band_name) in band_names.iter().enumerate() {
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

    // Close dataset to flush
    drop(ds);

    // Read from /vsimem
    let bytes = gdal::vsi::get_vsi_mem_file_bytes_owned(&vsi_path)
        .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL vsimem read: {}", e)))?;

    let _ = gdal::vsi::unlink_mem_file(&vsi_path);

    Ok(bytes)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn extract_geotransform(props: &serde_json::Value, bbox: [f64; 4], width: u32, height: u32) -> [f64; 6] {
    if let Some(tf) = props.get("earthgrid:transform").and_then(|v| v.as_array()) {
        if tf.len() >= 6 {
            let raw: Vec<f64> = (0..6).map(|i| tf[i].as_f64().unwrap_or(0.0)).collect();
            // earthgrid:transform may be stored as [px, rot, ox, rot, py, oy]
            // GDAL expects [origin_x, pixel_x, rot, origin_y, rot, pixel_y]
            return if raw[0].abs() < raw[2].abs() {
                [raw[2], raw[0], raw[1], raw[5], raw[3], raw[4]]
            } else {
                [raw[0], raw[1], raw[2], raw[3], raw[4], raw[5]]
            };
        }
    }
    geotransform_from_bbox(bbox, width, height)
}

fn geotransform_from_bbox(bbox: [f64; 4], width: u32, height: u32) -> [f64; 6] {
    let pixel_width = (bbox[2] - bbox[0]) / width as f64;
    let pixel_height = (bbox[3] - bbox[1]) / height as f64;
    [bbox[0], pixel_width, 0.0, bbox[3], 0.0, -pixel_height]
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
