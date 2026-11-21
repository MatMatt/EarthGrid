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

/// Most bytes one reconstruction may allocate for its output rasters.
///
/// The `earthgrid:*` properties that size these buffers are peer-supplied, so
/// every allocation below is checked against this budget first.
const MAX_RECONSTRUCT_BYTES: u64 = 2 * 1024 * 1024 * 1024;

/// Most chunk references one item may carry — the top-level list plus every
/// nested `earthgrid:band_hashes` list. Same limit the ingestion gate applies
/// to the top-level list.
const MAX_CHUNK_REFS: usize = 1_000_000;

/// Most bands one item may describe or reconstruct. Same limit as the
/// ingestion gate.
const MAX_BANDS: u32 = 4096;

/// Most bytes the `earthgrid:band_names` strings of one item may total: 16
/// bytes a name at `MAX_BANDS` names, which is also the most entries allowed.
const MAX_BAND_NAME_BYTES: usize = 64 * 1024;

/// Window `write_cog` converts and writes at a time. Both are multiples of the
/// 256-pixel block size, so a window only ever covers whole blocks, and the
/// typed copy GDAL needs stays at 4096 × 256 samples whatever the raster.
const WRITE_WINDOW_COLS: usize = 4096;
const WRITE_WINDOW_ROWS: usize = 256;

// ---------------------------------------------------------------------------
// Chunk source
// ---------------------------------------------------------------------------

/// Where reconstruction reads chunks from.
///
/// `ChunkStore` is the direct source. The download handler uses a second one
/// that takes the shared store lock per chunk, so a reconstruction running on
/// a blocking thread never holds that lock for its whole duration.
pub trait ChunkSource {
    /// Retrieve a chunk by hash; `None` when it is not stored.
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>>;
    /// Whether the chunk is stored.
    fn has(&self, hash: &str) -> bool;
    /// Stored size of the chunk in bytes.
    fn chunk_size(&self, hash: &str) -> Result<u64>;
}

impl ChunkSource for ChunkStore {
    fn get(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        ChunkStore::get(self, hash)
    }

    fn has(&self, hash: &str) -> bool {
        ChunkStore::has(self, hash)
    }

    fn chunk_size(&self, hash: &str) -> Result<u64> {
        ChunkStore::chunk_size(self, hash)
    }
}

// ---------------------------------------------------------------------------
// Band reconstruction (chunks → raw arrays)
// ---------------------------------------------------------------------------

/// Reconstruct per-band 2D arrays from stored chunks.
pub fn reconstruct_bands<S: ChunkSource>(
    item: &StacItem,
    store: &mut S,
    bands: Option<&[String]>,
) -> Result<HashMap<String, Vec<u8>>> {
    let props = &item.properties;

    check_chunk_refs(item)?;

    let width = props_u32(props, "earthgrid:width")?;
    let height = props_u32(props, "earthgrid:height")?;
    let tile_size = props_u32(props, "earthgrid:tile_size")?;
    let tile_cols = props_u32(props, "earthgrid:tile_cols")?;
    let _tile_rows = props_u32(props, "earthgrid:tile_rows")?;
    check_nonzero_dims(width, height)?;
    let n_bands = if props.get("earthgrid:bands").and_then(|v| v.as_u64()).is_some() {
        props_u32(props, "earthgrid:bands")?
    } else {
        1
    };
    if n_bands > MAX_BANDS {
        return Err(refused("earthgrid:bands", n_bands, &format!("more than {} bands", MAX_BANDS)));
    }
    check_band_names(props)?;
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
            let pixel_count = checked_pixel_count(width, height, bpp, n_bands as u64)?;
            check_tile_grid(item.chunk_hashes.len(), width, height, tile_size, tile_cols)?;
            let mut full: Vec<Vec<u8>> = (0..n_bands as usize)
                .map(|_| vec![0u8; pixel_count * bpp])
                .collect();

            for (idx, sha) in item.chunk_hashes.iter().enumerate() {
                let raw = match store.get(sha)? {
                    Some(data) => data,
                    None => continue,
                };
                let (x_off, y_off, w, h) = tile_rect(idx, width, height, tile_size, tile_cols)?;
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
            // Counted on the borrowed JSON, so an item with too many bands is
            // refused before its band lists are cloned.
            let band_hashes_json = props.get("earthgrid:band_hashes");
            let listed_bands = band_hashes_json
                .and_then(|v| v.as_object())
                .map_or(0, |bands| bands.len());
            if listed_bands > MAX_BANDS as usize {
                return Err(refused(
                    "earthgrid:band_hashes",
                    format!("{} bands", listed_bands),
                    &format!("more than {} bands", MAX_BANDS),
                ));
            }
            let band_hashes: HashMap<String, Vec<String>> = band_hashes_json
                .and_then(|v| serde_json::from_value(v.clone()).ok())
                .unwrap_or_default();

            // Every selected band allocates its own raster, so the budget is
            // checked against all of them — and each band's grid — up front.
            let mut selected = 0u64;
            for (band_name, hashes) in &band_hashes {
                if let Some(filter) = bands {
                    if !filter.iter().any(|b| b == band_name) {
                        continue;
                    }
                }
                check_tile_grid(hashes.len(), width, height, tile_size, tile_cols)?;
                selected += 1;
            }
            let pixel_count = checked_pixel_count(width, height, bpp, selected)?;

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
                    let (x_off, y_off, w, h) = tile_rect(idx, width, height, tile_size, tile_cols)?;
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
pub fn reconstruct_cog<S: ChunkSource>(
    item: &StacItem,
    store: &mut S,
    bands: Option<&[String]>,
) -> Result<Vec<u8>> {
    // Items produced by `ingest::ingest_file` — which is every item this node
    // can currently create, since `ingest_raster` is not wired to any caller —
    // carry no tile metadata: no `earthgrid:width`, `tile_size`, `tile_cols`.
    // The tiled path below would fail them all with "Missing: earthgrid:width",
    // which is why `GET /api/download/...` returned 500 for every item.
    //
    // `ingest_file` splits the source file into sequential raw byte chunks, so
    // concatenating them in order reproduces the original COG byte-for-byte.
    // Serve that directly.
    if item.properties.get("earthgrid:width").and_then(|v| v.as_u64()).is_none() {
        return reconstruct_raw(item, store);
    }

    // Refuse a CRS outside the allowlist, or one GDAL cannot build, before any
    // raster is allocated. Parsed on the borrowed JSON: nothing is copied first.
    let crs = item.properties.get("earthgrid:crs").and_then(|v| v.as_str()).unwrap_or("EPSG:4326");
    let crs = parse_allowed_crs(crs)
        .and_then(|crs| crs.build())
        .map_err(crate::error::EarthGridError::Other)?;

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

/// Reassemble an item stored as sequential raw byte chunks (the `ingest_file`
/// layout) by concatenating its chunks in order.
///
/// When the item records `earthgrid:file_hash` — which `ingest_file` always
/// writes — the result is checked against it, so a missing or corrupted chunk
/// surfaces as an integrity error instead of a silently truncated download.
pub fn reconstruct_raw<S: ChunkSource>(item: &StacItem, store: &mut S) -> Result<Vec<u8>> {
    if item.chunk_hashes.is_empty() {
        return Err(crate::error::EarthGridError::Other(format!(
            "Item {} has no chunks",
            item.id
        )));
    }
    check_chunk_refs(item)?;

    // Size the output before allocating any of it. Each distinct chunk is
    // looked up once, but every reference counts towards the total: the output
    // repeats a chunk as often as the item lists it.
    let mut sizes: HashMap<&str, u64> = HashMap::new();
    let mut total: u64 = 0;
    for sha in &item.chunk_hashes {
        let size = match sizes.get(sha.as_str()) {
            Some(size) => *size,
            None => {
                if !store.has(sha) {
                    return Err(crate::error::EarthGridError::ChunkNotFound(format!(
                        "{} (item {})",
                        sha, item.id
                    )));
                }
                // Sized from the index alone, never by reading the chunk. One the
                // index does not list counts as zero here; the copy loop below
                // still holds its bytes to the budget.
                let size = store.chunk_size(sha).unwrap_or(0);
                sizes.insert(sha.as_str(), size);
                size
            }
        };
        total = match total.checked_add(size) {
            Some(t) if t <= MAX_RECONSTRUCT_BYTES => t,
            _ => {
                return Err(refused(
                    &format!("item {} raw size", item.id),
                    format!("more than {} bytes", total),
                    &format!("budget is {} bytes", MAX_RECONSTRUCT_BYTES),
                ))
            }
        };
    }

    let mut out = Vec::new();
    for sha in &item.chunk_hashes {
        match store.get(sha)? {
            Some(data) => {
                // The index sized this chunk; hold the file on disk to the same budget.
                if (out.len() + data.len()) as u64 > MAX_RECONSTRUCT_BYTES {
                    return Err(refused(
                        &format!("item {} raw size", item.id),
                        format!("more than {} bytes", out.len()),
                        &format!("budget is {} bytes", MAX_RECONSTRUCT_BYTES),
                    ));
                }
                out.extend_from_slice(&data);
            }
            None => {
                return Err(crate::error::EarthGridError::ChunkNotFound(format!(
                    "{} (item {})",
                    sha, item.id
                )))
            }
        }
    }

    if let Some(expected) = item
        .properties
        .get("earthgrid:file_hash")
        .and_then(|v| v.as_str())
    {
        let actual = ChunkStore::hash_bytes(&out);
        if actual != expected {
            return Err(crate::error::EarthGridError::IntegrityViolation {
                expected: expected.to_string(),
                actual,
            });
        }
    }

    Ok(out)
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
    // Refuse a CRS outside the allowlist before anything is sized or allocated.
    let crs = parse_allowed_crs(crs).map_err(crate::error::EarthGridError::Other)?;

    // Output is one float32 band.
    let pixel_count = checked_pixel_count(width, height, 4, 1)?;

    // The reader below pairs samples until the shorter input runs out, so the
    // inputs — not `width` × `height` — decide how much is allocated.
    let sample_bytes = if red_data.len() == pixel_count * 2 { 2 } else { 4 };
    let samples = checked_ndvi_samples(red_data.len(), nir_data.len(), sample_bytes)?;

    // `write_cog` hands GDAL a `width` × `height` raster, so anything but one
    // sample per pixel is refused before the output buffer exists.
    if samples != pixel_count {
        return Err(refused(
            "NDVI input",
            format!("{} samples of {} byte(s)", samples, sample_bytes),
            &format!("the {}x{} raster has {} pixels", width, height, pixel_count),
        ));
    }

    // A CRS GDAL cannot build is an error too, still ahead of the allocation.
    let crs = crs.build().map_err(crate::error::EarthGridError::Other)?;

    let ndvi =|red: f32, nir: f32| -> f32 {
        if (nir + red).abs() < 1e-6 { 0.0 } else { (nir - red) / (nir + red) }
    };

    // Written straight into the byte buffer `write_cog` takes.
    let mut ndvi_bytes: Vec<u8> = Vec::with_capacity(samples * 4);
    if sample_bytes == 2 {
        for (r, n) in red_data.chunks_exact(2).zip(nir_data.chunks_exact(2)) {
            let red = u16::from_le_bytes([r[0], r[1]]) as f32;
            let nir = u16::from_le_bytes([n[0], n[1]]) as f32;
            ndvi_bytes.extend_from_slice(&ndvi(red, nir).to_le_bytes());
        }
    } else {
        for (r, n) in red_data.chunks_exact(4).zip(nir_data.chunks_exact(4)) {
            let red = f32::from_le_bytes([r[0], r[1], r[2], r[3]]);
            let nir = f32::from_le_bytes([n[0], n[1], n[2], n[3]]);
            ndvi_bytes.extend_from_slice(&ndvi(red, nir).to_le_bytes());
        }
    }

    let gt = geotransform.unwrap_or_else(|| geotransform_from_bbox(bbox, width, height));

    let mut band_data = HashMap::new();
    band_data.insert("NDVI".to_string(), ndvi_bytes);

    write_cog(
        width as usize, height as usize, 1, 4, true,
        &crs, &gt,
        &["NDVI".to_string()], &band_data,
    )
}

/// Sample pairs `ndvi_cog` consumes from inputs of `red_len` and `nir_len`
/// bytes at `sample_bytes` bytes per sample, refused unless the float32 output
/// for that many samples fits in `MAX_RECONSTRUCT_BYTES`.
///
/// Nothing is allocated here; `ndvi_cog` allocates only after this returns `Ok`.
fn checked_ndvi_samples(red_len: usize, nir_len: usize, sample_bytes: usize) -> Result<usize> {
    let samples = red_len.min(nir_len) / sample_bytes;
    match (samples as u64).checked_mul(4) {
        Some(bytes) if bytes <= MAX_RECONSTRUCT_BYTES => Ok(samples),
        _ => Err(refused(
            "NDVI input",
            format!("{} + {} bytes ({} samples of {} byte(s))", red_len, nir_len, samples, sample_bytes),
            &format!("output exceeds the budget of {} bytes", MAX_RECONSTRUCT_BYTES),
        )),
    }
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
    // Already through `parse_allowed_crs` and built: callers refuse a CRS
    // before they allocate the rasters handed in here.
    crs: &BuiltCrs,
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

    crs.assign_to(&mut ds).map_err(crate::error::EarthGridError::Other)?;

    for (band_idx, band_name) in band_names.iter().enumerate() {
        if let Some(data) = band_data.get(band_name) {
            let mut rb = ds.rasterband(band_idx + 1)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL band: {}", e)))?;

            if is_float {
                write_band_windows(&mut rb, data, width, height, |c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))?;
            } else if bpp == 1 {
                write_band_windows(&mut rb, data, width, height, |c| c[0])?;
            } else {
                write_band_windows(&mut rb, data, width, height, |c| u16::from_le_bytes([c[0], c[1]]))?;
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

/// Write one band from its little-endian sample bytes, a window at a time.
///
/// GDAL takes typed samples, so they have to be copied out of `data`. Copying a
/// window at a time keeps that copy at `WRITE_WINDOW_COLS` × `WRITE_WINDOW_ROWS`
/// samples, so `data` stays the only buffer holding the whole band. `data`
/// must be exactly `width` × `height` samples of `T`: anything else is an
/// error here, never GDAL's buffer-shape assertion.
fn write_band_windows<T: gdal::raster::GdalType + Copy>(
    rb: &mut gdal::raster::RasterBand<'_>,
    data: &[u8],
    width: usize,
    height: usize,
    decode: impl Fn(&[u8]) -> T,
) -> Result<()> {
    let sample_bytes = std::mem::size_of::<T>();
    let expected = width.saturating_mul(height).saturating_mul(sample_bytes);
    if data.len() != expected {
        return Err(refused(
            "band data",
            format!("{} bytes", data.len()),
            &format!(
                "a {}x{} raster of {}-byte samples is {} bytes",
                width, height, sample_bytes, expected
            ),
        ));
    }

    for y0 in (0..height).step_by(WRITE_WINDOW_ROWS) {
        let h = WRITE_WINDOW_ROWS.min(height - y0);
        for x0 in (0..width).step_by(WRITE_WINDOW_COLS) {
            let w = WRITE_WINDOW_COLS.min(width - x0);
            let mut pixels: Vec<T> = Vec::with_capacity(w * h);
            for row in y0..y0 + h {
                let start = (row * width + x0) * sample_bytes;
                pixels.extend(
                    data[start..start + w * sample_bytes]
                        .chunks_exact(sample_bytes)
                        .map(&decode),
                );
            }
            let mut buf = gdal::raster::Buffer::new((w, h), pixels);
            rb.write((x0 as isize, y0 as isize), (w, h), &mut buf)
                .map_err(|e| crate::error::EarthGridError::Other(format!("GDAL write: {}", e)))?;
        }
    }
    Ok(())
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
    let value = props.get(key).and_then(|v| v.as_u64())
        .ok_or_else(|| crate::error::EarthGridError::Other(format!("Missing: {}", key)))?;
    u32::try_from(value).map_err(|_| refused(key, value, "does not fit in 32 bits"))
}

fn refused(what: &str, value: impl std::fmt::Display, why: &str) -> crate::error::EarthGridError {
    crate::error::EarthGridError::Other(format!("Refusing {} = {}: {}", what, value, why))
}

/// A zero dimension would otherwise reach the divide in `geotransform_from_bbox`
/// and come back as an infinite pixel size instead of an error.
fn check_nonzero_dims(width: u32, height: u32) -> Result<()> {
    if width == 0 {
        return Err(refused("raster width", width, "must be at least 1"));
    }
    if height == 0 {
        return Err(refused("raster height", height, "must be at least 1"));
    }
    Ok(())
}

/// Pixel count of a `width` × `height` raster, refused unless `n_bands` bands
/// of it at `bpp` bytes per pixel fit in `MAX_RECONSTRUCT_BYTES`.
///
/// Nothing is allocated here; callers allocate only after this returns `Ok`.
fn checked_pixel_count(width: u32, height: u32, bpp: usize, n_bands: u64) -> Result<usize> {
    check_nonzero_dims(width, height)?;
    let pixels = (width as u64).checked_mul(height as u64);
    let bytes = pixels
        .and_then(|p| p.checked_mul(bpp as u64))
        .and_then(|b| b.checked_mul(n_bands));
    match (pixels, bytes) {
        (Some(pixels), Some(bytes)) if bytes <= MAX_RECONSTRUCT_BYTES => usize::try_from(pixels)
            .map_err(|_| refused("raster pixel count", pixels, "does not fit in memory")),
        _ => Err(refused(
            "raster size",
            format!("{}x{} pixels x {} band(s) x {} byte(s)", width, height, n_bands, bpp),
            &match bytes {
                Some(bytes) => format!("needs {} bytes, budget is {} bytes", bytes, MAX_RECONSTRUCT_BYTES),
                None => format!("byte count overflows, budget is {} bytes", MAX_RECONSTRUCT_BYTES),
            },
        )),
    }
}

/// Pixel rectangle `(x_off, y_off, w, h)` of tile `idx` in a grid `tile_cols`
/// tiles wide.
///
/// Errors when the tile's origin lies outside the raster — the case where
/// `width - col_i * tile_size` used to wrap.
fn tile_rect(
    idx: usize,
    width: u32,
    height: u32,
    tile_size: u32,
    tile_cols: u32,
) -> Result<(usize, usize, usize, usize)> {
    let idx = idx as u64;
    let x_off = idx
        .checked_rem(tile_cols as u64)
        .and_then(|col_i| col_i.checked_mul(tile_size as u64))
        .filter(|x_off| *x_off < width as u64);
    let y_off = idx
        .checked_div(tile_cols as u64)
        .and_then(|row_i| row_i.checked_mul(tile_size as u64))
        .filter(|y_off| *y_off < height as u64);
    let (Some(x_off), Some(y_off)) = (x_off, y_off) else {
        return Err(refused(
            "chunk index",
            idx,
            &format!(
                "tile lies outside the {}x{} raster (tile_size {}, tile_cols {})",
                width, height, tile_size, tile_cols
            ),
        ));
    };
    let w = (tile_size as u64).min(width as u64 - x_off);
    let h = (tile_size as u64).min(height as u64 - y_off);
    Ok((x_off as usize, y_off as usize, w as usize, h as usize))
}

/// Refuse a tile grid that places any of `n_chunks` listed chunks outside the
/// raster, before anything is allocated or read.
fn check_tile_grid(n_chunks: usize, width: u32, height: u32, tile_size: u32, tile_cols: u32) -> Result<()> {
    if tile_size == 0 {
        return Err(refused("earthgrid:tile_size", tile_size, "must be at least 1"));
    }
    if tile_cols == 0 {
        return Err(refused("earthgrid:tile_cols", tile_cols, "must be at least 1"));
    }
    if n_chunks == 0 {
        return Ok(());
    }
    // The rightmost column and the lowest row any listed chunk lands in; every
    // other chunk lies up and to the left of these two.
    tile_rect(n_chunks.min(tile_cols as usize) - 1, width, height, tile_size, tile_cols)?;
    tile_rect(n_chunks - 1, width, height, tile_size, tile_cols)?;
    Ok(())
}

/// Bound the chunk references an item carries: the top-level list plus every
/// nested `earthgrid:band_hashes` list, counted without copying them.
fn check_chunk_refs(item: &StacItem) -> Result<()> {
    let nested = item
        .properties
        .get("earthgrid:band_hashes")
        .and_then(|v| v.as_object())
        .map(|bands| {
            bands
                .values()
                .filter_map(|hashes| hashes.as_array())
                .fold(0usize, |n, hashes| n.saturating_add(hashes.len()))
        })
        .unwrap_or(0);
    let total = item.chunk_hashes.len().saturating_add(nested);
    if total > MAX_CHUNK_REFS {
        return Err(refused(
            &format!("item {} chunk references", item.id),
            total,
            &format!("more than {}", MAX_CHUNK_REFS),
        ));
    }
    Ok(())
}

/// Bound `earthgrid:band_names` on the borrowed JSON, before it is cloned: at
/// most `MAX_BANDS` entries, and `MAX_BAND_NAME_BYTES` bytes of names in total.
fn check_band_names(props: &serde_json::Value) -> Result<()> {
    let Some(names) = props.get("earthgrid:band_names").and_then(|v| v.as_array()) else {
        return Ok(());
    };
    if names.len() > MAX_BANDS as usize {
        return Err(refused(
            "earthgrid:band_names",
            format!("{} names", names.len()),
            &format!("more than {} names", MAX_BANDS),
        ));
    }
    let bytes = names
        .iter()
        .filter_map(|name| name.as_str())
        .fold(0usize, |n, name| n.saturating_add(name.len()));
    if bytes > MAX_BAND_NAME_BYTES {
        return Err(refused(
            "earthgrid:band_names",
            format!("{} bytes of names", bytes),
            &format!("more than {} bytes", MAX_BAND_NAME_BYTES),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CRS allowlist
// ---------------------------------------------------------------------------

/// Root keywords a WKT definition may start with.
const WKT_ROOTS: [&str; 6] = ["PROJCS", "GEOGCS", "PROJCRS", "GEOGCRS", "BOUNDCRS", "COMPD_CS"];

/// Text that can still carry an instruction to GDAL/PROJ — a URL, a `+init`
/// file reference, a `/vsi…` path, a Windows path, a PROJ string (`+proj`,
/// `+nadgrids`) — even inside a quoted WKT string. Matched case-insensitively,
/// anywhere in the value. Control characters, NUL included, are refused the
/// same way.
const REFUSED_CRS_FRAGMENTS: [&str; 6] = ["://", "+init", "/vsi", "\\", "+proj", "+nadgrids"];

/// An `@`-relative grid path and a `|` pipeline separator are refused only
/// outside double-quoted sections: inside a quoted WKT name such as
/// "Survey @ station | reference" they are descriptive text.
const REFUSED_UNQUOTED_CRS_FRAGMENTS: [&str; 2] = ["@", "|"];

/// A CRS definition that passed `parse_allowed_crs`.
pub(crate) enum AllowedCrs<'a> {
    Epsg(u32),
    Wkt(&'a str),
}

impl AllowedCrs<'_> {
    /// Build the spatial reference. The allowlist is lexical, so a value can
    /// pass it and still be one GDAL rejects; that is an error naming the value
    /// and GDAL's reason, never an output written without a CRS.
    pub(crate) fn build(&self) -> std::result::Result<BuiltCrs, String> {
        let (shown, srs) = match self {
            AllowedCrs::Epsg(code) => (format!("\"EPSG:{}\"", code), SpatialRef::from_epsg(*code)),
            AllowedCrs::Wkt(wkt) => (quote_crs(wkt), SpatialRef::from_wkt(wkt)),
        };
        match srs {
            Ok(srs) => Ok(BuiltCrs { shown, srs }),
            Err(e) => Err(format!("Cannot build CRS {}: {}", shown, e)),
        }
    }
}

/// An allowed CRS that GDAL has built, ready to assign to a dataset.
pub(crate) struct BuiltCrs {
    /// The value as errors quote it back: truncated.
    shown: String,
    srs: SpatialRef,
}

impl BuiltCrs {
    /// Assign the CRS to `ds`; a failure is an error, not a dataset left without one.
    pub(crate) fn assign_to(&self, ds: &mut gdal::Dataset) -> std::result::Result<(), String> {
        ds.set_spatial_ref(&self.srs)
            .map_err(|e| format!("Cannot assign CRS {} to the output: {}", self.shown, e))
    }
}

/// Allowlist for CRS text that did not originate on this node.
///
/// `SpatialRef::from_definition` (`OSRSetFromUserInput`) also accepts URLs,
/// local paths and `/vsi…` paths, and fetches or opens them — outside the
/// outbound URL policy. Only two forms get through here: `EPSG:<1-6 digits>`
/// and WKT opening with one of `WKT_ROOTS` (both case-insensitive). Everything
/// else is refused by name, and so is any value carrying one of
/// `REFUSED_CRS_FRAGMENTS` or a control character, wherever in the text it sits,
/// or one of `REFUSED_UNQUOTED_CRS_FRAGMENTS` outside a quoted section.
///
/// This is a lexical gate, not WKT validation: `AllowedCrs::build` is where a
/// value GDAL cannot construct becomes an error.
pub(crate) fn parse_allowed_crs(crs: &str) -> std::result::Result<AllowedCrs<'_>, String> {
    let s = crs.trim();

    if let Some(fragment) = REFUSED_CRS_FRAGMENTS.iter().find(|f| contains_ignore_ascii_case(s, f)) {
        return Err(refuse_crs(s, &format!("{:?} is refused anywhere in a CRS", fragment)));
    }
    if let Some(fragment) = REFUSED_UNQUOTED_CRS_FRAGMENTS
        .iter()
        .find(|f| parts_outside_quotes(s).any(|part| part.contains(**f)))
    {
        return Err(refuse_crs(
            s,
            &format!("{:?} is refused anywhere in a CRS outside a quoted name", fragment),
        ));
    }
    if let Some(control) = s.chars().find(|c| c.is_control()) {
        return Err(refuse_crs(
            s,
            &format!("control character {:?} is refused anywhere in a CRS", control),
        ));
    }

    if let Some(code) = s.get(..5).filter(|p| p.eq_ignore_ascii_case("EPSG:")).map(|_| &s[5..]) {
        if (1..=6).contains(&code.len()) && code.bytes().all(|b| b.is_ascii_digit()) {
            if let Ok(code) = code.parse::<u32>() {
                return Ok(AllowedCrs::Epsg(code));
            }
        }
    }

    let token_end = s
        .find(|c: char| !(c.is_ascii_alphanumeric() || c == '_'))
        .unwrap_or(s.len());
    let (token, rest) = s.split_at(token_end);
    // WKT names such as "WGS 84 / UTM zone 32N" carry a slash, but only ever
    // inside a quoted string.
    if WKT_ROOTS.iter().any(|root| root.eq_ignore_ascii_case(token))
        && rest.trim_start().starts_with(['[', '('])
        && !has_separator_outside_quotes(s)
    {
        return Ok(AllowedCrs::Wkt(s));
    }

    // URLs, `/vsi…` paths and backslashes never get this far.
    let reason = if s.contains('/') { "paths are refused" } else { "not on the allowlist" };
    Err(refuse_crs(s, reason))
}

/// The refusal message: the value quoted back truncated, then the rule.
fn refuse_crs(s: &str, reason: &str) -> String {
    format!(
        "Refusing CRS {}: {}; only EPSG:<1-6 digits> or WKT starting with {} is accepted",
        quote_crs(s),
        reason,
        WKT_ROOTS.join(", "),
    )
}

/// A CRS value as error messages quote it back: escaped, and truncated.
fn quote_crs(s: &str) -> String {
    let shown: String = s.chars().take(64).collect();
    let ellipsis = if shown.len() < s.len() { "…" } else { "" };
    format!("{:?}{}", shown, ellipsis)
}

fn contains_ignore_ascii_case(s: &str, fragment: &str) -> bool {
    s.as_bytes()
        .windows(fragment.len())
        .any(|w| w.eq_ignore_ascii_case(fragment.as_bytes()))
}

/// The parts of `s` outside double-quoted sections: every second piece of a
/// split on `"`. An unterminated quote runs to the end of the value.
fn parts_outside_quotes(s: &str) -> impl Iterator<Item = &str> {
    s.split('"').step_by(2)
}

fn has_separator_outside_quotes(s: &str) -> bool {
    let mut quoted = false;
    for c in s.chars() {
        match c {
            '"' => quoted = !quoted,
            '/' | '\\' if !quoted => return true,
            _ => {}
        }
    }
    false
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingest;

    /// End-to-end: an item ingested the way `/api/fetch` ingests one must be
    /// downloadable again, byte-for-byte.
    ///
    /// Regression for `GET /api/download/{collection}/{item}` returning 500 on
    /// every item. `ingest_file` writes no `earthgrid:width`, so `reconstruct_cog`
    /// hit `props_u32(..., "earthgrid:width")?` and bailed with
    /// "Missing: earthgrid:width" — there was no item in existence it could serve.
    #[test]
    fn reconstruct_roundtrips_ingest_file_items() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();

        // Multi-chunk payload so ordering is actually exercised.
        let original: Vec<u8> = (0..300_000u32).map(|i| (i % 251) as u8).collect();
        let src = dir.path().join("scene.tif");
        std::fs::write(&src, &original).unwrap();

        let item = ingest::ingest_file(&src, "test-collection", 64 * 1024, &mut store).unwrap();
        assert!(item.chunk_hashes.len() > 1, "expected a multi-chunk item");
        assert!(
            item.properties.get("earthgrid:width").is_none(),
            "ingest_file is not expected to record tile metadata"
        );

        let rebuilt = reconstruct_cog(&item, &mut store, None).unwrap();
        assert_eq!(rebuilt, original, "download must return the original bytes");
    }

    /// A missing chunk must be a hard error, never a truncated file.
    #[test]
    fn reconstruct_raw_rejects_missing_chunk() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = ChunkStore::new(&dir.path().join("store"), 0.0).unwrap();

        let original: Vec<u8> = (0..200_000u32).map(|i| (i % 197) as u8).collect();
        let src = dir.path().join("scene.tif");
        std::fs::write(&src, &original).unwrap();
        let item = ingest::ingest_file(&src, "c", 64 * 1024, &mut store).unwrap();

        store.delete(&item.chunk_hashes[1]).unwrap();

        let err = reconstruct_cog(&item, &mut store, None).unwrap_err();
        assert!(
            matches!(err, crate::error::EarthGridError::ChunkNotFound(_)),
            "expected ChunkNotFound, got {err:?}"
        );
    }

    fn test_store(dir: &tempfile::TempDir) -> ChunkStore {
        ChunkStore::new(&dir.path().join("store"), 0.0).unwrap()
    }

    fn tiled_item(properties: serde_json::Value, chunk_hashes: Vec<String>) -> StacItem {
        StacItem {
            id: "tiled".to_string(),
            collection: "c".to_string(),
            bbox: [0.0, 0.0, 1.0, 1.0],
            properties,
            chunk_hashes,
            created_at: 0.0,
            geometry: None,
        }
    }

    /// 65535 × 65535 float64 is ~34 GB. It must come back as an error; were the
    /// guard missing, this test would try to allocate all of it.
    #[test]
    fn reconstruct_bands_rejects_raster_over_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let item = tiled_item(
            serde_json::json!({
                "earthgrid:width": 65535, "earthgrid:height": 65535,
                "earthgrid:tile_size": 512, "earthgrid:tile_cols": 128, "earthgrid:tile_rows": 128,
                "earthgrid:bands": 1, "earthgrid:dtype": "float64",
                "earthgrid:chunk_format": "spatial-tile",
            }),
            vec![],
        );

        let err = reconstruct_bands(&item, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("65535x65535"), "error must name the refused size: {err}");

        // The guard itself, including the product that overflowed u32.
        assert!(checked_pixel_count(65535, 65535, 8, 1).is_err());
        assert!(checked_pixel_count(65536, 65536, 1, 1).is_err());
        assert_eq!(checked_pixel_count(4, 3, 2, 2).unwrap(), 12);
    }

    #[test]
    fn reconstruct_bands_rejects_zero_width() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let item = tiled_item(
            serde_json::json!({
                "earthgrid:width": 0, "earthgrid:height": 16,
                "earthgrid:tile_size": 16, "earthgrid:tile_cols": 1, "earthgrid:tile_rows": 1,
                "earthgrid:dtype": "uint8",
            }),
            vec![],
        );

        let err = reconstruct_bands(&item, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("width"), "error must name the zero dimension: {err}");
        assert!(checked_pixel_count(16, 0, 1, 1).is_err());
    }

    /// A 1×1 raster with a 1×1 tile grid has room for one chunk. Three present
    /// chunks used to compute `height - row_i * tile_size` = 1 - 2 in u32.
    #[test]
    fn reconstruct_bands_rejects_chunks_outside_tile_grid() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let chunk_hashes: Vec<String> = [b"a", b"b", b"c"]
            .iter()
            .map(|data| store.put(&data[..]).unwrap())
            .collect();
        let item = tiled_item(
            serde_json::json!({
                "earthgrid:width": 1, "earthgrid:height": 1,
                "earthgrid:tile_size": 1, "earthgrid:tile_cols": 1, "earthgrid:tile_rows": 1,
                "earthgrid:dtype": "uint8",
            }),
            chunk_hashes,
        );

        let err = reconstruct_bands(&item, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("outside the 1x1 raster"), "unexpected error: {err}");

        // A grid wider than the raster is refused too, and so is a zero divisor.
        assert!(check_tile_grid(2, 1, 1, 1, 2).is_err());
        assert!(check_tile_grid(1, 1, 1, 1, 0).is_err());
        assert!(check_tile_grid(1, 1, 1, 0, 1).is_err());
    }

    /// The guards must not refuse an honest grid: 3×2 pixels in 2×2 tiles, the
    /// right-hand tiles one pixel wide.
    #[test]
    fn reconstruct_bands_accepts_exact_tile_grid() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let chunk_hashes = vec![
            store.put(&[1, 2, 4, 5]).unwrap(),
            store.put(&[3, 6]).unwrap(),
        ];
        let item = tiled_item(
            serde_json::json!({
                "earthgrid:width": 3, "earthgrid:height": 2,
                "earthgrid:tile_size": 2, "earthgrid:tile_cols": 2, "earthgrid:tile_rows": 1,
                "earthgrid:dtype": "uint8", "earthgrid:band_names": ["B01"],
            }),
            chunk_hashes,
        );

        let bands = reconstruct_bands(&item, &mut store, None).unwrap();
        assert_eq!(bands["B01"], vec![1u8, 2, 3, 4, 5, 6]);
    }

    /// Nested `earthgrid:band_hashes` lists count towards the reference cap even
    /// though the top-level list is empty.
    #[test]
    fn reconstruct_bands_rejects_band_hashes_over_reference_cap() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let half = vec![serde_json::Value::String(String::new()); MAX_CHUNK_REFS / 2 + 1];
        let item = tiled_item(
            serde_json::json!({
                "earthgrid:width": 1, "earthgrid:height": 1,
                "earthgrid:tile_size": 1, "earthgrid:tile_cols": 1, "earthgrid:tile_rows": 1,
                "earthgrid:dtype": "uint8", "earthgrid:chunk_format": "band-level",
                "earthgrid:band_hashes": { "B01": half.clone(), "B02": half },
            }),
            vec![],
        );

        let err = reconstruct_bands(&item, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("chunk references"), "unexpected error: {err}");
        assert!(err.contains(&(MAX_CHUNK_REFS + 2).to_string()), "error must name the count: {err}");
    }

    /// Many references to one small chunk must be refused from the index sizes
    /// alone: 40 000 × 64 KiB is ~2.4 GiB, which this test would otherwise build.
    #[test]
    fn reconstruct_raw_rejects_total_over_budget() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let sha = store.put(&vec![7u8; 64 * 1024]).unwrap();
        let item = tiled_item(serde_json::json!({}), vec![sha; 40_000]);

        let err = reconstruct_cog(&item, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("budget"), "unexpected error: {err}");
    }

    #[test]
    fn crs_allowlist_accepts_epsg_and_wkt() {
        assert!(matches!(parse_allowed_crs("EPSG:4326"), Ok(AllowedCrs::Epsg(4326))));
        assert!(matches!(parse_allowed_crs("epsg:4326"), Ok(AllowedCrs::Epsg(4326))));

        // The slash in the name is inside a quoted string, as in every UTM WKT.
        let wkt = r#"PROJCS["WGS 84 / UTM zone 32N",GEOGCS["WGS 84",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]],PROJECTION["Transverse_Mercator"],PARAMETER["latitude_of_origin",0],PARAMETER["central_meridian",9],PARAMETER["scale_factor",0.9996],PARAMETER["false_easting",500000],PARAMETER["false_northing",0],UNIT["metre",1]]"#;
        assert!(matches!(parse_allowed_crs(wkt), Ok(AllowedCrs::Wkt(_))));
    }

    #[test]
    fn crs_allowlist_rejects_urls_paths_and_other_authorities() {
        for crs in [
            "http://169.254.169.254/",
            "/etc/passwd",
            "/vsi/curl/http://169.254.169.254/latest/meta-data",
            "/vsicurl/http://169.254.169.254/",
            "ESRI:4326",
            "EPSG:",
            "EPSG:1234567",
            "EPSG:4326/../x",
            r"C:\proj\x.prj",
            "+proj=longlat +datum=WGS84",
            "PROJCS",
            r#"PROJCS["x"]/etc/passwd"#,
            "",
        ] {
            let err = match parse_allowed_crs(crs) {
                Ok(_) => panic!("{crs:?} must be refused"),
                Err(e) => e,
            };
            assert!(err.contains("Refusing CRS"), "unexpected error for {crs:?}: {err}");
        }

        // The refused value is quoted back truncated, not in full.
        let long = format!("http://example.invalid/{}", "a".repeat(500));
        let err = parse_allowed_crs(&long).err().unwrap();
        assert!(err.len() < 300, "refused value must be truncated: {err}");
    }

    /// A refused CRS fails the COG write before GDAL is touched.
    #[test]
    fn write_cog_rejects_crs_outside_allowlist() {
        let err = ndvi_cog(&[0, 0], &[0, 0], 1, 1, [0.0, 0.0, 1.0, 1.0], "/etc/passwd", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Refusing CRS"), "unexpected error: {err}");
    }

    #[test]
    fn crs_allowlist_accepts_lowercase_wkt_root() {
        let wkt = r#"projcs["WGS 84 / UTM zone 32N",GEOGCS["WGS 84"]]"#;
        assert!(matches!(parse_allowed_crs(wkt), Ok(AllowedCrs::Wkt(_))));
        assert!(matches!(parse_allowed_crs(r#"GeogCRS["WGS 84"]"#), Ok(AllowedCrs::Wkt(_))));
    }

    /// Each of these opens like allowed WKT (or is bare PROJ text) and hides the
    /// instruction inside a quoted string, where the separator check never looked
    /// — or, for `@` and `|`, carries it outside one.
    #[test]
    fn crs_allowlist_rejects_instruction_text_anywhere() {
        for crs in [
            r#"GEOGCS["https://example.invalid/crs"]"#,
            r#"GEOGCS["x",EXTENSION["PROJ4","+init=epsg:4326"]]"#,
            r#"GEOGCS["/vsicurl/example.invalid"]"#,
            r#"GEOGCS["/VSIMEM/x"]"#,
            r#"GEOGCS["a\b"]"#,
            "+INIT=epsg:4326",
            r#"GEOGCS["x",EXTENSION["PROJ4","+PROJ=longlat +datum=WGS84"]]"#,
            r#"GEOGCS["x",EXTENSION["PROJ4","+nadgrids=ntv1_can.dat"]]"#,
            // `@` and `|` are refused outside quoted sections only.
            r#"GEOGCS["WGS 84"]@relative-grid"#,
            r#"GEOGCS["x"] | GEOGCS["y"]"#,
            "GEOGCS[\"x\0\"]",
            "GEOGCS[\"x\u{1b}[2J\"]",
        ] {
            let err = match parse_allowed_crs(crs) {
                Ok(_) => panic!("{crs:?} must be refused"),
                Err(e) => e,
            };
            assert!(err.contains("Refusing CRS"), "error must name the value for {crs:?}: {err}");
            assert!(err.contains("is refused anywhere in a CRS"), "error must name the rule for {crs:?}: {err}");
        }

        // Inside a quoted WKT name `@` and `|` are descriptive text: GDAL parses
        // this WGS84 definition, so the allowlist must let it through.
        let named = r#"GEOGCS["Survey @ station | reference",DATUM["WGS_1984",SPHEROID["WGS 84",6378137,298.257223563]],PRIMEM["Greenwich",0],UNIT["degree",0.0174532925199433]]"#;
        assert!(matches!(parse_allowed_crs(named), Ok(AllowedCrs::Wkt(_))));

        let long = format!(r#"GEOGCS["https://example.invalid/{}"]"#, "a".repeat(500));
        let err = parse_allowed_crs(&long).err().unwrap();
        assert!(err.len() < 300, "refused value must be truncated: {err}");
    }

    /// 65535 × 65535 is over the budget as well; the CRS refusal must win,
    /// because it is the check that runs before anything is sized.
    #[test]
    fn ndvi_cog_rejects_crs_before_sizing() {
        let err = ndvi_cog(&[], &[], 65535, 65535, [0.0, 0.0, 1.0, 1.0], "/etc/passwd", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("Refusing CRS"), "unexpected error: {err}");
    }

    /// A source whose index lists nothing, counting whole-chunk reads.
    struct UnindexedSource {
        data: Vec<u8>,
        reads: std::cell::Cell<usize>,
    }

    impl ChunkSource for UnindexedSource {
        fn get(&self, _hash: &str) -> Result<Option<Vec<u8>>> {
            self.reads.set(self.reads.get() + 1);
            Ok(Some(self.data.clone()))
        }

        fn has(&self, _hash: &str) -> bool {
            true
        }

        fn chunk_size(&self, hash: &str) -> Result<u64> {
            Err(crate::error::EarthGridError::ChunkNotFound(hash.to_string()))
        }
    }

    /// Sizing must not read a chunk, even one the index cannot size: three
    /// references are three reads, all of them from the copy loop.
    #[test]
    fn reconstruct_raw_sizes_without_reading_chunks() {
        let mut source = UnindexedSource { data: vec![1, 2, 3], reads: std::cell::Cell::new(0) };
        let item = tiled_item(serde_json::json!({}), vec!["sha".to_string(); 3]);

        let rebuilt = reconstruct_raw(&item, &mut source).unwrap();
        assert_eq!(rebuilt, vec![1u8, 2, 3, 1, 2, 3, 1, 2, 3]);
        assert_eq!(source.reads.get(), 3, "sizing must not read chunks");
    }

    /// The NDVI reader consumes every paired input sample, so the inputs are
    /// held to the budget whatever `width` × `height` says. Lengths only: the
    /// slices themselves would be over 2 GiB each.
    #[test]
    fn ndvi_rejects_input_over_budget() {
        let over = (MAX_RECONSTRUCT_BYTES + 4) as usize;
        let err = checked_ndvi_samples(over, over, 4).unwrap_err().to_string();
        assert!(err.contains("NDVI input") && err.contains("budget"), "unexpected error: {err}");

        // The shorter input decides how many pairs there are.
        assert_eq!(checked_ndvi_samples(over, 8, 4).unwrap(), 2);
        assert_eq!(checked_ndvi_samples(8, 8, 2).unwrap(), 4);
    }

    /// Both values pass the lexical gate and neither can be built: no such EPSG
    /// code, and WKT that never closes its bracket. Each must come back as an
    /// error naming the value — never as an output without a CRS.
    #[test]
    fn unbuildable_crs_is_an_error_not_an_output_without_crs() {
        for crs in ["EPSG:999999", r#"PROJCS["x""#] {
            assert!(parse_allowed_crs(crs).is_ok(), "{crs:?} must pass the parser");

            let err = ndvi_cog(&[0, 0], &[0, 0], 1, 1, [0.0, 0.0, 1.0, 1.0], crs, None)
                .unwrap_err()
                .to_string();
            assert!(err.contains("Cannot build CRS"), "unexpected error for {crs:?}: {err}");
            assert!(err.contains(&format!("{crs:?}")), "error must name the value {crs:?}: {err}");
        }

        // The tiled path refuses it too, before it reads a chunk.
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let item = tiled_item(
            serde_json::json!({
                "earthgrid:width": 1, "earthgrid:height": 1,
                "earthgrid:tile_size": 1, "earthgrid:tile_cols": 1, "earthgrid:tile_rows": 1,
                "earthgrid:dtype": "uint8", "earthgrid:crs": "EPSG:999999",
            }),
            vec![],
        );
        let err = reconstruct_cog(&item, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("Cannot build CRS") && err.contains("EPSG:999999"), "unexpected error: {err}");

        // The value is quoted back truncated here as well.
        let long = format!(r#"PROJCS["{}""#, "a".repeat(500));
        let err = parse_allowed_crs(&long).ok().unwrap().build().err().unwrap();
        assert!(err.contains("Cannot build CRS"), "unexpected error: {err}");
        assert!(!err.contains(&"a".repeat(200)), "CRS value must be truncated: {err}");
    }

    /// Too many names, or too many bytes of names, are refused on the borrowed
    /// JSON — before `reconstruct_bands` clones them.
    #[test]
    fn reconstruct_bands_rejects_band_names_over_limits() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = test_store(&dir);
        let item_with = |names: Vec<String>| {
            tiled_item(
                serde_json::json!({
                    "earthgrid:width": 1, "earthgrid:height": 1,
                    "earthgrid:tile_size": 1, "earthgrid:tile_cols": 1, "earthgrid:tile_rows": 1,
                    "earthgrid:dtype": "uint8", "earthgrid:band_names": names,
                }),
                vec![],
            )
        };

        let too_many = item_with(vec!["B".to_string(); MAX_BANDS as usize + 1]);
        let err = reconstruct_bands(&too_many, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("earthgrid:band_names"), "unexpected error: {err}");
        assert!(err.contains(&format!("{} names", MAX_BANDS + 1)), "error must name the count: {err}");

        let too_long = item_with(vec!["B".repeat(MAX_BAND_NAME_BYTES + 1)]);
        let err = reconstruct_bands(&too_long, &mut store, None).unwrap_err().to_string();
        assert!(err.contains("earthgrid:band_names"), "unexpected error: {err}");
        assert!(err.contains(&format!("{} bytes", MAX_BAND_NAME_BYTES + 1)), "error must name the bytes: {err}");

        // At the limits is fine.
        let at_limits = vec!["B".repeat(16); MAX_BANDS as usize];
        assert!(check_band_names(&serde_json::json!({ "earthgrid:band_names": at_limits })).is_ok());
    }

    /// One sample for a 2×2 raster: refused by both counts, before the output
    /// buffer exists and before GDAL's buffer-shape assertion could be reached.
    #[test]
    fn ndvi_cog_rejects_sample_count_mismatch() {
        let err = ndvi_cog(&[0; 4], &[0; 4], 2, 2, [0.0, 0.0, 1.0, 1.0], "EPSG:4326", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("1 samples"), "error must name the sample count: {err}");
        assert!(err.contains("4 pixels"), "error must name the pixel count: {err}");

        // A red band that fits and a short NIR band is a mismatch as well.
        let err = ndvi_cog(&[0; 8], &[0; 4], 2, 2, [0.0, 0.0, 1.0, 1.0], "EPSG:4326", None)
            .unwrap_err()
            .to_string();
        assert!(err.contains("2 samples") && err.contains("4 pixels"), "unexpected error: {err}");
    }

    /// A raster wider and taller than one write window, so the windowed writer
    /// crosses both seams; GDAL must read back every sample where it was put.
    #[test]
    fn write_cog_windows_preserve_every_sample() {
        let (width, height) = (WRITE_WINDOW_COLS + 3, WRITE_WINDOW_ROWS + 2);
        let red: Vec<u8> = (0..width * height).flat_map(|i| ((i % 1000) as u16).to_le_bytes()).collect();
        let nir: Vec<u8> = (0..width * height).flat_map(|i| ((i % 1000) as u16 * 3).to_le_bytes()).collect();

        let cog = ndvi_cog(&red, &nir, width as u32, height as u32, [0.0, 0.0, 1.0, 1.0], "EPSG:4326", None).unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ndvi.tif");
        std::fs::write(&path, cog).unwrap();
        let ds = gdal::Dataset::open(&path).unwrap();
        assert_eq!(ds.raster_size(), (width, height));
        assert!(ds.spatial_ref().is_ok(), "the output must carry its CRS");
        let buf = ds.rasterband(1).unwrap()
            .read_as::<f32>((0, 0), (width, height), (width, height), None)
            .unwrap();
        for (i, got) in buf.data().iter().enumerate() {
            // (3r - r) / (3r + r) = 0.5 wherever r > 0.
            let want = if i % 1000 == 0 { 0.0 } else { 0.5 };
            assert!((got - want).abs() < 1e-6, "sample {i}: got {got}, want {want}");
        }
    }
}
