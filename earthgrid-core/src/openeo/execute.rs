//! Synchronous openEO process graph execution against the local catalog + chunk store.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::NaiveDate;
use tokio::sync::Mutex;

use crate::catalog::{Catalog, DatetimeFilter, StacItem};
use crate::chunk_store::ChunkStore;
use crate::openeo::graph::{
    detect_spectral_dtype, extract_aggregate_temporal_period, extract_band_from_item_id,
    extract_operation, extract_resample_spatial, extract_ymd_from_item_id, parse_requirements,
    resolve_band_alias, truncate_pair, truncate_three,
};
use crate::openeo::output::{
    apply_resample_if_needed, decode_geotiff_first_band_f32, wrap_geotiff,
};
use crate::openeo::types::{ProcessGraph, RasterMeta};
use super::geoprocess;
use crate::processing;

pub fn load_item_bytes(item: &StacItem, store: &mut ChunkStore) -> Result<Vec<u8>, String> {
    let mut raw = Vec::new();
    for hash in &item.chunk_hashes {
        if let Ok(Some(chunk)) = store.get(hash) {
            raw.extend_from_slice(&chunk);
        }
    }
    if raw.is_empty() {
        return Err(format!("Could not load chunk data for item '{}'", item.id));
    }
    Ok(raw)
}

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
    let agg_cfg = extract_aggregate_temporal_period(graph);
    let res_cfg = extract_resample_spatial(graph);
    let search_limit = if agg_cfg.is_some() { 2000 } else { 500 };
    let req = &reqs[0];

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

    let cat = catalog.lock().await;
    let items = cat
        .search(
            Some(&req.collection_id),
            bbox_arr,
            dt_filter.as_ref(),
            search_limit,
            0,
        )
        .map_err(|e| format!("Catalog search error: {}", e))?;
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

    let mut available_bands_initial: Vec<String> = Vec::new();
    for item in &items {
        if let Some(band) = extract_band_from_item_id(&item.id) {
            if !available_bands_initial.contains(&band) {
                available_bands_initial.push(band);
            }
        }
    }

    let missing_bands: Vec<String> = needed_bands
        .iter()
        .filter(|b| !available_bands_initial.contains(b))
        .cloned()
        .collect();

    let items = if !missing_bands.is_empty() && bbox.is_some() {
        let (w, s, e, n) = bbox.unwrap();
        let datetime = req.temporal_extent.as_ref().map(|te| {
            if te.len() >= 2 {
                (te[0].as_str(), te[1].as_str())
            } else {
                (te[0].as_str(), te[0].as_str())
            }
        });
        let (start, end) = datetime.unwrap_or(("2020-01-01", "2030-01-01"));

        tracing::info!(
            "Auto-fetching missing bands {:?} from Element84 (have: {:?})",
            missing_bands,
            available_bands_initial
        );
        let fetch_bands = if needed_bands.is_empty() {
            missing_bands.clone()
        } else {
            needed_bands.clone()
        };
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
            None,
        )
        .await;

        tracing::info!(
            "Auto-fetch done: {} downloaded, {} skipped, {} errors",
            fetch_result.items_downloaded,
            fetch_result.items_skipped,
            fetch_result.errors.len()
        );

        let cat = catalog.lock().await;
        let refreshed = cat
            .search(
                Some(&req.collection_id),
                bbox_arr,
                dt_filter.as_ref(),
                search_limit,
                0,
            )
            .map_err(|e| format!("Catalog re-search error: {}", e))?;
        drop(cat);
        refreshed
    } else {
        items
    };

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

    fn meta_from_item(item: &StacItem) -> Option<RasterMeta> {
        let p = &item.properties;
        let width = p.get("earthgrid:width")?.as_u64()? as usize;
        let height = p.get("earthgrid:height")?.as_u64()? as usize;
        let crs = p.get("earthgrid:crs")?.as_str()?.to_string();
        let dtype = p
            .get("earthgrid:dtype")
            .and_then(|v| v.as_str())
            .unwrap_or("float32")
            .to_string();
        let tf = p.get("earthgrid:transform")?.as_array()?;
        if tf.len() < 6 {
            return None;
        }
        // earthgrid:transform is stored as [pixel_x, rot, origin_x, rot, pixel_y, origin_y]
        // GDAL expects [origin_x, pixel_x, rot, origin_y, rot, pixel_y]
        let raw: Vec<f64> = (0..6).map(|i| tf[i].as_f64().unwrap_or(0.0)).collect();
        let transform = if raw[0].abs() < raw[2].abs() {
            // Stored order: [px, rot, ox, rot, py, oy] → swap to GDAL order
            [raw[2], raw[0], raw[1], raw[5], raw[3], raw[4]]
        } else {
            // Already GDAL order: [ox, px, rot, oy, rot, py]
            [raw[0], raw[1], raw[2], raw[3], raw[4], raw[5]]
        };
        Some(RasterMeta {
            width,
            height,
            crs,
            transform,
            dtype,
            band_count: 1,
        })
    }

    let load_band = |band: &str, store_locked: &mut ChunkStore| -> Result<(Vec<u8>, Option<RasterMeta>), String> {
        let resolved = band_items
            .get(band)
            .or_else(|| band_items.get(resolve_band_alias(band)));
        let items = resolved.ok_or_else(|| {
            format!(
                "Band '{}' not found. Needed: {:?}. Available: {:?}. Items checked: {}.",
                band,
                needed_bands,
                avail,
                items.len()
            )
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
            let (pixels, meta) = decode_geotiff_first_band_f32(&raw)?;
            Ok((pixels, Some(meta)))
        } else {
            let meta = meta_from_item(item);
            Ok((raw, meta))
        }
    };

    if let Some(ref acfg) = agg_cfg {
        if op.operation.as_deref() != Some("ndvi") {
            return Err(
                "aggregate_temporal_period is only wired for NDVI process graphs on this backend"
                    .to_string(),
            );
        }

        let mut by_date: BTreeMap<NaiveDate, HashMap<String, &StacItem>> = BTreeMap::new();
        for item in &items {
            let Some(band) = extract_band_from_item_id(&item.id) else {
                continue;
            };
            let Some(d) = extract_ymd_from_item_id(&item.id) else {
                continue;
            };
            let m = by_date.entry(d).or_default();
            m.entry(band)
                .and_modify(|cur| {
                    if item.chunk_hashes.len() > cur.chunk_hashes.len() {
                        *cur = item;
                    }
                })
                .or_insert(item);
        }

        if by_date.is_empty() {
            return Err(
                "aggregate_temporal_period: no catalog items with YYYYMMDD dates in their ids"
                    .to_string(),
            );
        }

        let mut layers_by_period: HashMap<String, Vec<Vec<f32>>> = HashMap::new();
        let mut ref_meta: Option<RasterMeta> = None;
        let mut store = store.lock().await;
        for (d, bands_map) in by_date.iter() {
            let red_item = bands_map
                .get(&op.red)
                .or_else(|| bands_map.get(resolve_band_alias(&op.red)));
            let nir_item = bands_map
                .get(&op.nir)
                .or_else(|| bands_map.get(resolve_band_alias(&op.nir)));
            let (Some(ri), Some(ni)) = (red_item, nir_item) else {
                continue;
            };
            let raw_r = load_item_bytes(ri, &mut store)?;
            let raw_n = load_item_bytes(ni, &mut store)?;
            let (red_data, mr) = if is_tiff(&raw_r) {
                let (p, m) = decode_geotiff_first_band_f32(&raw_r)?;
                (p, Some(m))
            } else {
                (raw_r, meta_from_item(ri))
            };
            let nir_data = if is_tiff(&raw_n) {
                decode_geotiff_first_band_f32(&raw_n)?.0
            } else {
                raw_n
            };
            let Some(base_meta) = mr else {
                return Err(
                    "aggregate_temporal_period: each date needs GeoTIFF-based bands with CRS/transform"
                        .to_string(),
                );
            };
            if ref_meta.is_none() {
                ref_meta = Some(base_meta.clone());
            }
            let (red_data, nir_data) = truncate_pair("NDVI", &red_data, &nir_data)?;
            let dtype = base_meta.dtype.as_str();
            let ndvi_u8 = processing::compute_ndvi(red_data, nir_data, dtype);
            let ndvi_f32: Vec<f32> = ndvi_u8
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            let label = geoprocess::temporal_period_label(&acfg.period, *d)?;
            layers_by_period.entry(label).or_default().push(ndvi_f32);
        }
        drop(store);

        if layers_by_period.is_empty() {
            return Err(
                "aggregate_temporal_period: no dates with both red and NIR assets".to_string(),
            );
        }

        let mut sorted_labels: Vec<String> = layers_by_period.keys().cloned().collect();
        sorted_labels.sort();
        let mut reduced_bands: Vec<Vec<f32>> = Vec::new();
        for label in &sorted_labels {
            let layers = layers_by_period
                .get(label)
                .ok_or_else(|| "aggregate_temporal_period: internal label mismatch".to_string())?;
            reduced_bands.push(geoprocess::reduce_layers(acfg.reducer, layers)?);
        }

        let mut pixels_out = Vec::new();
        for band in &reduced_bands {
            for v in band {
                pixels_out.extend_from_slice(&v.to_le_bytes());
            }
        }
        let mut meta = ref_meta
            .ok_or_else(|| "aggregate_temporal_period: no valid scenes processed".to_string())?;
        meta.band_count = reduced_bands.len().max(1);
        return apply_resample_if_needed(pixels_out, Some(meta), res_cfg.as_ref());
    }

    let (pixels, meta): (Vec<u8>, Option<RasterMeta>) = match op.operation.as_deref() {
        Some("ndvi") => {
            let mut store = store.lock().await;
            let (red_data, meta) = load_band(&op.red, &mut store)?;
            let (nir_data, _) = load_band(&op.nir, &mut store)?;
            let (red_data, nir_data) = truncate_pair("NDVI", &red_data, &nir_data)?;
            let dtype = meta.as_ref().map(|m| m.dtype.as_str()).unwrap_or_else(|| detect_spectral_dtype(red_data));
            Ok((processing::compute_ndvi(red_data, nir_data, dtype), meta))
        }
        Some("ndwi") => {
            let mut store = store.lock().await;
            let (green_data, meta) = load_band(&op.green, &mut store)?;
            let (nir_data, _) = load_band(&op.nir, &mut store)?;
            let (green_data, nir_data) = truncate_pair("NDWI", &green_data, &nir_data)?;
            let dtype = meta.as_ref().map(|m| m.dtype.as_str()).unwrap_or_else(|| detect_spectral_dtype(green_data));
            Ok((processing::compute_ndwi(green_data, nir_data, dtype), meta))
        }
        Some("evi") => {
            let mut store = store.lock().await;
            let (blue_data, _) = load_band(&op.blue, &mut store)?;
            let (red_data, meta) = load_band(&op.red, &mut store)?;
            let (nir_data, _) = load_band(&op.nir, &mut store)?;
            let (blue_data, red_data, nir_data) =
                truncate_three("EVI", &blue_data, &red_data, &nir_data)?;
            let dtype = meta.as_ref().map(|m| m.dtype.as_str()).unwrap_or_else(|| detect_spectral_dtype(red_data));
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
                Err("No chunk data found".to_string())
            } else {
                let meta = meta_from_item(first);
                Ok((raw, meta))
            }
        }
    }?;

    let (pixels, meta) = apply_resample_if_needed(pixels, meta, res_cfg.as_ref())?;

    // Clip to requested bbox (spatial_extent from load_collection)
    if let (Some((w, s, e, n)), Some(m)) = (bbox, &meta) {
        let tiff = wrap_geotiff(&pixels, m)?;
        let clipped = geoprocess::gdal_clip_bbox(&tiff, w, s, e, n)?;
        let (px, m2) = decode_geotiff_first_band_f32(&clipped)?;
        Ok((px, Some(m2)))
    } else {
        Ok((pixels, meta))
    }
}
