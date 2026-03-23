//! Parsing `load_collection` requirements and extracting operations from process graphs.

use chrono::NaiveDate;

use crate::catalog::StacItem;
use crate::openeo::types::{DataRequirement, OpenEOBBox, OperationInfo, ProcessGraph};
use super::geoprocess::{self, AggregateTemporalPeriodConfig, ResampleSpatialConfig};

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

pub fn detect_spectral_dtype(data: &[u8]) -> &'static str {
    if data.len() % 4 == 0 {
        "float32"
    } else if data.len() % 2 == 0 {
        "uint16"
    } else {
        "float32"
    }
}

pub fn truncate_pair<'a>(op: &str, a: &'a [u8], b: &'a [u8]) -> Result<(&'a [u8], &'a [u8]), String> {
    let min_len = a.len().min(b.len());
    if min_len == 0 {
        return Err(format!("{op}: empty input buffer"));
    }
    if a.len() != b.len() {
        tracing::warn!(
            "{}: input size mismatch ({} vs {}), truncating both to {} bytes",
            op,
            a.len(),
            b.len(),
            min_len
        );
    }
    Ok((&a[..min_len], &b[..min_len]))
}

pub fn truncate_three<'a>(
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
            op,
            a.len(),
            b.len(),
            c.len(),
            min_len
        );
    }
    Ok((&a[..min_len], &b[..min_len], &c[..min_len]))
}

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

pub fn extract_aggregate_temporal_period(graph: &ProcessGraph) -> Option<AggregateTemporalPeriodConfig> {
    let mut keys: Vec<&String> = graph.process_graph.keys().collect();
    keys.sort();
    for k in keys {
        let node = &graph.process_graph[k];
        if node.process_id != "aggregate_temporal_period" {
            continue;
        }
        let period = node
            .arguments
            .get("period")
            .and_then(|v| v.as_str())
            .unwrap_or("month")
            .to_string();
        let dimension = node
            .arguments
            .get("dimension")
            .and_then(|v| v.as_str())
            .map(String::from);
        let reducer = node
            .arguments
            .get("reducer")
            .map(geoprocess::detect_reducer_from_value)
            .unwrap_or(geoprocess::ReducerKind::Mean);
        return Some(AggregateTemporalPeriodConfig {
            period,
            dimension,
            reducer,
        });
    }
    None
}

pub fn extract_resample_spatial(graph: &ProcessGraph) -> Option<ResampleSpatialConfig> {
    let mut keys: Vec<&String> = graph.process_graph.keys().collect();
    keys.sort();
    for k in keys {
        let node = &graph.process_graph[k];
        if node.process_id != "resample_spatial" {
            continue;
        }
        let args = &node.arguments;
        let (res_x, res_y, change_resolution) = match args.get("resolution") {
            Some(v) if v.is_null() => (0.0, 0.0, false),
            Some(v) if v.as_f64().is_some() => {
                let r = v.as_f64().unwrap_or(0.0);
                if r > 0.0 {
                    (r, r, true)
                } else {
                    (0.0, 0.0, false)
                }
            }
            Some(v) if v.as_array().is_some() => {
                let a = v.as_array().unwrap();
                let x = a.first().and_then(|x| x.as_f64()).unwrap_or(0.0);
                let y = a.get(1).and_then(|x| x.as_f64()).unwrap_or(x);
                if x > 0.0 && y > 0.0 {
                    (x, y, true)
                } else {
                    (0.0, 0.0, false)
                }
            }
            _ => (0.0, 0.0, false),
        };

        let (epsg, wkt) = match args.get("projection") {
            Some(v) if v.is_null() => (None, None),
            Some(v) if v.as_i64().is_some() => (v.as_i64().map(|i| i as i32), None),
            Some(v) if v.as_str().is_some() => (None, v.as_str().map(String::from)),
            _ => (None, None),
        };

        let method = args
            .get("method")
            .and_then(|v| v.as_str())
            .unwrap_or("near")
            .to_string();

        if !change_resolution && epsg.is_none() && wkt.is_none() {
            return None;
        }

        return Some(ResampleSpatialConfig {
            res_x,
            res_y,
            change_resolution,
            epsg,
            wkt,
            method,
        });
    }
    None
}

pub fn resolve_band_alias(band: &str) -> &str {
    match band {
        "B01" => "coastal",
        "coastal" => "B01",
        "B02" => "blue",
        "blue" => "B02",
        "B03" => "green",
        "green" => "B03",
        "B04" => "red",
        "red" => "B04",
        "B05" => "rededge1",
        "rededge1" => "B05",
        "B06" => "rededge2",
        "rededge2" => "B06",
        "B07" => "rededge3",
        "rededge3" => "B07",
        "B08" => "nir",
        "nir" => "B08",
        "B8A" => "nir08",
        "nir08" => "B8A",
        "B09" => "nir09",
        "nir09" => "B09",
        "B11" => "swir16",
        "swir16" => "B11",
        "B12" => "swir22",
        "swir22" => "B12",
        "SCL" => "scl",
        "scl" => "SCL",
        _ => band,
    }
}

pub fn extract_band_from_item_id(item_id: &str) -> Option<String> {
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
    let parts: Vec<&str> = item_id.rsplitn(2, '_').collect();
    if parts.len() == 2 {
        let last = parts[0];
        let band = last.trim_end_matches(|c: char| c.is_ascii_digit() || c == 'm');
        if !band.is_empty() && band.len() <= 3 {
            return Some(band.to_string());
        }
    }
    None
}

pub fn extract_ymd_from_item_id(id: &str) -> Option<NaiveDate> {
    for part in id.split('_') {
        if part.len() == 8 && part.chars().all(|c| c.is_ascii_digit()) {
            return NaiveDate::parse_from_str(part, "%Y%m%d").ok();
        }
    }
    None
}

#[allow(dead_code)]
pub fn extract_date_from_item(item: &StacItem) -> String {
    if let Some(dt) = item.properties.get("datetime").and_then(|v| v.as_str()) {
        if dt != "None" && !dt.is_empty() {
            return dt[..10.min(dt.len())].to_string();
        }
    }
    let parts: Vec<&str> = item.id.split('_').collect();
    for p in &parts {
        if p.len() == 8 && p.chars().all(|c| c.is_ascii_digit()) {
            return format!("{}-{}-{}", &p[..4], &p[4..6], &p[6..8]);
        }
    }
    "unknown".to_string()
}
