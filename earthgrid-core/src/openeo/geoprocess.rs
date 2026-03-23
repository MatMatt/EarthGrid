//! openEO helpers: `aggregate_temporal_period` labels/reducers and `resample_spatial` via `gdalwarp`.

use std::fs;
use std::process::Command;

use chrono::{Datelike, NaiveDate};
use serde_json::Value;

// ---------------------------------------------------------------------------
// Reducer + temporal period labels
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReducerKind {
    Mean,
    Min,
    Max,
    Median,
}

#[derive(Debug, Clone)]
pub struct AggregateTemporalPeriodConfig {
    pub period: String,
    #[allow(dead_code)]
    pub dimension: Option<String>,
    pub reducer: ReducerKind,
}

#[derive(Debug, Clone)]
pub struct ResampleSpatialConfig {
    pub res_x: f64,
    pub res_y: f64,
    pub change_resolution: bool,
    pub epsg: Option<i32>,
    #[allow(dead_code)]
    pub wkt: Option<String>,
    pub method: String,
}

/// Detect reducer from openEO callback / process_graph JSON (R/Python clients vary slightly).
pub fn detect_reducer_from_value(reducer: &Value) -> ReducerKind {
    let s = reducer.to_string();
    if s.contains("\"process_id\":\"median\"") || s.contains("\"process_id\": \"median\"") {
        return ReducerKind::Median;
    }
    if s.contains("\"process_id\":\"min\"") || s.contains("\"process_id\": \"min\"") {
        return ReducerKind::Min;
    }
    if s.contains("\"process_id\":\"max\"") || s.contains("\"process_id\": \"max\"") {
        return ReducerKind::Max;
    }
    ReducerKind::Mean
}

/// Calendar / hierarchy label per [openEO aggregate_temporal_period](https://processes.openeo.org/#aggregate_temporal_period).
pub fn temporal_period_label(period: &str, date: NaiveDate) -> Result<String, String> {
    let y = date.year();
    let m = date.month();
    let d = date.day();
    match period {
        // EarthGrid items are usually daily; use 12:00 UTC as nominal hour for the label.
        "hour" => Ok(format!("{}-12", date.format("%Y-%m-%d"))),
        "day" => Ok(format!("{}-{:03}", y, date.ordinal())),
        "week" => {
            let iw = date.iso_week();
            Ok(format!("{}-{:02}", iw.year(), iw.week()))
        }
        "dekad" => {
            let bracket = if d <= 10 {
                0
            } else if d <= 20 {
                1
            } else {
                2
            };
            let idx = (m - 1) * 3 + bracket;
            Ok(format!("{}-{:02}", y, idx))
        }
        "month" => Ok(format!("{}-{:02}", y, m)),
        "season" => Ok(calendar_season_label(date)),
        "tropical-season" => Ok(tropical_season_label(date)),
        "year" => Ok(format!("{y}")),
        "decade" => {
            let start = (y / 10) * 10;
            Ok(format!("{start}"))
        }
        "decade-ad" => {
            let start = (y - 1) / 10 * 10 + 1;
            Ok(format!("{start}"))
        }
        _ => Err(format!("Unsupported aggregate_temporal_period period: {period}")),
    }
}

fn calendar_season_label(d: NaiveDate) -> String {
    let m = d.month();
    let y = d.year();
    let (yy, tag) = match m {
        12 | 1 | 2 => {
            let yy = if m == 12 { y } else { y - 1 };
            (yy, "djf")
        }
        3 | 4 | 5 => (y, "mam"),
        6 | 7 | 8 => (y, "jja"),
        _ => (y, "son"),
    };
    format!("{yy}-{tag}")
}

fn tropical_season_label(d: NaiveDate) -> String {
    let m = d.month();
    let y = d.year();
    if (5..=10).contains(&m) {
        format!("{y}-mjjaso")
    } else {
        let yy = if m >= 11 { y } else { y - 1 };
        format!("{yy}-ndjfma")
    }
}

/// Per-pixel reduction across scenes (each layer is `width*height` f32 values).
pub fn reduce_layers(kind: ReducerKind, layers: &[Vec<f32>]) -> Result<Vec<f32>, String> {
    if layers.is_empty() {
        return Err("aggregate_temporal_period: no input layers for reducer".to_string());
    }
    let n = layers[0].len();
    if layers.iter().any(|l| l.len() != n) {
        return Err("aggregate_temporal_period: layer size mismatch".to_string());
    }
    let mut out = vec![0.0f32; n];
    match kind {
        ReducerKind::Mean => {
            for i in 0..n {
                let mut s = 0.0f64;
                for layer in layers {
                    s += layer[i] as f64;
                }
                out[i] = (s / layers.len() as f64) as f32;
            }
        }
        ReducerKind::Min => {
            for i in 0..n {
                out[i] = layers
                    .iter()
                    .map(|l| l[i])
                    .fold(f32::INFINITY, f32::min);
            }
        }
        ReducerKind::Max => {
            for i in 0..n {
                out[i] = layers
                    .iter()
                    .map(|l| l[i])
                    .fold(f32::NEG_INFINITY, f32::max);
            }
        }
        ReducerKind::Median => {
            let mut buf = Vec::with_capacity(layers.len());
            for i in 0..n {
                buf.clear();
                for layer in layers {
                    buf.push(layer[i]);
                }
                buf.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let mid = buf.len() / 2;
                out[i] = if buf.len() % 2 == 1 {
                    buf[mid]
                } else {
                    0.5 * (buf[mid - 1] + buf[mid])
                };
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// GDAL resample / warp
// ---------------------------------------------------------------------------

fn gdal_resample_flag(method: &str) -> &'static str {
    match method {
        "average" => "average",
        "bilinear" => "bilinear",
        "cubic" => "cubic",
        "cubicspline" => "cubicspline",
        "lanczos" => "lanczos",
        "max" => "max",
        "med" => "med",
        "min" => "min",
        "mode" => "mode",
        "near" => "near",
        "q1" => "q1",
        "q3" => "q3",
        "rms" => "rms",
        "sum" => "sum",
        _ => "near",
    }
}

/// Warp a GeoTIFF using the `gdalwarp` executable (ships with GDAL; e.g. `brew install gdal`).
pub fn gdal_warp_geotiff(src_tiff: &[u8], cfg: &ResampleSpatialConfig) -> Result<Vec<u8>, String> {
    if !cfg.change_resolution && cfg.epsg.is_none() && cfg.wkt.is_none() {
        return Err(
            "resample_spatial: at least one of resolution or projection must be set".to_string(),
        );
    }

    let id = uuid::Uuid::new_v4();
    let tmp_dir = std::env::temp_dir();
    let src_path = tmp_dir.join(format!("eg_warp_src_{id}.tif"));
    let dst_path = tmp_dir.join(format!("eg_warp_dst_{id}.tif"));

    fs::write(&src_path, src_tiff).map_err(|e| format!("write warp src: {e}"))?;

    let mut cmd = Command::new("gdalwarp");
    cmd.arg("-q")
        .arg("-overwrite")
        .arg("-of")
        .arg("GTiff")
        .arg("-r")
        .arg(gdal_resample_flag(&cfg.method));

    if let Some(epsg) = cfg.epsg {
        cmd.arg("-t_srs").arg(format!("EPSG:{epsg}"));
    } else if let Some(ref wkt) = cfg.wkt {
        cmd.arg("-t_srs").arg(wkt);
    }

    if cfg.change_resolution {
        cmd.arg("-tr")
            .arg(format!("{}", cfg.res_x))
            .arg(format!("{}", cfg.res_y));
    }

    cmd.arg(&src_path).arg(&dst_path);

    let status = cmd
        .status()
        .map_err(|e| format!("failed to spawn gdalwarp: {e}. Is gdalwarp on PATH?"))?;
    if !status.success() {
        let _ = fs::remove_file(&src_path);
        let _ = fs::remove_file(&dst_path);
        return Err(format!(
            "gdalwarp exited with {status}. Check resolution, projection, and GDAL install."
        ));
    }

    let bytes = fs::read(&dst_path).map_err(|e| format!("read warp output: {e}"))?;
    let _ = fs::remove_file(&src_path);
    let _ = fs::remove_file(&dst_path);
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn temporal_label_month() {
        let d = NaiveDate::from_ymd_opt(2024, 3, 15).unwrap();
        assert_eq!(temporal_period_label("month", d).unwrap(), "2024-03");
    }

    #[test]
    fn temporal_label_ordinal_day() {
        let d = NaiveDate::from_ymd_opt(2024, 1, 1).unwrap();
        assert_eq!(temporal_period_label("day", d).unwrap(), "2024-001");
    }

    #[test]
    fn temporal_label_season_djf_january() {
        let d = NaiveDate::from_ymd_opt(2025, 1, 10).unwrap();
        assert_eq!(temporal_period_label("season", d).unwrap(), "2024-djf");
    }

    #[test]
    fn temporal_label_season_mam() {
        let d = NaiveDate::from_ymd_opt(2025, 4, 1).unwrap();
        assert_eq!(temporal_period_label("season", d).unwrap(), "2025-mam");
    }

    #[test]
    fn reduce_mean_two_layers() {
        let a = vec![1.0f32, 2.0, 3.0];
        let b = vec![3.0f32, 4.0, 5.0];
        let out = reduce_layers(ReducerKind::Mean, &[a, b]).unwrap();
        assert_eq!(out, vec![2.0, 3.0, 4.0]);
    }

    #[test]
    fn reduce_median_three() {
        let a = vec![1.0f32, 0.0];
        let b = vec![3.0f32, 10.0];
        let c = vec![2.0f32, 20.0];
        let out = reduce_layers(ReducerKind::Median, &[a, b, c]).unwrap();
        assert_eq!(out[0], 2.0);
    }

    #[test]
    #[ignore = "requires gdalwarp on PATH (e.g. gdal-bin / brew install gdal)"]
    fn gdal_warp_same_crs_change_resolution_roundtrip_meta() {
        use gdal::raster::{Buffer, RasterCreationOptions};
        use gdal::spatial_ref::SpatialRef;
        use gdal::Dataset;
        use gdal::DriverManager;

        let meta_w = 20usize;
        let meta_h = 10usize;
        let transform = [12.4f64, 0.001, 0.0, 55.8, 0.0, -0.001];
        let mut floats = vec![0.0f32; meta_w * meta_h];
        for (i, v) in floats.iter_mut().enumerate() {
            *v = i as f32;
        }

        let disk_src = std::env::temp_dir().join(format!("eg_test_warp_src_{}.tif", uuid::Uuid::new_v4()));
        let driver = DriverManager::get_driver_by_name("GTiff").expect("GTiff");
        let mut ds = driver
            .create_with_band_type_with_options::<f32, _>(
                disk_src.to_string_lossy().as_ref(),
                meta_w,
                meta_h,
                1,
                &RasterCreationOptions::from_iter(["TILED=YES"]),
            )
            .expect("create");
        ds.set_geo_transform(&transform).expect("gt");
        let srs = SpatialRef::from_epsg(4326).expect("epsg");
        ds.set_spatial_ref(&srs).expect("srs");
        let mut band = ds.rasterband(1).expect("band");
        let mut buf = Buffer::new((meta_w, meta_h), floats);
        band.write((0, 0), (meta_w, meta_h), &mut buf)
            .expect("write");
        drop(band);
        drop(ds);

        let src_bytes = fs::read(&disk_src).expect("read src");
        let _ = fs::remove_file(&disk_src);

        let cfg = ResampleSpatialConfig {
            res_x: 0.002,
            res_y: 0.002,
            change_resolution: true,
            epsg: None,
            wkt: None,
            method: "near".to_string(),
        };
        let warped = gdal_warp_geotiff(&src_bytes, &cfg).expect("warp");
        assert!(warped.len() > 100);

        let tmp = std::env::temp_dir().join(format!("eg_warp_check_{}.tif", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, &warped).expect("write tmp");
        let ds2 = Dataset::open(&tmp).expect("open warped");
        let (w2, h2) = ds2.raster_size();
        assert!(w2 > 0 && h2 > 0);
        assert_ne!((w2, h2), (meta_w, meta_h));
        let _ = std::fs::remove_file(&tmp);
    }
}
