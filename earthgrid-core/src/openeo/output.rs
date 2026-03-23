//! GeoTIFF / netCDF packaging and decode helpers for openEO results.

use gdal::raster::{Buffer, RasterCreationOptions};
use gdal::spatial_ref::SpatialRef;
use gdal::DriverManager;

use crate::openeo::types::RasterMeta;
use super::geoprocess::{self, ResampleSpatialConfig};

pub fn canonical_format(fmt: &str) -> &str {
    match fmt.to_ascii_lowercase().as_str() {
        "gtiff" | "geotiff" | "tiff" | "tif" => "GTiff",
        "netcdf" | "nc" => "netCDF",
        "geojson" | "json" => "GeoJSON",
        "geoparquet" | "parquet" => "GeoParquet",
        _ => "GTiff",
    }
}

pub fn wrap_output(
    pixels: &[u8],
    meta: &RasterMeta,
    format: &str,
) -> Result<(Vec<u8>, &'static str), String> {
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

pub fn wrap_geotiff(pixels: &[u8], meta: &RasterMeta) -> Result<Vec<u8>, String> {
    let bands = meta.band_count.max(1);
    let pixel_count = (meta.width * meta.height) as usize;
    let per_band = pixel_count * 4;
    let expected = per_band * bands;
    if pixels.len() < expected {
        return Err(format!(
            "wrap_geotiff: buffer too small ({} bytes, need {} for {}x{} x {} bands f32)",
            pixels.len(),
            expected,
            meta.width,
            meta.height,
            bands
        ));
    }

    let vsi_path = format!("/vsimem/earthgrid_wrap_{}.tif", uuid::Uuid::new_v4());
    let driver = DriverManager::get_driver_by_name("GTiff")
        .map_err(|e| format!("GDAL GTiff driver: {e}"))?;
    let w = meta.width;
    let h = meta.height;
    let mut ds = driver
        .create_with_band_type_with_options::<f32, _>(
            &vsi_path,
            w,
            h,
            bands,
            &RasterCreationOptions::from_iter(["TILED=YES", "COMPRESS=LZW"]),
        )
        .map_err(|e| format!("GDAL create: {e}"))?;

    ds.set_geo_transform(&meta.transform)
        .map_err(|e| format!("GDAL set geotransform: {e}"))?;
    if let Ok(srs) = SpatialRef::from_definition(&meta.crs) {
        let _ = ds.set_spatial_ref(&srs);
    }

    for b in 0..bands {
        let off = b * per_band;
        let slice = &pixels[off..off + per_band];
        let floats: Vec<f32> = slice
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
            .collect();
        let mut band = ds
            .rasterband(b + 1)
            .map_err(|e| format!("GDAL rasterband {}: {e}", b + 1))?;
        let mut buf = Buffer::new((w, h), floats);
        band.write((0, 0), (w, h), &mut buf)
            .map_err(|e| format!("GDAL write band {}: {e}", b + 1))?;
    }
    drop(ds);

    let bytes = gdal::vsi::get_vsi_mem_file_bytes_owned(&vsi_path)
        .map_err(|e| format!("GDAL vsimem read: {e}"))?;
    let _ = gdal::vsi::unlink_mem_file(&vsi_path);
    Ok(bytes)
}

pub fn wrap_netcdf(pixels: &[u8], meta: &RasterMeta) -> Result<Vec<u8>, String> {
    if meta.band_count > 1 {
        return Err(
            "netCDF export with multiple bands is not yet supported; use GTiff.".to_string(),
        );
    }
    let pixel_count = meta.width * meta.height;
    let expected = pixel_count * 4;
    if pixels.len() < expected {
        return Err(format!(
            "wrap_netcdf: buffer too small ({} bytes, need {} for {}x{} f32)",
            pixels.len(),
            expected,
            meta.width,
            meta.height
        ));
    }
    let floats: Vec<f32> = pixels[..expected]
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect();

    let tmp_path = std::env::temp_dir().join(format!("earthgrid_wrap_{}.nc", uuid::Uuid::new_v4()));
    let tmp_str = tmp_path.to_string_lossy().replace('\\', "/");

    let driver = DriverManager::get_driver_by_name("netCDF")
        .map_err(|e| format!("GDAL netCDF driver: {e}"))?;
    let w = meta.width;
    let h = meta.height;
    let opts_nc4 = RasterCreationOptions::from_iter(["FORMAT=NC4", "COMPRESS=DEFLATE"]);
    let mut ds = driver
        .create_with_band_type_with_options::<f32, _>(&tmp_str, w, h, 1, &opts_nc4)
        .or_else(|e| {
            driver
                .create_with_band_type::<f32, _>(&tmp_str, w, h, 1)
                .map_err(|e2| format!("GDAL netCDF create: {e}; fallback: {e2}"))
        })?;

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

    let bytes = std::fs::read(&tmp_path).map_err(|e| format!("read netCDF tmp: {e}"))?;
    let _ = std::fs::remove_file(&tmp_path);
    Ok(bytes)
}

pub fn decode_geotiff_bsq_f32(raw: &[u8]) -> Result<(Vec<u8>, RasterMeta), String> {
    let tmp = std::env::temp_dir().join(format!("eg_dec_{}.tif", uuid::Uuid::new_v4()));
    std::fs::write(&tmp, raw).map_err(|e| format!("write tmp tiff: {e}"))?;
    let ds = gdal::Dataset::open(&tmp).map_err(|e| format!("GDAL open decode: {e}"))?;
    let (w, h) = ds.raster_size();
    let n = ds.raster_count() as usize;
    if n == 0 {
        return Err("decode_geotiff_bsq_f32: raster has no bands".to_string());
    }
    let gt = ds.geo_transform().map_err(|e| format!("GDAL geotransform: {e}"))?;
    let crs = ds
        .spatial_ref()
        .ok()
        .map(|s| format!("EPSG:{}", s.auth_code().unwrap_or(4326)))
        .unwrap_or_else(|| "EPSG:4326".to_string());
    let mut out = Vec::new();
    for bi in 1..=n {
        let band = ds
            .rasterband(bi)
            .map_err(|e| format!("GDAL band {bi}: {e}"))?;
        let buf = band
            .read_as::<f32>((0, 0), (w, h), (w, h), None)
            .map_err(|e| format!("GDAL read band {bi}: {e}"))?;
        for v in buf.data() {
            out.extend_from_slice(&v.to_le_bytes());
        }
    }
    drop(ds);
    let _ = std::fs::remove_file(&tmp);
    Ok((
        out,
        RasterMeta {
            width: w,
            height: h,
            crs,
            transform: [gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]],
            dtype: "float32".to_string(),
            band_count: n,
        },
    ))
}

/// First band only, f32 (same as pipeline decode inside `execute`).
pub fn decode_geotiff_first_band_f32(raw: &[u8]) -> Result<(Vec<u8>, RasterMeta), String> {
    let tmp = std::env::temp_dir().join(format!("eg_band_{}.tif", uuid::Uuid::new_v4()));
    std::fs::write(&tmp, raw).map_err(|e| format!("write tmp tiff: {e}"))?;
    let ds = gdal::Dataset::open(&tmp).map_err(|e| format!("GDAL open tmp: {e}"))?;
    let (w, h) = ds.raster_size();
    let gt = ds.geo_transform().map_err(|e| format!("GDAL geotransform: {e}"))?;
    let crs = ds
        .spatial_ref()
        .ok()
        .map(|s| format!("EPSG:{}", s.auth_code().unwrap_or(4326)))
        .unwrap_or_else(|| "EPSG:4326".to_string());
    let band = ds.rasterband(1).map_err(|e| format!("GDAL band 1: {e}"))?;
    let buf = band
        .read_as::<f32>((0, 0), (w, h), (w, h), None)
        .map_err(|e| format!("GDAL read: {e}"))?;
    let pixels: Vec<u8> = buf.data().iter().flat_map(|v| v.to_le_bytes()).collect();
    drop(band);
    drop(ds);
    let _ = std::fs::remove_file(&tmp);
    let meta = RasterMeta {
        width: w,
        height: h,
        crs,
        transform: [gt[0], gt[1], gt[2], gt[3], gt[4], gt[5]],
        dtype: "float32".to_string(),
        band_count: 1,
    };
    Ok((pixels, meta))
}

pub fn apply_resample_if_needed(
    pixels: Vec<u8>,
    meta: Option<RasterMeta>,
    res_cfg: Option<&ResampleSpatialConfig>,
) -> Result<(Vec<u8>, Option<RasterMeta>), String> {
    let Some(cfg) = res_cfg else {
        return Ok((pixels, meta));
    };
    let Some(m) = meta else {
        return Err(
            "resample_spatial requires a georeferenced raster (e.g. NDVI or GeoTIFF assets)"
                .to_string(),
        );
    };
    let tiff = wrap_geotiff(&pixels, &m)?;
    let warped = geoprocess::gdal_warp_geotiff(&tiff, cfg)?;
    let (px, m2) = decode_geotiff_bsq_f32(&warped)?;
    Ok((px, Some(m2)))
}
