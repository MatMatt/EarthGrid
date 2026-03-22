//! MGRS tile name → approximate WGS84 bounding box.
//!
//! Converts Sentinel-2 MGRS tile IDs like "32TPS" into a tight center bbox
//! suitable for STAC searches. Not centimeter-accurate — just precise enough
//! to hit the right tile without including neighbors.

use gdal::spatial_ref::{CoordTransform, SpatialRef};

/// Latitude band letters and their southern edge latitude.
/// Bands C-X (skipping I and O), each 8° except X which is 12°.
const BANDS: &[(char, f64)] = &[
    ('C', -80.0), ('D', -72.0), ('E', -64.0), ('F', -56.0),
    ('G', -48.0), ('H', -40.0), ('J', -32.0), ('K', -24.0),
    ('L', -16.0), ('M', -8.0),  ('N', 0.0),   ('P', 8.0),
    ('Q', 16.0),  ('R', 24.0),  ('S', 32.0),  ('T', 40.0),
    ('U', 48.0),  ('V', 56.0),  ('W', 64.0),  ('X', 72.0),
];

/// Column letters for 100km squares, by set (zone % 3).
/// Set 1 (zone%3==1): A-H, Set 2 (zone%3==2): J-R, Set 3 (zone%3==0): S-Z
/// All skip I and O.
const COL_SET1: &[char] = &['A','B','C','D','E','F','G','H'];
const COL_SET2: &[char] = &['J','K','L','M','N','P','Q','R'];
const COL_SET3: &[char] = &['S','T','U','V','W','X','Y','Z'];

/// Row letters cycle: A-H, J-N, P-Z (skip I and O) = 20 letters.
const ROW_LETTERS: &[char] = &[
    'A','B','C','D','E','F','G','H','J','K',
    'L','M','N','P','Q','R','S','T','U','V',
];

fn utm_to_lonlat(zone: u32, is_northern: bool, easting: f64, northing: f64) -> Result<(f64, f64), String> {
    let proj4 = if is_northern {
        format!("+proj=utm +zone={} +datum=WGS84 +units=m +no_defs", zone)
    } else {
        format!("+proj=utm +zone={} +south +datum=WGS84 +units=m +no_defs", zone)
    };
    let src = SpatialRef::from_proj4(&proj4).map_err(|e| format!("UTM SRS error: {e}"))?;
    // Use explicit longlat proj4 to avoid EPSG:4326 axis-order ambiguity (lat/lon vs lon/lat).
    let dst = SpatialRef::from_proj4("+proj=longlat +datum=WGS84 +no_defs")
        .map_err(|e| format!("WGS84 SRS error: {e}"))?;
    let transform = CoordTransform::new(&src, &dst).map_err(|e| format!("CoordTransform error: {e}"))?;

    let mut x = [easting];
    let mut y = [northing];
    let mut z = [0.0];
    transform
        .transform_coords(&mut x, &mut y, &mut z)
        .map_err(|e| format!("Coordinate transform failed: {e}"))?;

    Ok((x[0], y[0]))
}

/// Parse an MGRS tile name (e.g. "32TPS") and return an approximate center bbox.
///
/// Returns `[west, south, east, north]` — a small box (~0.3° wide) centered
/// on the approximate tile center.
pub fn tile_to_bbox(tile: &str) -> Result<[f64; 4], String> {
    let tile = tile.trim().to_uppercase();

    // Parse: 1-2 digit zone + 1 band letter + 2 grid square letters
    if tile.len() < 4 || tile.len() > 5 {
        return Err(format!("Invalid MGRS tile '{}': expected 4-5 chars (e.g. '4QFJ' or '32TPS')", tile));
    }

    // Find where digits end
    let digit_end = tile.chars().take_while(|c| c.is_ascii_digit()).count();
    if digit_end == 0 || digit_end > 2 {
        return Err(format!("Invalid MGRS tile '{}': must start with 1-2 digit zone number", tile));
    }

    let zone: u32 = tile[..digit_end].parse()
        .map_err(|_| format!("Invalid zone in '{}'", tile))?;
    if zone < 1 || zone > 60 {
        return Err(format!("Zone {} out of range (1-60)", zone));
    }

    let remaining = &tile[digit_end..];
    let chars: Vec<char> = remaining.chars().collect();
    if chars.len() != 3 {
        return Err(format!("Expected 3 letters after zone in '{}', got {}", tile, chars.len()));
    }

    let band_letter = chars[0];
    let col_letter = chars[1];
    let row_letter = chars[2];

    // --- Latitude from band letter ---
    let band_south = BANDS.iter()
        .find(|(c, _)| *c == band_letter)
        .map(|(_, lat)| *lat)
        .ok_or_else(|| format!("Invalid band letter '{}' in '{}'", band_letter, tile))?;

    let band_height = if band_letter == 'X' { 12.0 } else { 8.0 };
    let band_north = band_south + band_height;
    let is_northern = band_letter >= 'N';

    // Row letter → 100km northing block in the repeating 20-row cycle.
    let row_idx = ROW_LETTERS.iter().position(|&c| c == row_letter)
        .ok_or_else(|| format!("Invalid row letter '{}' in '{}'", row_letter, tile))?;
    // MGRS set index (1..=6) controls row origin (A for odd sets, F for even sets).
    let set_idx = ((zone - 1) % 6) + 1;
    let row_origin_letter = if set_idx % 2 == 0 { 'F' } else { 'A' };
    let row_origin_idx = ROW_LETTERS
        .iter()
        .position(|&c| c == row_origin_letter)
        .ok_or_else(|| "Internal error: invalid row origin".to_string())?;
    let row_100k = ((row_idx + 20 - row_origin_idx) % 20) as f64;
    let northing_base = row_100k * 100_000.0;

    // Column letter → easting offset
    let col_set = match zone % 3 {
        1 => COL_SET1,
        2 => COL_SET2,
        _ => COL_SET3,
    };

    let col_idx = col_set.iter().position(|&c| c == col_letter)
        .ok_or_else(|| format!(
            "Invalid column letter '{}' for zone {} (expected one of {:?})",
            col_letter, zone, col_set
        ))?;

    // 100km grid square center in UTM coordinates.
    let easting = (col_idx as f64 + 1.5) * 100_000.0;

    // Northing repeats every 2,000,000m. Pick the cycle whose transformed latitude
    // falls inside the tile's latitude band.
    let mut chosen: Option<(f64, f64)> = None;
    let band_center = (band_south + band_north) / 2.0;
    for cycle in 0..=5 {
        let northing = northing_base + 50_000.0 + cycle as f64 * 2_000_000.0;
        let (lon, lat) = match utm_to_lonlat(zone, is_northern, easting, northing) {
            Ok(v) => v,
            Err(_) => continue,
        };
        if lat >= band_south && lat <= band_north {
            let score = (lat - band_center).abs();
            match chosen {
                Some((_, prev_lat)) if (prev_lat - band_center).abs() <= score => {}
                _ => chosen = Some((lon, lat)),
            }
        }
    }
    let (center_lon, center_lat) = chosen.ok_or_else(|| {
        format!(
            "Could not resolve MGRS tile '{}' to a lat/lon within band {} ({}..{})",
            tile, band_letter, band_south, band_north
        )
    })?;

    // --- Build tight bbox ---
    let half_size = 0.15; // ±0.15° ≈ ~17km, well within a 110km tile
    Ok([
        center_lon - half_size,
        center_lat - half_size,
        center_lon + half_size,
        center_lat + half_size,
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_32tps() {
        let bbox = tile_to_bbox("32TPS").unwrap();
        // 32TPS is around Bolzano/South Tyrol: ~11.3°E, ~46.5°N
        assert!(bbox[0] > 10.0 && bbox[0] < 12.0, "west={}", bbox[0]);
        assert!(bbox[1] > 45.5 && bbox[1] < 47.0, "south={}", bbox[1]);
        assert!(bbox[2] > 10.5 && bbox[2] < 12.5, "east={}", bbox[2]);
        assert!(bbox[3] > 46.0 && bbox[3] < 47.5, "north={}", bbox[3]);
    }

    #[test]
    fn test_invalid() {
        assert!(tile_to_bbox("").is_err());
        assert!(tile_to_bbox("99ZZZ").is_err());
        assert!(tile_to_bbox("ABCDE").is_err());
    }

    #[test]
    fn test_center_lat_within_band_t() {
        let bbox = tile_to_bbox("32TPS").unwrap();
        let center_lat = (bbox[1] + bbox[3]) / 2.0;
        assert!((40.0..=48.0).contains(&center_lat), "center_lat={center_lat}");
    }

    #[test]
    fn test_zone_longitude_bounds() {
        let bbox = tile_to_bbox("32TPS").unwrap();
        // Zone 32 spans 6E..12E.
        assert!(bbox[0] >= 6.0 && bbox[2] <= 12.5, "bbox={bbox:?}");
    }

    #[test]
    fn test_southern_band_j_bounds() {
        // Band J spans -32..-24 latitude.
        let bbox = tile_to_bbox("32JLT").unwrap();
        let center_lat = (bbox[1] + bbox[3]) / 2.0;
        assert!((-32.0..=-24.0).contains(&center_lat), "center_lat={center_lat}");

        // Zone 32 spans 6E..12E.
        assert!(bbox[0] >= 6.0 && bbox[2] <= 12.5, "bbox={bbox:?}");
    }
}
