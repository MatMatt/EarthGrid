//! MGRS tile name → approximate WGS84 bounding box.
//!
//! Converts Sentinel-2 MGRS tile IDs like "32TPS" into a tight center bbox
//! suitable for STAC searches. Not centimeter-accurate — just precise enough
//! to hit the right tile without including neighbors.

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

    // Row letter → northing offset within band
    let row_idx = ROW_LETTERS.iter().position(|&c| c == row_letter)
        .ok_or_else(|| format!("Invalid row letter '{}' in '{}'", row_letter, tile))?;

    // Offset row index by 5 for even zones
    let effective_row = if zone % 2 == 0 {
        (row_idx + 20 - 5) % 20
    } else {
        row_idx
    };

    // Each row letter = 100km ≈ ~0.9° latitude
    // Use fraction within band to refine latitude
    let row_fraction = (effective_row as f64 % 9.0) / 9.0; // rough mapping
    let center_lat = band_south + band_height * (row_fraction + 0.05);

    // Clamp to band
    let center_lat = center_lat.max(band_south + 0.1).min(band_south + band_height - 0.1);

    // --- Longitude from zone + column letter ---
    let zone_central_meridian = (zone as f64 - 1.0) * 6.0 - 180.0 + 3.0;

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

    // Column index 0 starts at 100km easting, each step = 100km ≈ ~1° longitude
    // At equator, 100km ≈ 0.9° lon; at 45°N ≈ 1.27° lon; at 60°N ≈ 1.8° lon
    let cos_lat = (center_lat.to_radians()).cos().max(0.3);
    let deg_per_100km = 100.0 / (111.32 * cos_lat);

    // Easting: col_idx relative to zone center (index 4 ≈ 500km easting ≈ center)
    let easting_offset = (col_idx as f64 - 3.5) * deg_per_100km;
    let center_lon = zone_central_meridian + easting_offset;

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
}
