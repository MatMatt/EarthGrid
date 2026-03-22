//! Raster processing operations for EarthGrid.
//!
//! Pure computation functions — no I/O, no state.
//! All inputs are raw pixel bytes (u16 little-endian or f32 little-endian).
//! All outputs are f32 little-endian bytes.
//!
//! Ported from processing.py.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Pixel decoding helpers
// ---------------------------------------------------------------------------

/// Decode raw bytes to f32 pixel values based on dtype.
/// Supports "uint16" (u16 LE) and "float32" (f32 LE).
fn decode_pixels(data: &[u8], dtype: &str) -> Vec<f32> {
    match dtype {
        "uint16" | "u16" => data
            .chunks_exact(2)
            .map(|b| u16::from_le_bytes([b[0], b[1]]) as f32)
            .collect(),
        "float32" | "f32" => data
            .chunks_exact(4)
            .map(|b| f32::from_le_bytes([b[0], b[1], b[2], b[3]]))
            .collect(),
        _ => {
            // Default: try uint16
            data.chunks_exact(2)
                .map(|b| u16::from_le_bytes([b[0], b[1]]) as f32)
                .collect()
        }
    }
}

/// Encode f32 pixel values to raw bytes (f32 LE).
fn encode_f32(values: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(values.len() * 4);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
    out
}

// ---------------------------------------------------------------------------
// Spectral index functions
// ---------------------------------------------------------------------------

/// Compute NDVI = (NIR - Red) / (NIR + Red).
///
/// Input bytes are raw u16 or f32 pixels. Output is f32 bytes.
/// Returns values in [-1.0, 1.0]; zero where denominator is 0.
pub fn compute_ndvi(red: &[u8], nir: &[u8], dtype: &str) -> Vec<u8> {
    let r = decode_pixels(red, dtype);
    let n = decode_pixels(nir, dtype);
    let result: Vec<f32> = r.iter().zip(n.iter()).map(|(r, n)| {
        let denom = n + r;
        if denom == 0.0 { 0.0 } else { (n - r) / denom }
    }).collect();
    encode_f32(&result)
}

/// Compute NDWI = (Green - NIR) / (Green + NIR).
///
/// Input bytes are raw u16 or f32 pixels. Output is f32 bytes.
pub fn compute_ndwi(green: &[u8], nir: &[u8], dtype: &str) -> Vec<u8> {
    let g = decode_pixels(green, dtype);
    let n = decode_pixels(nir, dtype);
    let result: Vec<f32> = g.iter().zip(n.iter()).map(|(g, n)| {
        let denom = g + n;
        if denom == 0.0 { 0.0 } else { (g - n) / denom }
    }).collect();
    encode_f32(&result)
}

/// Compute EVI = 2.5 * (NIR - Red) / (NIR + 6*Red - 7.5*Blue + 1).
///
/// Input bytes are raw u16 or f32 pixels. Output is f32 bytes.
/// Values are clamped to [-1.0, 1.0].
pub fn compute_evi(blue: &[u8], red: &[u8], nir: &[u8], dtype: &str) -> Vec<u8> {
    let b = decode_pixels(blue, dtype);
    let r = decode_pixels(red, dtype);
    let n = decode_pixels(nir, dtype);
    let result: Vec<f32> = b.iter().zip(r.iter()).zip(n.iter()).map(|((b, r), n)| {
        let denom = n + 6.0 * r - 7.5 * b + 1.0;
        if denom == 0.0 {
            0.0
        } else {
            (2.5 * (n - r) / denom).clamp(-1.0, 1.0)
        }
    }).collect();
    encode_f32(&result)
}

/// Compute binary cloud mask from SCL (Scene Classification Layer).
///
/// SCL classes 8 (cloud medium prob), 9 (cloud high prob), 10 (thin cirrus)
/// are treated as cloud (output 0.0). All other classes → 1.0 (clear).
///
/// Input: u8 SCL pixel values (one byte per pixel).
/// Output: f32 bytes (1.0 = clear, 0.0 = cloud).
pub fn cloud_mask(scl: &[u8]) -> Vec<u8> {
    let result: Vec<f32> = scl.iter().map(|&v| {
        match v {
            8 | 9 | 10 => 0.0_f32, // cloud
            _ => 1.0_f32,           // clear
        }
    }).collect();
    encode_f32(&result)
}

/// Evaluate a simple band-math expression over named pixel arrays.
///
/// Supported operators: `+`, `-`, `*`, `/`.
/// Band names must match keys in `bands`. Expression must be of the form:
/// `B04 / B08` or `(B08 - B04) / (B08 + B04)` etc.
///
/// Input bytes are raw u16 or f32 pixels. Output is f32 bytes.
///
/// # Supported expression syntax
/// This is a minimal arithmetic evaluator. It supports:
/// - Band identifiers (alphanumeric + underscore)
/// - Numeric literals (integer and float)
/// - Binary operators: `+`, `-`, `*`, `/`
/// - Parentheses
///
/// # Panics / Errors
/// Returns all-zeros on parse failure.
pub fn band_math(
    expr: &str,
    bands: &HashMap<String, &[u8]>,
    dtype: &str,
) -> Vec<u8> {
    // Decode all referenced bands
    let decoded: HashMap<String, Vec<f32>> = bands
        .iter()
        .map(|(k, v)| (k.clone(), decode_pixels(v, dtype)))
        .collect();

    let n_pixels = decoded.values().next().map(|v| v.len()).unwrap_or(0);
    if n_pixels == 0 {
        return Vec::new();
    }

    // Evaluate expression pixel-by-pixel
    let mut result = vec![0.0f32; n_pixels];
    for i in 0..n_pixels {
        let pixel_vals: HashMap<&str, f32> = decoded
            .iter()
            .map(|(k, v)| (k.as_str(), v[i]))
            .collect();
        result[i] = eval_expr(expr.trim(), &pixel_vals).unwrap_or(0.0);
    }
    encode_f32(&result)
}

// ---------------------------------------------------------------------------
// Minimal arithmetic expression evaluator
// ---------------------------------------------------------------------------

fn eval_expr(expr: &str, vars: &HashMap<&str, f32>) -> Option<f32> {
    let tokens = tokenize(expr)?;
    let mut pos = 0;
    let val = parse_add_sub(&tokens, &mut pos, vars)?;
    if pos == tokens.len() { Some(val) } else { None }
}

#[derive(Debug, Clone)]
enum Token {
    Num(f32),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
}

fn tokenize(s: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' => { i += 1; }
            '+' => { tokens.push(Token::Plus);   i += 1; }
            '-' => { tokens.push(Token::Minus);  i += 1; }
            '*' => { tokens.push(Token::Star);   i += 1; }
            '/' => { tokens.push(Token::Slash);  i += 1; }
            '(' => { tokens.push(Token::LParen); i += 1; }
            ')' => { tokens.push(Token::RParen); i += 1; }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                while i < chars.len() && (chars[i].is_ascii_digit() || chars[i] == '.') {
                    i += 1;
                }
                let num: f32 = chars[start..i].iter().collect::<String>().parse().ok()?;
                tokens.push(Token::Num(num));
            }
            c if c.is_alphanumeric() || c == '_' => {
                let start = i;
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                tokens.push(Token::Ident(chars[start..i].iter().collect()));
            }
            _ => return None,
        }
    }
    Some(tokens)
}

fn parse_add_sub(tokens: &[Token], pos: &mut usize, vars: &HashMap<&str, f32>) -> Option<f32> {
    let mut left = parse_mul_div(tokens, pos, vars)?;
    while *pos < tokens.len() {
        match &tokens[*pos] {
            Token::Plus  => { *pos += 1; left += parse_mul_div(tokens, pos, vars)?; }
            Token::Minus => { *pos += 1; left -= parse_mul_div(tokens, pos, vars)?; }
            _ => break,
        }
    }
    Some(left)
}

fn parse_mul_div(tokens: &[Token], pos: &mut usize, vars: &HashMap<&str, f32>) -> Option<f32> {
    let mut left = parse_unary(tokens, pos, vars)?;
    while *pos < tokens.len() {
        match &tokens[*pos] {
            Token::Star  => { *pos += 1; left *= parse_unary(tokens, pos, vars)?; }
            Token::Slash => {
                *pos += 1;
                let right = parse_unary(tokens, pos, vars)?;
                left = if right == 0.0 { 0.0 } else { left / right };
            }
            _ => break,
        }
    }
    Some(left)
}

fn parse_unary(tokens: &[Token], pos: &mut usize, vars: &HashMap<&str, f32>) -> Option<f32> {
    if *pos < tokens.len() {
        if let Token::Minus = &tokens[*pos] {
            *pos += 1;
            return Some(-parse_primary(tokens, pos, vars)?);
        }
    }
    parse_primary(tokens, pos, vars)
}

fn parse_primary(tokens: &[Token], pos: &mut usize, vars: &HashMap<&str, f32>) -> Option<f32> {
    if *pos >= tokens.len() {
        return None;
    }
    match &tokens[*pos].clone() {
        Token::Num(n) => { *pos += 1; Some(*n) }
        Token::Ident(name) => {
            *pos += 1;
            vars.get(name.as_str()).copied()
        }
        Token::LParen => {
            *pos += 1;
            let val = parse_add_sub(tokens, pos, vars)?;
            if *pos < tokens.len() {
                if let Token::RParen = &tokens[*pos] {
                    *pos += 1;
                    return Some(val);
                }
            }
            None
        }
        _ => None,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ndvi_uniform() {
        // NIR = 8000, Red = 4000 → NDVI = (8000-4000)/(8000+4000) ≈ 0.333
        let red = 4000u16.to_le_bytes().to_vec();
        let nir = 8000u16.to_le_bytes().to_vec();
        let result = compute_ndvi(&red, &nir, "uint16");
        assert_eq!(result.len(), 4);
        let v = f32::from_le_bytes([result[0], result[1], result[2], result[3]]);
        assert!((v - 0.3333).abs() < 0.001, "NDVI = {v}");
    }

    #[test]
    fn test_ndvi_zero_denom() {
        let red = 0u16.to_le_bytes().to_vec();
        let nir = 0u16.to_le_bytes().to_vec();
        let result = compute_ndvi(&red, &nir, "uint16");
        let v = f32::from_le_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(v, 0.0);
    }

    #[test]
    fn test_cloud_mask() {
        let scl = vec![1u8, 8, 9, 10, 4, 6];
        let result = cloud_mask(&scl);
        let expected = vec![1.0f32, 0.0, 0.0, 0.0, 1.0, 1.0];
        for (i, exp) in expected.iter().enumerate() {
            let v = f32::from_le_bytes(result[i*4..(i+1)*4].try_into().unwrap());
            assert_eq!(v, *exp, "pixel {i}");
        }
    }

    #[test]
    fn test_band_math_ndvi() {
        let red_bytes: Vec<u8> = vec![0xa0, 0x0f, 0xa0, 0x0f]; // 4000 LE twice
        let nir_bytes: Vec<u8> = vec![0x40, 0x1f, 0x40, 0x1f]; // 8000 LE twice
        let mut bands: HashMap<String, &[u8]> = HashMap::new();
        bands.insert("red".into(), &red_bytes);
        bands.insert("nir".into(), &nir_bytes);
        let result = band_math("(nir - red) / (nir + red)", &bands, "uint16");
        assert_eq!(result.len(), 8); // 2 pixels × 4 bytes
        let v = f32::from_le_bytes(result[0..4].try_into().unwrap());
        assert!((v - 0.3333).abs() < 0.001, "band_math NDVI = {v}");
    }

    fn read_f32(bytes: &[u8], idx: usize) -> f32 {
        f32::from_le_bytes(bytes[idx * 4..(idx + 1) * 4].try_into().unwrap())
    }

    #[test]
    fn test_ndvi_float32() {
        let red: Vec<u8> = 2000.0f32.to_le_bytes().to_vec();
        let nir: Vec<u8> = 6000.0f32.to_le_bytes().to_vec();
        let result = compute_ndvi(&red, &nir, "float32");
        let v = read_f32(&result, 0);
        // (6000-2000)/(6000+2000) = 0.5
        assert!((v - 0.5).abs() < 1e-5, "NDVI f32 = {v}");
    }

    #[test]
    fn test_ndvi_negative() {
        // Red > NIR → negative NDVI (bare soil / water)
        let red = 8000u16.to_le_bytes().to_vec();
        let nir = 2000u16.to_le_bytes().to_vec();
        let result = compute_ndvi(&red, &nir, "uint16");
        let v = read_f32(&result, 0);
        assert!((v - (-0.6)).abs() < 0.001, "negative NDVI = {v}");
    }

    #[test]
    fn test_ndvi_multi_pixel() {
        let mut red = Vec::new();
        let mut nir = Vec::new();
        for (r, n) in [(1000u16, 3000u16), (5000, 5000), (0, 8000)] {
            red.extend_from_slice(&r.to_le_bytes());
            nir.extend_from_slice(&n.to_le_bytes());
        }
        let result = compute_ndvi(&red, &nir, "uint16");
        assert_eq!(result.len(), 12); // 3 pixels × 4 bytes
        assert!((read_f32(&result, 0) - 0.5).abs() < 0.001);    // (3000-1000)/4000
        assert!((read_f32(&result, 1) - 0.0).abs() < 0.001);    // (5000-5000)/10000
        assert!((read_f32(&result, 2) - 1.0).abs() < 0.001);    // (8000-0)/8000
    }

    #[test]
    fn test_ndwi_basic() {
        // Green=6000, NIR=2000 → NDWI = (6000-2000)/(6000+2000) = 0.5
        let green = 6000u16.to_le_bytes().to_vec();
        let nir = 2000u16.to_le_bytes().to_vec();
        let result = compute_ndwi(&green, &nir, "uint16");
        let v = read_f32(&result, 0);
        assert!((v - 0.5).abs() < 0.001, "NDWI = {v}");
    }

    #[test]
    fn test_ndwi_zero_denom() {
        let green = 0u16.to_le_bytes().to_vec();
        let nir = 0u16.to_le_bytes().to_vec();
        let result = compute_ndwi(&green, &nir, "uint16");
        assert_eq!(read_f32(&result, 0), 0.0);
    }

    #[test]
    fn test_ndwi_negative() {
        // NIR > Green → negative (land)
        let green = 1000u16.to_le_bytes().to_vec();
        let nir = 5000u16.to_le_bytes().to_vec();
        let result = compute_ndwi(&green, &nir, "uint16");
        let v = read_f32(&result, 0);
        // (1000-5000)/(1000+5000) = -0.6667
        assert!((v - (-0.6667)).abs() < 0.001, "NDWI negative = {v}");
    }

    #[test]
    fn test_ndwi_float32() {
        let green: Vec<u8> = 3000.0f32.to_le_bytes().to_vec();
        let nir: Vec<u8> = 1000.0f32.to_le_bytes().to_vec();
        let result = compute_ndwi(&green, &nir, "float32");
        let v = read_f32(&result, 0);
        assert!((v - 0.5).abs() < 1e-5, "NDWI f32 = {v}");
    }

    #[test]
    fn test_evi_basic() {
        // B=1000, R=2000, N=8000 → EVI = 2.5*(8000-2000)/(8000+6*2000-7.5*1000+1)
        // denom = 8000 + 12000 - 7500 + 1 = 12501
        // EVI = 2.5 * 6000 / 12501 ≈ 1.1999 → clamped to 1.0
        let blue = 1000u16.to_le_bytes().to_vec();
        let red = 2000u16.to_le_bytes().to_vec();
        let nir = 8000u16.to_le_bytes().to_vec();
        let result = compute_evi(&blue, &red, &nir, "uint16");
        let v = read_f32(&result, 0);
        assert!(v <= 1.0, "EVI should be clamped: {v}");
        assert!(v > 0.0, "EVI should be positive: {v}");
    }

    #[test]
    fn test_evi_zero_denom() {
        // Craft values so denom ≈ 0: N + 6R - 7.5B + 1 = 0
        // N=0, R=0, B=0 → denom = 1 → EVI = 2.5*0/1 = 0
        let z = 0u16.to_le_bytes().to_vec();
        let result = compute_evi(&z, &z, &z, "uint16");
        assert_eq!(read_f32(&result, 0), 0.0);
    }

    #[test]
    fn test_evi_clamp_range() {
        let blue = 100u16.to_le_bytes().to_vec();
        let red = 100u16.to_le_bytes().to_vec();
        let nir = 10000u16.to_le_bytes().to_vec();
        let result = compute_evi(&blue, &red, &nir, "uint16");
        let v = read_f32(&result, 0);
        assert!((-1.0..=1.0).contains(&v), "EVI out of [-1,1]: {v}");
    }

    #[test]
    fn test_evi_float32() {
        let blue: Vec<u8> = 500.0f32.to_le_bytes().to_vec();
        let red: Vec<u8> = 1000.0f32.to_le_bytes().to_vec();
        let nir: Vec<u8> = 5000.0f32.to_le_bytes().to_vec();
        let result = compute_evi(&blue, &red, &nir, "float32");
        let v = read_f32(&result, 0);
        assert!((-1.0..=1.0).contains(&v), "EVI f32 = {v}");
        assert!(v > 0.0, "EVI should be positive for vegetation");
    }

    #[test]
    fn test_cloud_mask_all_clear() {
        let scl = vec![4u8, 5, 6, 7, 2, 3, 11];
        let result = cloud_mask(&scl);
        for i in 0..scl.len() {
            assert_eq!(read_f32(&result, i), 1.0, "pixel {i} should be clear");
        }
    }

    #[test]
    fn test_cloud_mask_all_cloud() {
        let scl = vec![8u8, 9, 10];
        let result = cloud_mask(&scl);
        for i in 0..scl.len() {
            assert_eq!(read_f32(&result, i), 0.0, "pixel {i} should be cloud");
        }
    }
}
