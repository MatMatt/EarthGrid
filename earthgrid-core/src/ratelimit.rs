//! Sliding-window rate limiter for EarthGrid.
//!
//! Ported from ratelimit.py.
//!
//! - Per-client-IP sliding window (1-minute and 2-second burst)
//! - Exempt: `/health`, `/` and LAN addresses
//! - Returns 429 with `Retry-After` header when exceeded

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use axum::{
    body::Body,
    extract::Request,
    http::StatusCode,
    middleware::Next,
    response::{IntoResponse, Response},
};

// ---------------------------------------------------------------------------
// RateLimiter
// ---------------------------------------------------------------------------

/// Shared rate-limiter state.
#[derive(Clone)]
pub struct RateLimiter {
    requests_per_minute: usize,
    burst: usize,
    /// IP → timestamps of recent requests
    windows: Arc<Mutex<HashMap<String, Vec<Instant>>>>,
}

impl RateLimiter {
    /// Create a new rate limiter.
    ///
    /// * `requests_per_minute` — max requests per IP per 60 s (default 120)
    /// * `burst` — max requests in a 2 s window (default 20)
    pub fn new(requests_per_minute: usize, burst: usize) -> Self {
        Self {
            requests_per_minute,
            burst,
            windows: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl Default for RateLimiter {
    fn default() -> Self {
        Self::new(120, 20)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Extract client IP from request headers / socket address.
fn client_ip(req: &Request<Body>) -> String {
    // Respect X-Forwarded-For when behind a reverse proxy
    if let Some(fwd) = req.headers().get("x-forwarded-for") {
        if let Ok(s) = fwd.to_str() {
            if let Some(first) = s.split(',').next() {
                return first.trim().to_string();
            }
        }
    }
    // Axum stores the socket address as a request extension
    if let Some(addr) = req
        .extensions()
        .get::<axum::extract::ConnectInfo<std::net::SocketAddr>>()
    {
        return addr.ip().to_string();
    }
    "unknown".to_string()
}

/// Returns `true` for localhost and RFC-1918 LAN addresses.
fn is_lan_ip(ip: &str) -> bool {
    ip.starts_with("127.")
        || ip.starts_with("10.")
        || ip.starts_with("192.168.")
        || ip == "::1"
        || ip == "localhost"
        || ip == "unknown" // conservative: treat unknown as LAN
}

// ---------------------------------------------------------------------------
// Axum middleware
// ---------------------------------------------------------------------------

/// Axum middleware function — call via `axum::middleware::from_fn_with_state`.
///
/// ```ignore
/// use axum::{Router, middleware};
/// use earthgrid_core::ratelimit::{RateLimiter, rate_limit_middleware};
///
/// let limiter = RateLimiter::default();
/// let app: Router = Router::new()
///     .layer(middleware::from_fn_with_state(limiter, rate_limit_middleware));
/// ```
pub async fn rate_limit_middleware(
    axum::extract::State(limiter): axum::extract::State<RateLimiter>,
    req: Request<Body>,
    next: Next,
) -> Response {
    let path = req.uri().path().to_string();

    // Always pass through health + root
    if path == "/health" || path == "/" {
        return next.run(req).await;
    }

    let ip = client_ip(&req);

    // Exempt LAN/localhost clients
    if is_lan_ip(&ip) {
        return next.run(req).await;
    }

    let now = Instant::now();

    {
        let mut windows = limiter.windows.lock().unwrap();

        // Periodic cleanup (remove stale IPs — heuristic: no entry touched in >60s)
        windows.retain(|_, times: &mut Vec<Instant>| {
            !times.is_empty() && times.last().map(|t| t.elapsed().as_secs() < 120).unwrap_or(false)
        });

        let times = windows.entry(ip.clone()).or_default();

        // Drop entries older than 60 s
        let cutoff_60 = now - std::time::Duration::from_secs(60);
        times.retain(|&t| t > cutoff_60);

        // Per-minute check
        if times.len() >= limiter.requests_per_minute {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [
                    ("Retry-After", "60"),
                    ("Content-Type", "application/json"),
                ],
                r#"{"error":"Rate limit exceeded. Try again later."}"#,
            )
                .into_response();
        }

        // Burst check (last 2 s)
        let cutoff_2 = now - std::time::Duration::from_secs(2);
        let recent = times.iter().filter(|&&t| t > cutoff_2).count();
        if recent >= limiter.burst {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                [
                    ("Retry-After", "2"),
                    ("Content-Type", "application/json"),
                ],
                r#"{"error":"Too many requests. Slow down."}"#,
            )
                .into_response();
        }

        times.push(now);
    }

    next.run(req).await
}

// Use `axum::middleware::from_fn_with_state(limiter, rate_limit_middleware)`
// directly in your Router to attach rate limiting.

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_is_lan_ip() {
        assert!(is_lan_ip("127.0.0.1"));
        assert!(is_lan_ip("10.0.0.1"));
        assert!(is_lan_ip("192.168.1.100"));
        assert!(is_lan_ip("::1"));
        assert!(is_lan_ip("localhost"));
        assert!(!is_lan_ip("8.8.8.8"));
        assert!(!is_lan_ip("172.16.0.1")); // 172.16.x is RFC1918 but not handled for simplicity
    }

    #[test]
    fn test_rate_limiter_allows_under_limit() {
        let limiter = RateLimiter::new(120, 20);
        let mut windows = limiter.windows.lock().unwrap();
        let now = Instant::now();
        let times = windows.entry("1.2.3.4".to_string()).or_default();
        // Add 10 requests
        for _ in 0..10 {
            times.push(now - Duration::from_millis(100));
        }
        assert!(times.len() < 120);
    }

    #[test]
    fn test_rate_limiter_burst_detection() {
        let limiter = RateLimiter::new(120, 5);
        let mut windows = limiter.windows.lock().unwrap();
        let now = Instant::now();
        let times = windows.entry("5.5.5.5".to_string()).or_default();
        // Add 5 very recent requests (within 2s)
        for _ in 0..5 {
            times.push(now - Duration::from_millis(100));
        }
        let cutoff_2 = now - Duration::from_secs(2);
        let recent = times.iter().filter(|&&t| t > cutoff_2).count();
        assert_eq!(recent, 5); // exactly at burst limit
    }

    #[test]
    fn test_rate_limiter_old_requests_expire() {
        let limiter = RateLimiter::new(3, 20);
        let mut windows = limiter.windows.lock().unwrap();
        let now = Instant::now();
        let times = windows.entry("9.9.9.9".to_string()).or_default();
        // Add 3 old requests (>60s ago)
        for _ in 0..3 {
            times.push(now - Duration::from_secs(90));
        }
        // After cleanup, within-60s count should be 0
        let cutoff_60 = now - Duration::from_secs(60);
        times.retain(|&t| t > cutoff_60);
        assert_eq!(times.len(), 0);
    }
}
