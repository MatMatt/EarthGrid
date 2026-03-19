//! Bandwidth Control — Token bucket rate limiter with Unix-style nice levels.
//!
//! Higher priority requests (lower nice) get proportionally more bandwidth.
//! Supports time-based schedules and per-stream tracking.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::sync::Mutex;

// ---------------------------------------------------------------------------
// Nice level multipliers
// ---------------------------------------------------------------------------

fn nice_multiplier(nice: i8) -> f64 {
    match nice.clamp(-10, 19) {
        -10 => 4.0,
        -9 => 3.5,
        -8 => 3.0,
        -7 => 2.5,
        -6 => 2.0,
        -5 => 1.75,
        -4 => 1.5,
        -3 => 1.25,
        -2 => 1.1,
        -1 => 1.05,
        0 => 1.0,
        1 => 0.95,
        2 => 0.9,
        3 => 0.85,
        4 => 0.8,
        5 => 0.7,
        6 => 0.6,
        7 => 0.5,
        8 => 0.4,
        9 => 0.3,
        10 => 0.25,
        11 => 0.22,
        12 => 0.20,
        13 => 0.18,
        14 => 0.16,
        15 => 0.14,
        16 => 0.13,
        17 => 0.12,
        18 => 0.11,
        19 => 0.10,
        _ => 1.0,
    }
}

// ---------------------------------------------------------------------------
// Stream tracking
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct BandwidthStream {
    pub stream_id: String,
    pub nice_level: i8,
    pub bytes_transferred: u64,
    pub started_at: f64,
}

// ---------------------------------------------------------------------------
// Status response
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize)]
pub struct BandwidthStatus {
    pub max_mbps: f64,
    pub effective_mbps: serde_json::Value,
    pub schedule: Option<HashMap<u8, f64>>,
    pub active_streams: usize,
    pub streams: Vec<StreamInfo>,
    pub total_bytes: u64,
    pub total_gb: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamInfo {
    pub id: String,
    pub nice: i8,
    pub bytes: u64,
    pub mb: f64,
}

// ---------------------------------------------------------------------------
// BandwidthManager
// ---------------------------------------------------------------------------

pub struct BandwidthManager {
    max_mbps: f64,
    schedule: HashMap<u8, f64>,
    inner: Mutex<Inner>,
}

struct Inner {
    tokens: f64,
    last_refill: f64,
    streams: HashMap<String, BandwidthStream>,
    total_bytes: u64,
}

fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

fn current_hour() -> u8 {
    // Use seconds since midnight (UTC-based; Python version uses local time)
    let secs = now_secs() as u64;
    ((secs % 86400) / 3600) as u8
}

impl BandwidthManager {
    /// Create a new bandwidth manager.
    ///
    /// `max_mbps`: Global limit in Mbps (0 = unlimited).
    /// `schedule`: Optional hour→max_mbps mapping for time-based limits.
    pub fn new(max_mbps: f64, schedule: HashMap<u8, f64>) -> Self {
        let effective = if max_mbps > 0.0 {
            max_mbps * 1_000_000.0 / 8.0
        } else {
            f64::INFINITY
        };
        Self {
            max_mbps,
            schedule,
            inner: Mutex::new(Inner {
                tokens: effective,
                last_refill: now_secs(),
                streams: HashMap::new(),
                total_bytes: 0,
            }),
        }
    }

    /// Create from environment variable `EARTHGRID_BW_LIMIT_MBPS`.
    pub fn from_env() -> Self {
        let max_mbps: f64 = std::env::var("EARTHGRID_BW_LIMIT_MBPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(0.0);
        Self::new(max_mbps, HashMap::new())
    }

    fn max_bytes_per_sec(&self) -> f64 {
        if self.max_mbps <= 0.0 && self.schedule.is_empty() {
            return f64::INFINITY;
        }

        let mut effective_mbps = self.max_mbps;
        if !self.schedule.is_empty() {
            let hour = current_hour();
            let mut applicable: Option<u8> = None;
            for &h in self.schedule.keys() {
                if h <= hour {
                    match applicable {
                        Some(prev) if h > prev => applicable = Some(h),
                        None => applicable = Some(h),
                        _ => {}
                    }
                }
            }
            if let Some(h) = applicable {
                if let Some(&mbps) = self.schedule.get(&h) {
                    effective_mbps = mbps;
                }
            }
        }

        if effective_mbps <= 0.0 {
            return f64::INFINITY;
        }
        effective_mbps * 1_000_000.0 / 8.0
    }

    fn refill(inner: &mut Inner, max_bps: f64) {
        let now = now_secs();
        let elapsed = now - inner.last_refill;
        inner.last_refill = now;

        if max_bps.is_infinite() {
            inner.tokens = f64::INFINITY;
            return;
        }

        inner.tokens = (inner.tokens + elapsed * max_bps).min(max_bps * 2.0);
    }

    fn effective_rate(&self, inner: &Inner, nice_level: i8) -> f64 {
        let base = self.max_bytes_per_sec();
        if base.is_infinite() {
            return f64::INFINITY;
        }
        let mult = nice_multiplier(nice_level);
        let active = inner
            .streams
            .values()
            .filter(|s| s.nice_level <= nice_level)
            .count()
            .max(1) as f64;
        (base * mult) / active
    }

    /// Acquire bandwidth for `nbytes`. Returns wait time in seconds.
    /// Sleeps internally if rate limit requires it.
    pub async fn acquire(&self, nbytes: u64, nice_level: i8, stream_id: &str) -> f64 {
        let max_bps = self.max_bytes_per_sec();
        if max_bps.is_infinite() {
            let mut inner = self.inner.lock().await;
            inner.total_bytes += nbytes;
            if let Some(s) = inner.streams.get_mut(stream_id) {
                s.bytes_transferred += nbytes;
            }
            return 0.0;
        }

        let wait_time;
        {
            let mut inner = self.inner.lock().await;
            Self::refill(&mut inner, max_bps);

            let rate = self.effective_rate(&inner, nice_level);
            if rate.is_infinite() || inner.tokens >= nbytes as f64 {
                inner.tokens -= nbytes as f64;
                inner.total_bytes += nbytes;
                if let Some(s) = inner.streams.get_mut(stream_id) {
                    s.bytes_transferred += nbytes;
                }
                return 0.0;
            }

            let deficit = nbytes as f64 - inner.tokens;
            wait_time = deficit / rate;
            inner.tokens = 0.0;
        }

        if wait_time > 0.0 {
            tokio::time::sleep(tokio::time::Duration::from_secs_f64(wait_time)).await;
        }

        {
            let mut inner = self.inner.lock().await;
            inner.total_bytes += nbytes;
            if let Some(s) = inner.streams.get_mut(stream_id) {
                s.bytes_transferred += nbytes;
            }
        }

        wait_time
    }

    /// Register a new bandwidth stream for tracking.
    pub async fn register_stream(&self, stream_id: &str, nice_level: i8) {
        let mut inner = self.inner.lock().await;
        inner.streams.insert(
            stream_id.to_string(),
            BandwidthStream {
                stream_id: stream_id.to_string(),
                nice_level,
                bytes_transferred: 0,
                started_at: now_secs(),
            },
        );
    }

    /// Unregister a stream. Returns total bytes transferred.
    pub async fn unregister_stream(&self, stream_id: &str) -> u64 {
        let mut inner = self.inner.lock().await;
        inner
            .streams
            .remove(stream_id)
            .map(|s| s.bytes_transferred)
            .unwrap_or(0)
    }

    /// Current bandwidth status.
    pub async fn status(&self) -> BandwidthStatus {
        let inner = self.inner.lock().await;
        let max_bps = self.max_bytes_per_sec();
        let effective_mbps = if max_bps.is_infinite() {
            serde_json::json!("unlimited")
        } else {
            serde_json::json!((max_bps * 8.0 / 1_000_000.0 * 10.0).round() / 10.0)
        };

        let streams: Vec<StreamInfo> = inner
            .streams
            .values()
            .map(|s| StreamInfo {
                id: s.stream_id.clone(),
                nice: s.nice_level,
                bytes: s.bytes_transferred,
                mb: (s.bytes_transferred as f64 / (1024.0 * 1024.0) * 10.0).round() / 10.0,
            })
            .collect();

        BandwidthStatus {
            max_mbps: self.max_mbps,
            effective_mbps,
            schedule: if self.schedule.is_empty() {
                None
            } else {
                Some(self.schedule.clone())
            },
            active_streams: inner.streams.len(),
            streams,
            total_bytes: inner.total_bytes,
            total_gb: (inner.total_bytes as f64 / (1024.0 * 1024.0 * 1024.0) * 100.0).round()
                / 100.0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nice_multipliers() {
        assert_eq!(nice_multiplier(0), 1.0);
        assert_eq!(nice_multiplier(-10), 4.0);
        assert_eq!(nice_multiplier(19), 0.10);
        assert_eq!(nice_multiplier(10), 0.25);
    }

    #[tokio::test]
    async fn test_unlimited_bandwidth() {
        let bw = BandwidthManager::new(0.0, HashMap::new());
        let wait = bw.acquire(1_000_000, 0, "test").await;
        assert_eq!(wait, 0.0);
    }

    #[tokio::test]
    async fn test_stream_tracking() {
        let bw = BandwidthManager::new(0.0, HashMap::new());
        bw.register_stream("s1", 0).await;
        bw.acquire(1024, 0, "s1").await;
        let bytes = bw.unregister_stream("s1").await;
        assert_eq!(bytes, 1024);
    }

    #[tokio::test]
    async fn test_status() {
        let bw = BandwidthManager::new(100.0, HashMap::new());
        bw.register_stream("s1", 5).await;
        bw.acquire(500, 5, "s1").await;
        let status = bw.status().await;
        assert_eq!(status.max_mbps, 100.0);
        assert_eq!(status.active_streams, 1);
        assert_eq!(status.total_bytes, 500);
    }
}
