//! Lock-free performance recorder for EarthGrid endpoints.
//!
//! Uses atomic counters and log-spaced latency buckets, flushed every 60s
//! to stats.db. Zero allocations on the hot path — two Instant::now() calls
//! and a few relaxed atomic adds per request.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Latency histogram buckets (log-spaced, in milliseconds).
/// Bounds: 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, +inf
pub const BUCKET_BOUNDS_MS: &[u64] = &[1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000];

/// Tracked endpoints.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Endpoint {
    ChunkGet,
    StacSearch,
    Coverage,
}

impl Endpoint {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ChunkGet => "chunk_get",
            Self::StacSearch => "stac_search",
            Self::Coverage => "coverage",
        }
    }

    pub fn all() -> [Self; 3] {
        [Self::ChunkGet, Self::StacSearch, Self::Coverage]
    }
}

/// Per-endpoint atomic counters.
struct EndpointCounters {
    count: AtomicU64,
    bytes: AtomicU64,
    sum_us: AtomicU64,
    max_us: AtomicU64,
    buckets: [AtomicU64; 14], // one for each bound + overflow
}

impl EndpointCounters {
    fn new() -> Self {
        Self {
            count: AtomicU64::new(0),
            bytes: AtomicU64::new(0),
            sum_us: AtomicU64::new(0),
            max_us: AtomicU64::new(0),
            buckets: [
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0),
                AtomicU64::new(0), AtomicU64::new(0),
            ],
        }
    }
}

/// A single minute of drained data for one endpoint.
#[derive(Debug, Clone)]
pub struct PerfSnapshot {
    pub endpoint: &'static str,
    pub count: u64,
    pub bytes: u64,
    pub sum_us: u64,
    pub max_us: u64,
    pub buckets: [u64; 14],
}

/// Lock-free, allocation-free performance recorder.
pub struct PerfRecorder {
    counters: [EndpointCounters; 3],
}

impl PerfRecorder {
    pub fn new() -> Self {
        Self {
            counters: [
                EndpointCounters::new(),
                EndpointCounters::new(),
                EndpointCounters::new(),
            ],
        }
    }

    fn idx(ep: Endpoint) -> usize {
        match ep {
            Endpoint::ChunkGet => 0,
            Endpoint::StacSearch => 1,
            Endpoint::Coverage => 2,
        }
    }

    /// Record a single request. `latency_us` in microseconds, `bytes` is response size.
    pub fn record(&self, ep: Endpoint, latency_us: u64, bytes: u64) {
        let c = &self.counters[Self::idx(ep)];
        c.count.fetch_add(1, Ordering::Relaxed);
        c.bytes.fetch_add(bytes, Ordering::Relaxed);
        c.sum_us.fetch_add(latency_us, Ordering::Relaxed);
        // Update max_us with CAS
        let mut current = c.max_us.load(Ordering::Relaxed);
        while latency_us > current {
            match c.max_us.compare_exchange_weak(current, latency_us, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
        // Bucket by latency — binary search against BUCKET_BOUNDS_MS (bounds in ms,
        // converted to µs for integer comparison). Fallback to overflow bucket 13.
        let b = BUCKET_BOUNDS_MS
            .iter()
            .position(|&bound_ms| latency_us <= bound_ms * 1000)
            .unwrap_or(13);
        c.buckets[b].fetch_add(1, Ordering::Relaxed);
    }

    /// Drain all counters (swap to zero) and return per-endpoint snapshots.
    /// Only endpoints with count > 0 are returned.
    pub fn drain(&self) -> Vec<PerfSnapshot> {
        Endpoint::all()
            .iter()
            .filter_map(|&ep| {
                let c = &self.counters[Self::idx(ep)];
                let count = c.count.swap(0, Ordering::Relaxed);
                if count == 0 {
                    return None;
                }
                let bytes = c.bytes.swap(0, Ordering::Relaxed);
                let sum_us = c.sum_us.swap(0, Ordering::Relaxed);
                let max_us = c.max_us.swap(0, Ordering::Relaxed);
                let mut buckets = [0u64; 14];
                for (i, b) in buckets.iter_mut().enumerate() {
                    *b = c.buckets[i].swap(0, Ordering::Relaxed);
                }
                Some(PerfSnapshot {
                    endpoint: ep.as_str(),
                    count,
                    bytes,
                    sum_us,
                    max_us,
                    buckets,
                })
            })
            .collect()
    }
}

impl Default for PerfRecorder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_and_drain() {
        let p = PerfRecorder::new();
        p.record(Endpoint::ChunkGet, 4500, 524288); // 4.5ms, 512KB
        p.record(Endpoint::ChunkGet, 12000, 524288); // 12ms
        p.record(Endpoint::ChunkGet, 500, 524288); // 0.5ms → bucket 0

        let snaps = p.drain();
        assert_eq!(snaps.len(), 1);
        let s = &snaps[0];
        assert_eq!(s.endpoint, "chunk_get");
        assert_eq!(s.count, 3);
        assert_eq!(s.bytes, 524288 * 3);
        assert_eq!(s.sum_us, 17000);
        assert_eq!(s.max_us, 12000);
        // 500µs → bucket 0 (≤1ms), 4500µs → bucket 2 (≤5ms), 12000µs → bucket 4 (≤25ms)
        assert_eq!(s.buckets[0], 1);
        assert_eq!(s.buckets[2], 1);
        assert_eq!(s.buckets[4], 1);

        // Drain again → empty
        let snaps2 = p.drain();
        assert!(snaps2.is_empty());
    }
}