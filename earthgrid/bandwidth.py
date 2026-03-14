"""EarthGrid Bandwidth Control — Priority-based rate limiting.

Implements a token bucket rate limiter with Unix-style nice levels.
Higher priority requests (lower nice) get more bandwidth.
"""
from __future__ import annotations
import asyncio
import logging
import os
import time
from dataclasses import dataclass, field

logger = logging.getLogger("earthgrid.bandwidth")


@dataclass
class BandwidthStream:
    """A tracked bandwidth stream."""
    stream_id: str
    nice_level: int = 0
    bytes_transferred: int = 0
    started_at: float = field(default_factory=time.time)
    last_acquire: float = 0.0


class BandwidthManager:
    """Token bucket rate limiter with nice-level priority.
    
    Nice levels: -10 (highest priority) to 19 (lowest).
    
    Bandwidth is allocated proportionally:
      - nice -10: gets 4x the base rate
      - nice 0: gets 1x (base rate)  
      - nice 10: gets 0.25x
      - nice 19: gets 0.1x
    """

    # Nice level to bandwidth multiplier
    NICE_MULTIPLIERS = {
        -10: 4.0, -9: 3.5, -8: 3.0, -7: 2.5, -6: 2.0,
        -5: 1.75, -4: 1.5, -3: 1.25, -2: 1.1, -1: 1.05,
        0: 1.0,
        1: 0.95, 2: 0.9, 3: 0.85, 4: 0.8, 5: 0.7,
        6: 0.6, 7: 0.5, 8: 0.4, 9: 0.3, 10: 0.25,
        11: 0.22, 12: 0.20, 13: 0.18, 14: 0.16, 15: 0.14,
        16: 0.13, 17: 0.12, 18: 0.11, 19: 0.10,
    }

    def __init__(self, max_mbps: float = 0, schedule: dict[int, float] = None):
        """
        Args:
            max_mbps: Global bandwidth limit in Mbps (0 = unlimited).
            schedule: Optional hour→max_mbps mapping for time-based limits.
                      e.g., {0: 100, 8: 50, 18: 75, 22: 100}
                      Hours not listed inherit from the previous defined hour.
        """
        self._max_mbps = max_mbps or float(
            os.environ.get("EARTHGRID_BW_LIMIT_MBPS", "0")
        )
        self._schedule = schedule or {}
        self._tokens: float = self._max_bytes_per_sec()  # available bytes
        self._last_refill: float = time.time()
        self._lock = asyncio.Lock()
        self._streams: dict[str, BandwidthStream] = {}
        self._total_bytes: int = 0

    def _max_bytes_per_sec(self) -> float:
        """Current max bytes/sec based on schedule and global limit."""
        if not self._max_mbps and not self._schedule:
            return float("inf")

        effective_mbps = self._max_mbps
        if self._schedule:
            current_hour = time.localtime().tm_hour
            # Find the applicable schedule entry
            applicable = 0
            for hour in sorted(self._schedule.keys()):
                if hour <= current_hour:
                    applicable = hour
            if applicable in self._schedule:
                effective_mbps = self._schedule[applicable]
            elif self._max_mbps:
                effective_mbps = self._max_mbps

        if not effective_mbps:
            return float("inf")
        return effective_mbps * 1_000_000 / 8  # Mbps → bytes/sec

    def _refill(self):
        """Refill token bucket based on elapsed time."""
        now = time.time()
        elapsed = now - self._last_refill
        self._last_refill = now
        max_bps = self._max_bytes_per_sec()
        if max_bps == float("inf"):
            self._tokens = float("inf")
            return
        self._tokens = min(
            max_bps * 2,  # burst buffer = 2 seconds
            self._tokens + elapsed * max_bps
        )

    def _effective_rate(self, nice_level: int) -> float:
        """Bytes/sec allowed for a given nice level."""
        base = self._max_bytes_per_sec()
        if base == float("inf"):
            return float("inf")
        clamp = max(-10, min(19, nice_level))
        mult = self.NICE_MULTIPLIERS.get(clamp, 1.0)
        # Divide by active streams at same or higher priority
        active_count = sum(1 for s in self._streams.values()
                          if s.nice_level <= clamp)
        active_count = max(1, active_count)
        return (base * mult) / active_count

    async def acquire(self, nbytes: int, nice_level: int = 0,
                      stream_id: str = "") -> float:
        """Acquire bandwidth for nbytes. Returns wait time in seconds.
        
        Call this before each chunk of data transfer. It will sleep if
        necessary to stay within bandwidth limits.
        
        Args:
            nbytes: Number of bytes to transfer.
            nice_level: Priority (-10 highest, 19 lowest).
            stream_id: Optional stream identifier for tracking.
            
        Returns:
            Actual wait time in seconds (0 if no wait needed).
        """
        if self._max_bytes_per_sec() == float("inf"):
            # No limit configured
            self._total_bytes += nbytes
            if stream_id and stream_id in self._streams:
                self._streams[stream_id].bytes_transferred += nbytes
            return 0.0

        async with self._lock:
            self._refill()

            rate = self._effective_rate(nice_level)
            if rate == float("inf"):
                self._tokens -= nbytes
                self._total_bytes += nbytes
                return 0.0

            # How long should we wait for this many bytes?
            if self._tokens >= nbytes:
                self._tokens -= nbytes
                self._total_bytes += nbytes
                if stream_id and stream_id in self._streams:
                    self._streams[stream_id].bytes_transferred += nbytes
                return 0.0

            # Need to wait
            deficit = nbytes - self._tokens
            wait_time = deficit / rate
            self._tokens = 0

        # Sleep outside lock so others can proceed
        if wait_time > 0:
            await asyncio.sleep(wait_time)

        async with self._lock:
            self._total_bytes += nbytes
            if stream_id and stream_id in self._streams:
                self._streams[stream_id].bytes_transferred += nbytes

        return wait_time

    def register_stream(self, stream_id: str, nice_level: int = 0) -> BandwidthStream:
        """Register a new bandwidth stream for tracking."""
        stream = BandwidthStream(stream_id=stream_id, nice_level=nice_level)
        self._streams[stream_id] = stream
        logger.debug(f"Registered stream {stream_id} (nice={nice_level})")
        return stream

    def unregister_stream(self, stream_id: str) -> int:
        """Unregister a stream. Returns total bytes transferred."""
        stream = self._streams.pop(stream_id, None)
        if stream:
            logger.debug(f"Unregistered stream {stream_id} "
                         f"({stream.bytes_transferred} bytes)")
            return stream.bytes_transferred
        return 0

    def status(self) -> dict:
        """Current bandwidth status."""
        max_bps = self._max_bytes_per_sec()
        return {
            "max_mbps": self._max_mbps,
            "effective_mbps": round(max_bps * 8 / 1_000_000, 1) if max_bps != float("inf") else "unlimited",
            "schedule": self._schedule or None,
            "active_streams": len(self._streams),
            "streams": [
                {"id": s.stream_id, "nice": s.nice_level,
                 "bytes": s.bytes_transferred,
                 "mb": round(s.bytes_transferred / (1024**2), 1)}
                for s in self._streams.values()
            ],
            "total_bytes": self._total_bytes,
            "total_gb": round(self._total_bytes / (1024**3), 2),
        }
