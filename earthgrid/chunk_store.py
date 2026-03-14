"""Content-Addressable Storage (CAS) — SHA-256 hash-based chunk storage."""
from __future__ import annotations
import hashlib
import json
import time
from pathlib import Path


class StorageLimitError(Exception):
    """Raised when storage limit would be exceeded."""
    pass


class ChunkStore:
    """Store and retrieve data chunks by their SHA-256 hash."""

    def __init__(self, store_path: Path, limit_gb: float = 0):
        self.store_path = store_path
        self.store_path.mkdir(parents=True, exist_ok=True)
        self.limit_bytes = int(limit_gb * 1024 * 1024 * 1024) if limit_gb > 0 else 0

        # Cached counters (lazy-init on first access)
        self._cached_total_bytes: int | None = None
        self._cached_chunk_count: int | None = None

        # Stats tracking
        self._stats_file = store_path.parent / "stats.json" if store_path.parent.exists() else None
        self._stats = self._load_stats()

    def _load_stats(self) -> dict:
        if self._stats_file and self._stats_file.exists():
            try:
                return json.loads(self._stats_file.read_text())
            except Exception:
                pass
        return {
            "started": time.time(),
            "chunks_served": 0,
            "bytes_served": 0,
            "chunks_stored": 0,
            "bytes_ingested": 0,
            "requests_total": 0,
            "requests_today": 0,
            "today": time.strftime("%Y-%m-%d"),
            "daily_history": {},  # date → {served, bytes, requests}
        }

    def _save_stats(self):
        if self._stats_file:
            try:
                self._stats_file.write_text(json.dumps(self._stats, indent=2))
            except Exception:
                pass

    def _track_day(self):
        today = time.strftime("%Y-%m-%d")
        if self._stats.get("today") != today:
            self._stats["requests_today"] = 0
            self._stats["today"] = today

    def _track_serve(self, size: int):
        self._track_day()
        self._stats["chunks_served"] += 1
        self._stats["bytes_served"] += size
        self._stats["requests_total"] += 1
        self._stats["requests_today"] += 1
        today = self._stats["today"]
        day = self._stats["daily_history"].setdefault(today, {"served": 0, "bytes": 0, "requests": 0})
        day["served"] += 1
        day["bytes"] += size
        day["requests"] += 1
        # Keep last 90 days only
        if len(self._stats["daily_history"]) > 90:
            keys = sorted(self._stats["daily_history"].keys())
            for k in keys[:-90]:
                del self._stats["daily_history"][k]
        self._save_stats()

    def _track_store(self, size: int):
        self._stats["chunks_stored"] += 1
        self._stats["bytes_ingested"] += size
        self._save_stats()

    @property
    def stats(self) -> dict:
        self._track_day()
        uptime = time.time() - self._stats.get("started", time.time())
        return {
            "uptime_hours": round(uptime / 3600, 1),
            "chunks_served": self._stats["chunks_served"],
            "bytes_served": self._stats["bytes_served"],
            "chunks_stored": self._stats["chunks_stored"],
            "bytes_ingested": self._stats["bytes_ingested"],
            "requests_total": self._stats["requests_total"],
            "requests_today": self._stats["requests_today"],
            "storage_used_bytes": self.total_bytes,
            "chunk_count": self.chunk_count,
        }

    def _chunk_path(self, sha: str) -> Path:
        """Two-level directory: ab/cd/abcd1234..."""
        return self.store_path / sha[:2] / sha[2:4] / sha

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        """SHA-256 hash of raw bytes."""
        return hashlib.sha256(data).hexdigest()

    def put(self, data: bytes) -> str:
        """Store chunk, return its SHA-256 hash. Raises StorageLimitError if full."""
        sha = self.hash_bytes(data)
        path = self._chunk_path(sha)
        if not path.exists():
            if self.limit_bytes > 0 and (self.total_bytes + len(data)) > self.limit_bytes:
                raise StorageLimitError(
                    f"Storage limit reached ({self.limit_bytes / 1024**3:.1f} GB)"
                )
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
            # Update cached counters
            if self._cached_total_bytes is not None:
                self._cached_total_bytes += len(data)
            if self._cached_chunk_count is not None:
                self._cached_chunk_count += 1
            self._track_store(len(data))
        return sha

    def get(self, sha: str) -> bytes | None:
        """Retrieve chunk by hash. Returns None if not found."""
        path = self._chunk_path(sha)
        if path.exists():
            data = path.read_bytes()
            self._track_serve(len(data))
            return data
        return None

    def has(self, sha: str) -> bool:
        """Check if chunk exists."""
        return self._chunk_path(sha).exists()

    def delete(self, sha: str) -> bool:
        """Delete chunk. Returns True if it existed."""
        path = self._chunk_path(sha)
        if path.exists():
            path.unlink()
            return True
        return False

    def list_chunks(self) -> list[str]:
        """List all chunk hashes."""
        chunks = []
        for p in self.store_path.rglob("*"):
            if p.is_file() and len(p.name) == 64:
                chunks.append(p.name)
        return chunks

    @property
    def chunk_count(self) -> int:
        if self._cached_chunk_count is None:
            self._cached_chunk_count = len(self.list_chunks())
        return self._cached_chunk_count

    @property
    def total_bytes(self) -> int:
        if self._cached_total_bytes is None:
            total = 0
            for p in self.store_path.rglob("*"):
                if p.is_file() and len(p.name) == 64:
                    total += p.stat().st_size
            self._cached_total_bytes = total
        return self._cached_total_bytes
