"""Content-Addressable Storage (CAS) — SHA-256 hash-based chunk storage."""
import hashlib
from pathlib import Path


class ChunkStore:
    """Store and retrieve data chunks by their SHA-256 hash."""

    def __init__(self, store_path: Path):
        self.store_path = store_path
        self.store_path.mkdir(parents=True, exist_ok=True)

    def _chunk_path(self, sha: str) -> Path:
        """Two-level directory: ab/cd/abcd1234..."""
        return self.store_path / sha[:2] / sha[2:4] / sha

    @staticmethod
    def hash_bytes(data: bytes) -> str:
        """SHA-256 hash of raw bytes."""
        return hashlib.sha256(data).hexdigest()

    def put(self, data: bytes) -> str:
        """Store chunk, return its SHA-256 hash."""
        sha = self.hash_bytes(data)
        path = self._chunk_path(sha)
        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        return sha

    def get(self, sha: str) -> bytes | None:
        """Retrieve chunk by hash. Returns None if not found."""
        path = self._chunk_path(sha)
        if path.exists():
            return path.read_bytes()
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
        return len(self.list_chunks())

    @property
    def total_bytes(self) -> int:
        total = 0
        for p in self.store_path.rglob("*"):
            if p.is_file() and len(p.name) == 64:
                total += p.stat().st_size
        return total
