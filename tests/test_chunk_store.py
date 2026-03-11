"""Tests for ChunkStore."""
import tempfile
from pathlib import Path

from earthgrid.chunk_store import ChunkStore


def test_put_and_get():
    with tempfile.TemporaryDirectory() as tmp:
        store = ChunkStore(Path(tmp) / "store")
        data = b"Hello EarthGrid!"
        sha = store.put(data)

        assert len(sha) == 64  # SHA-256 hex
        assert store.has(sha)
        assert store.get(sha) == data
        assert store.chunk_count == 1


def test_duplicate_put():
    with tempfile.TemporaryDirectory() as tmp:
        store = ChunkStore(Path(tmp) / "store")
        data = b"same data"
        sha1 = store.put(data)
        sha2 = store.put(data)

        assert sha1 == sha2
        assert store.chunk_count == 1  # no duplicate


def test_delete():
    with tempfile.TemporaryDirectory() as tmp:
        store = ChunkStore(Path(tmp) / "store")
        sha = store.put(b"delete me")

        assert store.delete(sha) is True
        assert store.has(sha) is False
        assert store.delete(sha) is False  # already gone


def test_get_missing():
    with tempfile.TemporaryDirectory() as tmp:
        store = ChunkStore(Path(tmp) / "store")
        assert store.get("0" * 64) is None


def test_list_chunks():
    with tempfile.TemporaryDirectory() as tmp:
        store = ChunkStore(Path(tmp) / "store")
        hashes = set()
        for i in range(5):
            hashes.add(store.put(f"chunk-{i}".encode()))

        listed = set(store.list_chunks())
        assert listed == hashes


def test_total_bytes():
    with tempfile.TemporaryDirectory() as tmp:
        store = ChunkStore(Path(tmp) / "store")
        store.put(b"1234567890")  # 10 bytes
        store.put(b"abcde")  # 5 bytes

        assert store.total_bytes == 15
