"""Tests for Catalog."""
import tempfile
from pathlib import Path

from earthgrid.catalog import Catalog, STACCollection, STACItem


def _make_catalog():
    tmp = tempfile.mkdtemp()
    return Catalog(Path(tmp) / "test.db")


def test_add_and_get_collection():
    cat = _make_catalog()
    col = STACCollection(id="sentinel-2", title="Sentinel-2 L2A", description="Test collection")
    cat.add_collection(col)

    result = cat.get_collection("sentinel-2")
    assert result is not None
    assert result.id == "sentinel-2"
    assert result.title == "Sentinel-2 L2A"


def test_list_collections():
    cat = _make_catalog()
    cat.add_collection(STACCollection(id="a", title="A", description="A"))
    cat.add_collection(STACCollection(id="b", title="B", description="B"))

    cols = cat.list_collections()
    assert len(cols) == 2


def test_add_and_search_item():
    cat = _make_catalog()
    cat.add_collection(STACCollection(id="test", title="Test", description="Test"))

    item = STACItem(
        id="tile-001",
        collection="test",
        geometry={"type": "Point", "coordinates": [11.4, 47.3]},
        bbox=[11.0, 47.0, 12.0, 48.0],
        properties={"datetime": "2024-07-01T00:00:00Z"},
        assets={"data": {"href": "/chunks", "type": "application/octet-stream"}},
        chunk_hashes=["abc123"],
    )
    cat.add_item(item)

    results = cat.search(collections=["test"])
    assert len(results) == 1
    assert results[0].id == "tile-001"


def test_bbox_search():
    cat = _make_catalog()
    cat.add_collection(STACCollection(id="test", title="Test", description="Test"))

    # Item in Alps
    cat.add_item(STACItem(
        id="alps", collection="test",
        geometry={"type": "Point", "coordinates": [11.4, 47.3]},
        bbox=[11.0, 47.0, 12.0, 48.0],
        properties={"datetime": "2024-07-01T00:00:00Z"},
        assets={}, chunk_hashes=[],
    ))

    # Item in Brazil
    cat.add_item(STACItem(
        id="brazil", collection="test",
        geometry={"type": "Point", "coordinates": [-50.0, -15.0]},
        bbox=[-51.0, -16.0, -49.0, -14.0],
        properties={"datetime": "2024-07-01T00:00:00Z"},
        assets={}, chunk_hashes=[],
    ))

    # Search Alps region
    results = cat.search(bbox=[10.0, 46.0, 13.0, 49.0])
    assert len(results) == 1
    assert results[0].id == "alps"


def test_datetime_search():
    cat = _make_catalog()
    cat.add_collection(STACCollection(id="test", title="Test", description="Test"))

    cat.add_item(STACItem(
        id="july", collection="test",
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[-1, -1, 1, 1],
        properties={"datetime": "2024-07-15T00:00:00Z"},
        assets={}, chunk_hashes=[],
    ))

    cat.add_item(STACItem(
        id="jan", collection="test",
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[-1, -1, 1, 1],
        properties={"datetime": "2024-01-15T00:00:00Z"},
        assets={}, chunk_hashes=[],
    ))

    results = cat.search(datetime_range="2024-06-01T00:00:00Z/2024-08-01T00:00:00Z")
    assert len(results) == 1
    assert results[0].id == "july"


def test_item_count():
    cat = _make_catalog()
    assert cat.item_count() == 0
    cat.add_collection(STACCollection(id="t", title="T", description="T"))
    cat.add_item(STACItem(
        id="x", collection="t",
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[0, 0, 1, 1],
        properties={"datetime": "2024-01-01"},
        assets={}, chunk_hashes=[],
    ))
    assert cat.item_count() == 1
