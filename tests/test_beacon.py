"""Tests for the Beacon coordination layer."""
import pytest
from httpx import AsyncClient, ASGITransport

from earthgrid.beacon import beacon_app, registry


@pytest.fixture(autouse=True)
def clear_registry():
    """Clear beacon registry between tests."""
    registry.nodes.clear()
    yield
    registry.nodes.clear()


@pytest.fixture
def transport():
    return ASGITransport(app=beacon_app)


@pytest.mark.asyncio
async def test_beacon_info(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/")
        assert resp.status_code == 200
        data = resp.json()
        assert data["role"] == "beacon"
        assert data["name"] == "EarthGrid Beacon"
        assert data["nodes_alive"] == 0


@pytest.mark.asyncio
async def test_beacon_health(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/health")
        assert resp.json()["role"] == "beacon"


@pytest.mark.asyncio
async def test_register_node(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.post("/register", params={
            "node_id": "node-1",
            "node_name": "alpha",
            "url": "http://192.168.1.10:8400",
            "collections": "sentinel-2-l2a,landsat-9",
            "item_count": 42,
            "chunk_count": 1000,
            "chunks_bytes": 500000,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "registered"
        assert data["node"]["node_id"] == "node-1"
        assert data["node"]["collections"] == ["sentinel-2-l2a", "landsat-9"]

        # Verify in node list
        resp = await client.get("/nodes")
        assert resp.json()["count"] == 1


@pytest.mark.asyncio
async def test_heartbeat(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Register first
        await client.post("/register", params={
            "node_id": "node-1",
            "node_name": "alpha",
            "url": "http://localhost:8400",
        })

        # Heartbeat with updated stats
        resp = await client.post("/heartbeat", params={
            "node_id": "node-1",
            "item_count": 99,
            "chunk_count": 5000,
        })
        assert resp.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_list_nodes_filter(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/register", params={
            "node_id": "n1", "node_name": "a", "url": "http://a:8400",
            "collections": "s2",
        })
        await client.post("/register", params={
            "node_id": "n2", "node_name": "b", "url": "http://b:8400",
            "collections": "landsat",
        })

        resp = await client.get("/nodes")
        assert resp.json()["count"] == 2


@pytest.mark.asyncio
async def test_seed_nodes(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Node with data
        await client.post("/register", params={
            "node_id": "n1", "node_name": "alpha", "url": "http://a:8400",
            "collections": "s2", "item_count": 10, "chunks_bytes": 1000,
        })
        # Node without data
        await client.post("/register", params={
            "node_id": "n2", "node_name": "beta",
            "collections": "", "item_count": 0,
        })

        resp = await client.get("/seed/nodes")
        seeds = resp.json()["seed_nodes"]
        assert len(seeds) == 1  # only the one with data + URL
        assert seeds[0]["node_id"] == "n1"


@pytest.mark.asyncio
async def test_add_peer_beacon(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Add self as peer (for testing — in reality it would be another beacon)
        # This will fail the sync (can't sync with self through ASGI transport)
        # but the peer should be registered
        resp = await client.post("/beacon/peer", params={"url": "http://other-beacon:8400"})
        assert resp.status_code == 200
        assert resp.json()["status"] == "added"

        # List peer beacons
        resp = await client.get("/beacon/peers")
        assert resp.json()["count"] == 1
        assert resp.json()["beacons"][0]["url"] == "http://other-beacon:8400"


@pytest.mark.asyncio
async def test_exchange_nodes(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Simulate receiving nodes from a peer beacon
        resp = await client.post("/beacon/exchange", json={
            "nodes": [
                {
                    "node_id": "remote-1",
                    "node_name": "remote-alpha",
                    "url": "http://remote:8400",
                    "collections": ["landsat-9"],
                    "item_count": 100,
                    "chunk_count": 5000,
                    "chunks_bytes": 1000000,
                },
                {
                    "node_id": "remote-2",
                    "node_name": "remote-beta",
                    "url": "http://remote2:8400",
                    "collections": ["sentinel-2-l2a"],
                    "item_count": 50,
                },
            ]
        })
        assert resp.json()["merged"] == 2

        # Verify they appear in our registry
        resp = await client.get("/nodes")
        assert resp.json()["count"] == 2

        # Exchange again — should not duplicate
        resp = await client.post("/beacon/exchange", json={
            "nodes": [{"node_id": "remote-1", "node_name": "remote-alpha", "url": "http://remote:8400"}]
        })
        assert resp.json()["merged"] == 0  # already exists


@pytest.mark.asyncio
async def test_network_stats(transport):
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        await client.post("/register", params={
            "node_id": "n1", "node_name": "a", "url": "http://a:8400",
            "collections": "s2,landsat", "item_count": 10, "chunk_count": 100, "chunks_bytes": 5000,
        })
        await client.post("/register", params={
            "node_id": "n2", "node_name": "b", "url": "http://b:8400",
            "collections": "s2", "item_count": 5, "chunk_count": 50, "chunks_bytes": 2000,
        })

        resp = await client.get("/")
        data = resp.json()
        assert data["nodes_alive"] == 2
        assert data["total_items"] == 15
        assert data["total_chunks"] == 150
        assert "s2" in data["collections"]
        assert "landsat" in data["collections"]
