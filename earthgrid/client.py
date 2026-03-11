"""EarthGrid Client — M2M access to any EarthGrid node."""
import httpx
from pathlib import Path
from typing import Optional


class Item:
    """A STAC item returned from search."""

    def __init__(self, data: dict, client: "Client"):
        self._data = data
        self._client = client
        self.id = data["id"]
        self.collection = data.get("collection", "")
        self.bbox = data.get("bbox", [])
        self.properties = data.get("properties", {})
        self.assets = data.get("assets", {})
        self.source_node = data.get("earthgrid:source_node", "")

    @property
    def datetime(self) -> str:
        return self.properties.get("datetime", "")

    @property
    def chunk_count(self) -> int:
        data_asset = self.assets.get("data", {})
        return data_asset.get("earthgrid:chunk_count", 0)

    def download(self, path: str | Path) -> Path:
        """Download reconstructed GeoTIFF."""
        return self._client.download(self, path)

    def __repr__(self):
        return f"Item({self.id}, collection={self.collection})"


class Client:
    """Connect to an EarthGrid node or beacon.

    Usage:
        eg = Client("http://localhost:8400")
        items = eg.search("sentinel-2-l2a", bbox=[11, 47, 12, 48])
        items[0].download("scene.tif")
    """

    def __init__(self, url: str, timeout: float = 30):
        self.url = url.rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, **params) -> dict:
        with httpx.Client(timeout=self.timeout) as c:
            resp = c.get(f"{self.url}{path}", params=params)
            resp.raise_for_status()
            return resp.json()

    def _get_bytes(self, path: str) -> bytes:
        with httpx.Client(timeout=self.timeout) as c:
            resp = c.get(f"{self.url}{path}")
            resp.raise_for_status()
            return resp.content

    # --- Node Info ---

    def info(self) -> dict:
        """Get node/beacon info."""
        return self._get("/")

    def health(self) -> bool:
        """Check if node is healthy."""
        try:
            data = self._get("/health")
            return data.get("status") == "ok"
        except Exception:
            return False

    def stats(self) -> dict:
        """Get detailed node statistics."""
        return self._get("/stats")

    # --- Search ---

    def search(
        self,
        collection: str | None = None,
        bbox: list[float] | None = None,
        datetime: str | None = None,
        limit: int = 100,
    ) -> list[Item]:
        """Search for items. Returns list of Item objects."""
        params = {"limit": limit}
        if collection:
            params["collections"] = collection
        if bbox:
            params["bbox"] = ",".join(str(b) for b in bbox)
        if datetime:
            params["datetime"] = datetime

        data = self._get("/stac/search", **params)
        return [Item(f, self) for f in data.get("features", [])]

    def collections(self) -> list[dict]:
        """List available collections."""
        data = self._get("/stac/collections")
        return data.get("collections", [])

    # --- Download ---

    def download(self, item: Item | str, path: str | Path, collection: str = "") -> Path:
        """Download a file (reconstructed GeoTIFF from chunks).

        Args:
            item: Item object or item_id string
            path: Local file path to save to
            collection: Required if item is a string
        """
        if isinstance(item, Item):
            item_id = item.id
            col = item.collection
        else:
            item_id = item
            col = collection

        if not col:
            raise ValueError("collection required when item is a string")

        path = Path(path)
        data = self._get_bytes(f"/download/{col}/{item_id}")
        path.write_bytes(data)
        return path

    # --- Chunk-Level Access ---

    def get_chunk(self, sha: str) -> bytes | None:
        """Get raw chunk by SHA-256 hash."""
        try:
            return self._get_bytes(f"/chunks/{sha}")
        except httpx.HTTPStatusError:
            return None

    def has_chunk(self, sha: str) -> bool:
        """Check if chunk exists on this node."""
        try:
            with httpx.Client(timeout=self.timeout) as c:
                resp = c.head(f"{self.url}/chunks/{sha}")
                return resp.status_code == 200
        except Exception:
            return False

    def list_chunks(self, limit: int = 100) -> list[str]:
        """List chunk hashes on this node."""
        data = self._get("/chunks", limit=limit)
        return data.get("hashes", [])

    # --- Federation ---

    def peers(self) -> list[dict]:
        """List known peers."""
        data = self._get("/peers")
        return data.get("peers", [])

    def federated_search(
        self,
        collection: str | None = None,
        bbox: list[float] | None = None,
        datetime: str | None = None,
        limit: int = 100,
    ) -> list[Item]:
        """Search across all federated nodes."""
        params = {"limit": limit}
        if collection:
            params["collections"] = collection
        if bbox:
            params["bbox"] = ",".join(str(b) for b in bbox)
        if datetime:
            params["datetime"] = datetime

        data = self._get("/federation/search", **params)
        return [Item(f, self) for f in data.get("features", [])]

    # --- Ingest ---

    def ingest(
        self,
        file_path: str | Path,
        collection: str = "default",
        item_id: str | None = None,
    ) -> dict:
        """Upload and ingest a GeoTIFF."""
        file_path = Path(file_path)
        with httpx.Client(timeout=300) as c:
            with open(file_path, "rb") as f:
                resp = c.post(
                    f"{self.url}/ingest",
                    params={"collection": collection, "item_id": item_id or file_path.stem},
                    files={"file": (file_path.name, f)},
                )
                resp.raise_for_status()
                return resp.json()

    def __repr__(self):
        return f"Client({self.url})"
