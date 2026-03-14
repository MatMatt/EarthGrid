"""CDSE (Copernicus Data Space Ecosystem) integration.

Fetch Sentinel products directly from CDSE OData API,
download bands, and ingest into the local EarthGrid node.

Requires a CDSE account: https://dataspace.copernicus.eu
"""
from __future__ import annotations
import asyncio
import logging
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path

import httpx

logger = logging.getLogger("earthgrid.cdse")

# CDSE endpoints
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
ODATA_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1"
DOWNLOAD_URL = "https://zipper.dataspace.copernicus.eu/odata/v1"

# Sentinel-2 band files at different resolutions
S2_BANDS_10M = ["B02", "B03", "B04", "B08", "TCI"]
S2_BANDS_20M = ["B05", "B06", "B07", "B8A", "B11", "B12", "SCL"]
S2_BANDS_60M = ["B01", "B09"]
S2_ALL_BANDS = S2_BANDS_10M + S2_BANDS_20M + S2_BANDS_60M


class CDSEClient:
    """Client for CDSE OData API."""

    def __init__(self, username: str = "", password: str = ""):
        self.username = username
        self.password = password
        self._token: str = ""
        self._token_expires: float = 0

    async def get_token(self) -> str:
        """Get or refresh CDSE access token."""
        import time
        if self._token and time.time() < self._token_expires - 60:
            return self._token

        if not self.username or not self.password:
            raise ValueError(
                "CDSE credentials required. Add a source user via CLI:\n"
                "  earthgrid users add --provider cdse --username <email>"
            )

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(TOKEN_URL, data={
                "grant_type": "password",
                "username": self.username,
                "password": self.password,
                "client_id": "cdse-public",
            })
            resp.raise_for_status()
            data = resp.json()
            self._token = data["access_token"]
            self._token_expires = time.time() + data.get("expires_in", 600)
            logger.info("CDSE token acquired")
            return self._token

    async def search(
        self,
        collection: str = "SENTINEL-2",
        product_type: str = "S2MSI2A",
        bbox: list[float] | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        cloud_cover: float = 30.0,
        limit: int = 10,
    ) -> list[dict]:
        """Search CDSE catalog for products.

        Args:
            collection: SENTINEL-2, SENTINEL-1, etc.
            product_type: S2MSI2A (L2A), S2MSI1C (L1C), etc.
            bbox: [west, south, east, north]
            start_date: YYYY-MM-DD
            end_date: YYYY-MM-DD
            cloud_cover: Max cloud cover %
            limit: Max results

        Returns:
            List of product dicts with id, name, date, geometry, etc.
        """
        filters = [
            f"Collection/Name eq '{collection}'",
        ]
        if product_type:
            filters.append(
                f"Attributes/OData.CSC.StringAttribute/any(att:att/Name eq "
                f"'productType' and att/OData.CSC.StringAttribute/Value eq '{product_type}')"
            )
        if bbox:
            w, s, e, n = bbox
            filters.append(
                f"OData.CSC.Intersects(area=geography'SRID=4326;POLYGON(("
                f"{w} {s},{e} {s},{e} {n},{w} {n},{w} {s}))')"
            )
        if start_date:
            filters.append(f"ContentDate/Start ge {start_date}T00:00:00.000Z")
        if end_date:
            filters.append(f"ContentDate/Start le {end_date}T23:59:59.999Z")
        if cloud_cover is not None:
            filters.append(
                f"Attributes/OData.CSC.DoubleAttribute/any(att:att/Name eq "
                f"'cloudCover' and att/OData.CSC.DoubleAttribute/Value le {cloud_cover})"
            )

        filter_str = " and ".join(filters)
        params = {
            "$filter": filter_str,
            "$orderby": "ContentDate/Start desc",
            "$top": limit,
            "$expand": "Attributes",
        }

        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.get(f"{ODATA_URL}/Products", params=params)
            resp.raise_for_status()
            results = resp.json().get("value", [])

        products = []
        for r in results:
            # Extract cloud cover from attributes
            cc = None
            for attr in r.get("Attributes", []):
                if attr.get("Name") == "cloudCover":
                    cc = attr.get("Value")
                    break

            products.append({
                "id": r["Id"],
                "name": r["Name"],
                "date": r.get("ContentDate", {}).get("Start", ""),
                "size_mb": round(r.get("ContentLength", 0) / 1024 / 1024, 1),
                "cloud_cover": cc,
                "geometry": r.get("GeoFootprint", {}),
                "online": r.get("Online", True),
            })

        return products

    async def download_product(
        self,
        product_id: str,
        output_dir: Path,
        bands: list[str] | None = None,
    ) -> list[Path]:
        """Download a CDSE product (or specific bands).

        Args:
            product_id: CDSE product UUID
            output_dir: Where to save files
            bands: Specific bands to extract (None = all)

        Returns:
            List of downloaded file paths.
        """
        token = await self.get_token()
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        url = f"{DOWNLOAD_URL}/Products({product_id})/$value"
        headers = {"Authorization": f"Bearer {token}"}

        zip_path = output_dir / f"{product_id}.zip"
        downloaded_files = []

        async with httpx.AsyncClient(timeout=300, follow_redirects=True) as client:
            logger.info(f"Downloading product {product_id}...")
            async with client.stream("GET", url, headers=headers) as resp:
                resp.raise_for_status()
                with open(zip_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=8192):
                        f.write(chunk)

        # Extract relevant bands from ZIP
        logger.info(f"Extracting bands from {zip_path.name}...")
        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                # Match band files (e.g. *_B04_10m.jp2, *_SCL_20m.jp2, *_TCI_10m.jp2)
                if not name.endswith((".jp2", ".tif", ".tiff")):
                    continue

                fname_upper = Path(name).stem.upper()

                # If bands filter specified, check match
                if bands:
                    if not any(b.upper() in fname_upper for b in bands):
                        continue

                # Extract
                out_path = output_dir / Path(name).name
                with zf.open(name) as src, open(out_path, "wb") as dst:
                    dst.write(src.read())
                downloaded_files.append(out_path)
                logger.info(f"  Extracted: {out_path.name}")

        # Clean up ZIP
        zip_path.unlink(missing_ok=True)

        return downloaded_files


async def fetch_and_ingest(
    cdse_client: CDSEClient,
    chunk_store,
    catalog,
    collection: str = "SENTINEL-2",
    product_type: str = "S2MSI2A",
    bbox: list[float] | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    cloud_cover: float = 30.0,
    bands: list[str] | None = None,
    limit: int = 1,
    earthgrid_collection: str = "sentinel-2-l2a",
) -> list[dict]:
    """Search CDSE, download, and ingest into EarthGrid.

    Returns list of ingested item summaries.
    """
    from .ingest import ingest_cog

    # Search
    products = await cdse_client.search(
        collection=collection,
        product_type=product_type,
        bbox=bbox,
        start_date=start_date,
        end_date=end_date,
        cloud_cover=cloud_cover,
        limit=limit,
    )

    if not products:
        return []

    results = []
    for product in products:
        with tempfile.TemporaryDirectory(prefix="earthgrid_cdse_") as tmpdir:
            tmpdir = Path(tmpdir)

            # Download
            files = await cdse_client.download_product(
                product_id=product["id"],
                output_dir=tmpdir,
                bands=bands,
            )

            # Ingest each band file
            for fpath in files:
                try:
                    # Convert JP2 to GeoTIFF if needed
                    if fpath.suffix.lower() == ".jp2":
                        tif_path = fpath.with_suffix(".tif")
                        import rasterio
                        with rasterio.open(fpath) as src:
                            profile = src.profile.copy()
                            profile.update(driver="GTiff", compress="lzw", tiled=True)
                            with rasterio.open(tif_path, "w", **profile) as dst:
                                for band_i in range(1, src.count + 1):
                                    dst.write(src.read(band_i), band_i)
                        fpath = tif_path

                    # Build item ID from filename
                    stem = fpath.stem.upper()
                    # Extract tile, date, band info
                    item_id = f"{product['name']}_{fpath.stem}"

                    item = ingest_cog(
                        file_path=fpath,
                        chunk_store=chunk_store,
                        catalog=catalog,
                        collection_id=earthgrid_collection,
                        item_id=item_id,
                    )
                    results.append({
                        "item_id": item.id,
                        "chunks": len(item.chunk_hashes),
                        "source": product["name"],
                        "band_file": fpath.name,
                    })
                except Exception as e:
                    logger.error(f"Failed to ingest {fpath.name}: {e}")
                    results.append({
                        "item_id": None,
                        "error": str(e),
                        "band_file": fpath.name,
                    })

    return results
