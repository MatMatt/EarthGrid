"""Element84 Earth Search STAC — search and download COGs."""
from __future__ import annotations
import asyncio
import logging
import tempfile
from pathlib import Path

import httpx
import rasterio
from rasterio.transform import from_bounds

logger = logging.getLogger("earthgrid.element84")

STAC_API = "https://earth-search.aws.element84.com/v1"

BAND_MAP_S2 = {
    "B02": "blue", "B03": "green", "B04": "red", "B08": "nir",
    "B05": "rededge1", "B06": "rededge2", "B07": "rededge3",
    "B8A": "nir08", "B09": "nir09", "B11": "swir16", "B12": "swir22",
    "B01": "coastal", "SCL": "scl",
}


async def search_element84(
    bbox: tuple[float, float, float, float],
    start_date: str | None = None,
    end_date: str | None = None,
    cloud_cover: float = 30.0,
    limit: int = 5,
    collection: str = "sentinel-2-l2a",
) -> list[dict]:
    """Search Element84 STAC for items."""
    body = {
        "collections": [collection],
        "bbox": list(bbox),
        "limit": min(limit, 100),
        "query": {"eo:cloud_cover": {"lte": cloud_cover}},
        "sortby": [{"field": "properties.datetime", "direction": "desc"}],
    }
    if start_date or end_date:
        start = start_date or "2015-01-01"
        end = end_date or "2099-12-31"
        body["datetime"] = f"{start}T00:00:00Z/{end}T23:59:59Z"

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.post(f"{STAC_API}/search", json=body)
        resp.raise_for_status()
        data = resp.json()

    items = data.get("features", [])
    results = []
    for item in items:
        props = item.get("properties", {})
        results.append({
            "id": item["id"],
            "name": item["id"],
            "date": props.get("datetime", ""),
            "cloud_cover": props.get("eo:cloud_cover", -1),
            "bbox": item.get("bbox"),
            "assets": item.get("assets", {}),
            "geometry": item.get("geometry"),
        })
    return results


async def fetch_and_ingest_element84(
    chunk_store,
    catalog,
    bbox: tuple[float, float, float, float],
    start_date: str | None = None,
    end_date: str | None = None,
    cloud_cover: float = 30.0,
    bands: list[str] | None = None,
    limit: int = 1,
    earthgrid_collection: str = "sentinel-2-l2a",
) -> list[dict]:
    """Search Element84 and ingest COGs into EarthGrid."""
    from .ingest import ingest_cog

    items = await search_element84(
        bbox=bbox,
        start_date=start_date,
        end_date=end_date,
        cloud_cover=cloud_cover,
        limit=limit,
        collection=earthgrid_collection,
    )

    if not items:
        logger.info("No items found on Element84")
        return []

    target_bands = bands or ["B02", "B03", "B04", "B08", "SCL"]
    results = []

    async with httpx.AsyncClient(timeout=120, follow_redirects=True) as client:
        for item in items:
            date_str = item["date"][:10] if item["date"] else "?"
            cc = item["cloud_cover"]
            print(f"
  📦 {item['id']}  ({date_str}, {cc:.0f}% cloud)")
            assets = item["assets"]
            for band_name in target_bands:
                asset_key = BAND_MAP_S2.get(band_name, band_name.lower())
                if asset_key not in assets:
                    logger.warning(f"Band {band_name} ({asset_key}) not in assets for {item['id']}")
                    results.append({"error": f"Band {band_name} not found", "product": item["id"]})
                    continue

                cog_url = assets[asset_key]["href"]
                logger.info(f"Downloading {band_name} from {item['id']}...")
                print(f"  ⬇ {item['id']} / {band_name}...")

                try:
                    resp = await client.get(cog_url)
                    resp.raise_for_status()

                    with tempfile.NamedTemporaryFile(suffix=".tif", delete=False) as tmp:
                        tmp.write(resp.content)
                        tmp_path = Path(tmp.name)

                    try:
                        item_obj = ingest_cog(
                            file_path=tmp_path,
                            chunk_store=chunk_store,
                            catalog=catalog,
                            collection_id=earthgrid_collection,
                            item_id=f"{item['id']}_{band_name}",
                        )
                        results.append({"item_id": item_obj.id, "band": band_name, "product": item["id"]})
                        logger.info(f"Ingested {band_name} from {item['id']}")
                    finally:
                        tmp_path.unlink(missing_ok=True)
                except Exception as e:
                    logger.error(f"Failed {band_name} from {item['id']}: {e}")
                    results.append({"error": str(e), "product": item["id"], "band": band_name})

    return results
