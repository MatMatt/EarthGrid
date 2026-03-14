"""EarthGrid — Distributed satellite data storage and access."""
__version__ = "0.3.0"

# Beacon discovery: fetch from GitHub, fall back to hardcoded
_BEACON_LIST_URL = "https://raw.githubusercontent.com/MatMatt/EarthGrid/master/beacons.json"
_FALLBACK_BEACON = "https://mattiuzzi.zapto.org/earthgrid"


def get_default_beacon() -> str:
    """Fetch the primary beacon URL from GitHub, with local fallback."""
    try:
        import urllib.request, json
        with urllib.request.urlopen(_BEACON_LIST_URL, timeout=5) as resp:
            data = json.loads(resp.read())
            beacons = data.get("beacons", [])
            if beacons:
                return beacons[0]["url"]
    except Exception:
        pass
    return _FALLBACK_BEACON


# Keep DEFAULT_BEACON as static fallback for backward compat
DEFAULT_BEACON = _FALLBACK_BEACON

from .client import Client, Item

__all__ = ["Client", "Item", "__version__", "get_default_beacon"]
