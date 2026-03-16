"""EarthGrid — Distributed satellite data storage and access."""
__version__ = "0.3.1"

# Bootstrap peer discovery: fetch from GitHub, fall back to hardcoded
_BOOTSTRAP_URL = "https://raw.githubusercontent.com/MatMatt/EarthGrid/master/beacons.json"
_FALLBACK_BOOTSTRAP = "https://mattiuzzi.zapto.org/earthgrid"


def get_bootstrap_peers() -> list[str]:
    """Fetch all bootstrap peer URLs from GitHub, with local fallback."""
    try:
        import urllib.request, json
        with urllib.request.urlopen(_BOOTSTRAP_URL, timeout=5) as resp:
            data = json.loads(resp.read())
            peers = data.get("bootstrap_peers", data.get("beacons", []))
            if peers:
                return [p["url"] for p in peers]
    except Exception:
        pass
    return [_FALLBACK_BOOTSTRAP]


def get_default_peer() -> str:
    """Return the first reachable bootstrap peer."""
    import urllib.request
    for url in get_bootstrap_peers():
        try:
            health_url = url.rstrip("/") + "/health"
            with urllib.request.urlopen(health_url, timeout=5) as resp:
                if resp.status == 200:
                    return url
        except Exception:
            continue
    # All unreachable — return first anyway
    return get_bootstrap_peers()[0]


# Backward compatibility
DEFAULT_BEACON = _FALLBACK_BOOTSTRAP

from .client import Client, Item

__all__ = ["Client", "Item", "__version__", "get_bootstrap_peers", "get_default_peer", "DEFAULT_BEACON"]
