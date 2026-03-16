"""EarthGrid — Distributed satellite data storage and access."""
__version__ = "0.3.2"

# Hardcoded bootstrap peers — updated via package releases (pip install --upgrade)
# To add your own peers, set EARTHGRID_BOOTSTRAP_PEERS env var (comma-separated URLs)
# or add them to ~/.earthgrid/config.json under "bootstrap_peers": [...]
BOOTSTRAP_PEERS = [
    "https://mattiuzzi.zapto.org/earthgrid",
]


def get_bootstrap_peers() -> list[str]:
    """Return bootstrap peer URLs (hardcoded + user-configured)."""
    import os, json
    peers = list(BOOTSTRAP_PEERS)

    # Allow user overrides via env var
    env_peers = os.environ.get("EARTHGRID_BOOTSTRAP_PEERS", "")
    if env_peers:
        peers.extend(u.strip() for u in env_peers.split(",") if u.strip())

    # Allow user overrides via config file
    from pathlib import Path; config_file = Path.home() / ".earthgrid" / "config.json"
    if config_file.exists():
        try:
            cfg = json.loads(config_file.read_text())
            peers.extend(cfg.get("bootstrap_peers", []))
        except Exception:
            pass

    # Deduplicate while preserving order
    seen = set()
    unique = []
    for p in peers:
        p = p.rstrip("/")
        if p not in seen:
            seen.add(p)
            unique.append(p)
    return unique


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
DEFAULT_BEACON = BOOTSTRAP_PEERS[0] if BOOTSTRAP_PEERS else ""

from .client import Client, Item

__all__ = [
    "Client", "Item", "__version__",
    "BOOTSTRAP_PEERS", "get_bootstrap_peers", "get_default_peer",
    "DEFAULT_BEACON",
]
