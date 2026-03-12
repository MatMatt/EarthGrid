"""EarthGrid — Distributed satellite data storage and access."""
__version__ = "0.3.0"

# Default beacon — the first node in the network
DEFAULT_BEACON = "http://mattiuzzi.zapto.org:8400"

from .client import Client, Item

__all__ = ["Client", "Item", "__version__"]
