"""EarthGrid — Distributed satellite data storage and access."""
__version__ = "0.2.0"

from .client import Client, Item

__all__ = ["Client", "Item", "__version__"]
