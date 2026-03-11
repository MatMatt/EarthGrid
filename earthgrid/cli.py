"""EarthGrid CLI — start a node or beacon."""
import argparse
import sys

import uvicorn

from . import __version__
from .config import settings


def main():
    parser = argparse.ArgumentParser(
        prog="earthgrid",
        description="EarthGrid — Distributed Earth observation data storage",
    )
    parser.add_argument("--version", action="version", version=f"EarthGrid {__version__}")

    sub = parser.add_subparsers(dest="command")

    # --- Node ---
    node_p = sub.add_parser("node", help="Run as data node (stores and serves chunks)")
    node_p.add_argument("--host", default=settings.host)
    node_p.add_argument("--port", type=int, default=settings.port)
    node_p.add_argument("--name", default=settings.node_name, help="Node name")
    node_p.add_argument("--beacon", default=settings.beacon_url, help="Beacon URL to register with")
    node_p.add_argument("--public-url", default=settings.public_url, help="This node's public URL")
    node_p.add_argument("--store", default=str(settings.store_path), help="Chunk store path")
    node_p.add_argument("--peers", nargs="*", default=[], help="Direct peer URLs (no beacon)")

    # --- Beacon ---
    beacon_p = sub.add_parser("beacon", help="Run as beacon (coordinates nodes, routes queries)")
    beacon_p.add_argument("--host", default=settings.host)
    beacon_p.add_argument("--port", type=int, default=settings.port)
    beacon_p.add_argument("--name", default=settings.node_name, help="Beacon name")
    beacon_p.add_argument("--beacon-peers", nargs="*", default=[], help="Other beacon URLs to federate with")

    # --- Info ---
    sub.add_parser("info", help="Show version and config")

    args = parser.parse_args()

    if args.command == "info" or args.command is None:
        print(f"EarthGrid v{__version__}")
        print(f"  Python: {sys.version.split()[0]}")
        print(f"  Platform: {sys.platform}")
        try:
            import rasterio
            print(f"  Rasterio: {rasterio.__version__} (geospatial ingest available)")
        except ImportError:
            print("  Rasterio: not installed (install with: pip install earthgrid[geo])")
        print()
        print("Usage:")
        print("  earthgrid node     Start a data node")
        print("  earthgrid beacon   Start a beacon (coordinator)")
        print()
        print("Quick start:")
        print("  earthgrid node --port 8400")
        print("  earthgrid beacon --port 8400 --beacon-peers http://other:8400")
        print("  earthgrid node --beacon http://beacon:8400")
        if args.command is None:
            return

    elif args.command == "node":
        settings.node_name = args.name
        settings.beacon_url = args.beacon
        settings.public_url = args.public_url
        if args.peers:
            settings.peers = args.peers

        print(f"🌍 EarthGrid Node v{__version__}")
        print(f"   Name: {settings.node_name}")
        print(f"   Store: {args.store}")
        print(f"   Listen: {args.host}:{args.port}")
        if args.beacon:
            print(f"   Beacon: {args.beacon}")
        if args.peers:
            print(f"   Peers: {', '.join(args.peers)}")
        print()
        uvicorn.run(
            "earthgrid.main:app",
            host=args.host,
            port=args.port,
            log_level="info",
        )

    elif args.command == "beacon":
        settings.node_name = args.name
        settings.role = "beacon"
        if args.beacon_peers:
            settings.beacon_peers = args.beacon_peers

        print(f"🌐 EarthGrid Beacon v{__version__}")
        print(f"   Name: {settings.node_name}")
        print(f"   Listen: {args.host}:{args.port}")
        if args.beacon_peers:
            print(f"   Peer beacons: {', '.join(args.beacon_peers)}")
        print()
        uvicorn.run(
            "earthgrid.beacon:beacon_app",
            host=args.host,
            port=args.port,
            log_level="info",
        )


if __name__ == "__main__":
    main()
