"""EarthGrid CLI — start a node or beacon."""
import argparse
import uvicorn

from .config import settings


def main():
    parser = argparse.ArgumentParser(
        prog="earthgrid",
        description="EarthGrid — Distributed satellite data storage",
    )
    parser.add_argument(
        "role",
        nargs="?",
        default=settings.role,
        choices=["node", "beacon"],
        help="Run as data node (default) or beacon",
    )
    parser.add_argument("--host", default=settings.host)
    parser.add_argument("--port", type=int, default=settings.port)
    parser.add_argument("--beacon", default=settings.beacon_url, help="Beacon URL to register with")
    parser.add_argument("--public-url", default=settings.public_url, help="This node's public URL")
    args = parser.parse_args()

    # Override settings
    settings.role = args.role
    settings.beacon_url = args.beacon
    settings.public_url = args.public_url

    if args.role == "beacon":
        print(f"🌐 EarthGrid Beacon starting on {args.host}:{args.port}")
        uvicorn.run("earthgrid.beacon:beacon_app", host=args.host, port=args.port)
    else:
        print(f"🌍 EarthGrid Node starting on {args.host}:{args.port}")
        if args.beacon:
            print(f"   → Registering with beacon: {args.beacon}")
        uvicorn.run("earthgrid.main:app", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
