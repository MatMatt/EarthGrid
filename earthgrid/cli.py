"""EarthGrid CLI — start a node or beacon."""
import argparse
import json
import sys
from pathlib import Path

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

    # --- Start ---
    start_p = sub.add_parser("start", help="Start EarthGrid node (uses setup config)")
    start_p.add_argument("--host", default=settings.host)
    start_p.add_argument("--port", type=int, default=settings.port)
    start_p.add_argument("--name", default=settings.node_name, help="Node name")
    start_p.add_argument("--beacon", default=settings.beacon_url, help="Beacon URL to register with")
    start_p.add_argument("--also-beacon", action="store_true", default=settings.also_beacon, help="Also act as beacon")
    start_p.add_argument("--public-url", default=settings.public_url, help="Public URL for this node")
    start_p.add_argument("--peers", nargs="*", default=[], help="Direct peer URLs")
    start_p.add_argument("--beacon-peers", nargs="*", default=[], help="Other beacon URLs to federate with")

    # --- Setup ---
    setup_p = sub.add_parser("setup", help="Interactive first-time setup")
    setup_p.add_argument("--port", type=int, default=8400)

    # --- Info ---
    sub.add_parser("info", help="Show version and config")

    # --- Status ---
    sub.add_parser("status", help="Show node status and storage usage")

    # --- Resize ---
    p_resize = sub.add_parser("resize", help="Resize storage allocation")
    p_resize.add_argument("size_gb", type=float, help="New storage limit in GB")
    p_resize.add_argument("--force", action="store_true", help="Evict chunks if over limit")

    # --- Process ---
    p_process = sub.add_parser("process", help="Run a processing operation on STAC item(s)")
    p_process.add_argument("item_id", nargs="+", help="Source STAC item ID(s)")
    p_process.add_argument("--op", required=True, help="Operation: ndvi, ndwi, ndsi, evi, cloud_mask, true_color, band_math")
    p_process.add_argument("--expression", default="", help="Band math expression")
    p_process.add_argument("--output-collection", default=None)
    p_process.add_argument("--output-id", default=None)

    # --- Ops ---
    sub.add_parser("ops", help="List available processing operations")

    args = parser.parse_args()

    if args.command == "setup":
        _interactive_setup(args)
        return

    if args.command == "status":
        _cmd_status()
        return

    if args.command == "resize":
        _cmd_resize(args.size_gb, args.force)
        return

    if args.command == "ops":
        _cmd_ops()
        return

    if args.command == "process":
        _cmd_process(args)
        return

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

    elif args.command == "start":
        # Load config from setup if exists
        config_file = Path.home() / ".earthgrid" / "config.json"
        if config_file.exists():
            import json as _json
            cfg = _json.loads(config_file.read_text())
            if not args.name or args.name == "earthgrid-node":
                settings.node_name = cfg.get("node_name", settings.node_name)
            settings.also_beacon = args.also_beacon or cfg.get("also_beacon", False)
            if not args.beacon:
                settings.beacon_url = cfg.get("beacon_url", "")
            if not args.beacon_peers:
                settings.beacon_peers = cfg.get("beacon_peers", [])

        settings.node_name = args.name
        settings.beacon_url = args.beacon
        settings.public_url = args.public_url
        settings.also_beacon = args.also_beacon
        if args.peers:
            settings.peers = args.peers
        if args.beacon_peers:
            settings.beacon_peers = args.beacon_peers

        print(f"🌍 EarthGrid v{__version__}")
        print(f"   Name:    {settings.node_name}")
        print(f"   Listen:  {args.host}:{args.port}")
        print(f"   Beacon:  {'yes' if settings.also_beacon else 'no'}")
        if settings.beacon_url:
            print(f"   Joins:   {settings.beacon_url}")
        if args.peers:
            print(f"   Peers:   {', '.join(args.peers)}")
        if settings.beacon_peers:
            print(f"   Beacon peers: {', '.join(settings.beacon_peers)}")
        print()
        uvicorn.run(
            "earthgrid.main:app",
            host=args.host,
            port=args.port,
            log_level="info",
        )


def _human_bytes(b: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"


def _load_config() -> dict:
    config_file = Path.home() / ".earthgrid" / "config.json"
    if config_file.exists():
        return json.loads(config_file.read_text())
    return {}


def _save_config(cfg: dict):
    config_file = Path.home() / ".earthgrid" / "config.json"
    config_file.parent.mkdir(parents=True, exist_ok=True)
    config_file.write_text(json.dumps(cfg, indent=2) + "\n")


def _store_usage(store_path: Path) -> tuple[int, int]:
    """Returns (total_bytes, chunk_count)."""
    total, count = 0, 0
    if store_path.exists():
        for p in store_path.rglob("*"):
            if p.is_file() and len(p.name) == 64:
                total += p.stat().st_size
                count += 1
    return total, count


def _cmd_status():
    cfg = _load_config()
    if not cfg:
        print("No config found. Run 'earthgrid setup' first.")
        sys.exit(1)
    store_path = Path(cfg.get("store_path", "./data/store"))
    limit_gb = cfg.get("storage_limit_gb", 50.0)
    limit_bytes = int(limit_gb * 1024**3)
    used, chunks = _store_usage(store_path)
    pct = (used / limit_bytes * 100) if limit_bytes > 0 else 0

    config_file = Path.home() / ".earthgrid" / "config.json"
    print(f"EarthGrid Node v{__version__}")
    print(f"  Config:    {config_file}")
    print(f"  Name:      {cfg.get('node_name', 'earthgrid-node')}")
    print(f"  Port:      {cfg.get('port', 8400)}")
    print(f"  Beacon:    {'yes' if cfg.get('also_beacon') else 'no'}")
    print(f"  Storage:   {_human_bytes(used)} / {limit_gb:.1f} GB ({pct:.1f}%)")
    print(f"  Chunks:    {chunks}")
    print(f"  Store:     {store_path}")
    peers = cfg.get("peers", [])
    print(f"  Peers:     {len(peers)}")


def _cmd_resize(new_gb: float, force: bool):
    if new_gb <= 0:
        print("Error: size must be > 0 GB")
        sys.exit(1)
    cfg = _load_config()
    if not cfg:
        print("No config found. Run 'earthgrid setup' first.")
        sys.exit(1)
    old_gb = cfg.get("storage_limit_gb", 50.0)
    store_path = Path(cfg.get("store_path", "./data/store"))
    used, _ = _store_usage(store_path)
    used_gb = used / 1024**3

    if new_gb < used_gb and not force:
        print(f"Error: current usage ({used_gb:.2f} GB) exceeds new limit ({new_gb:.1f} GB).")
        print(f"Use --force to evict chunks.")
        sys.exit(1)

    if new_gb < used_gb and force:
        target_bytes = int(new_gb * 1024**3)
        chunks = []
        for p in store_path.rglob("*"):
            if p.is_file() and len(p.name) == 64:
                chunks.append((p.stat().st_mtime, p.stat().st_size, p))
        chunks.sort(key=lambda x: x[0])  # oldest first
        current = sum(s for _, s, _ in chunks)
        evicted = 0
        for _, size, path in chunks:
            if current <= target_bytes:
                break
            path.unlink()
            current -= size
            evicted += 1
        print(f"Evicted {evicted} chunks to fit new limit.")

    cfg["storage_limit_gb"] = new_gb
    _save_config(cfg)
    direction = "↑" if new_gb > old_gb else "↓"
    print(f"Storage resized: {old_gb:.1f} GB → {new_gb:.1f} GB {direction}")


def _cmd_ops():
    from .processing import OPERATIONS
    print("Available operations:")
    for name, fn in OPERATIONS.items():
        doc = (fn.__doc__ or "").strip().split("\n")[0]
        print(f"  {name:15s} {doc}")


def _cmd_process(args):
    from .chunk_store import ChunkStore
    from .catalog import Catalog
    from .processing import Processor

    cfg = _load_config()
    store_path = Path(cfg.get("store_path", "./data/store"))
    catalog_path = Path(cfg.get("catalog_path", "./data/catalog.db"))

    cs = ChunkStore(store_path, limit_gb=cfg.get("storage_limit_gb", 50.0))
    cat = Catalog(catalog_path)
    proc = Processor(cs, cat)

    item_ids = args.item_id if len(args.item_id) > 1 else args.item_id[0]
    try:
        result = proc.process(
            item_id=item_ids,
            operation=args.op,
            output_collection=args.output_collection,
            output_item_id=args.output_id,
            expression=args.expression,
        )
        print(f"✅ {result.properties.get('earthgrid:description', args.op)}")
        print(f"   Source:  {item_ids}")
        print(f"   Result:  {result.id} ({result.collection})")
        print(f"   Chunks:  {len(result.chunk_hashes)}")
        print(f"   Bands:   {result.properties.get('earthgrid:band_names', [])}")
    except (ValueError, KeyError) as e:
        print(f"Error: {e}")
        sys.exit(1)


def _interactive_setup(args):
    """Interactive first-time setup."""
    import json
    from pathlib import Path

    print(f"🌍 EarthGrid v{__version__} — Setup\n")

    # Storage
    while True:
        try:
            gb = input("How much disk space to contribute? [50] GB: ").strip()
            gb = float(gb) if gb else 50.0
            if gb < 1:
                print("  Minimum 1 GB")
                continue
            break
        except ValueError:
            print("  Please enter a number")

    # Participation mode
    print("\nHow do you want to participate?")
    print("  [1] Node + Beacon (recommended — store data AND help others find it)")
    print("  [2] Node only (store data, but don't coordinate)")
    mode_input = input("Choose [1]: ").strip()
    also_beacon = mode_input != "2"

    # Ingest capability
    ingest_input = input("Will you ingest GeoTIFF/satellite data? [y/N]: ").strip().lower()
    want_ingest = ingest_input in ("y", "yes")

    if want_ingest:
        try:
            import rasterio
            print("  ✓ rasterio already installed")
        except ImportError:
            print("  Installing rasterio (this may take a minute)...")
            import subprocess as _sp
            result = _sp.run(
                [sys.executable, "-m", "pip", "install", "rasterio>=1.3", "numpy>=1.24"],
                capture_output=True, text=True,
            )
            if result.returncode == 0:
                print("  ✓ rasterio installed")
            else:
                print("  ⚠ rasterio install failed — you can install it manually later:")
                print("    pip install rasterio numpy")

    # Store path
    default_store = Path.home() / ".earthgrid" / "data"
    store_input = input(f"Data directory? [{default_store}]: ").strip()
    store_path = Path(store_input) if store_input else default_store

    # Beacon URL (to connect to the network)
    beacon_url = input("Beacon URL to join? (leave empty for standalone): ").strip()

    # Port
    port = args.port

    # Write config
    config_dir = Path.home() / ".earthgrid"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "config.json"

    config = {
        "storage_limit_gb": gb,
        "also_beacon": also_beacon,
        "store_path": str(store_path / "store"),
        "catalog_path": str(store_path / "catalog.db"),
        "port": port,
    }
    if beacon_url:
        config["beacon_url"] = beacon_url

    config_file.write_text(json.dumps(config, indent=2))

    # Create store directory
    store_path.mkdir(parents=True, exist_ok=True)

    # Write .env for easy override
    env_file = config_dir / ".env"
    env_lines = [
        f"EARTHGRID_STORAGE_LIMIT_GB={gb}",
        f"EARTHGRID_ALSO_BEACON={'true' if also_beacon else 'false'}",
        f"EARTHGRID_STORE_PATH={store_path / 'store'}",
        f"EARTHGRID_CATALOG_PATH={store_path / 'catalog.db'}",
        f"EARTHGRID_PORT={port}",
    ]
    if beacon_url:
        env_lines.append(f"EARTHGRID_BEACON_URL={beacon_url}")
    env_file.write_text("\n".join(env_lines) + "\n")

    print(f"\n✅ EarthGrid configured!")
    print(f"   Storage:  {gb} GB at {store_path}")
    print(f"   Beacon:   {'yes' if also_beacon else 'no'}")
    print(f"   Port:     {port}")
    print(f"   Config:   {config_file}")
    if beacon_url:
        print(f"   Joins:    {beacon_url}")
    print(f"\nStart with:")
    if also_beacon:
        print(f"   earthgrid node --also-beacon")
    else:
        print(f"   earthgrid node")


if __name__ == "__main__":
    main()
