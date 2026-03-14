"""EarthGrid CLI — start a node or beacon."""
from __future__ import annotations
import argparse
import json
import sys
from pathlib import Path

import uvicorn

from . import __version__
from .config import settings

GITHUB_SEEDS_URL = "https://matmatt.github.io/EarthGrid/peers.json"


def _fetch_github_seeds() -> str | None:
    """Fetch beacon URL from GitHub Pages seed list (bootstrap fallback)."""
    try:
        import urllib.request
        with urllib.request.urlopen(GITHUB_SEEDS_URL, timeout=5) as r:
            data = json.loads(r.read())
        seeds = data.get("seeds", [])
        if seeds:
            # Return the first seed URL as beacon
            print(f"   Seeds:   fetched {len(seeds)} from GitHub")
            return seeds[0]["url"]
    except Exception:
        pass
    return None


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

    # --- Fetch (CDSE) ---
    p_fetch = sub.add_parser("fetch", help="Fetch Sentinel data from CDSE and ingest")
    p_fetch.add_argument("--bbox", required=True, help="west,south,east,north")
    p_fetch.add_argument("--start", help="Start date YYYY-MM-DD")
    p_fetch.add_argument("--end", help="End date YYYY-MM-DD")
    p_fetch.add_argument("--cloud", type=float, default=30.0, help="Max cloud cover %% (default: 30)")
    p_fetch.add_argument("--bands", help="Comma-separated bands (e.g. B02,B03,B04,B08,SCL)")
    p_fetch.add_argument("--product-type", default="S2MSI2A", help="Product type (default: S2MSI2A)")
    p_fetch.add_argument("--limit", type=int, default=1, help="Max products to fetch")
    p_fetch.add_argument("--collection", default="sentinel-2-l2a", help="EarthGrid collection name")
    p_fetch.add_argument("--search-only", action="store_true", help="Only search, don't download")

    # --- Users (local credential management) ---
    p_users = sub.add_parser("users", help="Manage source user credentials (local only)")
    users_sub = p_users.add_subparsers(dest="users_action")
    users_sub.add_parser("list", help="List source users")
    p_users_add = users_sub.add_parser("add", help="Add a source user")
    p_users_add.add_argument("--name", required=True, help="Display name")
    p_users_add.add_argument("--provider", default="cdse", help="Provider (cdse, element84)")
    p_users_add.add_argument("--username", required=True, help="Login username/email")
    p_users_add.add_argument("--password", default="", help="Password (prompted if empty)")
    p_users_rm = users_sub.add_parser("remove", help="Remove a source user")
    p_users_rm.add_argument("user_id", type=int, help="User ID to remove")

    # --- Sync ---
    p_sync = sub.add_parser("sync", help="Pull data from a remote peer")
    p_sync.add_argument("peer_url", help="Remote node URL (e.g. http://host:8400)")
    p_sync.add_argument("--collections", default=None, help="Only sync these collections (comma-separated)")
    p_sync.add_argument("--max-items", type=int, default=0, help="Limit items to sync (0=all)")
    p_sync.add_argument("--dry-run", action="store_true", help="Only report what would be synced")

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

    if args.command == "fetch":
        _cmd_fetch(args)
        return

    if args.command == "users":
        _cmd_users(args)
        return

    if args.command == "sync":
        _cmd_sync(args)
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

        # Bootstrap: if no beacon configured, try GitHub seeds
        if not settings.beacon_url and not settings.also_beacon:
            seed_url = _fetch_github_seeds()
            if seed_url:
                settings.beacon_url = seed_url

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


def _cmd_users(args):
    """Manage source user credentials locally."""
    cfg = _load_config()
    db_path = Path(cfg.get("source_users_db", "./data/source_users.db")) if cfg else Path("./data/source_users.db")
    key = cfg.get("source_key", "") if cfg else ""

    from .source_users import SourceUserManager
    mgr = SourceUserManager(db_path, encryption_key=key)

    if args.users_action == "list":
        users = mgr.list_users(include_disabled=True)
        if not users:
            print("No source users configured.")
            print("Add one: earthgrid users add --name MyAccount --username me@example.com")
            return
        for u in users:
            status = "✓" if u["is_enabled"] and u["is_healthy"] else "✗"
            print(f"  {status} [{u['id']}] {u['name']} ({u['provider']}/{u['username']}) "
                  f"— {u['success_count']} ok, {u['fail_count']} fail, "
                  f"{u['total_downloaded_gb']:.1f} GB downloaded")

    elif args.users_action == "add":
        password = args.password
        if not password:
            import getpass
            password = getpass.getpass(f"Password for {args.username}: ")
        uid = mgr.add_user(
            name=args.name, provider=args.provider,
            username=args.username, password=password,
        )
        print(f"✓ Added source user '{args.name}' (id={uid})")

    elif args.users_action == "remove":
        ok = mgr.remove_user(args.user_id)
        if ok:
            print(f"✓ Removed source user {args.user_id}")
        else:
            print(f"✗ User {args.user_id} not found")

    else:
        print("Usage: earthgrid users [list|add|remove]")


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


def _cmd_fetch(args):
    """Fetch Sentinel data from CDSE."""
    import asyncio
    import os
    from .cdse import CDSEClient, fetch_and_ingest
    from .chunk_store import ChunkStore
    from .catalog import Catalog

    cfg = _load_config()

    # CDSE credentials from config or env
    username = cfg.get("cdse_username", os.environ.get("EARTHGRID_CDSE_USERNAME", ""))
    password = cfg.get("cdse_password", os.environ.get("EARTHGRID_CDSE_PASSWORD", ""))

    if not username or not password:
        print("⚠ CDSE credentials required for direct fetch from Copernicus.")
        print("  Each node needs its own free account — register at:")
        print("  https://dataspace.copernicus.eu")
        print("\n  Then configure via:")
        print("  earthgrid setup          (interactive)")
        print("  earthgrid config cdse_username <your-email>")
        print("  earthgrid config cdse_password <your-password>")
        print("\n  Without CDSE: use 'earthgrid replicate' to get data from other nodes.")
        sys.exit(1)

    client = CDSEClient(username=username, password=password)
    bbox = [float(x) for x in args.bbox.split(",")]

    if args.search_only:
        # Just search and display
        products = asyncio.run(client.search(
            bbox=bbox,
            start_date=args.start,
            end_date=args.end,
            cloud_cover=args.cloud,
            product_type=args.product_type,
            limit=args.limit,
        ))
        if not products:
            print("No products found.")
            return
        print(f"Found {len(products)} products:\n")
        for p in products:
            date = p["date"][:10] if p["date"] else "?"
            print(f"  {p['name']}")
            print(f"    Date: {date}  Cloud: {p['cloud_cover']}%  Size: {p['size_mb']} MB  Online: {p['online']}")
            print()
        return

    # Fetch and ingest
    store_path = Path(cfg.get("store_path", "./data/store"))
    catalog_path = Path(cfg.get("catalog_path", "./data/catalog.db"))
    cs = ChunkStore(store_path, limit_gb=cfg.get("storage_limit_gb", 50.0))
    cat = Catalog(catalog_path)

    band_list = [b.strip() for b in args.bands.split(",")] if args.bands else None

    print(f"Fetching from CDSE (bbox={args.bbox}, cloud≤{args.cloud}%)...")
    results = asyncio.run(fetch_and_ingest(
        cdse_client=client,
        chunk_store=cs,
        catalog=cat,
        bbox=bbox,
        start_date=args.start,
        end_date=args.end,
        cloud_cover=args.cloud,
        bands=band_list,
        product_type=args.product_type,
        limit=args.limit,
        earthgrid_collection=args.collection,
    ))

    ok = [r for r in results if r.get("item_id")]
    err = [r for r in results if r.get("error")]

    if ok:
        print(f"\n✅ Ingested {len(ok)} bands:")
        for r in ok:
            print(f"  {r['item_id']} ({r['chunks']} chunks)")
    if err:
        print(f"\n⚠ {len(err)} errors:")
        for r in err:
            print(f"  {r['band_file']}: {r['error']}")


def _cmd_sync(args):
    """Pull data from a remote peer."""
    import asyncio
    from .chunk_store import ChunkStore
    from .catalog import Catalog
    from .replication import Replicator

    cfg = _load_config()
    store_path = Path(cfg.get("store_path", "./data/store"))
    catalog_path = Path(cfg.get("catalog_path", "./data/catalog.db"))

    cs = ChunkStore(store_path, limit_gb=cfg.get("storage_limit_gb", 50.0))
    cat = Catalog(catalog_path)
    repl = Replicator(cs, cat)

    peer = args.peer_url.rstrip("/")
    col_list = [c.strip() for c in args.collections.split(",")] if args.collections else None

    print(f"{'[DRY RUN] ' if args.dry_run else ''}Syncing from {peer}...")

    result = asyncio.run(repl.sync_from_peer(
        peer_url=peer,
        collections=col_list,
        max_items=args.max_items,
        dry_run=args.dry_run,
    ))

    if result["errors"]:
        for e in result["errors"][:5]:
            print(f"  ⚠ {e}")

    print(f"\n{'Would sync' if args.dry_run else 'Synced'}:")
    print(f"  Collections:  {result['collections_synced']}")
    print(f"  Items:        {result['items_synced']}")
    print(f"  Chunks:       {result['chunks_downloaded']} downloaded, {result['chunks_skipped']} skipped")
    if result['bytes_downloaded']:
        print(f"  Data:         {_human_bytes(result['bytes_downloaded'])}")

    if not args.dry_run and result['items_synced']:
        print(f"\n✅ Data replicated from {peer}")


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

    # Node name
    import socket
    default_name = socket.gethostname()
    node_name = input(f"Name your node? [{default_name}]: ").strip()
    if not node_name:
        node_name = default_name

    # Beacon URL (to connect to the network)
    from . import DEFAULT_BEACON
    beacon_url = input(f"Beacon URL to join? [{DEFAULT_BEACON}]: ").strip()
    if not beacon_url:
        beacon_url = DEFAULT_BEACON

    # Source data provider
    print("\n📡 Data Sources — where should your node fetch satellite data from?")
    print("  [1] CDSE (Copernicus) — full Sentinel archive, requires free account")
    print("  [2] Element84 (AWS) — S2/S1/Landsat mirror, no account needed")
    print("  [3] Both (recommended)")
    print("  [4] None — redistribute data from other nodes only")
    provider_input = input("Choose [3]: ").strip()
    if not provider_input:
        provider_input = "3"

    cdse_username = ""
    cdse_password = ""
    add_element84 = False

    if provider_input in ("1", "3"):
        print("\n  🔑 CDSE — Register free at: https://dataspace.copernicus.eu")
        cdse_username = input("  CDSE email: ").strip()
        if cdse_username:
            import getpass
            cdse_password = getpass.getpass("  CDSE password: ")
        elif provider_input == "1":
            print("  ⚠ No CDSE email — skipping CDSE")

    if provider_input in ("2", "3"):
        add_element84 = True
        print("  ✓ Element84 (no credentials needed)")

    # Port
    port = args.port

    # Write config
    config_dir = Path.home() / ".earthgrid"
    config_dir.mkdir(parents=True, exist_ok=True)
    config_file = config_dir / "config.json"

    # Generate encryption key for source credentials
    from cryptography.fernet import Fernet
    source_key = Fernet.generate_key().decode()

    config = {
        "storage_limit_gb": gb,
        "also_beacon": also_beacon,
        "store_path": str(store_path / "store"),
        "catalog_path": str(store_path / "catalog.db"),
        "source_users_db": str(store_path / "source_users.db"),
        "source_key": source_key,
        "node_name": node_name,
        "port": port,
    }
    if beacon_url:
        config["beacon_url"] = beacon_url

    config_file.write_text(json.dumps(config, indent=2))

    # Store credentials securely in encrypted DB (never in plaintext config)
    from .source_users import SourceUserManager
    su_mgr = SourceUserManager(store_path / "source_users.db", encryption_key=source_key)
    if cdse_username and cdse_password:
        su_mgr.add_user(
            name="cdse-setup", provider="cdse",
            username=cdse_username, password=cdse_password,
        )
        print("  ✓ CDSE credentials stored (encrypted)")
    if add_element84:
        su_mgr.add_user(
            name="element84", provider="element84",
            username="public", password="",
        )
        print("  ✓ Element84 provider added")

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
    env_lines.append(f"EARTHGRID_NODE_NAME={node_name}")
    # Credentials are NEVER stored in plaintext — only in encrypted source_users.db
    env_file.write_text("\n".join(env_lines) + "\n")

    # Summary
    providers_str = []
    if cdse_username:
        providers_str.append(f"CDSE ({cdse_username})")
    if add_element84:
        providers_str.append("Element84 (public)")
    if not providers_str:
        providers_str.append("none — redistribute only")

    print(f"\n✅ EarthGrid configured!")
    print(f"   Node:     {node_name}")
    print(f"   Storage:  {gb} GB at {store_path}")
    print(f"   Beacon:   {'yes (also coordinator)' if also_beacon else 'no (data node only)'}")
    print(f"   Sources:  {', '.join(providers_str)}")
    print(f"   Port:     {port}")
    print(f"   Config:   {config_file}")
    if beacon_url:
        print(f"   Network:  {beacon_url}")
    print(f"\n🚀 Start with:")
    if also_beacon:
        print(f"   earthgrid start --also-beacon")
    else:
        print(f"   earthgrid start")
    print(f"\n📊 Manage source accounts later:")
    print(f"   earthgrid users list")
    print(f"   earthgrid users add --provider cdse --username your@email.com")


if __name__ == "__main__":
    main()
