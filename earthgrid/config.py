"""Node configuration."""
from __future__ import annotations
import json
import uuid
from pathlib import Path
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    node_id: str = ""
    node_name: str = "earthgrid-node"
    store_path: Path = Path("./data/store")
    catalog_path: Path = Path("./data/catalog.db")
    host: str = "0.0.0.0"
    port: int = 8400
    peers: list[str] = []
    role: str = "node"  # "node" or "beacon"
    beacon_url: str = ""  # beacon URL to register with (for data nodes)
    beacon_peers: list[str] = []  # peer beacon URLs (for beacon federation)
    public_url: str = ""  # this node's public URL (for beacon registration)
    storage_limit_gb: float = 50.0  # max GB to use for chunk storage
    also_beacon: bool = False  # run beacon alongside data node
    api_key: str = ""  # required for write operations (ingest/process/delete)
    admin_key: str = ""  # required for destructive operations (delete)
    require_auth_read: bool = False  # if True, reads also need api_key

    # --- Source Users ---
    source_key: str = ""  # Fernet encryption key for source user credentials
    source_users_db: str = "./data/source_users.db"

    # --- Bandwidth Control ---
    bw_limit_mbps: float = 0  # Global bandwidth limit in Mbps (0 = unlimited)
    bw_schedule: str = ""  # JSON string: {"8": 50, "18": 75, "22": 100}
    max_download_volume_gb: float = 0  # Max total download volume in GB (0 = unlimited)

    # --- Stats ---
    stats_db: str = "./data/stats.db"
    stats_retain_days: int = 90  # Auto-cleanup logs older than this

    # --- Beacon ---
    beacon_db: str = "./data/beacon.db"

    model_config = {"env_prefix": "EARTHGRID_", "env_file": ".env", "env_file_encoding": "utf-8"}

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.node_id:
            id_file = self.store_path.parent / ".node_id"
            if id_file.exists():
                self.node_id = id_file.read_text().strip()
            else:
                self.node_id = uuid.uuid4().hex[:12]
                id_file.parent.mkdir(parents=True, exist_ok=True)
                id_file.write_text(self.node_id)

    @property
    def base_url(self) -> str:
        return f"http://{self.host}:{self.port}"

    @property
    def bw_schedule_dict(self) -> dict[int, float]:
        """Parse bandwidth schedule JSON string."""
        if not self.bw_schedule:
            return {}
        try:
            raw = json.loads(self.bw_schedule)
            return {int(k): float(v) for k, v in raw.items()}
        except (json.JSONDecodeError, ValueError):
            return {}


settings = Settings()
