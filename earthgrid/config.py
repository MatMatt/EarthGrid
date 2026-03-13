"""Node configuration."""
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

    model_config = {"env_prefix": "EARTHGRID_"}

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


settings = Settings()
