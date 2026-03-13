"""EarthGrid Source Users — Encrypted credential pool for data acquisition.

Source users donate their CDSE/Copernicus accounts so EarthGrid can download
data that isn't yet in the grid. Credentials are encrypted at rest using Fernet.
"""
import json
import logging
import os
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

from cryptography.fernet import Fernet

logger = logging.getLogger("earthgrid.source_users")


@dataclass
class SourceUser:
    """A source user account for data downloads."""
    id: int = 0
    name: str = ""
    provider: str = "cdse"  # cdse, element84, cmems, cds
    username: str = ""
    # encrypted fields (stored encrypted, decrypted on access)
    _encrypted_password: str = ""
    _encrypted_token: str = ""
    # rate limiting
    max_requests_hour: int = 100
    max_download_gb: float = 50.0  # max download volume in GB
    # health tracking
    last_used: float = 0.0
    success_count: int = 0
    fail_count: int = 0
    consecutive_fails: int = 0
    is_healthy: bool = True
    is_enabled: bool = True
    total_downloaded_bytes: int = 0
    created_at: float = field(default_factory=time.time)


class SourceUserManager:
    """Manage encrypted source user credentials with LRU selection."""

    MAX_CONSECUTIVE_FAILS = 5

    def __init__(self, db_path: Path, encryption_key: str = ""):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)

        # Encryption key from param or env
        key = encryption_key or os.environ.get("EARTHGRID_SOURCE_KEY", "")
        if not key:
            # Generate and log warning — in production, always set the key
            key = Fernet.generate_key().decode()
            logger.warning("No EARTHGRID_SOURCE_KEY set — generated ephemeral key. "
                           "Credentials will be lost on restart!")
        elif len(key) < 44:
            # Derive a proper Fernet key from arbitrary string
            import hashlib, base64
            dk = hashlib.pbkdf2_hmac("sha256", key.encode(), b"earthgrid-salt", 100000)
            key = base64.urlsafe_b64encode(dk).decode()

        self._fernet = Fernet(key.encode() if isinstance(key, str) else key)
        self._init_db()
        self._hourly_requests: dict[int, list[float]] = {}  # user_id → [timestamps]

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS source_users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                provider TEXT NOT NULL DEFAULT 'cdse',
                username TEXT NOT NULL,
                encrypted_password TEXT NOT NULL DEFAULT '',
                encrypted_token TEXT NOT NULL DEFAULT '',
                max_requests_hour INTEGER NOT NULL DEFAULT 100,
                max_download_gb REAL NOT NULL DEFAULT 50.0,
                last_used REAL NOT NULL DEFAULT 0,
                success_count INTEGER NOT NULL DEFAULT 0,
                fail_count INTEGER NOT NULL DEFAULT 0,
                consecutive_fails INTEGER NOT NULL DEFAULT 0,
                is_healthy INTEGER NOT NULL DEFAULT 1,
                is_enabled INTEGER NOT NULL DEFAULT 1,
                total_downloaded_bytes INTEGER NOT NULL DEFAULT 0,
                created_at REAL NOT NULL
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS download_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                timestamp REAL NOT NULL,
                collection TEXT NOT NULL DEFAULT '',
                item_id TEXT NOT NULL DEFAULT '',
                bytes_downloaded INTEGER NOT NULL DEFAULT 0,
                success INTEGER NOT NULL DEFAULT 1,
                error_msg TEXT NOT NULL DEFAULT '',
                FOREIGN KEY (user_id) REFERENCES source_users(id)
            )""")

    def _encrypt(self, plaintext: str) -> str:
        if not plaintext:
            return ""
        return self._fernet.encrypt(plaintext.encode()).decode()

    def _decrypt(self, ciphertext: str) -> str:
        if not ciphertext:
            return ""
        return self._fernet.decrypt(ciphertext.encode()).decode()

    def add_user(self, name: str, provider: str, username: str,
                 password: str = "", token: str = "",
                 max_requests_hour: int = 100,
                 max_download_gb: float = 50.0) -> int:
        """Add a source user with encrypted credentials. Returns user ID."""
        enc_pw = self._encrypt(password)
        enc_token = self._encrypt(token)
        with sqlite3.connect(self.db_path) as conn:
            cur = conn.execute(
                """INSERT INTO source_users 
                   (name, provider, username, encrypted_password, encrypted_token,
                    max_requests_hour, max_download_gb, created_at)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (name, provider, username, enc_pw, enc_token,
                 max_requests_hour, max_download_gb, time.time())
            )
            uid = cur.lastrowid
        logger.info(f"Added source user '{name}' ({provider}/{username}) id={uid}")
        return uid

    def remove_user(self, user_id: int) -> bool:
        """Remove a source user by ID."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("DELETE FROM source_users WHERE id = ?", (user_id,)).rowcount
        return rows > 0

    def list_users_with_creds(self, provider: str = None) -> list[dict]:
        """List enabled source users WITH decrypted credentials (internal use only).
        
        Used by openEO gateway for parallel downloads across user pool.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            q = "SELECT * FROM source_users WHERE is_enabled = 1"
            params = []
            if provider:
                q += " AND provider = ?"
                params.append(provider)
            rows = conn.execute(q, params).fetchall()
        result = []
        for r in rows:
            pw = r["encrypted_password"]
            if pw:
                try:
                    pw = self._decrypt(pw)
                except Exception:
                    pw = ""
            result.append({
                "user_id": r["id"], "name": r["name"],
                "provider": r["provider"], "username": r["username"],
                "password": pw or "",
            })
        return result

    def list_users(self, include_disabled: bool = False) -> list[dict]:
        """List source users (credentials excluded)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            q = "SELECT * FROM source_users"
            if not include_disabled:
                q += " WHERE is_enabled = 1"
            rows = conn.execute(q).fetchall()
        return [{
            "id": r["id"], "name": r["name"], "provider": r["provider"],
            "username": r["username"], "max_requests_hour": r["max_requests_hour"],
            "max_download_gb": r["max_download_gb"],
            "last_used": r["last_used"], "success_count": r["success_count"],
            "fail_count": r["fail_count"], "consecutive_fails": r["consecutive_fails"],
            "is_healthy": bool(r["is_healthy"]), "is_enabled": bool(r["is_enabled"]),
            "total_downloaded_gb": round(r["total_downloaded_bytes"] / (1024**3), 2),
            "created_at": r["created_at"],
        } for r in rows]

    def get_credentials(self, user_id: int) -> Optional[dict]:
        """Get decrypted credentials for a source user."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute("SELECT * FROM source_users WHERE id = ?", (user_id,)).fetchone()
        if not row:
            return None
        return {
            "username": row["username"],
            "password": self._decrypt(row["encrypted_password"]),
            "token": self._decrypt(row["encrypted_token"]),
            "provider": row["provider"],
        }

    def select_user(self, provider: str = "cdse") -> Optional[dict]:
        """Select best available source user (LRU strategy).
        
        Returns decrypted credentials of the least-recently-used healthy user,
        respecting rate limits and download quotas.
        """
        now = time.time()
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """SELECT * FROM source_users 
                   WHERE provider = ? AND is_enabled = 1 AND is_healthy = 1
                   ORDER BY last_used ASC""",
                (provider,)
            ).fetchall()

        for row in rows:
            uid = row["id"]
            # Check rate limit
            self._clean_hourly_requests(uid, now)
            if len(self._hourly_requests.get(uid, [])) >= row["max_requests_hour"]:
                continue
            # Check download volume quota
            max_bytes = row["max_download_gb"] * (1024**3)
            if max_bytes > 0 and row["total_downloaded_bytes"] >= max_bytes:
                logger.warning(f"Source user {uid} ({row['name']}) exceeded download quota "
                               f"({row['total_downloaded_bytes']/(1024**3):.1f} GB / {row['max_download_gb']} GB)")
                continue

            # This user is available
            creds = self.get_credentials(uid)
            if creds:
                creds["user_id"] = uid
                creds["name"] = row["name"]
                # Record rate limit hit
                self._hourly_requests.setdefault(uid, []).append(now)
                return creds

        logger.warning(f"No available source user for provider '{provider}'")
        return None

    def _clean_hourly_requests(self, user_id: int, now: float):
        """Remove request timestamps older than 1 hour."""
        cutoff = now - 3600
        if user_id in self._hourly_requests:
            self._hourly_requests[user_id] = [
                t for t in self._hourly_requests[user_id] if t > cutoff
            ]

    def record_success(self, user_id: int, bytes_downloaded: int = 0,
                       collection: str = "", item_id: str = ""):
        """Record a successful download."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """UPDATE source_users SET 
                   last_used = ?, success_count = success_count + 1,
                   consecutive_fails = 0,
                   total_downloaded_bytes = total_downloaded_bytes + ?
                   WHERE id = ?""",
                (time.time(), bytes_downloaded, user_id)
            )
            conn.execute(
                """INSERT INTO download_log 
                   (user_id, timestamp, collection, item_id, bytes_downloaded, success)
                   VALUES (?, ?, ?, ?, ?, 1)""",
                (user_id, time.time(), collection, item_id, bytes_downloaded)
            )

    def record_failure(self, user_id: int, error_msg: str = "",
                       collection: str = "", item_id: str = ""):
        """Record a failed download. Auto-disables after MAX_CONSECUTIVE_FAILS."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """UPDATE source_users SET 
                   last_used = ?, fail_count = fail_count + 1,
                   consecutive_fails = consecutive_fails + 1
                   WHERE id = ?""",
                (time.time(), user_id)
            )
            # Check if we should auto-disable
            row = conn.execute(
                "SELECT consecutive_fails, name FROM source_users WHERE id = ?",
                (user_id,)
            ).fetchone()
            if row and row[0] >= self.MAX_CONSECUTIVE_FAILS:
                conn.execute(
                    "UPDATE source_users SET is_healthy = 0 WHERE id = ?",
                    (user_id,)
                )
                logger.error(f"Source user {user_id} ({row[1]}) auto-disabled "
                             f"after {row[0]} consecutive failures")
            conn.execute(
                """INSERT INTO download_log 
                   (user_id, timestamp, collection, item_id, success, error_msg)
                   VALUES (?, ?, ?, ?, 0, ?)""",
                (user_id, time.time(), collection, item_id, error_msg)
            )

    def reset_health(self, user_id: int):
        """Reset health status for a source user (admin action)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """UPDATE source_users SET 
                   is_healthy = 1, consecutive_fails = 0 WHERE id = ?""",
                (user_id,)
            )

    def get_download_stats(self, user_id: int = None, hours: int = 24) -> list[dict]:
        """Get download logs, optionally filtered by user and time window."""
        cutoff = time.time() - (hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            if user_id:
                rows = conn.execute(
                    "SELECT * FROM download_log WHERE user_id = ? AND timestamp > ? ORDER BY timestamp DESC",
                    (user_id, cutoff)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM download_log WHERE timestamp > ? ORDER BY timestamp DESC",
                    (cutoff,)
                ).fetchall()
        return [dict(r) for r in rows]
