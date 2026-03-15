"""EarthGrid User Authentication — Network-wide per-user API keys.

Users register on any node and their keys work across the entire network.
User records sync between nodes via federation.
"""
from __future__ import annotations
import logging
import secrets
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Optional

logger = logging.getLogger("earthgrid.user_auth")


class UserAuth:
    """SQLite-backed user registry with network-wide API key validation."""

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS users (
                user_id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                api_key TEXT UNIQUE NOT NULL,
                node_origin TEXT NOT NULL DEFAULT '',
                role TEXT NOT NULL DEFAULT 'member',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                active INTEGER NOT NULL DEFAULT 1
            )""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_users_api_key
                ON users(api_key)""")
            conn.execute("""CREATE INDEX IF NOT EXISTS idx_users_active
                ON users(active)""")

    def create_user(self, username: str, role: str = "member",
                    node_origin: str = "") -> dict:
        """Create a new user and return user info with API key.

        Returns dict with user_id, username, api_key, role.
        Raises ValueError if username already exists.
        """
        user_id = uuid.uuid4().hex[:16]
        api_key = secrets.token_urlsafe(32)
        now = time.time()
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """INSERT INTO users
                       (user_id, username, api_key, node_origin, role, created_at, updated_at, active)
                       VALUES (?, ?, ?, ?, ?, ?, ?, 1)""",
                    (user_id, username, api_key, node_origin, role, now, now)
                )
        except sqlite3.IntegrityError:
            raise ValueError(f"Username '{username}' already exists")
        logger.info(f"Created user '{username}' (id={user_id}, role={role})")
        return {
            "user_id": user_id,
            "username": username,
            "api_key": api_key,
            "role": role,
            "node_origin": node_origin,
        }

    def validate_key(self, api_key: str) -> Optional[dict]:
        """Validate an API key. Returns user info or None if invalid."""
        if not api_key:
            return None
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM users WHERE api_key = ? AND active = 1",
                (api_key,)
            ).fetchone()
        if not row:
            return None
        return {
            "user_id": row["user_id"],
            "username": row["username"],
            "role": row["role"],
            "node_origin": row["node_origin"],
        }

    def get_user(self, user_id: str) -> Optional[dict]:
        """Get user by ID."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM users WHERE user_id = ?", (user_id,)
            ).fetchone()
        if not row:
            return None
        return {
            "user_id": row["user_id"],
            "username": row["username"],
            "role": row["role"],
            "node_origin": row["node_origin"],
            "active": bool(row["active"]),
            "created_at": row["created_at"],
        }

    def list_users(self, include_inactive: bool = False) -> list[dict]:
        """List all users (excluding API keys)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            q = "SELECT * FROM users"
            if not include_inactive:
                q += " WHERE active = 1"
            q += " ORDER BY created_at"
            rows = conn.execute(q).fetchall()
        return [{
            "user_id": r["user_id"],
            "username": r["username"],
            "role": r["role"],
            "node_origin": r["node_origin"],
            "active": bool(r["active"]),
            "created_at": r["created_at"],
        } for r in rows]

    def delete_user(self, user_id: str) -> bool:
        """Soft-delete a user (set active=False)."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                "UPDATE users SET active = 0, updated_at = ? WHERE user_id = ?",
                (time.time(), user_id)
            ).rowcount
        if rows:
            logger.info(f"Deactivated user {user_id}")
        return rows > 0

    def export_users(self) -> list[dict]:
        """Export all users for federation sync (includes inactive for propagation)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("SELECT * FROM users").fetchall()
        return [{
            "user_id": r["user_id"],
            "username": r["username"],
            "api_key": r["api_key"],
            "node_origin": r["node_origin"],
            "role": r["role"],
            "active": bool(r["active"]),
            "created_at": r["created_at"],
            "updated_at": r["updated_at"],
        } for r in rows]

    def import_users(self, users: list[dict]) -> dict:
        """Import/merge users from another node.

        Rules:
        - New users (unknown user_id) → insert
        - Existing users → update if remote updated_at is newer
        - Deactivations propagate (active=False wins if newer)

        Returns dict with counts: added, updated, skipped.
        """
        added = updated = skipped = 0
        with sqlite3.connect(self.db_path) as conn:
            for u in users:
                existing = conn.execute(
                    "SELECT updated_at, active FROM users WHERE user_id = ?",
                    (u["user_id"],)
                ).fetchone()

                if existing is None:
                    # New user — insert
                    try:
                        conn.execute(
                            """INSERT INTO users
                               (user_id, username, api_key, node_origin, role,
                                created_at, updated_at, active)
                               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                            (u["user_id"], u["username"], u["api_key"],
                             u.get("node_origin", ""), u.get("role", "member"),
                             u.get("created_at", time.time()),
                             u.get("updated_at", time.time()),
                             1 if u.get("active", True) else 0)
                        )
                        added += 1
                    except sqlite3.IntegrityError:
                        skipped += 1  # username or api_key collision
                else:
                    local_updated = existing[0]
                    remote_updated = u.get("updated_at", 0)
                    if remote_updated > local_updated:
                        conn.execute(
                            """UPDATE users SET
                               role = ?, active = ?, updated_at = ?
                               WHERE user_id = ?""",
                            (u.get("role", "member"),
                             1 if u.get("active", True) else 0,
                             remote_updated,
                             u["user_id"])
                        )
                        updated += 1
                    else:
                        skipped += 1

        result = {"added": added, "updated": updated, "skipped": skipped}
        if added or updated:
            logger.info(f"User sync: {result}")
        return result

    def ensure_admin(self, node_name: str = "") -> Optional[dict]:
        """Create admin user if no users exist. Returns admin info or None."""
        with sqlite3.connect(self.db_path) as conn:
            count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        if count > 0:
            return None
        admin = self.create_user("admin", role="admin", node_origin=node_name)
        logger.warning(f"══════════════════════════════════════════════")
        logger.warning(f"  AUTO-CREATED ADMIN USER")
        logger.warning(f"  Username: admin")
        logger.warning(f"  API Key:  {admin['api_key']}")
        logger.warning(f"  Save this key! It won't be shown again.")
        logger.warning(f"══════════════════════════════════════════════")
        return admin
