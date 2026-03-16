"""EarthGrid Statistics — Access tracking and replication advisor.

Tracks chunk and collection access patterns to drive intelligent replication
decisions. Hot data gets more replicas, cold data gets fewer.
"""
from __future__ import annotations
import logging
import sqlite3
import time
from pathlib import Path
from typing import Optional

logger = logging.getLogger("earthgrid.stats")


class StatsEngine:
    """Track access patterns and advise on replication factors."""

    # Replication thresholds (accesses per week)
    HOT_THRESHOLD = 50      # promote to 4-6 replicas
    WARM_THRESHOLD = 10     # keep at 3 replicas (default)
    COLD_DAYS = 30          # demote to 2 replicas after 30 days inactivity

    def __init__(self, db_path: Path):
        self.db_path = db_path
        db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self):
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""CREATE TABLE IF NOT EXISTS chunk_access (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chunk_sha TEXT NOT NULL,
                timestamp REAL NOT NULL,
                access_type TEXT NOT NULL DEFAULT 'read',
                node_id TEXT NOT NULL DEFAULT '',
                collection_id TEXT NOT NULL DEFAULT '',
                item_id TEXT NOT NULL DEFAULT ''
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS collection_access (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                collection_id TEXT NOT NULL,
                timestamp REAL NOT NULL,
                access_type TEXT NOT NULL DEFAULT 'query',
                query_bbox TEXT NOT NULL DEFAULT '',
                query_time_range TEXT NOT NULL DEFAULT ''
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS bandwidth_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                node_id TEXT NOT NULL DEFAULT '',
                direction TEXT NOT NULL DEFAULT 'download',
                bytes_transferred INTEGER NOT NULL DEFAULT 0,
                nice_level INTEGER NOT NULL DEFAULT 0,
                source_user_id INTEGER NOT NULL DEFAULT 0
            )""")
            conn.execute("""CREATE TABLE IF NOT EXISTS download_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                origin TEXT NOT NULL DEFAULT 'source',
                provider TEXT NOT NULL DEFAULT '',
                collection_id TEXT NOT NULL DEFAULT '',
                item_id TEXT NOT NULL DEFAULT '',
                bytes_transferred INTEGER NOT NULL DEFAULT 0,
                bbox TEXT NOT NULL DEFAULT '',
                client_ip TEXT NOT NULL DEFAULT ''
            )""")
            # origin: 'source' = fetched from CDSE/WEkEO/etc
            #         'user'   = served to an EarthGrid user
            # Indexes for fast aggregation
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunk_ts ON chunk_access(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_chunk_sha ON chunk_access(chunk_sha)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_coll_ts ON collection_access(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_coll_id ON collection_access(collection_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dl_ts ON download_log(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_dl_origin ON download_log(origin)")

            conn.execute("""CREATE TABLE IF NOT EXISTS uptake_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp REAL NOT NULL,
                collection_id TEXT NOT NULL DEFAULT '',
                job_type TEXT NOT NULL DEFAULT 'sync',
                bbox TEXT NOT NULL DEFAULT '',
                temporal_extent TEXT NOT NULL DEFAULT '',
                bytes_out INTEGER NOT NULL DEFAULT 0,
                process_ids TEXT NOT NULL DEFAULT '',
                bbox_km2 REAL NOT NULL DEFAULT 0
            )""")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_uptake_ts ON uptake_log(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_uptake_coll ON uptake_log(collection_id)")

    def record_chunk_access(self, chunk_sha: str, access_type: str = "read",
                            node_id: str = "", collection_id: str = "",
                            item_id: str = ""):
        """Record a chunk access event."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO chunk_access 
                   (chunk_sha, timestamp, access_type, node_id, collection_id, item_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (chunk_sha, time.time(), access_type, node_id, collection_id, item_id)
            )

    def record_collection_access(self, collection_id: str, access_type: str = "query",
                                 bbox: str = "", time_range: str = ""):
        """Record a collection-level access event."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO collection_access 
                   (collection_id, timestamp, access_type, query_bbox, query_time_range)
                   VALUES (?, ?, ?, ?, ?)""",
                (collection_id, time.time(), access_type, bbox, time_range)
            )

    def record_bandwidth(self, bytes_transferred: int, direction: str = "download",
                         nice_level: int = 0, node_id: str = "",
                         source_user_id: int = 0):
        """Record bandwidth usage."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO bandwidth_log 
                   (timestamp, node_id, direction, bytes_transferred, nice_level, source_user_id)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (time.time(), node_id, direction, bytes_transferred, nice_level, source_user_id)
            )

    def record_download(self, origin: str, collection_id: str = "",
                        item_id: str = "", bytes_transferred: int = 0,
                        provider: str = "", bbox: str = "",
                        client_ip: str = ""):
        """Record a download event.
        
        Args:
            origin: 'source' (fetched from CDSE/WEkEO) or 'user' (served to EarthGrid user)
            provider: upstream provider name (cdse, wekeo, element84, cmems)
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO download_log
                   (timestamp, origin, provider, collection_id, item_id,
                    bytes_transferred, bbox, client_ip)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), origin, provider, collection_id, item_id,
                 bytes_transferred, bbox, client_ip)
            )

    def download_stats(self, period_hours: int = 720) -> dict:
        """Download statistics split by source vs user.
        
        Default period: 30 days (720 hours).
        Returns counts and bytes for both origins, broken down by collection and provider.
        """
        cutoff = time.time() - (period_hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            # Totals by origin
            totals = conn.execute(
                """SELECT origin,
                   COUNT(*) as count,
                   SUM(bytes_transferred) as bytes
                   FROM download_log WHERE timestamp > ?
                   GROUP BY origin""",
                (cutoff,)
            ).fetchall()
            # Source downloads by provider
            by_provider = conn.execute(
                """SELECT provider, collection_id,
                   COUNT(*) as count,
                   SUM(bytes_transferred) as bytes
                   FROM download_log
                   WHERE timestamp > ? AND origin = 'source'
                   GROUP BY provider, collection_id
                   ORDER BY bytes DESC""",
                (cutoff,)
            ).fetchall()
            # User downloads by collection
            by_collection = conn.execute(
                """SELECT collection_id,
                   COUNT(*) as count,
                   SUM(bytes_transferred) as bytes
                   FROM download_log
                   WHERE timestamp > ? AND origin = 'user'
                   GROUP BY collection_id
                   ORDER BY bytes DESC""",
                (cutoff,)
            ).fetchall()
            # Daily trend (last 30 days)
            daily = conn.execute(
                """SELECT date(timestamp, 'unixepoch') as day, origin,
                   COUNT(*) as count,
                   SUM(bytes_transferred) as bytes
                   FROM download_log WHERE timestamp > ?
                   GROUP BY day, origin
                   ORDER BY day""",
                (cutoff,)
            ).fetchall()

        source = next((dict(r) for r in totals if r["origin"] == "source"), {"count": 0, "bytes": 0})
        user = next((dict(r) for r in totals if r["origin"] == "user"), {"count": 0, "bytes": 0})

        return {
            "period_hours": period_hours,
            "source_downloads": {
                "count": source.get("count", 0),
                "bytes": source.get("bytes", 0) or 0,
                "gb": round((source.get("bytes", 0) or 0) / (1024**3), 2),
                "by_provider": [
                    {"provider": r["provider"], "collection": r["collection_id"],
                     "count": r["count"], "gb": round((r["bytes"] or 0) / (1024**3), 2)}
                    for r in by_provider
                ],
            },
            "user_downloads": {
                "count": user.get("count", 0),
                "bytes": user.get("bytes", 0) or 0,
                "gb": round((user.get("bytes", 0) or 0) / (1024**3), 2),
                "by_collection": [
                    {"collection": r["collection_id"],
                     "count": r["count"], "gb": round((r["bytes"] or 0) / (1024**3), 2)}
                    for r in by_collection
                ],
            },
            "daily_trend": [
                {"date": r["day"], "origin": r["origin"],
                 "count": r["count"], "gb": round((r["bytes"] or 0) / (1024**3), 2)}
                for r in daily
            ],
        }

    def top_collections(self, period_hours: int = 168, limit: int = 20) -> list[dict]:
        """Top accessed collections in the given period (default: 1 week)."""
        cutoff = time.time() - (period_hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT collection_id, COUNT(*) as access_count,
                   MAX(timestamp) as last_access
                   FROM collection_access WHERE timestamp > ?
                   GROUP BY collection_id
                   ORDER BY access_count DESC LIMIT ?""",
                (cutoff, limit)
            ).fetchall()
        return [{"collection_id": r[0], "access_count": r[1],
                 "last_access": r[2]} for r in rows]

    def chunk_heat_map(self, period_hours: int = 168, limit: int = 50) -> list[dict]:
        """Most accessed chunks in the given period."""
        cutoff = time.time() - (period_hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT chunk_sha, COUNT(*) as access_count,
                   collection_id, MAX(timestamp) as last_access
                   FROM chunk_access WHERE timestamp > ?
                   GROUP BY chunk_sha
                   ORDER BY access_count DESC LIMIT ?""",
                (cutoff, limit)
            ).fetchall()
        return [{"chunk_sha": r[0], "access_count": r[1],
                 "collection_id": r[2], "last_access": r[3]} for r in rows]

    def replication_advice(self) -> dict:
        """Advise on replication factor changes based on access patterns.
        
        Returns:
            {"promote": [...], "demote": [...], "ok": [...]}
        """
        now = time.time()
        week_ago = now - 7 * 86400
        cold_cutoff = now - self.COLD_DAYS * 86400

        with sqlite3.connect(self.db_path) as conn:
            # Hot chunks (accessed > HOT_THRESHOLD times this week)
            hot = conn.execute(
                """SELECT chunk_sha, COUNT(*) as cnt, collection_id
                   FROM chunk_access WHERE timestamp > ?
                   GROUP BY chunk_sha HAVING cnt >= ?""",
                (week_ago, self.HOT_THRESHOLD)
            ).fetchall()

            # All chunks ever accessed
            all_chunks = conn.execute(
                """SELECT chunk_sha, MAX(timestamp) as last_ts, COUNT(*) as total,
                   collection_id
                   FROM chunk_access GROUP BY chunk_sha"""
            ).fetchall()

        promote = [{"chunk_sha": r[0], "weekly_accesses": r[1],
                     "collection_id": r[2],
                     "recommended_replicas": min(6, 3 + r[1] // self.HOT_THRESHOLD)}
                   for r in hot]

        demote = [{"chunk_sha": r[0], "last_access": r[1],
                    "total_accesses": r[2], "collection_id": r[3],
                    "recommended_replicas": 2}
                  for r in all_chunks if r[1] < cold_cutoff]

        ok_count = len(all_chunks) - len(promote) - len(demote)

        return {
            "promote": promote,
            "demote": demote,
            "ok_count": ok_count,
            "total_tracked_chunks": len(all_chunks),
        }

    def bandwidth_summary(self, period_hours: int = 24) -> dict:
        """Bandwidth usage summary for the given period."""
        cutoff = time.time() - (period_hours * 3600)
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute(
                """SELECT 
                   SUM(CASE WHEN direction='download' THEN bytes_transferred ELSE 0 END) as dl,
                   SUM(CASE WHEN direction='upload' THEN bytes_transferred ELSE 0 END) as ul,
                   COUNT(*) as transfers
                   FROM bandwidth_log WHERE timestamp > ?""",
                (cutoff,)
            ).fetchone()
            # Per source user
            per_user = conn.execute(
                """SELECT source_user_id, SUM(bytes_transferred) as total,
                   COUNT(*) as transfers
                   FROM bandwidth_log 
                   WHERE timestamp > ? AND source_user_id > 0
                   GROUP BY source_user_id ORDER BY total DESC""",
                (cutoff,)
            ).fetchall()

        return {
            "period_hours": period_hours,
            "download_bytes": row[0] or 0,
            "upload_bytes": row[1] or 0,
            "download_gb": round((row[0] or 0) / (1024**3), 2),
            "upload_gb": round((row[1] or 0) / (1024**3), 2),
            "total_transfers": row[2],
            "per_source_user": [
                {"user_id": r[0], "bytes": r[1], "gb": round(r[1] / (1024**3), 2),
                 "transfers": r[2]}
                for r in per_user
            ],
        }

    def overview(self) -> dict:
        """Full stats overview for dashboard."""
        return {
            "top_collections_7d": self.top_collections(period_hours=168, limit=10),
            "top_collections_24h": self.top_collections(period_hours=24, limit=10),
            "chunk_heat_map_7d": self.chunk_heat_map(period_hours=168, limit=20),
            "replication_advice": self.replication_advice(),
            "bandwidth_24h": self.bandwidth_summary(period_hours=24),
            "bandwidth_7d": self.bandwidth_summary(period_hours=168),
        }



    @staticmethod
    def _bbox_area_km2(bbox_str: str) -> float:
        """Calculate approximate area of a WGS84 bbox in km².
        
        bbox format: "west,south,east,north" or "[west, south, east, north]"
        Uses spherical approximation (good enough for reporting).
        """
        try:
            clean = bbox_str.strip("[]() ")
            if not clean:
                return 0.0
            parts = [float(x.strip()) for x in clean.split(",")]
            if len(parts) != 4:
                return 0.0
            west, south, east, north = parts
            # Handle antimeridian crossing
            dlon = east - west
            if dlon < 0:
                dlon += 360
            dlat = north - south
            if dlat <= 0 or dlon <= 0:
                return 0.0
            # Approximate area using mid-latitude
            import math
            mid_lat = math.radians((north + south) / 2)
            # 1 degree lat ≈ 111.32 km, 1 degree lon ≈ 111.32 * cos(lat) km
            km_lat = dlat * 111.32
            km_lon = dlon * 111.32 * math.cos(mid_lat)
            return round(km_lat * km_lon, 2)
        except Exception:
            return 0.0

    def record_uptake(self, collection_id: str, job_type: str = "sync",
                      bbox: str = "", temporal_extent: str = "",
                      bytes_out: int = 0, process_ids: str = ""):
        """Record an anonymous uptake event. NO user info stored."""
        area = self._bbox_area_km2(bbox)
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """INSERT INTO uptake_log
                   (timestamp, collection_id, job_type, bbox, temporal_extent,
                    bytes_out, process_ids, bbox_km2)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (time.time(), collection_id, job_type, bbox, temporal_extent,
                 bytes_out, process_ids, area)
            )

    def uptake_report(self, period_days: int = 30) -> dict:
        """Anonymous uptake report for EU Commission reporting.

        Returns aggregate statistics only — no user identification possible.
        """
        cutoff = time.time() - (period_days * 86400)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Total requests + volume + AOI
            totals = conn.execute(
                """SELECT COUNT(*) as requests,
                   SUM(bytes_out) as total_bytes,
                   COUNT(DISTINCT collection_id) as collections_used,
                   SUM(bbox_km2) as total_km2
                   FROM uptake_log WHERE timestamp > ?""",
                (cutoff,)
            ).fetchone()

            # Per collection
            by_collection = conn.execute(
                """SELECT collection_id,
                   COUNT(*) as requests,
                   SUM(bytes_out) as total_bytes,
                   SUM(bbox_km2) as total_km2,
                   MIN(timestamp) as first_access,
                   MAX(timestamp) as last_access
                   FROM uptake_log WHERE timestamp > ?
                   GROUP BY collection_id
                   ORDER BY requests DESC""",
                (cutoff,)
            ).fetchall()

            # Per job type (sync vs batch vs download)
            by_type = conn.execute(
                """SELECT job_type,
                   COUNT(*) as requests,
                   SUM(bytes_out) as total_bytes
                   FROM uptake_log WHERE timestamp > ?
                   GROUP BY job_type
                   ORDER BY requests DESC""",
                (cutoff,)
            ).fetchall()

            # Monthly trend
            monthly = conn.execute(
                """SELECT strftime('%Y-%m', timestamp, 'unixepoch') as month,
                   COUNT(*) as requests,
                   SUM(bytes_out) as total_bytes,
                   COUNT(DISTINCT collection_id) as collections
                   FROM uptake_log WHERE timestamp > ?
                   GROUP BY month ORDER BY month""",
                (cutoff,)
            ).fetchall()

            # Daily trend (last 30 days)
            daily = conn.execute(
                """SELECT date(timestamp, 'unixepoch') as day,
                   COUNT(*) as requests,
                   SUM(bytes_out) as total_bytes
                   FROM uptake_log WHERE timestamp > ?
                   GROUP BY day ORDER BY day""",
                (cutoff,)
            ).fetchall()

            # Process usage (which openEO processes are popular)
            # process_ids is comma-separated, we aggregate
            all_procs = conn.execute(
                """SELECT process_ids FROM uptake_log
                   WHERE timestamp > ? AND process_ids != ''""",
                (cutoff,)
            ).fetchall()

        # Count process usage
        proc_counts = {}
        for row in all_procs:
            for p in row["process_ids"].split(","):
                p = p.strip()
                if p:
                    proc_counts[p] = proc_counts.get(p, 0) + 1
        top_processes = sorted(proc_counts.items(), key=lambda x: -x[1])[:20]

        return {
            "report_type": "EarthGrid Uptake Statistics",
            "period_days": period_days,
            "privacy": "anonymous — no user identification stored",
            "summary": {
                "total_requests": totals["requests"] or 0,
                "total_gb": round((totals["total_bytes"] or 0) / (1024**3), 3),
                "total_aoi_km2": round(totals["total_km2"] or 0, 1),
                "collections_accessed": totals["collections_used"] or 0,
            },
            "by_collection": [
                {
                    "collection": r["collection_id"],
                    "requests": r["requests"],
                    "gb": round((r["total_bytes"] or 0) / (1024**3), 3),
                    "aoi_km2": round(r["total_km2"] or 0, 1),
                }
                for r in by_collection
            ],
            "by_job_type": [
                {"type": r["job_type"], "requests": r["requests"],
                 "gb": round((r["total_bytes"] or 0) / (1024**3), 3)}
                for r in by_type
            ],
            "top_processes": [{"process": p, "count": c} for p, c in top_processes],
            "monthly_trend": [
                {"month": r["month"], "requests": r["requests"],
                 "gb": round((r["total_bytes"] or 0) / (1024**3), 3),
                 "collections": r["collections"]}
                for r in monthly
            ],
            "daily_trend": [
                {"date": r["day"], "requests": r["requests"],
                 "gb": round((r["total_bytes"] or 0) / (1024**3), 3)}
                for r in daily
            ],
        }

    def uptake_csv(self, period_days: int = 30) -> str:
        """Export uptake as CSV for Commission reporting."""
        cutoff = time.time() - (period_days * 86400)
        import csv, io, datetime
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute(
                """SELECT date(timestamp, 'unixepoch') as day,
                   collection_id, job_type,
                   COUNT(*) as requests,
                   SUM(bytes_out) as total_bytes,
                   SUM(bbox_km2) as total_km2
                   FROM uptake_log WHERE timestamp > ?
                   GROUP BY day, collection_id, job_type
                   ORDER BY day, collection_id""",
                (cutoff,)
            ).fetchall()
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(["date", "collection", "job_type", "requests", "bytes", "gb", "aoi_km2"])
        for r in rows:
            w.writerow([r[0], r[1], r[2], r[3], r[4] or 0,
                        round((r[4] or 0) / (1024**3), 3), round(r[5] or 0, 1)])
        return buf.getvalue()


    def ingest_history(self, period_days: int = 365) -> dict:
        """Daily ingest history — how much data was fetched from sources over time."""
        cutoff = time.time() - (period_days * 86400)
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            # Daily ingest from sources
            daily = conn.execute(
                """SELECT date(timestamp, 'unixepoch') as day,
                   COUNT(*) as items,
                   SUM(bytes_transferred) as total_bytes
                   FROM download_log
                   WHERE timestamp > ? AND origin = 'source'
                   GROUP BY day ORDER BY day""",
                (cutoff,)
            ).fetchall()
            # Totals
            totals = conn.execute(
                """SELECT COUNT(*) as items,
                   SUM(bytes_transferred) as total_bytes,
                   COUNT(DISTINCT collection_id) as collections
                   FROM download_log
                   WHERE origin = 'source'"""
            ).fetchone()
        return {
            "total_items_fetched": totals["items"] or 0,
            "total_gb_fetched": round((totals["total_bytes"] or 0) / (1024**3), 2),
            "collections": totals["collections"] or 0,
            "daily": [
                {"date": r["day"], "items": r["items"],
                 "gb": round((r["total_bytes"] or 0) / (1024**3), 3)}
                for r in daily
            ],
        }

    def cleanup(self, retain_days: int = 90):
        """Remove access logs older than retain_days."""
        cutoff = time.time() - (retain_days * 86400)
        with sqlite3.connect(self.db_path) as conn:
            c1 = conn.execute("DELETE FROM chunk_access WHERE timestamp < ?", (cutoff,)).rowcount
            c2 = conn.execute("DELETE FROM collection_access WHERE timestamp < ?", (cutoff,)).rowcount
            c3 = conn.execute("DELETE FROM bandwidth_log WHERE timestamp < ?", (cutoff,)).rowcount
            c4 = conn.execute("DELETE FROM uptake_log WHERE timestamp < ?", (cutoff,)).rowcount
        logger.info(f"Stats cleanup: removed {c1} chunk + {c2} collection + {c3} bandwidth + {c4} uptake records (>{retain_days}d)")
