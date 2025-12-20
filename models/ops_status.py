#!/usr/bin/env python3
"""Status and reporting helpers for the database queue."""

from __future__ import annotations

from time import time
from typing import Any, Dict, List

from sqlite3 import Error

from config import get_logger

logger = get_logger("models")


class StatusOpsMixin:
    conn: Any

    def _bucket_query(self, sql: str, params: List[Any]) -> Dict[int, int]:
        """Run a simple bucketed count query and return bucket->count mapping."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(sql, params)
            rows = cursor.fetchall() or []
            return {int(row[0]): int(row[1]) for row in rows}
        except Error as exc:
            logger.error("Bucket query failed: %s", exc)
            return {}
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_status_metrics(self, now_ts: int | None = None) -> Dict[str, Any]:
        """Return counts and bucketed activity for reporting feeds."""
        now = int(now_ts or time())
        start_24h = now - 24 * 3600
        start_7d = now - 7 * 24 * 3600

        def count(sql: str, params: List[Any]) -> int:
            cursor = None
            try:
                cursor = self.conn.cursor()
                cursor.execute(sql, params)
                row = cursor.fetchone()
                return int(row[0]) if row and row[0] is not None else 0
            except Error as exc:
                logger.error("Count query failed: %s", exc)
                return 0
            finally:
                if cursor:
                    try:
                        cursor.close()
                    except Exception:
                        pass

        counts_24h = {
            "items": count("SELECT COUNT(*) FROM items WHERE date >= ?", [start_24h]),
            "summaries": count("SELECT COUNT(*) FROM summaries WHERE generated_date >= ?", [start_24h]),
            "published": count(
                "SELECT COUNT(*) FROM summaries WHERE published_date IS NOT NULL AND published_date >= ?",
                [start_24h],
            ),
            "bulletins": count("SELECT COUNT(*) FROM bulletins WHERE created_date >= ?", [start_24h]),
        }

        counts_7d = {
            "items": count("SELECT COUNT(*) FROM items WHERE date >= ?", [start_7d]),
            "summaries": count("SELECT COUNT(*) FROM summaries WHERE generated_date >= ?", [start_7d]),
            "published": count(
                "SELECT COUNT(*) FROM summaries WHERE published_date IS NOT NULL AND published_date >= ?",
                [start_7d],
            ),
            "bulletins": count("SELECT COUNT(*) FROM bulletins WHERE created_date >= ?", [start_7d]),
        }

        per_bulletin_counts = self._bucket_query(
            "SELECT created_date AS bucket, summary_count FROM bulletins WHERE created_date >= ?",
            [start_7d],
        )
        bulletin_totals = [v for v in per_bulletin_counts.values() if v is not None]
        if not bulletin_totals:
            bulletin_totals = [0]
        bulletin_stats = {
            "avg": sum(bulletin_totals) / max(len(bulletin_totals), 1),
            "max": max(bulletin_totals) if bulletin_totals else 0,
        }

        hourly = {
            "items": self._bucket_query(
                "SELECT (date / ?) * ? AS bucket, COUNT(*) FROM items WHERE date >= ? GROUP BY bucket ORDER BY bucket",
                [3600, 3600, start_24h],
            ),
            "summaries": self._bucket_query(
                "SELECT (generated_date / ?) * ? AS bucket, COUNT(*) FROM summaries WHERE generated_date >= ? GROUP BY bucket ORDER BY bucket",
                [3600, 3600, start_24h],
            ),
            "published": self._bucket_query(
                "SELECT (published_date / ?) * ? AS bucket, COUNT(*) FROM summaries WHERE published_date IS NOT NULL AND published_date >= ? GROUP BY bucket ORDER BY bucket",
                [3600, 3600, start_24h],
            ),
            "bulletins": self._bucket_query(
                "SELECT (created_date / ?) * ? AS bucket, COUNT(*) FROM bulletins WHERE created_date >= ? GROUP BY bucket ORDER BY bucket",
                [3600, 3600, start_24h],
            ),
        }

        daily = {
            "items": self._bucket_query(
                "SELECT (date / ?) * ? AS bucket, COUNT(*) FROM items WHERE date >= ? GROUP BY bucket ORDER BY bucket",
                [86400, 86400, start_7d],
            ),
            "summaries": self._bucket_query(
                "SELECT (generated_date / ?) * ? AS bucket, COUNT(*) FROM summaries WHERE generated_date >= ? GROUP BY bucket ORDER BY bucket",
                [86400, 86400, start_7d],
            ),
            "published": self._bucket_query(
                "SELECT (published_date / ?) * ? AS bucket, COUNT(*) FROM summaries WHERE published_date IS NOT NULL AND published_date >= ? GROUP BY bucket ORDER BY bucket",
                [86400, 86400, start_7d],
            ),
            "bulletins": self._bucket_query(
                "SELECT (created_date / ?) * ? AS bucket, COUNT(*) FROM bulletins WHERE created_date >= ? GROUP BY bucket ORDER BY bucket",
                [86400, 86400, start_7d],
            ),
        }

        return {
            "now": now,
            "counts": {"24h": counts_24h, "7d": counts_7d},
            "per_bulletin": bulletin_stats,
            "hourly": hourly,
            "daily": daily,
        }


__all__ = ["StatusOpsMixin"]
