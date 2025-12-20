#!/usr/bin/env python3
"""Maintenance operations for the database queue."""

from __future__ import annotations

from time import time
from typing import Any, Dict

from sqlite3 import Error

from config import get_logger

logger = get_logger("models")


class MaintenanceOpsMixin:
    conn: Any

    def perform_maintenance(
        self,
        checkpoint_mode: str = "TRUNCATE",
        vacuum: bool = False,
        optimize: bool = True,
        busy_timeout_ms: int = 10000,
    ) -> Dict[str, Any]:
        """Run SQLite maintenance operations on the active connection."""
        if self.conn is None:
            raise RuntimeError("Database connection is not initialized")

        mode = str(checkpoint_mode or "TRUNCATE").strip().upper()
        allowed = {"PASSIVE", "FULL", "RESTART", "TRUNCATE"}
        if mode not in allowed:
            mode = "TRUNCATE"

        timeout_ms = 10000
        try:
            timeout_ms = int(busy_timeout_ms)
            if timeout_ms < 1000:
                timeout_ms = 1000
        except Exception:
            timeout_ms = 10000

        cursor = self.conn.cursor()
        result: Dict[str, Any] = {
            "checkpoint_mode": mode,
            "busy_timeout_ms": timeout_ms,
            "did_optimize": False,
            "did_vacuum": False,
            "wal_checkpoint": None,
        }
        try:
            cursor.execute(f"PRAGMA busy_timeout={timeout_ms}")
            try:
                cursor.execute("PRAGMA journal_mode=WAL")
            except Exception:
                pass
            try:
                cursor.execute(f"PRAGMA wal_checkpoint({mode})")
                row = cursor.fetchone()
                if row is not None:
                    result["wal_checkpoint"] = tuple(row)
            except Exception as e:
                result["wal_checkpoint_error"] = str(e)

            if optimize:
                try:
                    cursor.execute("PRAGMA optimize")
                    result["did_optimize"] = True
                except Exception as e:
                    result["optimize_error"] = str(e)

            try:
                self.conn.commit()
            except Exception:
                pass

            if vacuum:
                try:
                    cursor.execute("VACUUM")
                    result["did_vacuum"] = True
                except Exception as e:
                    result["vacuum_error"] = str(e)

            try:
                cursor.execute("PRAGMA journal_mode=WAL")
            except Exception as e:
                result["restore_wal_error"] = str(e)

            try:
                self.conn.commit()
            except Exception:
                pass

            return result
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def expire_old_entries(self, expiration_days: int) -> int:
        """Delete items and their summaries older than the specified number of days."""
        if expiration_days <= 0:
            logger.warning("Invalid expiration_days value, skipping expiration")
            return 0

        cursor = None
        try:
            cutoff_timestamp = int(time()) - (expiration_days * 24 * 60 * 60)
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM items WHERE date < ?", (cutoff_timestamp,))
            items_to_delete = cursor.fetchone()[0]
            if items_to_delete == 0:
                logger.debug(f"No items older than {expiration_days} days found")
                return 0

            cursor.execute(
                """
                DELETE FROM summaries
                WHERE id IN (
                    SELECT id FROM items WHERE date < ?
                )
                """,
                (cutoff_timestamp,),
            )
            summaries_deleted = cursor.rowcount

            cursor.execute("DELETE FROM items WHERE date < ?", (cutoff_timestamp,))
            items_deleted = cursor.rowcount

            self.conn.commit()
            logger.info(
                "Database maintenance: deleted %d items and %d summaries older than %d days",
                items_deleted,
                summaries_deleted,
                expiration_days,
            )
            return items_deleted
        except Error as e:
            logger.error(f"Error during database maintenance (expiring old entries): {e}")
            if self.conn:
                self.conn.rollback()
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
