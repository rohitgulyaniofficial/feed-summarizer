#!/usr/bin/env python3
"""Bulletin-related operations for the database queue."""

from __future__ import annotations

import json
from time import time
from typing import Any, Dict, List, Optional

from sqlite3 import Error, Row

from config import get_logger
from utils import decode_int64

logger = get_logger("models")


class BulletinsOpsMixin:
    conn: Any

    def create_bulletin(
        self,
        group_name: str,
        session_key: str,
        introduction: str,
        summary_ids: List[int],
        feed_slugs: List[str],
        title: Optional[str] = None,
        entries: Optional[List[Dict[str, Any]]] = None,
    ) -> Optional[int]:
        """Create a new bulletin with the given parameters."""
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    """
                    INSERT OR REPLACE INTO bulletins
                        (group_name, session_key, created_date, title, introduction, summary_count, feed_slugs)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        group_name,
                        session_key,
                        int(time()),
                        title,
                        introduction,
                        len(summary_ids),
                        json.dumps(feed_slugs),
                    ),
                )
                bulletin_id = cursor.lastrowid

                if summary_ids:
                    cursor.execute(
                        "DELETE FROM bulletin_summaries WHERE bulletin_id = ?",
                        (bulletin_id,),
                    )
                    relationships = [(bulletin_id, summary_id) for summary_id in summary_ids]
                    cursor.executemany(
                        """
                        INSERT OR IGNORE INTO bulletin_summaries (bulletin_id, summary_id)
                        VALUES (?, ?)
                        """,
                        relationships,
                    )

                # Persist ordered bulletin entries (shared payload for HTML/RSS)
                cursor.execute(
                    "DELETE FROM bulletin_entries WHERE bulletin_id = ?",
                    (bulletin_id,),
                )
                if entries:
                    entry_rows = []
                    for idx, entry in enumerate(entries):
                        try:
                            entry_rows.append(
                                (
                                    bulletin_id,
                                    idx,
                                    entry.get("topic"),
                                    entry.get("item_date"),
                                    json.dumps(entry),
                                )
                            )
                        except Exception as exc:
                            logger.warning(
                                "Skipping bulletin entry for %s/%s idx %d: %s",
                                group_name,
                                session_key,
                                idx,
                                exc,
                            )
                    if entry_rows:
                        cursor.executemany(
                            """
                            INSERT INTO bulletin_entries (
                                bulletin_id,
                                position,
                                topic,
                                item_date,
                                entry_json
                            ) VALUES (?, ?, ?, ?, ?)
                            """,
                            entry_rows,
                        )

                logger.info(
                    "Created bulletin %d for group '%s' session '%s' with %d summaries",
                    bulletin_id,
                    group_name,
                    session_key,
                    len(summary_ids),
                )
                return bulletin_id
        except Error as e:
            logger.error(
                f"Error creating bulletin for group '{group_name}' session '{session_key}': {e}"
            )
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_bulletin(self, group_name: str, session_key: str) -> Optional[Dict[str, Any]]:
        """Get a bulletin by group name and session key."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            cursor.execute(
                """
                SELECT id, group_name, session_key, created_date, title, introduction,
                       summary_count, feed_slugs
                FROM bulletins
                WHERE group_name = ? AND session_key = ?
                """,
                (group_name, session_key),
            )
            bulletin_row = cursor.fetchone()
            if not bulletin_row:
                return None

            cursor.execute(
                "SELECT position, entry_json FROM bulletin_entries WHERE bulletin_id = ? ORDER BY position",
                (bulletin_row["id"],),
            )
            entry_rows = cursor.fetchall() or []
            entries: List[Dict[str, Any]] = []
            for erow in entry_rows:
                try:
                    payload = json.loads(erow[1]) if erow[1] else {}
                    entries.append(payload)
                except (json.JSONDecodeError, TypeError):
                    continue

            # Backward compatibility: if entries are missing, reconstruct from joins
            summaries: List[Dict[str, Any]] = []
            if not entries:
                cursor.execute(
                    """
                    SELECT
                        s.id, s.summary_text, s.topic, s.generated_date, s.published_date,
                        s.simhash,
                        s.merge_simhash,
                        i.title as item_title, i.url as item_url, i.date as item_date,
                        f.title as feed_title, f.slug as feed_slug
                    FROM bulletin_summaries bs
                    JOIN summaries s ON bs.summary_id = s.id
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE bs.bulletin_id = ?
                    ORDER BY i.date DESC
                    """,
                    (bulletin_row["id"],),
                )
                summary_rows = cursor.fetchall()
                for row in summary_rows:
                    summaries.append(
                        {
                            "id": row["id"],
                            "summary_text": row["summary_text"],
                            "topic": row["topic"],
                            "simhash": decode_int64(row["simhash"]),
                            "merge_simhash": decode_int64(row["merge_simhash"]),
                            "generated_date": row["generated_date"],
                            "published_date": row["published_date"],
                            "item_title": row["item_title"],
                            "item_url": row["item_url"],
                            "item_date": row["item_date"],
                            "feed_title": row["feed_title"],
                            "feed_slug": row["feed_slug"],
                        }
                    )
                # Store legacy rows as entries so subsequent reads are consistent
                for idx, summary in enumerate(summaries):
                    try:
                        entries.append(
                            {
                                "id": summary.get("id"),
                                "summary_text": summary.get("summary_text"),
                                "topic": summary.get("topic"),
                                "merged_ids": [summary.get("id")],
                                "merged_links": [
                                    {
                                        "url": summary.get("item_url"),
                                        "title": summary.get("item_title") or summary.get("title") or "Read more",
                                        "feed_slug": summary.get("feed_slug"),
                                    }
                                ]
                                if summary.get("item_url")
                                else [],
                                "item_date": summary.get("item_date"),
                            }
                        )
                    except Exception:
                        continue

            feed_slugs: List[str] = []
            try:
                if bulletin_row["feed_slugs"]:
                    feed_slugs = json.loads(bulletin_row["feed_slugs"])
            except (json.JSONDecodeError, TypeError):
                logger.warning(f"Could not parse feed_slugs for bulletin {bulletin_row['id']}")

            return {
                "id": bulletin_row["id"],
                "group_name": bulletin_row["group_name"],
                "session_key": bulletin_row["session_key"],
                "created_date": bulletin_row["created_date"],
                "title": bulletin_row["title"] if "title" in bulletin_row.keys() else None,
                "introduction": bulletin_row["introduction"],
                "summary_count": bulletin_row["summary_count"],
                "feed_slugs": feed_slugs,
                "entries": entries,
            }
        except Error as e:
            logger.error(
                f"Error getting bulletin for group '{group_name}' session '{session_key}': {e}"
            )
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_bulletins_for_group(self, group_name: str, days_back: int = 7) -> List[Dict[str, Any]]:
        """Get all bulletins for a group within the specified time period."""
        cursor = None
        try:
            cutoff_time = int(time()) - (days_back * 24 * 60 * 60)
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            cursor.execute(
                """
                SELECT id, group_name, session_key, created_date, title, introduction,
                       summary_count, feed_slugs
                FROM bulletins
                WHERE group_name = ? AND created_date >= ?
                ORDER BY created_date DESC
                """,
                (group_name, cutoff_time),
            )
            bulletin_rows = cursor.fetchall()
            bulletins: List[Dict[str, Any]] = []
            for row in bulletin_rows:
                feed_slugs: List[str] = []
                try:
                    if row["feed_slugs"]:
                        feed_slugs = json.loads(row["feed_slugs"])
                except (json.JSONDecodeError, TypeError):
                    logger.warning(f"Could not parse feed_slugs for bulletin {row['id']}")
                bulletins.append(
                    {
                        "id": row["id"],
                        "group_name": row["group_name"],
                        "session_key": row["session_key"],
                        "created_date": row["created_date"],
                        "title": row["title"] if "title" in row.keys() else None,
                        "introduction": row["introduction"],
                        "summary_count": row["summary_count"],
                        "feed_slugs": feed_slugs,
                    }
                )
            return bulletins
        except Error as e:
            logger.error(f"Error getting bulletins for group '{group_name}': {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def list_all_bulletins(self) -> List[Dict[str, Any]]:
        """Return a lightweight list of all bulletins (for backfill/maintenance)."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            cursor.execute(
                "SELECT id, group_name, session_key, created_date FROM bulletins ORDER BY created_date DESC"
            )
            rows = cursor.fetchall()
            out: List[Dict[str, Any]] = []
            for row in rows or []:
                out.append(
                    {
                        "id": row["id"],
                        "group_name": row["group_name"],
                        "session_key": row["session_key"],
                        "created_date": row["created_date"],
                    }
                )
            return out
        except Error as e:
            logger.error(f"Error listing bulletins: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def update_bulletin_title(self, group_name: str, session_key: str, title: str) -> bool:
        """Update the title for an existing bulletin."""
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute(
                    "UPDATE bulletins SET title = ? WHERE group_name = ? AND session_key = ?",
                    (title, group_name, session_key),
                )
                return cursor.rowcount > 0
        except Error as e:
            logger.error(f"Error updating bulletin title for {group_name}/{session_key}: {e}")
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def delete_old_bulletins(self, days_to_keep: int = 7) -> int:
        """Delete bulletins older than the specified number of days."""
        cursor = None
        try:
            cutoff_time = int(time()) - (days_to_keep * 24 * 60 * 60)
            with self.conn:
                cursor = self.conn.cursor()
                cursor.execute("DELETE FROM bulletins WHERE created_date < ?", (cutoff_time,))
                deleted_count = cursor.rowcount
                if deleted_count > 0:
                    logger.info(
                        f"Deleted {deleted_count} old bulletins (older than {days_to_keep} days)"
                    )
                return deleted_count
        except Error as e:
            logger.error(f"Error deleting old bulletins: {e}")
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
