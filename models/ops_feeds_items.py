#!/usr/bin/env python3
"""Feed and item operations for the database queue."""

from __future__ import annotations

import json
from os import path
from time import time
from typing import Any, Dict, List, Optional, Set

from sqlite3 import Error, Row

from config import get_logger
from utils import decode_int64

logger = get_logger("models")


class FeedsItemsOpsMixin:
    conn: Any

    def register_feed(self, slug: str, url: str) -> bool:
        """Register a feed in the database."""
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                "INSERT OR IGNORE INTO feeds (slug, url, last_fetched) VALUES (?, ?, 0)",
                (slug, url),
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error registering feed {slug}: {e}")
            return False

    def get_feed_id(self, slug: str) -> Optional[int]:
        """Get the ID of a feed by its slug."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM feeds WHERE slug = ?", (slug,))
            result = cursor.fetchone()
            return result["id"] if result else None
        except Error as e:
            logger.error(f"Error getting feed ID for {slug}: {e}")
            return None

    def update_feed_title(self, feed_id: int, title: str) -> bool:
        """Update the title of a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE feeds SET title = ? WHERE id = ?", (title, feed_id))
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating feed title for ID {feed_id}: {e}")
            return False

    def update_last_fetched(self, feed_id: int) -> bool:
        """Update the last_fetched timestamp for a feed."""
        try:
            current_time = int(time())
            cursor = self.conn.cursor()
            cursor.execute(
                "UPDATE feeds SET last_fetched = ? WHERE id = ?",
                (current_time, feed_id),
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating last_fetched for feed ID {feed_id}: {e}")
            return False

    def get_feed_last_fetched(self, feed_id: int) -> int:
        """Get the last_fetched timestamp of a feed by its ID."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT last_fetched FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            return result["last_fetched"] if result else 0
        except Error as e:
            logger.error(f"Error getting last_fetched for feed ID {feed_id}: {e}")
            return 0

    def list_feeds(self) -> List[Dict[str, Any]]:
        """List all feeds with id, slug, and last_fetched."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id, slug, last_fetched FROM feeds")
            rows = cursor.fetchall()
            return [
                {
                    "id": row[0],
                    "slug": row[1],
                    "last_fetched": row[2] if row[2] is not None else 0,
                }
                for row in rows
            ]
        except Error as e:
            logger.error(f"Error listing feeds: {e}")
            return []

    def save_items(self, feed_id: int, entries_data: List[Dict[str, Any]]) -> int:
        """Save feed items to the database."""
        new_items = 0
        try:
            cursor = self.conn.cursor()
            for entry_data in entries_data:
                try:
                    cursor.execute(
                        """
                    INSERT OR IGNORE INTO items (feed_id, title, url, guid, body, date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                        (
                            feed_id,
                            entry_data["title"],
                            entry_data["url"],
                            entry_data["guid"],
                            entry_data["body"],
                            entry_data["date"],
                        ),
                    )
                    if cursor.rowcount > 0:
                        new_items += 1
                except Error as e:
                    logger.error(f"Error inserting item {entry_data['url']}: {e}")
                    continue
            self.conn.commit()
            return new_items
        except Exception as e:
            logger.error(f"Error saving items for feed ID {feed_id}: {e}")
            return 0

    def check_existing_guids(self, feed_id: int, guids: List[str]) -> Set[str]:
        """Check which GUIDs already exist in the database for this feed."""
        try:
            if not guids:
                return set()
            cursor = self.conn.cursor()
            placeholders = ",".join(["?" for _ in guids])
            query = f"SELECT guid FROM items WHERE feed_id = ? AND guid IN ({placeholders})"
            cursor.execute(query, [feed_id] + list(guids))
            existing_guids = {row[0] for row in cursor.fetchall()}
            return existing_guids
        except Error as e:
            logger.error(f"Error checking existing GUIDs for feed ID {feed_id}: {e}")
            return set()

    def check_existing_urls(self, urls: List[str]) -> Set[str]:
        """Check which URLs already exist globally (across all feeds)."""
        try:
            if not urls:
                return set()
            cursor = self.conn.cursor()
            placeholders = ",".join(["?" for _ in urls])
            query = f"SELECT url FROM items WHERE url IN ({placeholders})"
            cursor.execute(query, urls)
            existing = {row[0] for row in cursor.fetchall()}
            return existing
        except Error as e:
            logger.error(f"Error checking existing URLs: {e}")
            return set()
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def count_items(self) -> int:
        """Return total number of rows in items table (utility for tests)."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM items")
            result = cursor.fetchone()
            return int(result[0]) if result else 0
        except Error as e:
            logger.error(f"Error counting items: {e}")
            return 0
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def get_feed_etag(self, feed_id: int) -> Optional[str]:
        """Get the stored ETag for a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT etag FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            return result["etag"] if result and "etag" in result else None
        except Error as e:
            logger.error(f"Error getting etag for feed ID {feed_id}: {e}")
            return None

    def get_feed_last_modified(self, feed_id: int) -> Optional[str]:
        """Get the stored Last-Modified header for a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT last_modified FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            return result["last_modified"] if result and "last_modified" in result else None
        except Error as e:
            logger.error(f"Error getting last_modified for feed ID {feed_id}: {e}")
            return None

    def update_feed_headers(
        self, feed_id: int, etag: Optional[str] = None, last_modified: Optional[str] = None
    ) -> bool:
        """Update the HTTP headers for a feed."""
        try:
            current_time = int(time())
            cursor = self.conn.cursor()
            update_parts = ["last_fetched = ?"]
            params: List[Any] = [current_time]
            if etag is not None:
                update_parts.append("etag = ?")
                params.append(etag)
            if last_modified is not None:
                update_parts.append("last_modified = ?")
                params.append(last_modified)
            params.append(feed_id)
            cursor.execute(
                f"UPDATE feeds SET {', '.join(update_parts)} WHERE id = ?",
                tuple(params),
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating headers for feed ID {feed_id}: {e}")
            return False

    def update_feed_error(
        self, feed_id: int, error_count: int, last_error: Optional[str] = None
    ) -> bool:
        """Update error tracking for a feed."""
        try:
            cursor = self.conn.cursor()
            current_time = int(time())
            cursor.execute(
                "UPDATE feeds SET error_count = ?, last_error = ?, last_fetched = ? WHERE id = ?",
                (error_count, last_error, current_time, feed_id),
            )
            self.conn.commit()
            return True
        except Error as e:
            logger.error(f"Error updating feed error info for feed ID {feed_id}: {e}")
            return False

    def get_feed_error_info(self, feed_id: int) -> Dict[str, Any]:
        """Get error tracking information for a feed."""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT error_count, last_error FROM feeds WHERE id = ?", (feed_id,))
            result = cursor.fetchone()
            if result:
                return {"error_count": result[0] or 0, "last_error": result[1]}
            return {"error_count": 0, "last_error": None}
        except Error as e:
            logger.error(f"Error retrieving feed error info for feed ID {feed_id}: {e}")
            return {"error_count": 0, "last_error": None}

    def prune_items_per_feed(self, feed_id: int, max_items: int) -> int:
        """Retain only the newest `max_items` items for a feed."""
        if max_items <= 0:
            return 0
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT id FROM items WHERE feed_id = ? ORDER BY date DESC", (feed_id,))
            rows = cursor.fetchall()
            total = len(rows)
            if total <= max_items:
                return 0
            ids_to_delete = [r[0] for r in rows[max_items:]]
            if not ids_to_delete:
                return 0
            placeholders = ",".join(["?" for _ in ids_to_delete])
            cursor.execute(
                f"DELETE FROM summaries WHERE id IN ({placeholders})",
                ids_to_delete,
            )
            cursor.execute(
                f"DELETE FROM items WHERE id IN ({placeholders})",
                ids_to_delete,
            )
            deleted = cursor.rowcount
            self.conn.commit()
            logger.info(
                "Pruned %d old items for feed_id=%d (kept %d of %d)",
                deleted,
                feed_id,
                max_items,
                total,
            )
            return deleted
        except Error as e:
            logger.error(f"Error pruning items for feed ID {feed_id}: {e}")
            try:
                self.conn.rollback()
            except Exception:
                pass
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def query_raw_feeds(
        self, slugs: List[str], cutoff_age_hours: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Query unsummarized items for specified feed slugs."""
        if not slugs:
            return []
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row

            placeholders = ",".join(["?" for _ in slugs])
            params: List[Any] = list(slugs)
            age_clause = ""
            if cutoff_age_hours is not None and cutoff_age_hours > 0:
                try:
                    cutoff_ts = int(time()) - int(cutoff_age_hours * 3600)
                    age_clause = " AND i.date >= ?"
                    params.append(cutoff_ts)
                except Exception:
                    age_clause = ""

            limit_items = getattr(self, "SUMMARY_WINDOW_ITEMS_OVERRIDE", 50)
            if not isinstance(limit_items, int) or limit_items <= 0:
                limit_items = 50

            query = f"""
                SELECT
                    i.url as url,
                    i.title as title,
                    i.body as body,
                    i.id as id,
                    strftime('%Y-%m-%dT%H:%M:%SZ', i.date, 'unixepoch') as date,
                    i.date as pubdate,
                    f.url as feed_url,
                    f.title as feed_title
                FROM feeds f
                JOIN items i ON i.feed_id = f.id
                LEFT JOIN summaries s ON i.id = s.id
                WHERE s.id IS NULL AND f.slug IN ({placeholders}){age_clause}
                ORDER BY i.date DESC
                LIMIT ?
            """
            cursor.execute(query, params + [limit_items])
            rows = cursor.fetchall()

            items: List[Dict[str, Any]] = []
            for row in rows:
                items.append(
                    {
                        "url": row["url"],
                        "title": row["title"],
                        "body": row["body"],
                        "id": row["id"],
                        "date": row["date"],
                        "pubdate": row["pubdate"],
                        "feed_url": row["feed_url"],
                        "feed_title": row["feed_title"],
                    }
                )

            logger.debug("Found %d unsummarized items for feeds: %s", len(items), slugs)
            if len(items) > 0:
                logger.info(
                    "Processing %d unsummarized items from feeds: %s",
                    len(items),
                    ",".join(slugs),
                )
            return items
        except Error as e:
            logger.error(f"Error querying raw feeds for {slugs}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def get_feed_by_slug(self, slug: str) -> Optional[Dict[str, Any]]:
        """Return basic feed metadata for a given slug."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            cursor.execute("SELECT id, slug, url, title FROM feeds WHERE slug = ?", (slug,))
            row = cursor.fetchone()
            if not row:
                return None
            return {
                "id": row["id"],
                "slug": row["slug"],
                "url": row["url"],
                "title": row["title"] or slug,
            }
        except Error as e:
            logger.error(f"Error getting feed by slug {slug}: {e}")
            return None
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def query_latest_items_for_feed(self, slug: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Query the latest items for a specific feed slug (raw passthrough)."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.row_factory = Row
            query = (
                """
                SELECT i.id, i.title, i.url, i.guid, i.date, i.body
                FROM items i
                JOIN feeds f ON f.id = i.feed_id
                WHERE f.slug = ?
                ORDER BY i.date DESC
                LIMIT ?
                """
            )
            cursor.execute(query, (slug, limit))
            rows = cursor.fetchall()
            items: List[Dict[str, Any]] = []
            for row in rows:
                items.append(
                    {
                        "id": row["id"],
                        "title": row["title"],
                        "url": row["url"],
                        "guid": row["guid"],
                        "date": row["date"],
                        "body": row["body"],
                    }
                )
            return items
        except Error as e:
            logger.error(f"Error querying latest items for feed {slug}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def reset_feed_error(self, feed_id: int) -> bool:
        """Reset the error count and last error for a feed."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            cursor.execute(
                """
                UPDATE feeds
                SET error_count = 0, last_error = NULL
                WHERE id = ?
                """,
                (feed_id,),
            )
            success = cursor.rowcount > 0
            if success:
                self.conn.commit()
                logger.debug(f"Reset error status for feed {feed_id}")
            else:
                logger.warning(f"No feed found with ID {feed_id} to reset errors")
            return success
        except Error as e:
            logger.error(f"Error resetting feed error for feed {feed_id}: {e}")
            return False
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
