#!/usr/bin/env python3
"""Summary-related operations for the database queue."""

from __future__ import annotations

from time import time
from typing import Any, Dict, List, Optional, Set, Tuple

from sqlite3 import Error, Row

from config import get_logger
from utils import decode_int64, encode_int64

logger = get_logger("models")


class SummariesOpsMixin:
    conn: Any

    def _fts_available(self, cursor) -> bool:
        try:
            row = cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='summary_fts'"
            ).fetchone()
            return bool(row)
        except Exception:
            return False

    def bm25_candidates(
        self,
        query_id: int,
        query_text: str,
        topic: Optional[str],
        candidate_ids: List[int],
        limit: int = 10,
    ) -> Dict[str, Any]:
        """Return BM25 scores for candidate summaries using SQLite FTS5."""
        if not candidate_ids:
            return {"self_score": None, "candidates": []}
        if self.conn is None:
            return {"self_score": None, "candidates": []}

        cursor = self.conn.cursor()
        try:
            if not self._fts_available(cursor):
                return {"self_score": None, "candidates": []}

            q = (query_text or "").strip()
            if not q:
                return {"self_score": None, "candidates": []}

            topic_norm = None
            if isinstance(topic, str):
                tn = topic.strip()
                topic_norm = tn if tn else None

            self_row = cursor.execute(
                "SELECT bm25(summary_fts) AS score FROM summary_fts WHERE rowid = ? AND summary_fts MATCH ?",
                (int(query_id), q),
            ).fetchone()
            self_score = None
            if self_row and self_row[0] is not None:
                try:
                    self_score = float(self_row[0])
                except Exception:
                    self_score = None

            placeholders = ",".join(["?"] * len(candidate_ids))
            if topic_norm is None:
                sql = f"""
                    SELECT rowid AS id, bm25(summary_fts) AS score
                    FROM summary_fts
                    WHERE summary_fts MATCH ?
                      AND rowid IN ({placeholders})
                    ORDER BY score
                    LIMIT ?
                """
                params = [q, *[int(x) for x in candidate_ids], int(limit)]
            else:
                sql = f"""
                    SELECT rowid AS id, bm25(summary_fts) AS score
                    FROM summary_fts
                    WHERE summary_fts MATCH ?
                      AND topic = ?
                      AND rowid IN ({placeholders})
                    ORDER BY score
                    LIMIT ?
                """
                params = [q, topic_norm, *[int(x) for x in candidate_ids], int(limit)]

            rows = cursor.execute(sql, params).fetchall()
            out = []
            for r in rows:
                try:
                    out.append({"id": int(r[0]), "score": float(r[1])})
                except Exception:
                    continue
            return {"self_score": self_score, "candidates": out}
        finally:
            try:
                cursor.close()
            except Exception:
                pass

    def mark_summaries_as_published(self, summary_ids: List[int]) -> int:
        """Mark summaries as published by setting the published_date timestamp."""
        if not summary_ids:
            return 0
        try:
            cursor = self.conn.cursor()
            current_time = int(time())
            placeholders = ",".join(["?" for _ in summary_ids])
            cursor.execute(
                f"UPDATE summaries SET published_date = ? WHERE id IN ({placeholders})",
                [current_time] + summary_ids,
            )
            rows_affected = cursor.rowcount
            self.conn.commit()
            logger.debug(f"Database: marked {rows_affected} summaries as published")
            return rows_affected
        except Error as e:
            logger.error(f"Error marking summaries as published {summary_ids}: {e}")
            return 0

    def query_summaries_for_feeds(
        self,
        feed_slugs: List[str],
        limit: int = 50,
        per_feed_limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Query unpublished summaries for specific feeds (fair per-feed sampling)."""
        if not feed_slugs or limit <= 0:
            return []

        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.row_factory = Row
                per_feed = per_feed_limit or limit
                aggregated: List[Dict[str, Any]] = []
                seen_ids: Set[int] = set()

                base_query = """
                    SELECT
                        s.id,
                        s.summary_text,
                        s.topic,
                        s.simhash,
                        s.merge_simhash,
                        s.generated_date,
                        s.published_date,
                        i.title as item_title,
                        i.url as item_url,
                        i.date as item_date,
                        f.title as feed_title,
                        f.slug as feed_slug
                    FROM summaries s
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE f.slug = ?
                    AND s.summary_text IS NOT NULL
                    AND s.summary_text != ''
                    AND s.published_date IS NULL
                    ORDER BY i.date DESC
                    LIMIT ?
                """

                for slug in feed_slugs:
                    cursor.execute(base_query, (slug, per_feed))
                    rows = cursor.fetchall()
                    for row in rows:
                        summary_id = row["id"]
                        if summary_id in seen_ids:
                            continue
                        seen_ids.add(summary_id)
                        aggregated.append(
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

                aggregated.sort(key=lambda row: row["item_date"] or 0, reverse=True)
                summaries = aggregated[:limit]
                logger.debug(
                    "Found %d summaries across %d feeds (per_feed_limit=%d)",
                    len(summaries),
                    len(feed_slugs),
                    per_feed,
                )
                return summaries
        except Error as e:
            logger.error(f"Error querying summaries for feeds {feed_slugs}: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def find_bulletin_sessions_for_summaries(
        self, group_name: str, summary_ids: List[int]
    ) -> List[str]:
        """Return session keys for bulletins that already reference the provided summaries."""
        if not summary_ids:
            return []
        cursor = None
        try:
            cursor = self.conn.cursor()
            placeholders = ",".join("?" for _ in summary_ids)
            query = f"""
                SELECT DISTINCT b.session_key
                FROM bulletins b
                JOIN bulletin_summaries bs ON b.id = bs.bulletin_id
                WHERE b.group_name = ?
                AND bs.summary_id IN ({placeholders})
            """
            params = [group_name] + summary_ids
            cursor.execute(query, params)
            rows = cursor.fetchall()
            return [row[0] for row in rows]
        except Error as e:
            logger.error(f"Error finding bulletin sessions for {group_name}: {e}")
            raise
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def query_unpublished_summaries_for_feeds(
        self, feed_slugs: List[str], limit: int = 50
    ) -> List[Dict[str, Any]]:
        """Query unpublished summaries for specific feeds."""
        if not feed_slugs:
            return []
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.row_factory = Row
                placeholders = ",".join(["?" for _ in feed_slugs])
                query = f"""
                    SELECT
                        s.id,
                        s.summary_text,
                        s.topic,
                        s.simhash,
                        s.merge_simhash,
                        s.generated_date,
                        s.published_date,
                        i.title as item_title,
                        i.url as item_url,
                        i.date as item_date,
                        f.title as feed_title,
                        f.slug as feed_slug
                    FROM summaries s
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE f.slug IN ({placeholders})
                    AND s.summary_text IS NOT NULL
                    AND s.summary_text != ''
                    AND s.published_date IS NULL
                    ORDER BY i.date DESC
                    LIMIT ?
                """
                cursor.execute(query, feed_slugs + [limit])
                rows = cursor.fetchall()

                summaries: List[Dict[str, Any]] = []
                for row in rows:
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
                logger.debug(
                    f"Found {len(summaries)} unpublished summaries for feeds: {feed_slugs}"
                )
                return summaries
        except Error as e:
            logger.error(f"Error querying unpublished summaries for feeds {feed_slugs}: {e}")
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def query_published_summaries_by_date(
        self, feed_slugs: List[str], cutoff_time: int
    ) -> List[Dict[str, Any]]:
        """Query published summaries for specific feeds after cutoff_time."""
        if not feed_slugs:
            return []
        cursor = None
        try:
            with self.conn:
                cursor = self.conn.cursor()
                cursor.row_factory = Row
                placeholders = ",".join(["?" for _ in feed_slugs])
                query = f"""
                    SELECT
                        s.id,
                        s.summary_text,
                        s.topic,
                        s.simhash,
                        s.merge_simhash,
                        s.generated_date,
                        s.published_date,
                        i.title as item_title,
                        i.url as item_url,
                        i.date as item_date,
                        f.title as feed_title,
                        f.slug as feed_slug
                    FROM summaries s
                    JOIN items i ON s.id = i.id
                    JOIN feeds f ON i.feed_id = f.id
                    WHERE f.slug IN ({placeholders})
                    AND s.summary_text IS NOT NULL
                    AND s.summary_text != ''
                    AND s.published_date IS NOT NULL
                    AND s.published_date >= ?
                    ORDER BY s.published_date DESC, i.date DESC
                """
                cursor.execute(query, feed_slugs + [cutoff_time])
                rows = cursor.fetchall()

                summaries: List[Dict[str, Any]] = []
                for row in rows:
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
                logger.debug(
                    f"Found {len(summaries)} published summaries for feeds: {feed_slugs}"
                )
                return summaries
        except Error as e:
            logger.error(
                f"Error querying published summaries by date for feeds {feed_slugs}: {e}"
            )
            return []
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass

    def verify_and_mark_as_summarized(
        self, ids: List[int], summaries: Dict[int, Tuple[str, str, Optional[int]]]
    ) -> int:
        """Verify that IDs exist and upsert summaries for them."""
        cursor = None
        try:
            cursor = self.conn.cursor()
            placeholders = ",".join(["?" for _ in ids])
            query = f"""
                SELECT id FROM items
                WHERE id IN ({placeholders})
            """
            cursor.execute(query, ids)
            existing_ids = [row[0] for row in cursor.fetchall()]
            if not existing_ids:
                logger.warning(f"None of the provided IDs {ids} exist in the items table")
                return 0

            title_map: Dict[int, str] = {}
            try:
                title_rows = cursor.execute(
                    f"SELECT id, title FROM items WHERE id IN ({placeholders})",
                    ids,
                ).fetchall()
                for row in title_rows:
                    try:
                        title_map[int(row[0])] = str(row[1] or "")
                    except Exception:
                        continue
            except Exception:
                title_map = {}

            insert_count = 0
            for item_id in existing_ids:
                summary_info = summaries.get(item_id)
                if summary_info:
                    simhash = None
                    merge_simhash = None
                    if len(summary_info) == 4:
                        summary_text, topic, simhash, merge_simhash = summary_info  # type: ignore[misc]
                    elif len(summary_info) == 3:
                        summary_text, topic, simhash = summary_info
                    else:
                        summary_text, topic = summary_info  # type: ignore[misc]

                    simhash_db = encode_int64(simhash)
                    merge_simhash_db = encode_int64(merge_simhash)
                    try:
                        cursor.execute(
                            """
                            INSERT OR REPLACE INTO summaries (id, summary_text, topic, generated_date, simhash, merge_simhash)
                            VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                item_id,
                                summary_text,
                                topic,
                                int(time()),
                                simhash_db,
                                merge_simhash_db,
                            ),
                        )

                        try:
                            cursor.execute(
                                """
                                INSERT OR REPLACE INTO summary_fts(rowid, title, summary_text, topic)
                                VALUES(?, ?, ?, ?)
                                """,
                                (item_id, title_map.get(item_id, ""), summary_text, topic),
                            )
                        except Exception:
                            pass

                        insert_count += 1
                    except Error as e:
                        logger.error(f"Error inserting summary for item {item_id}: {e}")
                        continue
            self.conn.commit()
            logger.debug(f"Successfully marked {insert_count} items as summarized")
            return insert_count
        except Error as e:
            logger.error(f"Error verifying and marking items as summarized: {e}")
            return 0
        finally:
            if cursor:
                try:
                    cursor.close()
                except Exception:
                    pass
