#!/usr/bin/env python3
"""Database migrations for feed-summarizer.

Separated from schema.py to keep initialization concise and testable.
"""
from __future__ import annotations

import json
from typing import Any

from config import get_logger

logger = get_logger("models.migrations")


def run_migrations(conn: Any) -> None:
    """Run best-effort database migrations on an existing schema."""
    cursor = conn.cursor()
    try:
        cursor.execute("PRAGMA table_info(summaries)")
        columns = [column[1] for column in cursor.fetchall()]

        if "published_date" not in columns:
            logger.info("Adding published_date column to summaries table")
            cursor.execute("ALTER TABLE summaries ADD COLUMN published_date INTEGER")
            conn.commit()
            logger.info("Migration completed: added published_date column")
        if "simhash" not in columns:
            logger.info("Adding simhash column to summaries table")
            cursor.execute("ALTER TABLE summaries ADD COLUMN simhash INTEGER")
            conn.commit()
            logger.info("Migration completed: added simhash column")
        if "merge_simhash" not in columns:
            logger.info("Adding merge_simhash column to summaries table")
            cursor.execute("ALTER TABLE summaries ADD COLUMN merge_simhash INTEGER")
            conn.commit()
            logger.info("Migration completed: added merge_simhash column")

        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_summaries_simhash ON summaries(simhash)")
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_summaries_simhash (may already exist): %s", exc)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_summaries_merge_simhash ON summaries(merge_simhash)")
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_summaries_merge_simhash (may already exist): %s", exc)

        # FTS5 table for BM25 matching (best-effort)
        try:
            cursor.execute(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS summary_fts
                USING fts5(
                    title,
                    summary_text,
                    topic UNINDEXED,
                    tokenize='unicode61 remove_diacritics 1'
                )
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning("FTS5 unavailable or failed to initialize summary_fts: %s", exc)

        # Bulletins tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bulletins'")
        bulletins_exists = cursor.fetchone() is not None
        if not bulletins_exists:
            logger.info("Creating bulletins table")
            cursor.execute(
                """
                CREATE TABLE bulletins (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    group_name TEXT NOT NULL,
                    session_key TEXT NOT NULL,
                    created_date INTEGER NOT NULL,
                    title TEXT,
                    introduction TEXT,
                    summary_count INTEGER DEFAULT 0,
                    feed_slugs TEXT,
                    UNIQUE(group_name, session_key)
                )
                """
            )
            cursor.execute(
                """
                CREATE TABLE bulletin_summaries (
                    bulletin_id INTEGER,
                    summary_id INTEGER,
                    PRIMARY KEY (bulletin_id, summary_id),
                    FOREIGN KEY (bulletin_id) REFERENCES bulletins(id) ON DELETE CASCADE,
                    FOREIGN KEY (summary_id) REFERENCES summaries(id) ON DELETE CASCADE
                )
                """
            )
            conn.commit()
            logger.info("Migration completed: created bulletins and bulletin_summaries tables")
        else:
            # Ensure bulletins.title exists
            try:
                cursor.execute("PRAGMA table_info(bulletins)")
                bcols = [row[1] for row in cursor.fetchall()]
                if "title" not in bcols:
                    logger.info("Adding title column to bulletins table")
                    cursor.execute("ALTER TABLE bulletins ADD COLUMN title TEXT")
                    conn.commit()
                    logger.info("Migration completed: added title column to bulletins")
            except Exception as exc:
                logger.warning("Could not add title column to bulletins (may already exist): %s", exc)

        # bulletin_entries table for persisted grouped payload
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='bulletin_entries'")
        entries_exists = cursor.fetchone() is not None
        created_entries_table = False
        if not entries_exists:
            logger.info("Creating bulletin_entries table")
            cursor.execute(
                """
                CREATE TABLE bulletin_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    bulletin_id INTEGER NOT NULL,
                    position INTEGER NOT NULL,
                    topic TEXT,
                    item_date INTEGER,
                    entry_json TEXT NOT NULL,
                    FOREIGN KEY (bulletin_id) REFERENCES bulletins(id) ON DELETE CASCADE
                )
                """
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_bulletin_entries_bulletin ON bulletin_entries(bulletin_id)"
            )
            cursor.execute(
                "CREATE INDEX IF NOT EXISTS idx_bulletin_entries_pos ON bulletin_entries(bulletin_id, position)"
            )
            conn.commit()
            created_entries_table = True
            logger.info("Migration completed: created bulletin_entries table")
        if created_entries_table:
            try:
                _backfill_bulletin_entries(conn)
            except Exception as backfill_err:
                logger.warning("Backfill of bulletin_entries failed: %s", backfill_err)

    except Exception as exc:
        logger.error("Error running migrations: %s", exc)
        raise
    finally:
        cursor.close()


def _as_int(value: Any) -> Any:
    try:
        return int(value)
    except Exception:
        return None


def _backfill_bulletin_entries(conn: Any) -> None:
    """Best-effort backfill of bulletin_entries for legacy bulletins.

    Runs synchronously during migration when the bulletin_entries table is first created.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) FROM bulletins")
        total_bulletins = cursor.fetchone()[0]
        if total_bulletins == 0:
            logger.info("No bulletins to backfill")
            return

        cursor.execute("SELECT id, group_name, session_key FROM bulletins ORDER BY created_date DESC")
        bulletins = cursor.fetchall() or []
        updated = 0
        skipped = 0

        for row in bulletins:
            bid = row[0]
            cursor.execute("SELECT COUNT(*) FROM bulletin_entries WHERE bulletin_id = ?", (bid,))
            if cursor.fetchone()[0] > 0:
                skipped += 1
                continue

            cursor.execute(
                """
                SELECT
                    s.id, s.summary_text, s.topic, s.generated_date, s.published_date,
                    i.title as item_title, i.url as item_url, i.date as item_date,
                    f.title as feed_title, f.slug as feed_slug
                FROM bulletin_summaries bs
                JOIN summaries s ON bs.summary_id = s.id
                JOIN items i ON s.id = i.id
                JOIN feeds f ON i.feed_id = f.id
                WHERE bs.bulletin_id = ?
                ORDER BY i.date DESC
                """,
                (bid,),
            )
            summaries = cursor.fetchall() or []
            if not summaries:
                skipped += 1
                continue

            entry_rows = []
            for idx, srow in enumerate(summaries):
                try:
                    item_date = _as_int(srow["item_date"])
                    if item_date is None and hasattr(srow["item_date"], "timestamp"):
                        item_date = int(srow["item_date"].timestamp())
                except Exception:
                    item_date = None
                try:
                    entry_rows.append(
                        (
                            bid,
                            idx,
                            srow["topic"],
                            item_date,
                            json.dumps(
                                {
                                    "id": srow["id"],
                                    "summary_text": srow["summary_text"],
                                    "topic": srow["topic"],
                                    "merged_ids": [srow["id"]],
                                    "merged_links": [
                                        {
                                            "url": srow["item_url"],
                                            "title": srow["item_title"] or srow["feed_title"] or "Read more",
                                            "feed_slug": srow["feed_slug"],
                                        }
                                    ]
                                    if srow["item_url"]
                                    else [],
                                    "item_date": item_date,
                                    "published_date": srow["published_date"] or item_date,
                                    "item_title": srow["item_title"],
                                    "item_url": srow["item_url"],
                                    "feed_slug": srow["feed_slug"],
                                }
                            ),
                        )
                    )
                except Exception:
                    continue

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
                updated += 1
        conn.commit()
        logger.info("Backfill bulletin_entries complete: %d updated, %d skipped", updated, skipped)
    finally:
        cursor.close()


__all__ = ["run_migrations"]
