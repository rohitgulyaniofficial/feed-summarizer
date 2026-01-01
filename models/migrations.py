#!/usr/bin/env python3
"""Database migrations for feed-summarizer.

Separated from schema.py to keep initialization concise and testable.
"""
from __future__ import annotations

import json
from typing import Any

from config import get_logger
from utils.merge_policy import merge_fingerprint_from_text
from utils import encode_int64

logger = get_logger("models.migrations")


def run_migrations(conn: Any) -> None:
    """Run best-effort database migrations on an existing schema."""
    cursor = conn.cursor()
    try:
        # Create migration tracking table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS migration_log (
                migration_name TEXT PRIMARY KEY,
                applied_date INTEGER NOT NULL,
                notes TEXT
            )
            """
        )
        conn.commit()

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

        # New partial/covering indexes for hash-based workflows and published queries
        try:
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_summaries_merge_simhash_ready
                ON summaries(merge_simhash, generated_date, id)
                WHERE merge_simhash IS NOT NULL AND summary_text IS NOT NULL
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_summaries_merge_simhash_ready (may already exist): %s", exc)

        try:
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_summaries_simhash_recent
                ON summaries(simhash, generated_date DESC)
                WHERE simhash IS NOT NULL AND generated_date IS NOT NULL
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_summaries_simhash_recent (may already exist): %s", exc)

        try:
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_summaries_published_cover
                ON summaries(published_date, id, merge_simhash)
                WHERE published_date IS NOT NULL
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_summaries_published_cover (may already exist): %s", exc)

        # Partial index for failed feeds (status reporting/backoff visibility)
        try:
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_feeds_failed_recent
                ON feeds(last_fetched DESC)
                WHERE last_error IS NOT NULL AND error_count > 0
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_feeds_failed_recent (may already exist): %s", exc)

        # Items recency without feed filter
        try:
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_items_date_id
                ON items(date DESC, id)
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning("Could not create idx_items_date_id (may already exist): %s", exc)

        # Covering index for feed/date ordered scans when selecting pending summaries
        try:
            cursor.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_items_feed_date_id_cover
                ON items(feed_id, date DESC, id)
                """
            )
            conn.commit()
        except Exception as exc:
            logger.warning(
                "Could not create idx_items_feed_date_id_cover (may already exist): %s",
                exc,
            )

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

        # Check and recompute SimHash values if needed (one-time migration)
        _check_and_recompute_simhash(conn)

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


def _check_and_recompute_simhash(conn: Any) -> None:
    """Check if SimHash recomputation is needed and run it if necessary.
    
    This migration runs once per deployment to update merge_simhash values
    from the old English-only stopwords to the new multilingual stopwords.
    """
    cursor = conn.cursor()
    try:
        # Check if this migration has already been applied
        cursor.execute(
            "SELECT migration_name FROM migration_log WHERE migration_name = ?",
            ("recompute_merge_simhash_multilingual",),
        )
        if cursor.fetchone() is not None:
            logger.debug("SimHash migration already applied, skipping")
            return

        # Count summaries that need recomputation
        cursor.execute("""
            SELECT COUNT(*) as total
            FROM summaries s
            JOIN items i ON s.id = i.id
            WHERE s.summary_text IS NOT NULL AND s.summary_text != ''
        """)
        total = cursor.fetchone()[0]
        
        if total == 0:
            logger.info("No summaries to migrate")
            # Mark as applied even if nothing to do
            import time
            cursor.execute(
                "INSERT INTO migration_log (migration_name, applied_date, notes) VALUES (?, ?, ?)",
                ("recompute_merge_simhash_multilingual", int(time.time()), "No summaries to migrate"),
            )
            conn.commit()
            return

        logger.info(f"Starting SimHash migration for {total:,} summaries...")
        
        # Get all summaries with their titles
        cursor.execute("""
            SELECT s.id, i.title, s.summary_text, s.merge_simhash as old_simhash
            FROM summaries s
            JOIN items i ON s.id = i.id
            WHERE s.summary_text IS NOT NULL AND s.summary_text != ''
            ORDER BY s.id
        """)
        rows = cursor.fetchall()
        
        updated_count = 0
        batch_size = 1000
        batch_updates = []
        
        for row in rows:
            summary_id = row[0]
            title = row[1] or ""
            summary_text = row[2] or ""
            old_simhash = row[3]
            
            # Compute new merge_simhash using current algorithm with multilingual stopwords
            new_simhash = merge_fingerprint_from_text(title, summary_text)
            
            if new_simhash is not None:
                # Encode as signed int64 for SQLite
                new_simhash_encoded = encode_int64(new_simhash)
            else:
                new_simhash_encoded = None
            
            if new_simhash_encoded != old_simhash:
                batch_updates.append((new_simhash_encoded, summary_id))
                updated_count += 1
            
            # Process batch
            if len(batch_updates) >= batch_size:
                cursor.executemany(
                    "UPDATE summaries SET merge_simhash = ? WHERE id = ?",
                    batch_updates
                )
                conn.commit()
                logger.info(f"SimHash migration progress: {updated_count:,}/{total:,} updated")
                batch_updates = []
        
        # Process remaining batch
        if batch_updates:
            cursor.executemany(
                "UPDATE summaries SET merge_simhash = ? WHERE id = ?",
                batch_updates
            )
            conn.commit()
        
        # Mark migration as complete
        import time
        cursor.execute(
            "INSERT INTO migration_log (migration_name, applied_date, notes) VALUES (?, ?, ?)",
            (
                "recompute_merge_simhash_multilingual",
                int(time.time()),
                f"Updated {updated_count:,}/{total:,} summaries ({updated_count / total * 100:.1f}%)",
            ),
        )
        conn.commit()
        
        logger.info(
            f"SimHash migration complete: {updated_count:,}/{total:,} summaries updated "
            f"({updated_count / total * 100:.1f}%)"
        )
        
    except Exception as exc:
        logger.error(f"Error during SimHash migration: {exc}")
        # Don't raise - allow system to continue with partial migration
    finally:
        cursor.close()


__all__ = ["run_migrations"]
