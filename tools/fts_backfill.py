#!/usr/bin/env python3
"""Backfill SQLite FTS5 index for BM25 merging.

This populates the `summary_fts` virtual table from existing rows in `summaries`
(joined to `items` for titles).

It is safe to run multiple times (uses INSERT OR REPLACE with rowid=id).

Usage:
  python3 tools/fts_backfill.py --db /path/to/feeds.db --limit 20000
"""

from __future__ import annotations

import argparse
import sqlite3
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _fts_available(conn: sqlite3.Connection) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='summary_fts'"
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _ensure_fts(conn: sqlite3.Connection) -> None:
    """Best-effort create, matching schema.sql/models.py migration."""
    conn.execute(
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


def _iter_rows(conn: sqlite3.Connection, limit: int) -> Iterable[Tuple[int, str, str, str]]:
    sql = """
        SELECT s.id AS summary_id,
               i.title AS title,
               s.summary_text AS summary_text,
               s.topic AS topic
        FROM summaries s
        JOIN items i ON i.id = s.id
        WHERE s.summary_text IS NOT NULL AND s.summary_text != ''
        ORDER BY s.id DESC
        LIMIT ?
    """
    for row in conn.execute(sql, (int(limit),)):
        sid = int(row[0])
        title = str(row[1] or "")
        summary_text = str(row[2] or "")
        topic = str(row[3] or "")
        yield sid, title, summary_text, topic


def backfill(conn: sqlite3.Connection, limit: int, batch_size: int) -> Dict[str, Any]:
    _ensure_fts(conn)
    if not _fts_available(conn):
        return {"ok": False, "reason": "FTS5 unavailable (summary_fts missing)"}

    inserted = 0
    rows = list(_iter_rows(conn, limit))
    for i in range(0, len(rows), int(batch_size)):
        chunk = rows[i : i + int(batch_size)]
        conn.executemany(
            "INSERT OR REPLACE INTO summary_fts(rowid, title, summary_text, topic) VALUES (?, ?, ?, ?)",
            chunk,
        )
        conn.commit()
        inserted += len(chunk)

    return {"ok": True, "inserted": inserted}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill summary_fts for BM25 merging")
    parser.add_argument("--db", dest="db_path", default="feeds.db", help="Path to SQLite DB")
    parser.add_argument("--limit", type=int, default=20000, help="Max summaries to index (default: 20000)")
    parser.add_argument("--batch", type=int, default=500, help="Batch size (default: 500)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    conn = sqlite3.connect(args.db_path)
    try:
        result = backfill(conn, int(args.limit), int(args.batch))
    finally:
        conn.close()

    if not result.get("ok"):
        reason = result.get("reason") or "unknown"
        raise SystemExit(f"FTS backfill failed: {reason}")

    print(f"FTS backfill complete: inserted={int(result.get('inserted') or 0)}")


if __name__ == "__main__":
    main()
