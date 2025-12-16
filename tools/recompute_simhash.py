#!/usr/bin/env python3
"""Recompute SimHash values for all summaries in the database.

This script reads each item's body (falling back to summary_text when needed),
recomputes the SimHash using utils.compute_simhash, encodes it for SQLite via
utils.encode_int64, and updates summaries.simhash in place.

Usage (from project root):
  python -m tools.recompute_simhash [--batch-size N]

It is safe to run multiple times; it always overwrites simhash based on the
current compute_simhash implementation.
"""

from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path
from typing import Iterable, Tuple

from utils import compute_simhash, encode_int64, get_logger

logger = get_logger("recompute_simhash")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Recompute SimHash for all summaries")
    parser.add_argument(
        "--db",
        dest="db_path",
        default="feeds.db",
        help="Path to SQLite database (default: feeds.db)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows per UPDATE transaction batch (default: 500)",
    )
    return parser.parse_args()


def iter_items(conn: sqlite3.Connection, batch_size: int) -> Iterable[Tuple[int, str | None, str | None]]:
    cur = conn.cursor()
    offset = 0
    while True:
        cur.execute(
            """
            SELECT i.id, i.body, s.summary_text
            FROM items i
            JOIN summaries s ON s.id = i.id
            ORDER BY i.id
            LIMIT ? OFFSET ?
            """,
            (batch_size, offset),
        )
        rows = cur.fetchall()
        if not rows:
            break
        for row in rows:
            yield row
        offset += batch_size


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)
    conn.isolation_level = None  # autocommit controlled via explicit BEGIN/COMMIT

    cur = conn.cursor()

    batch: list[Tuple[int, int | None]] = []
    total = 0
    updated = 0

    for item_id, body, summary_text in iter_items(conn, args.batch_size):
        text_source = body or summary_text or ""
        if not text_source:
            encoded = None
        else:
            h = compute_simhash(text_source)
            encoded = encode_int64(h) if h is not None else None
        batch.append((encoded, item_id))
        total += 1

        if len(batch) >= args.batch_size:
            cur.execute("BEGIN")
            cur.executemany("UPDATE summaries SET simhash = ? WHERE id = ?", batch)
            cur.execute("COMMIT")
            updated += len(batch)
            logger.info(f"Updated simhash for {updated} summaries so far (processed {total})")
            batch.clear()

    if batch:
        cur.execute("BEGIN")
        cur.executemany("UPDATE summaries SET simhash = ? WHERE id = ?", batch)
        cur.execute("COMMIT")
        updated += len(batch)
        batch.clear()

    logger.info(f"Finished recomputing simhash for {updated} summaries (processed {total})")


if __name__ == "__main__":
    main()
