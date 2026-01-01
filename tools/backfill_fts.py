#!/usr/bin/env python3
"""Backfill SQLite FTS5 index for BM25 merging.

This populates the `summary_fts` virtual table from existing rows in `summaries`
(joined to `items` for titles).

It is safe to run multiple times (uses INSERT OR REPLACE with rowid=id).

Usage:
  python -m tools.backfill_fts --db feeds.db --limit 20000 --verbose
"""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from typing import Any, Dict, Iterable, Tuple

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.panel import Panel

# Suppress config logging noise
os.environ["LOG_LEVEL"] = "ERROR"

from tools.standard_args import create_standard_parser
from tools.common import validate_database_path, safe_database_connection
from utils.bm25_merge import fts_available as _fts_available

console = Console()


def _ensure_fts(conn: sqlite3.Connection) -> None:
    """Best-effort create, matching models/schema.sql and models/schema.py migrations."""
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
    parser = create_standard_parser(
        description="Backfill summary_fts for BM25 merging",
        with_db=True,
        with_verbosity=True,
    )
    parser.add_argument("--limit", type=int, default=20000, help="Max summaries to index (default: 20000)")
    parser.add_argument("--batch", type=int, default=500, help="Batch size (default: 500)")
    return parser.parse_args()


def main() -> int:
    # Suppress logging from config module
    logging.getLogger("FeedProcessor").setLevel(logging.CRITICAL)

    args = parse_args()

    if args.limit <= 0:
        console.print(f"[red]Error: limit must be positive, got {args.limit}[/red]")
        return 1

    if args.batch <= 0:
        console.print(f"[red]Error: batch size must be positive, got {args.batch}[/red]")
        return 1

    validate_database_path(args.db)

    console.print(Panel.fit("FTS5 Backfill (summary_fts)", style="bold blue"))
    console.print()
    console.print(f"[cyan]Database:[/cyan] {args.db}")
    console.print(f"[cyan]Limit:[/cyan] {args.limit:,} summaries")
    console.print(f"[cyan]Batch:[/cyan] {args.batch:,}")
    console.print()

    with safe_database_connection(args.db) as conn:
        rows = list(_iter_rows(conn, args.limit))

    total_rows = len(rows)
    if total_rows == 0:
        console.print("[yellow]No summaries with text found; nothing to backfill.[/yellow]")
        return 0

    console.print(f"[cyan]Preparing to backfill {total_rows:,} rows into summary_fts...[/cyan]")

    inserted = 0
    with safe_database_connection(args.db) as conn:
        _ensure_fts(conn)
        if not _fts_available(conn):
            console.print("[red]FTS5 unavailable (summary_fts missing).[/red]")
            return 1

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Backfilling", total=total_rows)

            for i in range(0, total_rows, int(args.batch)):
                chunk = rows[i : i + int(args.batch)]
                conn.executemany(
                    "INSERT OR REPLACE INTO summary_fts(rowid, title, summary_text, topic) VALUES (?, ?, ?, ?)",
                    chunk,
                )
                conn.commit()
                inserted += len(chunk)
                progress.update(task, advance=len(chunk))

    console.print()
    console.print(f"[green]FTS backfill complete: inserted {inserted:,} rows[/green]")
    return 0


if __name__ == "__main__":
    sys.exit(main())
