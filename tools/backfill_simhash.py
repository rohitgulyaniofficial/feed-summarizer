#!/usr/bin/env python3
"""Recompute merge_simhash for all summaries using updated SimHash algorithm."""

from __future__ import annotations

import argparse
import logging
import os
import sqlite3
import sys
from typing import List, Tuple

from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn

# Suppress config logging noise
os.environ["LOG_LEVEL"] = "ERROR"

from tools.standard_args import create_standard_parser
from tools.common import validate_database_path, safe_database_connection
from utils.merge_policy import merge_fingerprint_from_text
from utils import encode_int64


def parse_args() -> argparse.Namespace:
    parser = create_standard_parser(
        description="Recompute merge_simhash for all summaries",
        with_db=True,
        with_verbosity=True,
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Batch size for commits (default: 1000)")
    parser.add_argument("--dry-run", action="store_true", help="Don't commit changes")
    return parser.parse_args()


def load_rows(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Load rows for recomputation."""
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT s.id, s.summary_text, s.merge_simhash as old_simhash
        FROM summaries s
        WHERE s.summary_text IS NOT NULL AND s.summary_text != ''
        ORDER BY s.id
        """
    )
    return cursor.fetchall()


def count_rows(conn: sqlite3.Connection) -> int:
    cursor = conn.cursor()
    cursor.execute(
        """
        SELECT COUNT(*) as total
        FROM summaries s
        WHERE s.summary_text IS NOT NULL AND s.summary_text != ''
        """
    )
    result = cursor.fetchone()
    return int(result["total"] if result else 0)


def recompute_all_merge_simhash(db_path: str, batch_size: int, dry_run: bool, console: Console) -> Tuple[int, int, int]:
    with safe_database_connection(db_path) as conn:
        total = count_rows(conn)
        rows = load_rows(conn)

    if total == 0 or not rows:
        return 0, 0, 0

    updated_count = 0
    unchanged_count = 0
    batch_updates: List[Tuple[int, int]] = []

    with safe_database_connection(db_path) as conn:
        cursor = conn.cursor()

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Recomputing merge_simhash", total=total)

            for row in rows:
                summary_id = row["id"]
                summary_text = row["summary_text"] or ""
                old_simhash = row["old_simhash"]

                # merge_fingerprint_from_text ignores title (kept for API compat)
                new_simhash = merge_fingerprint_from_text("", summary_text)
                new_simhash_encoded = encode_int64(new_simhash) if new_simhash is not None else None

                if new_simhash_encoded != old_simhash:
                    batch_updates.append((new_simhash_encoded, summary_id))
                    updated_count += 1
                else:
                    unchanged_count += 1

                if len(batch_updates) >= batch_size:
                    if not dry_run:
                        cursor.executemany("UPDATE summaries SET merge_simhash = ? WHERE id = ?", batch_updates)
                        conn.commit()
                    batch_updates = []

                progress.update(task, advance=1)

            if batch_updates and not dry_run:
                cursor.executemany("UPDATE summaries SET merge_simhash = ? WHERE id = ?", batch_updates)
                conn.commit()

    return total, updated_count, unchanged_count


def main() -> int:
    logging.getLogger("FeedProcessor").setLevel(logging.CRITICAL)

    args = parse_args()
    console = Console(quiet=args.quiet)

    if args.batch_size <= 0:
        console.print(f"[red]Error: batch size must be positive, got {args.batch_size}[/red]")
        return 1

    db_path = validate_database_path(args.db)

    console.print(Panel.fit("Recompute merge_simhash", style="bold blue"))
    console.print()
    console.print(f"[cyan]Database:[/cyan] {db_path}")
    console.print(f"[cyan]Batch size:[/cyan] {args.batch_size:,}")
    if args.dry_run:
        console.print("[yellow]DRY RUN - No changes will be committed[/yellow]")
    console.print()

    total, updated_count, unchanged_count = recompute_all_merge_simhash(
        str(db_path), args.batch_size, args.dry_run, console
    )

    if total == 0:
        console.print("[yellow]No summaries with text found.[/yellow]")
        return 0

    console.print()
    pct_updated = (updated_count / total * 100) if total else 0
    pct_unchanged = (unchanged_count / total * 100) if total else 0

    console.print(
        Panel.fit(
            f"Processed: {total:,}\nUpdated: {updated_count:,} ({pct_updated:.1f}%)\nUnchanged: {unchanged_count:,} ({pct_unchanged:.1f}%)",
            title="Recomputation complete" + (" (dry run)" if args.dry_run else ""),
            style="bold green",
        )
    )

    return 0


if __name__ == "__main__":
    sys.exit(main())
