#!/usr/bin/env python3
"""SimHash distance report for recent summaries.

Usage:
  python -m tools.simhash_report [--days N] [--limit M] [--threshold K]

Defaults:
  days: 7 (look back this many days)
  limit: 1000 (max summaries to inspect)
  threshold: 32 (max Hamming distance to include in histogram)

The script prints a histogram of pairwise Hamming distances up to the
specified threshold, across all feeds, for summaries generated within
the given time window.
"""

from __future__ import annotations

import argparse
import sqlite3
import time
from collections import Counter
from pathlib import Path
from typing import List, Tuple


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="SimHash distance report for recent summaries")
    parser.add_argument("--db", dest="db_path", default="feeds.db", help="Path to SQLite database (default: feeds.db)")
    parser.add_argument("--days", type=int, default=7, help="Look back this many days (default: 7)")
    parser.add_argument("--limit", type=int, default=1000, help="Maximum number of summaries to inspect (default: 1000)")
    parser.add_argument(
        "--threshold",
        type=int,
        default=32,
        help="Maximum Hamming distance to include in histogram (default: 32)",
    )
    return parser.parse_args()


def hamming(a: int, b: int) -> int:
    diff = (a ^ b) & ((1 << 64) - 1)
    return diff.bit_count()


def load_recent_simhashes(conn: sqlite3.Connection, days: int, limit: int) -> List[Tuple[int, int]]:
    cutoff = int(time.time()) - days * 24 * 3600
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.simhash
        FROM summaries s
        WHERE s.simhash IS NOT NULL AND s.generated_date >= ?
        ORDER BY s.generated_date DESC
        LIMIT ?
        """,
        (cutoff, limit),
    )
    return cur.fetchall()


def build_histogram(rows: List[Tuple[int, int]], threshold: int) -> Counter:
    hist: Counter = Counter()
    n = len(rows)
    for i in range(n):
        _, sim_a = rows[i]
        for j in range(i + 1, n):
            _, sim_b = rows[j]
            d = hamming(sim_a, sim_b)
            if d <= threshold:
                hist[d] += 1
    return hist


def main() -> None:
    args = parse_args()

    db_path = Path(args.db_path)
    if not db_path.exists():
        raise SystemExit(f"Database not found: {db_path}")

    conn = sqlite3.connect(db_path)

    rows = load_recent_simhashes(conn, args.days, args.limit)
    total = len(rows)
    print(f"Loaded {total} summaries with non-NULL simhash from last {args.days} days (limit {args.limit}).")

    if total < 2:
        print("Not enough summaries to compute pairwise distances.")
        return

    print(f"Computing pairwise Hamming distances (threshold <= {args.threshold})...")
    hist = build_histogram(rows, args.threshold)

    if not hist:
        print("No pairs found within the specified threshold.")
        return

    print("\nDistance histogram (distance: count):")
    for dist in sorted(hist):
        print(f"{dist:3d}: {hist[dist]}")


if __name__ == "__main__":
    main()
