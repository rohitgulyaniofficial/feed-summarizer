#!/usr/bin/env python3
"""Merge eligibility report for recent bulletins.

This inspects the *stored* bulletin membership in the SQLite DB and simulates
publisher-side merging logic (SimHash + guardrails, optionally BM25/FTS5 fallback)
within each bulletin session.

Usage:
  python -m tools.merge_report --db feeds.db --hours 24

Notes:
- This mirrors the merge guardrails in workers/publisher.py:
        * require strong token overlap (title or summary), with a small exception for high-signal title tokens
- If `BM25_MERGE_ENABLED=true` and FTS5 is available, this report can also show
    whether BM25 fallback would merge a pair.
    collapse at render time.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

# Suppress config logging noise (must be set before importing config)
os.environ.setdefault("LOG_LEVEL", "ERROR")  # noqa: E402
logging.getLogger("FeedProcessor").setLevel(logging.ERROR)

from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.text import Text  # noqa: E402

from utils.hashed_cosine import build_hashed_tf_vector, cosine_similarity  # noqa: E402
from utils.clustering import cluster_indices  # noqa: E402
from utils.merge_policy import (  # noqa: E402
    merge_fingerprint_from_text,
    pair_merge_threshold_rows,
    should_merge_pair_rows,
    summary_token_set_from_text,
    title_token_set_from_text,
)
from utils.bm25_merge import (  # noqa: E402
    bm25_candidates as shared_bm25_candidates,
    bm25_ratio_map_for_items as shared_bm25_ratio_map_for_items,
    fts_available as shared_fts_available,
)
from utils import decode_int64, hamming_distance  # noqa: E402
from tools.merge_env import _env_bool, _env_int, _env_float, _hashed_cosine_env_settings  # noqa: E402
from tools.standard_args import (  # noqa: E402
    create_standard_parser,
    compute_lookback,
)

console = Console()
BulletinKey = Tuple[str, str, int]
SummaryRow = Dict[str, Any]


def _summaries_has_merge_simhash(conn: sqlite3.Connection) -> bool:
    try:
        rows = conn.execute("PRAGMA table_info(summaries)").fetchall()
        # PRAGMA table_info returns: cid, name, type, notnull, dflt_value, pk
        names = {r[1] for r in rows}
        return "merge_simhash" in names
    except Exception:
        return False


def parse_args() -> argparse.Namespace:
    parser = create_standard_parser(
        description="Report which bulletin items would merge under simhash",
        with_db=True,
        with_verbosity=False,
        with_time_window=True,
        with_threshold=True,
    )
    parser.add_argument(
        "--linkage",
        choices=["single", "complete"],
        default=os.environ.get("SIMHASH_MERGE_LINKAGE", "complete").strip().lower() or "complete",
        help="Clustering linkage: single (union-find) or complete (all pairs within threshold). Default: complete",
    )
    parser.add_argument("--top", type=int, default=20, help="Show top N sessions by reduction (default: 20)")
    parser.add_argument(
        "--keyword",
        type=str,
        default="",
        help="Optional keyword to filter example clusters (case-insensitive)",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="",
        help="Keyword to search in recent items/summaries and print closest simhash pairs (case-insensitive)",
    )
    parser.add_argument(
        "--query-any",
        type=str,
        default="",
        help="Additional query terms (comma-separated). Matches if any term matches.",
    )
    parser.add_argument(
        "--query-regex",
        action="store_true",
        help="Treat --query/--query-any as regex (OR'ed) and match in Python (slower).",
    )
    parser.add_argument(
        "--query-scope",
        choices=["bulletin", "global"],
        default="bulletin",
        help="For --query: search within bulletin sessions or across all recent summaries (default: bulletin)",
    )
    parser.add_argument(
        "--query-mode",
        choices=["pairs", "clusters"],
        default="pairs",
        help="For --query: show closest pairs or merge clusters within bulletin sessions (default: pairs)",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=2,
        help="For cluster modes: only print clusters with at least this many items (default: 2)",
    )
    parser.add_argument(
        "--max-clusters",
        type=int,
        default=20,
        help="For cluster modes: print at most this many clusters (default: 20)",
    )
    parser.add_argument(
        "--max-items-per-cluster",
        type=int,
        default=15,
        help="For cluster modes: print at most this many items per cluster (default: 15)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=30,
        help="Max matching summaries to consider for --query (default: 30)",
    )
    parser.add_argument(
        "--pairs",
        type=int,
        default=30,
        help="How many closest pairs to print for --query (default: 30)",
    )

    parser.add_argument(
        "--per-session-pairs",
        type=int,
        default=2,
        help="For --query bulletin output, show up to N closest pairs per session (default: 2)",
    )

    parser.add_argument(
        "--bm25-enabled",
        type=str,
        default="",
        help="Override BM25 merge fallback for reports: true/false (default: use env BM25_MERGE_ENABLED)",
    )
    parser.add_argument(
        "--bm25-max-extra-distance",
        type=int,
        default=-1,
        help="Override BM25 max extra SimHash distance (default: use env BM25_MERGE_MAX_EXTRA_DISTANCE)",
    )
    parser.add_argument(
        "--bm25-ratio-threshold",
        type=float,
        default=-1.0,
        help="Override BM25 mutual ratio threshold in [0,1] (default: use env BM25_MERGE_RATIO_THRESHOLD)",
    )
    parser.add_argument(
        "--bm25-max-query-tokens",
        type=int,
        default=-1,
        help="Override BM25 max query tokens (default: use env BM25_MERGE_MAX_QUERY_TOKENS)",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format (machine-readable)",
    )
    parser.add_argument(
        "--no-summaries",
        action="store_true",
        help="Hide summary text in cluster examples (show titles only)",
    )
    return parser.parse_args()


def _cluster_candidates(
    items: Sequence[SummaryRow],
    threshold: int,
    linkage: str,
    eligible: Any,
    fp_field: str = "merge_fp",
) -> List[List[SummaryRow]]:
    """Cluster items by SimHash distance under the requested linkage.

    Returns only clusters with size >= 2.
    """
    candidates = [
        it
        for it in items
        if isinstance(it.get("id"), int) and isinstance(it.get(fp_field), int)
    ]
    if len(candidates) < 2:
        return []
    if int(threshold) <= 0:
        return []

    def get_dist(i: int, j: int) -> Optional[int]:
        a = candidates[i].get(fp_field)
        b = candidates[j].get(fp_field)
        if not isinstance(a, int) or not isinstance(b, int):
            return None
        return hamming_distance(a, b)

    def get_thr(i: int, j: int) -> Optional[int]:
        a = candidates[i]
        b = candidates[j]
        if not eligible(a, b):
            return None
        return int(pair_merge_threshold_rows(a, b, int(threshold)))

    linkage_norm = str(linkage or "complete").strip().lower()
    clusters_idx = cluster_indices(len(candidates), linkage_norm, get_dist, get_thr)
    return [[candidates[i] for i in cluster] for cluster in clusters_idx]


def _parse_bool_override(value: str) -> Optional[bool]:
    v = (value or "").strip().lower()
    if not v:
        return None
    if v in {"1", "true", "yes", "on"}:
        return True
    if v in {"0", "false", "no", "off"}:
        return False
    return None


def _split_query_terms(query: str, query_any: str) -> List[str]:
    terms: List[str] = []
    q = (query or "").strip()
    if q:
        terms.append(q)
    extra = (query_any or "").strip()
    if extra:
        for part in extra.split(","):
            t = (part or "").strip()
            if t:
                terms.append(t)
    # Deduplicate while preserving order
    seen: Set[str] = set()
    out: List[str] = []
    for t in terms:
        key = t.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(t)
    return out


def _build_query_matcher(terms: Sequence[str], use_regex: bool) -> Tuple[str, Any]:
    """Return (label, predicate(text)->bool)."""
    cleaned = [t for t in terms if (t or "").strip()]
    if not cleaned:
        return ("", lambda _text: False)

    if use_regex:
        pattern = "|".join([f"(?:{t})" for t in cleaned])
        rx = re.compile(pattern, re.IGNORECASE)
        return (pattern, lambda text: bool(rx.search(text or "")))

    lowered = [t.lower() for t in cleaned]
    return (" OR ".join(cleaned), lambda text: any(t in (text or "").lower() for t in lowered))


def _bm25_ratio_map_for_items(
    conn: sqlite3.Connection,
    items: Sequence[SummaryRow],
    bm25_max_tokens: int,
    bm25_limit: int,
) -> Dict[int, Dict[int, float]]:
    return shared_bm25_ratio_map_for_items(conn, items, bm25_max_tokens, bm25_limit)


def _bm25_candidates(
    conn: sqlite3.Connection,
    query_id: int,
    query_text: str,
    candidate_ids: List[int],
    limit: int,
) -> Dict[str, Any]:
    return shared_bm25_candidates(conn, query_id, query_text, candidate_ids, limit)


def load_recent_bulletin_summaries(conn: sqlite3.Connection, cutoff_epoch: int) -> Dict[BulletinKey, List[SummaryRow]]:
    conn.row_factory = sqlite3.Row
    has_merge = _summaries_has_merge_simhash(conn)
    if has_merge:
        sql = """
            SELECT b.group_name, b.session_key, b.created_date,
                   s.id AS summary_id, s.summary_text, s.merge_simhash,
                   i.title AS item_title, i.url AS item_url,
                   f.slug AS feed_slug
            FROM bulletins b
            JOIN bulletin_summaries bs ON bs.bulletin_id = b.id
            JOIN summaries s ON s.id = bs.summary_id
            JOIN items i ON i.id = s.id
            JOIN feeds f ON f.id = i.feed_id
            WHERE b.created_date >= ?
              AND s.summary_text IS NOT NULL AND s.summary_text != ''
            ORDER BY b.created_date DESC
        """
    else:
        sql = """
            SELECT b.group_name, b.session_key, b.created_date,
                   s.id AS summary_id, s.summary_text,
                   i.title AS item_title, i.url AS item_url,
                   f.slug AS feed_slug
            FROM bulletins b
            JOIN bulletin_summaries bs ON bs.bulletin_id = b.id
            JOIN summaries s ON s.id = bs.summary_id
            JOIN items i ON i.id = s.id
            JOIN feeds f ON f.id = i.feed_id
            WHERE b.created_date >= ?
              AND s.summary_text IS NOT NULL AND s.summary_text != ''
            ORDER BY b.created_date DESC
        """

    rows = conn.execute(sql, (cutoff_epoch,)).fetchall()

    by_bulletin: Dict[BulletinKey, List[SummaryRow]] = defaultdict(list)
    for r in rows:
        title = str(r["item_title"] or "")
        summary_text = str(r["summary_text"] or "")
        stored_fp = None
        if has_merge:
            stored_merge = r["merge_simhash"]
            stored_fp = decode_int64(stored_merge) if isinstance(stored_merge, int) else None
        row: SummaryRow = {
            "id": int(r["summary_id"]),
            "feed_slug": str(r["feed_slug"] or ""),
            "title": title,
            "url": str(r["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": stored_fp if isinstance(stored_fp, int) else merge_fingerprint_from_text(title, summary_text),
            "title_tokens": title_token_set_from_text(title),
            "summary_tokens": summary_token_set_from_text(summary_text),
        }
        key: BulletinKey = (
            str(r["group_name"]),
            str(r["session_key"]),
            int(r["created_date"]),
        )
        by_bulletin[key].append(row)

    return by_bulletin


def load_recent_keyword_matches(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    keyword: str,
    limit: int,
) -> List[SummaryRow]:
    """Load recent summaries whose title or summary_text match the keyword.

    Uses the stored summaries.merge_simhash when available (decoded to unsigned 64-bit).
    Falls back to computing SimHash from summary_text when missing.
    """
    conn.row_factory = sqlite3.Row
    needle = f"%{(keyword or '').strip()}%"
    has_merge = _summaries_has_merge_simhash(conn)
    select_fp = "s.merge_simhash AS merge_simhash" if has_merge else "NULL AS merge_simhash"
    sql = f"""
         SELECT s.id AS summary_id,
             {select_fp},
             s.summary_text AS summary_text,
             s.generated_date AS generated_date,
             i.title AS item_title,
             i.url AS item_url,
             f.slug AS feed_slug
        FROM summaries s
        JOIN items i ON i.id = s.id
        JOIN feeds f ON f.id = i.feed_id
        WHERE s.generated_date >= ?
          AND s.summary_text IS NOT NULL AND s.summary_text != ''
          AND (i.title LIKE ? OR s.summary_text LIKE ?)
        ORDER BY s.generated_date DESC
        LIMIT ?
    """

    rows = conn.execute(sql, (cutoff_epoch, needle, needle, int(limit))).fetchall()

    matches: List[SummaryRow] = []
    for r in rows:
        title = str(r["item_title"] or "")
        summary_text = str(r["summary_text"] or "")
        stored = r["merge_simhash"]
        fp: Optional[int]
        if isinstance(stored, int):
            fp = decode_int64(stored)
        else:
            fp = merge_fingerprint_from_text(title, summary_text)
        matches.append(
            {
                "id": int(r["summary_id"]),
                "feed_slug": str(r["feed_slug"] or ""),
                "title": title,
                "url": str(r["item_url"] or ""),
                "summary_text": summary_text,
                "merge_fp": fp,
                "generated_date": int(r["generated_date"] or 0),
            }
        )
    return matches


def load_recent_keyword_matches_detailed(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    terms: Sequence[str],
    limit: int,
    use_regex: bool,
) -> List[SummaryRow]:
    """Load recent summaries matching any term.

    If use_regex=True, this loads a larger recent window and filters in Python.
    Rows include title/summary token sets for merge guardrails.
    """
    conn.row_factory = sqlite3.Row
    has_merge = _summaries_has_merge_simhash(conn)
    select_fp = "s.merge_simhash AS merge_simhash" if has_merge else "NULL AS merge_simhash"

    matcher_label, matcher = _build_query_matcher(terms, use_regex)
    if not matcher_label:
        return []

    # For regex matching we cannot efficiently filter in SQL, so we oversample.
    oversample = max(1000, int(limit) * 50)
    if oversample > 20000:
        oversample = 20000

    params: List[Any] = [int(cutoff_epoch)]
    where_extra = ""
    if not use_regex:
        # SQL LIKE filtering for each term.
        likes: List[str] = []
        for t in terms:
            likes.append("i.title LIKE ?")
            likes.append("s.summary_text LIKE ?")
            needle = f"%{t.strip()}%"
            params.append(needle)
            params.append(needle)
        where_extra = " AND (" + " OR ".join(likes) + ")"

    sql = f"""
         SELECT s.id AS summary_id,
             {select_fp},
             s.summary_text AS summary_text,
             s.generated_date AS generated_date,
             i.title AS item_title,
             i.url AS item_url,
             f.slug AS feed_slug
        FROM summaries s
        JOIN items i ON i.id = s.id
        JOIN feeds f ON f.id = i.feed_id
        WHERE s.generated_date >= ?
          AND s.summary_text IS NOT NULL AND s.summary_text != ''
          {where_extra}
        ORDER BY s.generated_date DESC
        LIMIT ?
    """
    params.append(int(limit) if not use_regex else int(oversample))

    rows = conn.execute(sql, params).fetchall()
    out: List[SummaryRow] = []
    for r in rows:
        title = str(r["item_title"] or "")
        summary_text = str(r["summary_text"] or "")

        if use_regex:
            blob = f"{title}\n{summary_text}"
            if not matcher(blob):
                continue

        stored = r["merge_simhash"]
        fp: Optional[int]
        if isinstance(stored, int):
            fp = decode_int64(stored)
        else:
            fp = merge_fingerprint_from_text(title, summary_text)

        row: SummaryRow = {
            "id": int(r["summary_id"]),
            "feed_slug": str(r["feed_slug"] or ""),
            "title": title,
            "url": str(r["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": fp,
            "generated_date": int(r["generated_date"] or 0),
            "title_tokens": title_token_set_from_text(title),
            "summary_tokens": summary_token_set_from_text(summary_text),
        }
        out.append(row)
        if len(out) >= int(limit):
            break
    return out


def load_recent_bulletin_keyword_matches(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    keyword: str,
    limit: int,
) -> Dict[BulletinKey, List[SummaryRow]]:
    """Load recent bulletin membership rows that match a keyword.

    Returns a mapping keyed by (group_name, session_key, created_date).
    """
    conn.row_factory = sqlite3.Row
    needle = f"%{(keyword or '').strip()}%"
    has_merge = _summaries_has_merge_simhash(conn)
    if has_merge:
                sql = """
                        SELECT b.group_name, b.session_key, b.created_date,
                                     s.id AS summary_id, s.summary_text, s.merge_simhash,
                                     i.title AS item_title, i.url AS item_url,
                                     f.slug AS feed_slug
                        FROM bulletins b
                        JOIN bulletin_summaries bs ON bs.bulletin_id = b.id
                        JOIN summaries s ON s.id = bs.summary_id
                        JOIN items i ON i.id = s.id
                        JOIN feeds f ON f.id = i.feed_id
                        WHERE b.created_date >= ?
                            AND s.summary_text IS NOT NULL AND s.summary_text != ''
                            AND (i.title LIKE ? OR s.summary_text LIKE ?)
                        ORDER BY b.created_date DESC
                        LIMIT ?
                """
    else:
                sql = """
                        SELECT b.group_name, b.session_key, b.created_date,
                                     s.id AS summary_id, s.summary_text,
                                     i.title AS item_title, i.url AS item_url,
                                     f.slug AS feed_slug
                        FROM bulletins b
                        JOIN bulletin_summaries bs ON bs.bulletin_id = b.id
                        JOIN summaries s ON s.id = bs.summary_id
                        JOIN items i ON i.id = s.id
                        JOIN feeds f ON f.id = i.feed_id
                        WHERE b.created_date >= ?
                            AND s.summary_text IS NOT NULL AND s.summary_text != ''
                            AND (i.title LIKE ? OR s.summary_text LIKE ?)
                        ORDER BY b.created_date DESC
                        LIMIT ?
                """

    rows = conn.execute(sql, (cutoff_epoch, needle, needle, int(limit))).fetchall()

    by_bulletin: Dict[BulletinKey, List[SummaryRow]] = defaultdict(list)
    for r in rows:
        title = str(r["item_title"] or "")
        summary_text = str(r["summary_text"] or "")
        stored_fp = None
        if has_merge:
            stored_merge = r["merge_simhash"]
            stored_fp = decode_int64(stored_merge) if isinstance(stored_merge, int) else None
        row: SummaryRow = {
            "id": int(r["summary_id"]),
            "feed_slug": str(r["feed_slug"] or ""),
            "title": title,
            "url": str(r["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": stored_fp if isinstance(stored_fp, int) else merge_fingerprint_from_text(title, summary_text),
            "title_tokens": title_token_set_from_text(title),
            "summary_tokens": summary_token_set_from_text(summary_text),
        }
        key: BulletinKey = (
            str(r["group_name"]),
            str(r["session_key"]),
            int(r["created_date"]),
        )
        by_bulletin[key].append(row)

    return by_bulletin


def print_keyword_bulletin_pair_report(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    keyword: str,
    threshold: int,
    limit: int,
    pairs: int,
    per_session_pairs: int,
    bm25_enabled_override: Optional[bool],
    bm25_max_extra_override: Optional[int],
    bm25_ratio_threshold_override: Optional[float],
    bm25_max_query_tokens_override: Optional[int],
) -> None:
    keyword = (keyword or "").strip()
    if not keyword:
        print("No keyword provided for --query")
        return

    by_bulletin = load_recent_bulletin_keyword_matches(conn, cutoff_epoch, keyword, limit)
    if not by_bulletin:
        print(f"No bulletin items matched keyword '{keyword}'")
        return

    # Only show sessions with at least 2 matching items.
    sessions = [(k, v) for k, v in by_bulletin.items() if len(v) >= 2]
    sessions.sort(key=lambda kv: (-kv[0][2], kv[0][0], kv[0][1]))

    print(f"\nKeyword-in-bulletin report for '{keyword}' (threshold={threshold})")
    print(f"Sessions with >=2 matches: {len(sessions)}")

    bm25_enabled = bm25_enabled_override if bm25_enabled_override is not None else _env_bool("BM25_MERGE_ENABLED", False)
    bm25_ratio_threshold = (
        float(bm25_ratio_threshold_override)
        if isinstance(bm25_ratio_threshold_override, (int, float))
        else _env_float("BM25_MERGE_RATIO_THRESHOLD", 0.80)
    )
    if bm25_ratio_threshold < 0:
        bm25_ratio_threshold = 0.0
    if bm25_ratio_threshold > 1:
        bm25_ratio_threshold = 1.0
    bm25_max_extra = (
        int(bm25_max_extra_override)
        if isinstance(bm25_max_extra_override, int) and int(bm25_max_extra_override) >= 0
        else _env_int("BM25_MERGE_MAX_EXTRA_DISTANCE", 6)
    )
    if bm25_max_extra < 0:
        bm25_max_extra = 0
    bm25_max_tokens = (
        int(bm25_max_query_tokens_override)
        if isinstance(bm25_max_query_tokens_override, int) and int(bm25_max_query_tokens_override) > 0
        else _env_int("BM25_MERGE_MAX_QUERY_TOKENS", 8)
    )
    if bm25_max_tokens <= 0:
        bm25_max_tokens = 8
    bm25_limit = 10

    fts_ok = shared_fts_available(conn)
    if bm25_enabled:
        print(
            f"BM25 enabled: fts={fts_ok} ratio>={bm25_ratio_threshold:.2f} extra_dist={bm25_max_extra} query_tokens={bm25_max_tokens} topk={bm25_limit}"
        )
        if not fts_ok:
            print("BM25 note: summary_fts missing/unavailable; bm25 will show -/-")

    cosine_enabled, cosine_min, cosine_buckets, cosine_max_tokens = _hashed_cosine_env_settings()
    if cosine_enabled:
        print(f"Hashed cosine enabled: min_sim>={cosine_min:.2f} buckets={cosine_buckets} max_tokens={cosine_max_tokens}")

    shown_pairs = 0
    for (group_name, session_key, created_date), items in sessions:
        hashed_vectors: List[Tuple[Dict[int, float], float]] = []
        if cosine_enabled:
            for row in items:
                title = (row.get("title") or "")
                body = (row.get("summary_text") or "")
                text = f"{title}\n{body}".strip()
                hashed_vectors.append(build_hashed_tf_vector(text, buckets=cosine_buckets, max_tokens=cosine_max_tokens))

        bm25_ratios: Dict[int, Dict[int, float]] = {}
        if bm25_enabled:
            bm25_ratios = _bm25_ratio_map_for_items(conn, items, bm25_max_tokens, bm25_limit)

        # Compute pair distances and annotate why they do/don't merge.
        scored: List[Tuple[int, bool, int, Optional[float], Optional[float], Optional[float], bool, bool, SummaryRow, SummaryRow]] = []
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a = items[i]
                b = items[j]
                dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
                dist_sort = int(dist) if dist is not None else 999
                overlap = len((a.get("title_tokens") or set()) & (b.get("title_tokens") or set()))
                eligible = should_merge_pair_rows(a, b)

                cos_val: Optional[float] = None
                cos_ok = True
                if cosine_enabled:
                    cos_val = cosine_similarity(hashed_vectors[i], hashed_vectors[j])
                    cos_ok = bool(cos_val >= cosine_min)

                sid_a = a.get("id")
                sid_b = b.get("id")
                ra = None
                rb = None
                if isinstance(sid_a, int) and isinstance(sid_b, int):
                    ra = bm25_ratios.get(sid_a, {}).get(sid_b)
                    rb = bm25_ratios.get(sid_b, {}).get(sid_a)

                pair_thr = pair_merge_threshold_rows(a, b, threshold) if eligible else int(threshold)
                simhash_merge = bool(eligible and dist is not None and int(dist) <= int(pair_thr) and cos_ok)

                bm25_applicable = bool(dist is None or int(dist) <= int(pair_thr) + int(bm25_max_extra))
                bm25_merge = bool(
                    bm25_enabled
                    and bm25_applicable
                    and isinstance(ra, float)
                    and isinstance(rb, float)
                    and ra >= bm25_ratio_threshold
                    and rb >= bm25_ratio_threshold
                    and cos_ok
                )

                would_merge = bool(simhash_merge or bm25_merge)
                scored.append((dist_sort, eligible, overlap, ra, rb, cos_val, simhash_merge, would_merge, a, b))

        if not scored:
            continue

        scored.sort(key=lambda t: (t[0], -t[2]))
        # Keep output small: show up to N closest pairs per session.
        keep_n = int(per_session_pairs)
        if keep_n <= 0:
            keep_n = 2
        keep = scored[:keep_n]

        print(f"- {group_name}/{session_key} matches={len(items)}")
        for dist_sort, eligible, overlap, ra, rb, cos_val, simhash_merge, would_merge, a, b in keep:
            dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
            dist_str = "n/a" if dist is None else f"{int(dist):2d}"
            cos_str = "-"
            if cosine_enabled and isinstance(cos_val, (int, float)):
                cos_str = f"{float(cos_val):.2f}"
            bm25_str = "-"
            if bm25_enabled:
                a_str = f"{ra:.2f}" if isinstance(ra, float) else "-"
                b_str = f"{rb:.2f}" if isinstance(rb, float) else "-"
                bm25_str = f"{a_str}/{b_str}"
            ta = (str(a.get("title") or "")).strip().replace("\n", " ")
            tb = (str(b.get("title") or "")).strip().replace("\n", " ")
            if len(ta) > 90:
                ta = ta[:87] + "..."
            if len(tb) > 90:
                tb = tb[:87] + "..."
            print(
                f"    dist={dist_str} cos={cos_str} overlap={overlap} eligible={eligible} simhash={simhash_merge} bm25={bm25_str} -> merge={would_merge}"
            )
            print(f"      A: [{a.get('feed_slug')}] #{a.get('id')} {ta}")
            print(f"      B: [{b.get('feed_slug')}] #{b.get('id')} {tb}")
            shown_pairs += 1
            if shown_pairs >= int(pairs):
                return


def print_keyword_bulletin_cluster_report(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    keyword: str,
    threshold: int,
    linkage: str,
    limit: int,
    min_cluster_size: int,
    max_clusters_to_show: int,
    max_items_per_cluster_show: int,
    bm25_enabled_override: Optional[bool],
    bm25_max_extra_override: Optional[int],
    bm25_ratio_threshold_override: Optional[float],
    bm25_max_query_tokens_override: Optional[int],
) -> None:
    """Report merge clusters for keyword matches within bulletin sessions."""
    keyword = (keyword or "").strip()
    if not keyword:
        print("No keyword provided for --query")
        return

    by_bulletin = load_recent_bulletin_keyword_matches(conn, cutoff_epoch, keyword, limit)
    if not by_bulletin:
        print(f"No bulletin items matched keyword '{keyword}'")
        return

    # Only sessions with at least 2 matching items can form clusters.
    sessions = [(k, v) for k, v in by_bulletin.items() if len(v) >= 2]
    sessions.sort(key=lambda kv: (-kv[0][2], kv[0][0], kv[0][1]))

    bm25_enabled = bm25_enabled_override if bm25_enabled_override is not None else _env_bool("BM25_MERGE_ENABLED", False)
    bm25_ratio_threshold = (
        float(bm25_ratio_threshold_override)
        if isinstance(bm25_ratio_threshold_override, (int, float))
        else _env_float("BM25_MERGE_RATIO_THRESHOLD", 0.80)
    )
    if bm25_ratio_threshold < 0:
        bm25_ratio_threshold = 0.0
    if bm25_ratio_threshold > 1:
        bm25_ratio_threshold = 1.0

    bm25_max_extra = (
        int(bm25_max_extra_override)
        if isinstance(bm25_max_extra_override, int) and int(bm25_max_extra_override) >= 0
        else _env_int("BM25_MERGE_MAX_EXTRA_DISTANCE", 6)
    )
    if bm25_max_extra < 0:
        bm25_max_extra = 0

    bm25_max_tokens = (
        int(bm25_max_query_tokens_override)
        if isinstance(bm25_max_query_tokens_override, int) and int(bm25_max_query_tokens_override) > 0
        else _env_int("BM25_MERGE_MAX_QUERY_TOKENS", 8)
    )
    if bm25_max_tokens <= 0:
        bm25_max_tokens = 8
    bm25_limit = 10

    fts_ok = shared_fts_available(conn)
    print(f"\nKeyword-in-bulletin cluster report for '{keyword}' (threshold={threshold})")
    print(f"Sessions with >=2 matches: {len(sessions)}")
    if bm25_enabled:
        print(
            f"BM25 enabled: fts={fts_ok} ratio>={bm25_ratio_threshold:.2f} extra_dist={bm25_max_extra} query_tokens={bm25_max_tokens} topk={bm25_limit}"
        )
        if not fts_ok:
            print("BM25 note: summary_fts missing/unavailable; bm25 merges disabled for clusters")

    cosine_enabled, cosine_min, cosine_buckets, cosine_max_tokens = _hashed_cosine_env_settings()
    if cosine_enabled:
        print(f"Hashed cosine enabled: min_sim>={cosine_min:.2f} buckets={cosine_buckets} max_tokens={cosine_max_tokens}")

    printed = 0
    min_size = int(min_cluster_size)
    if min_size <= 1:
        min_size = 2
    max_clusters = int(max_clusters_to_show)
    if max_clusters <= 0:
        max_clusters = 20
    max_items_per_cluster = int(max_items_per_cluster_show)
    if max_items_per_cluster <= 0:
        max_items_per_cluster = 15

    for (group_name, session_key, created_date), items in sessions:
        vec_by_id: Dict[int, Tuple[Dict[int, float], float]] = {}
        if cosine_enabled:
            for row in items:
                sid = row.get("id")
                if not isinstance(sid, int):
                    continue
                title = (row.get("title") or "")
                body = (row.get("summary_text") or "")
                text = f"{title}\n{body}".strip()
                vec_by_id[sid] = build_hashed_tf_vector(text, buckets=cosine_buckets, max_tokens=cosine_max_tokens)

        def eligible_with_cos(a: SummaryRow, b: SummaryRow) -> bool:
            if not should_merge_pair_rows(a, b):
                return False
            if not cosine_enabled:
                return True
            sid_a = a.get("id")
            sid_b = b.get("id")
            if not isinstance(sid_a, int) or not isinstance(sid_b, int):
                return False
            va = vec_by_id.get(sid_a)
            vb = vec_by_id.get(sid_b)
            if va is None or vb is None:
                return False
            return cosine_similarity(va, vb) >= cosine_min

        if str(linkage or "complete").strip().lower() == "complete":
            clusters_all = _cluster_candidates(
                items,
                int(threshold),
                "complete",
                eligible_with_cos,
                fp_field="merge_fp",
            )
            clusters = [c for c in clusters_all if len(c) >= min_size]
            if not clusters:
                continue
            clusters.sort(key=lambda g: -len(g))
            print(f"- {group_name}/{session_key} matches={len(items)} clusters={len(clusters)}")
            for cluster in clusters:
                cluster_sorted = sorted(cluster, key=lambda r: (str(r.get('feed_slug') or ''), int(r.get('id') or 0)))
                max_d = _max_pairwise_distance(cluster_sorted)
                print(f"    cluster size={len(cluster_sorted)} max_dist={max_d}")
                for r in cluster_sorted[:max_items_per_cluster]:
                    title = (str(r.get('title') or '')).strip().replace("\n", " ")
                    if len(title) > 110:
                        title = title[:107] + "..."
                    print(f"      - [{r.get('feed_slug')}] #{r.get('id')} {title}")
                printed += 1
                if printed >= max_clusters:
                    return
            continue

        candidates = [it for it in items if isinstance(it.get("merge_fp"), int) and isinstance(it.get("id"), int)]
        if len(candidates) < 2:
            continue

        bm25_ratios: Dict[int, Dict[int, float]] = {}
        if bm25_enabled and fts_ok:
            bm25_ratios = _bm25_ratio_map_for_items(conn, candidates, bm25_max_tokens, bm25_limit)

        extra_edges: Set[Tuple[int, int]] = set()

        for i in range(len(candidates)):
            for j in range(i + 1, len(candidates)):
                a = candidates[i]
                b = candidates[j]
                if not eligible_with_cos(a, b):
                    continue

                dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
                pair_thr = pair_merge_threshold_rows(a, b, threshold)
                simhash_merge = bool(dist is not None and int(dist) <= int(pair_thr))

                would_merge = simhash_merge
                if not would_merge and bm25_enabled:
                    bm25_applicable = bool(dist is None or int(dist) <= int(pair_thr) + int(bm25_max_extra))
                    if bm25_applicable:
                        sid_a = int(a["id"])
                        sid_b = int(b["id"])
                        ra = bm25_ratios.get(sid_a, {}).get(sid_b)
                        rb = bm25_ratios.get(sid_b, {}).get(sid_a)
                        would_merge = bool(
                            isinstance(ra, float)
                            and isinstance(rb, float)
                            and ra >= bm25_ratio_threshold
                            and rb >= bm25_ratio_threshold
                        )
                if would_merge:
                    extra_edges.add((i, j))

        def get_dist(i: int, j: int) -> Optional[int]:
            a, b = (i, j) if i < j else (j, i)
            return 0 if (a, b) in extra_edges else None

        def get_thr(i: int, j: int) -> Optional[int]:
            a, b = (i, j) if i < j else (j, i)
            return 0 if (a, b) in extra_edges else None

        clusters_idx = cluster_indices(len(candidates), "single", get_dist, get_thr)
        clusters = [[candidates[i] for i in cl] for cl in clusters_idx if len(cl) >= min_size]
        if not clusters:
            continue

        clusters.sort(key=lambda g: -len(g))
        print(f"- {group_name}/{session_key} matches={len(items)} clusters={len(clusters)}")
        for cluster in clusters:
            cluster_sorted = sorted(cluster, key=lambda r: (str(r.get("feed_slug") or ""), int(r.get("id") or 0)))
            max_d = _max_pairwise_distance(cluster_sorted)
            print(f"    cluster size={len(cluster_sorted)} max_dist={max_d}")
            for r in cluster_sorted[:max_items_per_cluster]:
                title = (str(r.get("title") or "")).strip().replace("\n", " ")
                if len(title) > 110:
                    title = title[:107] + "..."
                print(f"      - [{r.get('feed_slug')}] #{r.get('id')} {title}")
            printed += 1
            if printed >= max_clusters:
                return


def print_keyword_global_cluster_report(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    query: str,
    query_any: str,
    use_regex: bool,
    threshold: int,
    linkage: str,
    limit: int,
    min_cluster_size: int,
    max_clusters_to_show: int,
    max_items_per_cluster_show: int,
    bm25_enabled_override: Optional[bool],
    bm25_max_extra_override: Optional[int],
    bm25_ratio_threshold_override: Optional[float],
    bm25_max_query_tokens_override: Optional[int],
) -> None:
    terms = _split_query_terms(query, query_any)
    matcher_label, _matcher = _build_query_matcher(terms, use_regex)
    if not matcher_label:
        print("No keyword provided for --query")
        return

    matches = load_recent_keyword_matches_detailed(conn, cutoff_epoch, terms, limit, use_regex)
    candidates = [m for m in matches if isinstance(m.get("merge_fp"), int)]
    hours = int((time.time() - cutoff_epoch) // 3600)

    print(f"\nGlobal merge cluster report for query '{matcher_label}' (last {hours}h, threshold={threshold})")
    print(f"Matches loaded: {len(matches)} (with simhash: {len(candidates)})")
    if len(candidates) < 2:
        return

    cosine_enabled, cosine_min, cosine_buckets, cosine_max_tokens = _hashed_cosine_env_settings()
    vec_by_id: Dict[int, Tuple[Dict[int, float], float]] = {}
    if cosine_enabled:
        print(f"Hashed cosine enabled: min_sim>={cosine_min:.2f} buckets={cosine_buckets} max_tokens={cosine_max_tokens}")
        for row in candidates:
            sid = row.get("id")
            if not isinstance(sid, int):
                continue
            title = (row.get("title") or "")
            body = (row.get("summary_text") or "")
            text = f"{title}\n{body}".strip()
            vec_by_id[sid] = build_hashed_tf_vector(text, buckets=cosine_buckets, max_tokens=cosine_max_tokens)

    def eligible_with_cos(a: SummaryRow, b: SummaryRow) -> bool:
        if not should_merge_pair_rows(a, b):
            return False
        if not cosine_enabled:
            return True
        sid_a = a.get("id")
        sid_b = b.get("id")
        if not isinstance(sid_a, int) or not isinstance(sid_b, int):
            return False
        va = vec_by_id.get(sid_a)
        vb = vec_by_id.get(sid_b)
        if va is None or vb is None:
            return False
        return cosine_similarity(va, vb) >= cosine_min

    linkage_norm = str(linkage or "complete").strip().lower()
    if linkage_norm == "complete":
        # Conservative mode: pure SimHash complete-linkage clustering, no transitive bridging.
        clusters_all = _cluster_candidates(
            candidates,
            int(threshold),
            "complete",
            eligible_with_cos,
            fp_field="merge_fp",
        )

        min_size = int(min_cluster_size)
        if min_size <= 1:
            min_size = 2
        clusters = [c for c in clusters_all if len(c) >= min_size]
        clusters.sort(key=lambda g: (-len(g), -max(int(x.get("generated_date") or 0) for x in g)))
        print(f"Clusters found: {len(clusters)} (min_size={min_size}, linkage=complete, bm25=skipped)")
        if not clusters:
            return

        max_clusters = int(max_clusters_to_show)
        if max_clusters <= 0:
            max_clusters = 20
        max_items_per_cluster = int(max_items_per_cluster_show)
        if max_items_per_cluster <= 0:
            max_items_per_cluster = 15

        shown = 0
        for cluster in clusters:
            cluster_sorted = sorted(cluster, key=lambda r: (str(r.get("feed_slug") or ""), int(r.get("id") or 0)))
            max_d = _max_pairwise_distance(cluster_sorted)
            print(f"- cluster size={len(cluster_sorted)} max_dist={max_d}")
            for r in cluster_sorted[:max_items_per_cluster]:
                title = (str(r.get("title") or "")).strip().replace("\n", " ")
                if len(title) > 110:
                    title = title[:107] + "..."
                print(f"  - [{r.get('feed_slug')}] #{r.get('id')} {title}")
            shown += 1
            if shown >= max_clusters:
                break
        return

    bm25_enabled = bm25_enabled_override if bm25_enabled_override is not None else _env_bool("BM25_MERGE_ENABLED", False)
    bm25_ratio_threshold = (
        float(bm25_ratio_threshold_override)
        if isinstance(bm25_ratio_threshold_override, (int, float))
        else _env_float("BM25_MERGE_RATIO_THRESHOLD", 0.80)
    )
    if bm25_ratio_threshold < 0:
        bm25_ratio_threshold = 0.0
    if bm25_ratio_threshold > 1:
        bm25_ratio_threshold = 1.0

    bm25_max_extra = (
        int(bm25_max_extra_override)
        if isinstance(bm25_max_extra_override, int) and int(bm25_max_extra_override) >= 0
        else _env_int("BM25_MERGE_MAX_EXTRA_DISTANCE", 6)
    )
    if bm25_max_extra < 0:
        bm25_max_extra = 0

    bm25_max_tokens = (
        int(bm25_max_query_tokens_override)
        if isinstance(bm25_max_query_tokens_override, int) and int(bm25_max_query_tokens_override) > 0
        else _env_int("BM25_MERGE_MAX_QUERY_TOKENS", 8)
    )
    if bm25_max_tokens <= 0:
        bm25_max_tokens = 8
    bm25_limit = 10

    fts_ok = shared_fts_available(conn)
    if bm25_enabled:
        print(
            f"BM25 enabled: fts={fts_ok} ratio>={bm25_ratio_threshold:.2f} extra_dist={bm25_max_extra} query_tokens={bm25_max_tokens} topk={bm25_limit}"
        )
        if not fts_ok:
            print("BM25 note: summary_fts missing/unavailable; bm25 merges disabled")

    bm25_ratios: Dict[int, Dict[int, float]] = {}
    if bm25_enabled and fts_ok:
        bm25_ratios = _bm25_ratio_map_for_items(conn, candidates, bm25_max_tokens, bm25_limit)

    extra_edges: Set[Tuple[int, int]] = set()

    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a = candidates[i]
            b = candidates[j]
            if not eligible_with_cos(a, b):
                continue
            dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
            pair_thr = pair_merge_threshold_rows(a, b, threshold)
            simhash_merge = bool(dist is not None and int(dist) <= int(pair_thr))
            would_merge = simhash_merge
            if not would_merge and bm25_enabled:
                bm25_applicable = bool(dist is None or int(dist) <= int(pair_thr) + int(bm25_max_extra))
                if bm25_applicable:
                    sid_a = int(a["id"])
                    sid_b = int(b["id"])
                    ra = bm25_ratios.get(sid_a, {}).get(sid_b)
                    rb = bm25_ratios.get(sid_b, {}).get(sid_a)
                    would_merge = bool(
                        isinstance(ra, float)
                        and isinstance(rb, float)
                        and ra >= bm25_ratio_threshold
                        and rb >= bm25_ratio_threshold
                    )
            if would_merge:
                extra_edges.add((i, j))

    def get_dist(i: int, j: int) -> Optional[int]:
        a, b = (i, j) if i < j else (j, i)
        return 0 if (a, b) in extra_edges else None

    def get_thr(i: int, j: int) -> Optional[int]:
        a, b = (i, j) if i < j else (j, i)
        return 0 if (a, b) in extra_edges else None

    clusters_idx = cluster_indices(len(candidates), "single", get_dist, get_thr)
    clusters = [[candidates[i] for i in cl] for cl in clusters_idx]

    min_size = int(min_cluster_size)
    if min_size <= 1:
        min_size = 2
    clusters = [g for g in clusters if len(g) >= min_size]
    clusters.sort(key=lambda g: (-len(g), -max(int(x.get("generated_date") or 0) for x in g)))
    print(f"Clusters found: {len(clusters)} (min_size={min_size})")
    if not clusters:
        return

    max_clusters = int(max_clusters_to_show)
    if max_clusters <= 0:
        max_clusters = 20
    max_items_per_cluster = int(max_items_per_cluster_show)
    if max_items_per_cluster <= 0:
        max_items_per_cluster = 15

    shown = 0
    for cluster in clusters:
        cluster_sorted = sorted(cluster, key=lambda r: (str(r.get("feed_slug") or ""), int(r.get("id") or 0)))
        max_d = _max_pairwise_distance(cluster_sorted)
        print(f"- cluster size={len(cluster_sorted)} max_dist={max_d}")
        for r in cluster_sorted[:max_items_per_cluster]:
            title = (str(r.get("title") or "")).strip().replace("\n", " ")
            if len(title) > 110:
                title = title[:107] + "..."
            print(f"  - [{r.get('feed_slug')}] #{r.get('id')} {title}")
        shown += 1
        if shown >= max_clusters:
            break


def print_keyword_distance_report(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    keyword: str,
    limit: int,
    pairs: int,
) -> None:
    keyword = (keyword or "").strip()
    if not keyword:
        print("No keyword provided for --query")
        return

    matches = load_recent_keyword_matches(conn, cutoff_epoch, keyword, limit)
    candidates = [m for m in matches if isinstance(m.get("merge_fp"), int)]

    print(f"SimHash distance report for keyword '{keyword}' (last {int((time.time() - cutoff_epoch) // 3600)}h)")
    print(f"Matches loaded: {len(matches)} (with simhash: {len(candidates)})")
    if len(candidates) < 2:
        return

    scored: List[Tuple[int, SummaryRow, SummaryRow]] = []
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a = candidates[i]
            b = candidates[j]
            d = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
            if d is None:
                continue
            scored.append((int(d), a, b))

    if not scored:
        print("No pairwise distances computed (missing fingerprints)")
        return

    scored.sort(key=lambda t: (t[0], -max(int(t[1].get("generated_date") or 0), int(t[2].get("generated_date") or 0))))
    show = max(0, min(int(pairs), len(scored)))
    print(f"Closest pairs (showing {show} of {len(scored)}):")
    for dist, a, b in scored[:show]:
        ta = (str(a.get("title") or "").strip().replace("\n", " "))
        tb = (str(b.get("title") or "").strip().replace("\n", " "))
        if len(ta) > 80:
            ta = ta[:77] + "..."
        if len(tb) > 80:
            tb = tb[:77] + "..."
        print(
            f"- dist={dist:2d}  #{a.get('id')} [{a.get('feed_slug')}]  |  #{b.get('id')} [{b.get('feed_slug')}]"
        )
        print(f"    A: {ta}")
        print(f"    B: {tb}")


def _build_clusters(items: Sequence[SummaryRow], threshold: int, linkage: str) -> List[List[SummaryRow]]:
    linkage_norm = str(linkage or "complete").strip().lower()
    return _cluster_candidates(
        items,
        int(threshold),
        linkage_norm,
        should_merge_pair_rows,
        fp_field="merge_fp",
    )


def _max_pairwise_distance(group: Sequence[SummaryRow]) -> int:
    max_d = 0
    for i in range(len(group)):
        for j in range(i + 1, len(group)):
            a = group[i].get("merge_fp")
            b = group[j].get("merge_fp")
            if not isinstance(a, int) or not isinstance(b, int):
                continue
            d = hamming_distance(a, b)
            if d is not None and d > max_d:
                max_d = d
    return max_d


def main() -> None:
    args = parse_args()
    threshold = max(0, int(args.threshold))
    
    # Compute cutoff using shared helper
    lookback_hours, lookback_label = compute_lookback(args)
    cutoff = int(time.time()) - lookback_hours * 3600

    conn = sqlite3.connect(args.db)

    query = (args.query or "").strip()
    if query:
        bm25_enabled_override = _parse_bool_override(str(args.bm25_enabled))
        bm25_max_extra_override: Optional[int] = None
        if isinstance(getattr(args, "bm25_max_extra_distance", -1), int) and int(args.bm25_max_extra_distance) >= 0:
            bm25_max_extra_override = int(args.bm25_max_extra_distance)

        bm25_ratio_threshold_override: Optional[float] = None
        try:
            ratio_value = float(getattr(args, "bm25_ratio_threshold", -1.0))
        except Exception:
            ratio_value = -1.0
        if ratio_value >= 0.0:
            bm25_ratio_threshold_override = ratio_value

        bm25_max_query_tokens_override: Optional[int] = None
        if isinstance(getattr(args, "bm25_max_query_tokens", -1), int) and int(args.bm25_max_query_tokens) > 0:
            bm25_max_query_tokens_override = int(args.bm25_max_query_tokens)

        mode = (args.query_mode or "pairs").strip().lower()
        scope = (args.query_scope or "bulletin").strip().lower()
        min_cluster_size = int(getattr(args, "min_cluster_size", 2) or 2)
        max_clusters_to_show = int(getattr(args, "max_clusters", 20) or 20)
        max_items_per_cluster_show = int(getattr(args, "max_items_per_cluster", 15) or 15)

        if mode == "clusters":
            if scope == "global":
                print_keyword_global_cluster_report(
                    conn,
                    cutoff,
                    query,
                    str(args.query_any or ""),
                    bool(getattr(args, "query_regex", False)),
                    threshold,
                    args.linkage,
                    int(args.limit),
                    min_cluster_size,
                    max_clusters_to_show,
                    max_items_per_cluster_show,
                    bm25_enabled_override,
                    bm25_max_extra_override,
                    bm25_ratio_threshold_override,
                    bm25_max_query_tokens_override,
                )
            else:
                print_keyword_bulletin_cluster_report(
                    conn,
                    cutoff,
                    query,
                    threshold,
                    args.linkage,
                    int(args.limit),
                    min_cluster_size,
                    max_clusters_to_show,
                    max_items_per_cluster_show,
                    bm25_enabled_override,
                    bm25_max_extra_override,
                    bm25_ratio_threshold_override,
                    bm25_max_query_tokens_override,
                )
        else:
            print_keyword_distance_report(conn, cutoff, query, int(args.limit), int(args.pairs))
            print_keyword_bulletin_pair_report(
                conn,
                cutoff,
                query,
                threshold,
                int(args.limit),
                int(args.pairs),
                int(args.per_session_pairs),
                bm25_enabled_override,
                bm25_max_extra_override,
                bm25_ratio_threshold_override,
                bm25_max_query_tokens_override,
            )
        return

    by_bulletin = load_recent_bulletin_summaries(conn, cutoff)

    total_bulletins = len(by_bulletin)
    total_before = sum(len(items) for items in by_bulletin.values())

    session_rows: List[Tuple[int, BulletinKey, int, int, List[List[SummaryRow]]]] = []
    total_after = 0
    total_clusters = 0
    total_merged_items = 0

    for key, items in by_bulletin.items():
        before = len(items)
        clusters = _build_clusters(items, threshold, args.linkage)
        cluster_count = len(clusters)
        merged_items = sum(len(c) for c in clusters)
        after = before - (merged_items - cluster_count)

        total_after += after
        total_clusters += cluster_count
        total_merged_items += merged_items

        reduction = before - after
        if reduction > 0:
            session_rows.append((reduction, key, before, after, clusters))

    # BulletinKey is a plain tuple: (group_name, session_key, created_date)
    session_rows.sort(key=lambda t: (-t[0], t[1][0], t[1][1]))

    reduction_pct = ((total_before - total_after) / total_before * 100) if total_before > 0 else 0

    # JSON output mode
    if getattr(args, "json", False):
        keyword = (args.keyword or "").strip().lower()
        output = {
            "lookback": lookback_label,
            "threshold": threshold,
            "summary": {
                "bulletins_scanned": total_bulletins,
                "items_before": total_before,
                "items_after": total_after,
                "net_reduction": total_before - total_after,
                "reduction_percent": round(reduction_pct, 2),
                "merged_clusters": total_clusters,
                "merged_items": total_merged_items,
            },
            "sessions": [],
        }
        for reduction, key, before, after, clusters in session_rows[:int(args.top)]:
            group_name, session_key, _created = key
            session_data = {
                "session": f"{group_name}/{session_key}",
                "before": before,
                "after": after,
                "reduction": reduction,
                "clusters": [],
            }
            for cluster in sorted(clusters, key=lambda c: -len(c)):
                cluster_sorted = sorted(cluster, key=lambda r: (str(r.get("feed_slug") or ""), int(r.get("id") or 0)))
                if keyword:
                    blob = " ".join([str(r.get("title") or "") for r in cluster_sorted]).lower()
                    if keyword not in blob:
                        continue
                max_d = _max_pairwise_distance(cluster_sorted)
                session_data["clusters"].append({
                    "size": len(cluster_sorted),
                    "max_distance": max_d,
                    "items": [
                        {"id": r.get("id"), "feed": r.get("feed_slug"), "title": str(r.get("title") or "").strip()}
                        for r in cluster_sorted
                    ],
                })
            output["sessions"].append(session_data)
        print(json.dumps(output, indent=2))
        return

    # Rich formatted header
    console.print(Panel.fit(
        f"Merge Eligibility Report (last {lookback_label})",
        style="bold cyan"
    ))

    # Summary table
    summary_table = Table(show_header=False, box=None, padding=(0, 2))
    summary_table.add_column("Metric", style="dim")
    summary_table.add_column("Value", style="bold")
    
    summary_table.add_row("Threshold", str(threshold))
    summary_table.add_row("Bulletins scanned", str(total_bulletins))
    summary_table.add_row("Items before merge", str(total_before))
    summary_table.add_row("Items after merge", str(total_after))
    summary_table.add_row("Net reduction", f"{total_before - total_after} ({reduction_pct:.1f}%)")
    summary_table.add_row("Merged clusters", f"{total_clusters} ({total_merged_items} items)")
    
    console.print(summary_table)
    console.print()

    # Sessions table
    if session_rows:
        console.print(Panel.fit("Top Sessions by Reduction", style="bold green"))
        
        sessions_table = Table(show_header=True, header_style="bold")
        sessions_table.add_column("Session", style="cyan")
        sessions_table.add_column("Before", justify="right")
        sessions_table.add_column("After", justify="right")
        sessions_table.add_column("Δ", justify="right", style="green")
        sessions_table.add_column("Clusters", justify="right")
        
        shown = 0
        keyword = (args.keyword or "").strip().lower()
        
        for reduction, key, before, after, clusters in session_rows:
            group_name, session_key, _created = key
            sessions_table.add_row(
                f"{group_name}/{session_key}",
                str(before),
                str(after),
                f"-{reduction}",
                str(len(clusters))
            )
            shown += 1
            if shown >= int(args.top):
                break
        
        console.print(sessions_table)
        console.print()

        # Detailed cluster examples
        console.print(Panel.fit("Merge Cluster Examples", style="bold yellow"))
        
        shown = 0
        for reduction, key, before, after, clusters in session_rows:
            group_name, session_key, _created = key
            
            examples = 0
            for cluster in sorted(clusters, key=lambda c: -len(c)):
                if examples >= 2:
                    break
                cluster_sorted = sorted(cluster, key=lambda r: (str(r.get("feed_slug") or ""), int(r.get("id") or 0)))
                max_d = _max_pairwise_distance(cluster_sorted)

                if keyword:
                    blob = " ".join([str(r.get("title") or "") for r in cluster_sorted]).lower()
                    if keyword not in blob:
                        continue

                # Create cluster display
                cluster_text = Text()
                cluster_text.append(f"{group_name}/{session_key}", style="cyan")
                cluster_text.append(f"  size={len(cluster_sorted)} max_dist={max_d}", style="dim")
                console.print(cluster_text)
                
                for r in cluster_sorted:
                    title = (str(r.get("title") or "")).strip().replace("\n", " ")
                    if len(title) > 90:
                        title = title[:87] + "..."
                    feed = r.get('feed_slug', 'unknown')
                    console.print(f"  [dim]•[/dim] [magenta]\\[{feed}][/magenta] {title}")
                    
                    # Show summary by default (unless --no-summaries)
                    if not getattr(args, 'no_summaries', False):
                        summary = (str(r.get("summary_text") or "")).strip().replace("\n", " ")
                        if summary:
                            # Wrap summary text for readability
                            if len(summary) > 200:
                                summary = summary[:197] + "..."
                            console.print(f"    [dim italic]{summary}[/dim italic]")
                
                console.print()
                examples += 1

            shown += 1
            if shown >= min(int(args.top), 10):  # Limit detailed examples
                break

        if keyword:
            console.print(f"\n[dim](Examples filtered by keyword: {args.keyword})[/dim]")
    else:
        console.print("[green]No merge candidates found - all items are unique![/green]")


if __name__ == "__main__":
    main()
