#!/usr/bin/env python3
"""Merge eligibility report for recent bulletins.

This inspects the *stored* bulletin membership in the SQLite DB and simulates
publisher-side merging logic (SimHash + guardrails, optionally BM25/FTS5 fallback)
within each bulletin session.

Usage:
  python -m tools.merge_report --db feeds.db --hours 24

Notes:
- This mirrors the merge guardrails in publisher.py:
        * require strong token overlap (title or summary), with a small exception for high-signal title tokens
        * topic is not used as an elimination criterion
- If `BM25_MERGE_ENABLED=true` and FTS5 is available, this report can also show
    whether BM25 fallback would merge a pair.
- This report does NOT rewrite the DB; it only estimates how many items would
    collapse at render time.
"""

from __future__ import annotations

import argparse
import os
import re
import sqlite3
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Sequence, Set, Tuple

from utils import compute_simhash, decode_int64, hamming_distance


TITLE_STOPWORDS: Set[str] = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "but",
    "by",
    "for",
    "from",
    "has",
    "have",
    "he",
    "her",
    "his",
    "i",
    "in",
    "is",
    "it",
    "its",
    "of",
    "on",
    "or",
    "our",
    "s",
    "she",
    "so",
    "that",
    "the",
    "their",
    "they",
    "this",
    "to",
    "was",
    "we",
    "were",
    "will",
    "with",
    "you",
}


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
    parser = argparse.ArgumentParser(description="Report which bulletin items would merge under simhash")
    parser.add_argument("--db", dest="db_path", default="feeds.db", help="Path to SQLite DB (default: feeds.db)")
    parser.add_argument("--hours", type=int, default=24, help="Look back this many hours (default: 24)")
    parser.add_argument(
        "--threshold",
        type=int,
        default=12,
        help="Max SimHash Hamming distance for merging (default: 12)",
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
    return parser.parse_args()


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
    """Return a nested map: ratios[a_id][b_id] = bm25_ratio.

    Ratio is computed as abs(score(b)) / abs(score(a_self)).
    """
    if not items or not _fts_available(conn):
        return {}

    ids = [int(it["id"]) for it in items if isinstance(it.get("id"), int)]
    if len(ids) < 2:
        return {}
    id_to_row = {int(it["id"]): it for it in items if isinstance(it.get("id"), int)}

    ratios: Dict[int, Dict[int, float]] = {}
    for sid in ids:
        row = id_to_row.get(sid)
        if not row:
            continue
        query = _bm25_match_query(row, bm25_max_tokens)
        if not query:
            continue
        candidate_ids = [x for x in ids if x != sid]
        resp = _bm25_candidates(conn, sid, query, candidate_ids, bm25_limit)
        self_score = resp.get("self_score") if isinstance(resp, dict) else None
        if not isinstance(self_score, (int, float)) or float(self_score) == 0.0:
            continue
        denom = abs(float(self_score))
        if denom <= 0:
            continue
        out: Dict[int, float] = {}
        for cand in (resp.get("candidates") or []) if isinstance(resp, dict) else []:
            try:
                cid = int(cand.get("id"))
                score = float(cand.get("score"))
            except Exception:
                continue
            ratio = abs(score) / denom
            if ratio > 1:
                ratio = 1.0
            out[cid] = ratio
        if out:
            ratios[sid] = out
    return ratios


def _norm_topic(value: Any) -> str:
    if isinstance(value, str):
        t = (value or "General").strip()
        return t or "General"
    return str(value or "General")


def _title_token_set(title: str) -> Set[str]:
    tokens = re.findall(r"[a-z0-9]+", (title or "").lower())
    return {t for t in tokens if len(t) >= 3 and t not in TITLE_STOPWORDS}


def _merge_fingerprint(title: str, summary_text: str) -> Optional[int]:
    combined = f"{title or ''}\n{summary_text or ''}".strip()
    fp = compute_simhash(combined)
    return fp if isinstance(fp, int) else None


def _summary_token_set(summary_text: str) -> Set[str]:
    tokens = re.findall(r"[a-z0-9]+", (summary_text or "").lower())
    if not tokens:
        return set()
    stopwords = TITLE_STOPWORDS | {
        "new",
        "news",
        "report",
        "reports",
        "reported",
        "update",
        "updates",
        "today",
        "yesterday",
        "tomorrow",
    }
    return {t for t in tokens if len(t) >= 3 and t not in stopwords}


def _is_high_signal_token(token: str) -> bool:
    if not token:
        return False
    if len(token) >= 8:
        return True
    if len(token) >= 5 and any(ch.isdigit() for ch in token):
        return True
    return False


def _should_merge_pair(a: SummaryRow, b: SummaryRow) -> bool:
    title_shared = (a.get("title_tokens") or set()) & (b.get("title_tokens") or set())
    if len(title_shared) >= 2:
        return True

    summary_shared = (a.get("summary_tokens") or set()) & (b.get("summary_tokens") or set())
    if len(summary_shared) >= 3:
        return True

    if len(title_shared) == 1:
        token = next(iter(title_shared))
        return _is_high_signal_token(token)

    return False


def _env_bool(name: str, default: bool) -> bool:
    value = os.environ.get(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return int(value)
    except Exception:
        return default


def _env_float(name: str, default: float) -> float:
    value = os.environ.get(name)
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _fts_available(conn: sqlite3.Connection) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='summary_fts'"
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def _bm25_match_query(row: SummaryRow, max_tokens: int) -> str:
    max_n = 8
    try:
        max_n = int(max_tokens)
    except Exception:
        max_n = 8
    if max_n <= 0:
        max_n = 8

    tokens = set((row.get("title_tokens") or set()) | (row.get("summary_tokens") or set()))
    if not tokens:
        return ""

    ranked = sorted(tokens, key=lambda t: (len(t), t), reverse=True)[:max_n]
    parts = [f"{t}*" for t in ranked if t]
    return " OR ".join(parts)


def _bm25_candidates(
    conn: sqlite3.Connection,
    query_id: int,
    query_text: str,
    candidate_ids: List[int],
    limit: int,
) -> Dict[str, Any]:
    if not candidate_ids:
        return {"self_score": None, "candidates": []}
    if not _fts_available(conn):
        return {"self_score": None, "candidates": []}

    q = (query_text or "").strip()
    if not q:
        return {"self_score": None, "candidates": []}

    self_row = conn.execute(
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
    sql = f"""
        SELECT rowid AS id, bm25(summary_fts) AS score
        FROM summary_fts
        WHERE summary_fts MATCH ?
          AND rowid IN ({placeholders})
        ORDER BY score
        LIMIT ?
    """
    rows = conn.execute(sql, [q, *[int(x) for x in candidate_ids], int(limit)]).fetchall()
    out: List[Dict[str, Any]] = []
    for r in rows:
        try:
            out.append({"id": int(r[0]), "score": float(r[1])})
        except Exception:
            continue
    return {"self_score": self_score, "candidates": out}


def load_recent_bulletin_summaries(conn: sqlite3.Connection, cutoff_epoch: int) -> Dict[BulletinKey, List[SummaryRow]]:
    conn.row_factory = sqlite3.Row
    has_merge = _summaries_has_merge_simhash(conn)
    if has_merge:
        sql = """
            SELECT b.group_name, b.session_key, b.created_date,
                   s.id AS summary_id, s.topic, s.summary_text, s.merge_simhash,
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
                   s.id AS summary_id, s.topic, s.summary_text,
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
        topic = _norm_topic(r["topic"])
        stored_fp = None
        if has_merge:
            stored_merge = r["merge_simhash"]
            stored_fp = decode_int64(stored_merge) if isinstance(stored_merge, int) else None
        row: SummaryRow = {
            "id": int(r["summary_id"]),
            "feed_slug": str(r["feed_slug"] or ""),
            "topic": topic,
            "title": title,
            "url": str(r["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": stored_fp if isinstance(stored_fp, int) else _merge_fingerprint(title, summary_text),
            "title_tokens": _title_token_set(title),
            "summary_tokens": _summary_token_set(summary_text),
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
    Falls back to computing SimHash from title + summary_text when missing.
    """
    conn.row_factory = sqlite3.Row
    needle = f"%{(keyword or '').strip()}%"
    has_merge = _summaries_has_merge_simhash(conn)
    select_fp = "s.merge_simhash AS merge_simhash" if has_merge else "NULL AS merge_simhash"
    sql = f"""
        SELECT s.id AS summary_id,
               {select_fp},
               s.topic AS topic,
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
            fp = _merge_fingerprint(title, summary_text)
        matches.append(
            {
                "id": int(r["summary_id"]),
                "feed_slug": str(r["feed_slug"] or ""),
                "topic": _norm_topic(r["topic"]),
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
               s.topic AS topic,
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
            fp = _merge_fingerprint(title, summary_text)

        row: SummaryRow = {
            "id": int(r["summary_id"]),
            "feed_slug": str(r["feed_slug"] or ""),
            "topic": _norm_topic(r["topic"]),
            "title": title,
            "url": str(r["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": fp,
            "generated_date": int(r["generated_date"] or 0),
            "title_tokens": _title_token_set(title),
            "summary_tokens": _summary_token_set(summary_text),
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
                   s.id AS summary_id, s.topic, s.summary_text, s.merge_simhash,
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
                   s.id AS summary_id, s.topic, s.summary_text,
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
        topic = _norm_topic(r["topic"])
        stored_fp = None
        if has_merge:
            stored_merge = r["merge_simhash"]
            stored_fp = decode_int64(stored_merge) if isinstance(stored_merge, int) else None
        row: SummaryRow = {
            "id": int(r["summary_id"]),
            "feed_slug": str(r["feed_slug"] or ""),
            "topic": topic,
            "title": title,
            "url": str(r["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": stored_fp if isinstance(stored_fp, int) else _merge_fingerprint(title, summary_text),
            "title_tokens": _title_token_set(title),
            "summary_tokens": _summary_token_set(summary_text),
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

    fts_ok = _fts_available(conn)
    if bm25_enabled:
        print(
            f"BM25 enabled: fts={fts_ok} ratio>={bm25_ratio_threshold:.2f} extra_dist={bm25_max_extra} query_tokens={bm25_max_tokens} topk={bm25_limit}"
        )
        if not fts_ok:
            print("BM25 note: summary_fts missing/unavailable; bm25 will show -/-")

    shown_pairs = 0
    for (group_name, session_key, created_date), items in sessions:
        bm25_ratios: Dict[int, Dict[int, float]] = {}
        if bm25_enabled:
            bm25_ratios = _bm25_ratio_map_for_items(conn, items, bm25_max_tokens, bm25_limit)

        # Compute pair distances and annotate why they do/don't merge.
        scored: List[Tuple[int, bool, int, Optional[float], Optional[float], bool, bool, SummaryRow, SummaryRow]] = []
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                a = items[i]
                b = items[j]
                dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
                dist_sort = int(dist) if dist is not None else 999
                overlap = len((a.get("title_tokens") or set()) & (b.get("title_tokens") or set()))
                eligible = _should_merge_pair(a, b)

                sid_a = a.get("id")
                sid_b = b.get("id")
                ra = None
                rb = None
                if isinstance(sid_a, int) and isinstance(sid_b, int):
                    ra = bm25_ratios.get(sid_a, {}).get(sid_b)
                    rb = bm25_ratios.get(sid_b, {}).get(sid_a)

                simhash_merge = bool(eligible and dist is not None and int(dist) <= int(threshold))

                bm25_applicable = bool(dist is None or int(dist) <= int(threshold) + int(bm25_max_extra))
                bm25_merge = bool(
                    bm25_enabled
                    and bm25_applicable
                    and isinstance(ra, float)
                    and isinstance(rb, float)
                    and ra >= bm25_ratio_threshold
                    and rb >= bm25_ratio_threshold
                )

                would_merge = bool(simhash_merge or bm25_merge)
                scored.append((dist_sort, eligible, overlap, ra, rb, simhash_merge, would_merge, a, b))

        if not scored:
            continue

        scored.sort(key=lambda t: (t[0], -t[2]))
        # Keep output small: show up to N closest pairs per session.
        keep_n = int(per_session_pairs)
        if keep_n <= 0:
            keep_n = 2
        keep = scored[:keep_n]

        print(f"- {group_name}/{session_key} matches={len(items)}")
        for dist_sort, eligible, overlap, ra, rb, simhash_merge, would_merge, a, b in keep:
            dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
            dist_str = "n/a" if dist is None else f"{int(dist):2d}"
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
                f"    dist={dist_str} overlap={overlap} topic=({a.get('topic')} / {b.get('topic')}) eligible={eligible} simhash={simhash_merge} bm25={bm25_str} -> merge={would_merge}"
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

    fts_ok = _fts_available(conn)
    print(f"\nKeyword-in-bulletin cluster report for '{keyword}' (threshold={threshold})")
    print(f"Sessions with >=2 matches: {len(sessions)}")
    if bm25_enabled:
        print(
            f"BM25 enabled: fts={fts_ok} ratio>={bm25_ratio_threshold:.2f} extra_dist={bm25_max_extra} query_tokens={bm25_max_tokens} topk={bm25_limit}"
        )
        if not fts_ok:
            print("BM25 note: summary_fts missing/unavailable; bm25 merges disabled for clusters")

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
        candidates = [it for it in items if isinstance(it.get("merge_fp"), int) and isinstance(it.get("id"), int)]
        if len(candidates) < 2:
            continue

        ids = [int(it["id"]) for it in candidates]
        parent: Dict[int, int] = {sid: sid for sid in ids}
        index: Dict[int, int] = {sid: idx for idx, sid in enumerate(ids)}

        def find(x: int) -> int:
            while parent[x] != x:
                parent[x] = parent[parent[x]]
                x = parent[x]
            return x

        def union(a: int, b: int) -> None:
            ra, rb = find(a), find(b)
            if ra == rb:
                return
            if index.get(ra, 0) <= index.get(rb, 0):
                parent[rb] = ra
            else:
                parent[ra] = rb

        bm25_ratios: Dict[int, Dict[int, float]] = {}
        if bm25_enabled and fts_ok:
            bm25_ratios = _bm25_ratio_map_for_items(conn, candidates, bm25_max_tokens, bm25_limit)

        for i in range(len(candidates)):
            for j in range(i + 1, len(candidates)):
                a = candidates[i]
                b = candidates[j]
                if not _should_merge_pair(a, b):
                    continue

                dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
                simhash_merge = bool(dist is not None and int(dist) <= int(threshold))

                would_merge = simhash_merge
                if not would_merge and bm25_enabled:
                    bm25_applicable = bool(dist is None or int(dist) <= int(threshold) + int(bm25_max_extra))
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
                    union(int(a["id"]), int(b["id"]))

        groups: Dict[int, List[SummaryRow]] = defaultdict(list)
        for it in candidates:
            groups[find(int(it["id"]))].append(it)
        clusters = [g for g in groups.values() if len(g) >= min_size]
        if not clusters:
            continue

        clusters.sort(key=lambda g: -len(g))
        print(f"- {group_name}/{session_key} matches={len(items)} clusters={len(clusters)}")
        for cluster in clusters:
            cluster_sorted = sorted(cluster, key=lambda r: (str(r.get("feed_slug") or ""), int(r.get("id") or 0)))
            max_d = _max_pairwise_distance(cluster_sorted)
            topic = str(cluster_sorted[0].get("topic") or "General")
            print(f"    cluster size={len(cluster_sorted)} topic={topic} max_dist={max_d}")
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

    fts_ok = _fts_available(conn)
    if bm25_enabled:
        print(
            f"BM25 enabled: fts={fts_ok} ratio>={bm25_ratio_threshold:.2f} extra_dist={bm25_max_extra} query_tokens={bm25_max_tokens} topk={bm25_limit}"
        )
        if not fts_ok:
            print("BM25 note: summary_fts missing/unavailable; bm25 merges disabled")

    ids = [int(it["id"]) for it in candidates if isinstance(it.get("id"), int)]
    parent: Dict[int, int] = {sid: sid for sid in ids}
    index: Dict[int, int] = {sid: idx for idx, sid in enumerate(ids)}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if index.get(ra, 0) <= index.get(rb, 0):
            parent[rb] = ra
        else:
            parent[ra] = rb

    bm25_ratios: Dict[int, Dict[int, float]] = {}
    if bm25_enabled and fts_ok:
        bm25_ratios = _bm25_ratio_map_for_items(conn, candidates, bm25_max_tokens, bm25_limit)

    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a = candidates[i]
            b = candidates[j]
            if not _should_merge_pair(a, b):
                continue
            dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
            simhash_merge = bool(dist is not None and int(dist) <= int(threshold))
            would_merge = simhash_merge
            if not would_merge and bm25_enabled:
                bm25_applicable = bool(dist is None or int(dist) <= int(threshold) + int(bm25_max_extra))
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
                union(int(a["id"]), int(b["id"]))

    groups: Dict[int, List[SummaryRow]] = defaultdict(list)
    for it in candidates:
        groups[find(int(it["id"]))].append(it)

    min_size = int(min_cluster_size)
    if min_size <= 1:
        min_size = 2
    clusters = [g for g in groups.values() if len(g) >= min_size]
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
        topic = str(cluster_sorted[0].get("topic") or "General")
        print(f"- cluster size={len(cluster_sorted)} topic={topic} max_dist={max_d}")
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
            f"- dist={dist:2d}  #{a.get('id')} [{a.get('feed_slug')}] ({a.get('topic')})  |  #{b.get('id')} [{b.get('feed_slug')}] ({b.get('topic')})"
        )
        print(f"    A: {ta}")
        print(f"    B: {tb}")


def _build_clusters(items: Sequence[SummaryRow], threshold: int) -> List[List[SummaryRow]]:
    candidates = [it for it in items if isinstance(it.get("merge_fp"), int)]
    if len(candidates) < 2:
        return []

    ids = [int(it["id"]) for it in candidates]
    parent: Dict[int, int] = {sid: sid for sid in ids}
    index: Dict[int, int] = {sid: idx for idx, sid in enumerate(ids)}

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra == rb:
            return
        if index.get(ra, 0) <= index.get(rb, 0):
            parent[rb] = ra
        else:
            parent[ra] = rb

    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a = candidates[i]
            b = candidates[j]
            if not _should_merge_pair(a, b):
                continue
            dist = hamming_distance(a.get("merge_fp"), b.get("merge_fp"))
            if dist is not None and dist <= threshold:
                union(int(a["id"]), int(b["id"]))

    groups: Dict[int, List[SummaryRow]] = defaultdict(list)
    for it in candidates:
        groups[find(int(it["id"]))].append(it)

    return [g for g in groups.values() if len(g) > 1]


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
    cutoff = int(time.time()) - int(args.hours) * 3600

    conn = sqlite3.connect(args.db_path)

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
        clusters = _build_clusters(items, threshold)
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

    print(f"Merge eligibility report (last {args.hours}h, threshold={threshold})")
    print(f"Bulletins scanned: {total_bulletins}")
    print(f"Items before: {total_before}")
    print(f"Items after (if merged at render time): {total_after}")
    print(f"Net reduction: {total_before - total_after}")
    print(f"Merged clusters: {total_clusters} (total merged items involved: {total_merged_items})")

    print("\nTop sessions by reduction:")
    shown = 0
    keyword = (args.keyword or "").strip().lower()

    for reduction, key, before, after, clusters in session_rows:
        group_name, session_key, _created = key
        print(f"- {group_name}/{session_key}  {before} -> {after}  (reduction={reduction}, clusters={len(clusters)})")

        # Print examples (up to 2 clusters), optionally filtered by keyword.
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

            topic = str(cluster_sorted[0].get("topic") or "General")
            print(f"    cluster size={len(cluster_sorted)} topic={topic} max_dist={max_d}")
            for r in cluster_sorted:
                title = (str(r.get("title") or "")).strip().replace("\n", " ")
                if len(title) > 110:
                    title = title[:107] + "..."
                print(f"      - [{r.get('feed_slug')}] #{r.get('id')} {title}")
            examples += 1

        shown += 1
        if shown >= int(args.top):
            break

    if keyword:
        print(f"\n(Examples filtered by keyword: {args.keyword})")


if __name__ == "__main__":
    main()
