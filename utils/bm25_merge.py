#!/usr/bin/env python3
"""Shared BM25/FTS5 helpers.

This centralizes the BM25 fallback logic used for merge decisions and
merge-report diagnostics.

The helpers assume an SQLite FTS5 virtual table named `summary_fts` exists with
`rowid` matching summaries/items ids.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Dict, List, Mapping, Sequence, Set


def fts_available(conn: sqlite3.Connection) -> bool:
    try:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='summary_fts'"
        ).fetchone()
        return bool(row)
    except Exception:
        return False


def bm25_match_query_from_tokens(tokens: Set[str], max_tokens: int) -> str:
    """Build a conservative FTS5 MATCH query from a token set."""
    max_n = 8
    try:
        max_n = int(max_tokens)
    except Exception:
        max_n = 8
    if max_n <= 0:
        max_n = 8

    if not tokens:
        return ""

    ranked = sorted(tokens, key=lambda t: (len(t), t), reverse=True)[:max_n]
    parts = [f"{t}*" for t in ranked if t]
    return " OR ".join(parts)


def bm25_match_query_row(row: Mapping[str, Any], max_tokens: int) -> str:
    title_tokens = row.get("title_tokens")
    summary_tokens = row.get("summary_tokens")
    tt = title_tokens if isinstance(title_tokens, set) else set()
    st = summary_tokens if isinstance(summary_tokens, set) else set()
    return bm25_match_query_from_tokens(set(tt) | set(st), max_tokens)


def bm25_candidates(
    conn: sqlite3.Connection,
    query_id: int,
    query_text: str,
    candidate_ids: Sequence[int],
    limit: int,
) -> Dict[str, Any]:
    if not candidate_ids:
        return {"self_score": None, "candidates": []}
    if not fts_available(conn):
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


def bm25_ratio_map_for_items(
    conn: sqlite3.Connection,
    items: Sequence[Mapping[str, Any]],
    bm25_max_tokens: int,
    bm25_limit: int,
) -> Dict[int, Dict[int, float]]:
    """Return ratios[a_id][b_id] = abs(score(b)) / abs(score(a_self))."""
    if not items or not fts_available(conn):
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
        query = bm25_match_query_row(row, bm25_max_tokens)
        if not query:
            continue
        candidate_ids = [x for x in ids if x != sid]
        resp = bm25_candidates(conn, sid, query, candidate_ids, bm25_limit)
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
