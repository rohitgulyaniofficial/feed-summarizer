#!/usr/bin/env python3
"""Shared merge policy for clustering/merging summaries.

This module centralizes the *policy* around when two summaries are eligible to be
considered similar enough to merge. Both runtime code (publisher) and
diagnostics tooling (tools/merge_report.py) should use these helpers so behavior
stays consistent.

The policy is intentionally conservative and uses:
- token overlap guardrails (cheap, prevents many accidental SimHash collisions)
- an adaptive per-pair SimHash threshold (+1 allowance for strong overlap)
- a canonical merge fingerprint computed from title + "\n" + summary

All functions are dependency-free beyond the project's existing utils.
"""

from __future__ import annotations

import re
from typing import Any, Mapping, Optional, Set, Tuple

from utils.simhash import compute_simhash


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

SUMMARY_STOPWORDS: Set[str] = TITLE_STOPWORDS | {
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


def _word_tokens(text: str) -> Tuple[str, ...]:
    return tuple(re.findall(r"[a-z0-9]+", (text or "").lower()))


def title_token_set_from_text(title: str) -> Set[str]:
    tokens = _word_tokens(title)
    return {t for t in tokens if len(t) >= 3 and t not in TITLE_STOPWORDS}


def summary_token_set_from_text(summary_text: str) -> Set[str]:
    tokens = _word_tokens(summary_text)
    return {t for t in tokens if len(t) >= 3 and t not in SUMMARY_STOPWORDS}


def is_high_signal_token(token: str) -> bool:
    if not token:
        return False
    if len(token) >= 8:
        return True
    if len(token) >= 5 and any(ch.isdigit() for ch in token):
        return True
    return False


def merge_text(title: str, summary_text: str) -> str:
    return f"{title or ''}\n{summary_text or ''}".strip()


def merge_text_from_row(row: Mapping[str, Any]) -> str:
    title = str(row.get("item_title") or row.get("title") or "")
    summary_text = str(row.get("summary_text") or "")
    return merge_text(title, summary_text)


def merge_fingerprint_from_text(title: str, summary_text: str) -> Optional[int]:
    fp = compute_simhash(merge_text(title, summary_text))
    return fp if isinstance(fp, int) else None


def merge_fingerprint_from_row(
    row: Mapping[str, Any],
    *,
    prefer_stored_merge_simhash: bool = True,
    fallback_to_legacy_simhash: bool = True,
) -> Optional[int]:
    if prefer_stored_merge_simhash:
        existing_merge = row.get("merge_simhash")
        if isinstance(existing_merge, int):
            return existing_merge

    fp = compute_simhash(merge_text_from_row(row))
    if isinstance(fp, int):
        return fp

    if fallback_to_legacy_simhash:
        existing = row.get("simhash")
        if isinstance(existing, int):
            return existing

    return None


def _title_tokens(row: Mapping[str, Any]) -> Set[str]:
    existing = row.get("title_tokens")
    if isinstance(existing, set):
        return existing
    return title_token_set_from_text(str(row.get("item_title") or row.get("title") or ""))


def _summary_tokens(row: Mapping[str, Any]) -> Set[str]:
    existing = row.get("summary_tokens")
    if isinstance(existing, set):
        return existing
    return summary_token_set_from_text(str(row.get("summary_text") or ""))


def should_merge_pair_rows(a: Mapping[str, Any], b: Mapping[str, Any]) -> bool:
    title_shared = _title_tokens(a) & _title_tokens(b)
    if len(title_shared) >= 2:
        return True

    summary_shared = _summary_tokens(a) & _summary_tokens(b)

    if len(title_shared) == 0:
        if len(summary_shared) >= 6:
            return True
        if len(summary_shared) >= 4 and any(is_high_signal_token(t) for t in summary_shared):
            return True
        return False

    token = next(iter(title_shared))
    if is_high_signal_token(token):
        return True

    return len(summary_shared) >= 2


def pair_merge_threshold_rows(a: Mapping[str, Any], b: Mapping[str, Any], base_threshold: int) -> int:
    thr = max(0, int(base_threshold))
    if thr <= 0:
        return 0

    title_shared = _title_tokens(a) & _title_tokens(b)
    if len(title_shared) >= 3:
        return thr + 1

    summary_shared = _summary_tokens(a) & _summary_tokens(b)
    if len(summary_shared) >= 5:
        return thr + 1

    return thr
