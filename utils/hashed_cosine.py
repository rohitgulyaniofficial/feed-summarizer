#!/usr/bin/env python3
"""Fast CPU text similarity via hashed TF vectors + cosine.

This implements a lightweight "embedding-like" similarity using feature hashing.
It is:
- dependency-free (stdlib only)
- deterministic across runs (blake2b)
- fast enough for O(n^2) comparisons in bulletin-sized batches

Intended use:
- As an additional *confirmation* gate for merges:
  e.g. merge only if SimHash is within threshold AND cosine >= min_sim.

It is not meant to be a full semantic embedding.
"""

from __future__ import annotations

from collections import Counter
from hashlib import blake2b
from math import sqrt
import re
from typing import Dict, Iterable, Optional, Tuple


HashedVector = Tuple[Dict[int, float], float]


_DEFAULT_STOPWORDS = {
    "the",
    "and",
    "or",
    "but",
    "a",
    "an",
    "of",
    "to",
    "in",
    "on",
    "for",
    "with",
    "by",
    "from",
    "at",
    "as",
    "is",
    "are",
    "was",
    "were",
    "be",
    "been",
    "it",
    "its",
    "that",
    "this",
    "these",
    "those",
    "their",
    "they",
    "he",
    "she",
    "we",
    "you",
    "i",
    "his",
    "her",
    "our",
    "us",
    "will",
    "would",
    "can",
    "could",
    "may",
    "might",
    "should",
    "about",
    "after",
    "before",
    "over",
    "under",
    "into",
    "out",
    "up",
    "down",
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
    "year",
    "years",
    "month",
    "months",
}


def _tokenize(text: str) -> Iterable[str]:
    return re.findall(r"[a-z0-9]+", text.lower())


def _hash_bucket(token: str, buckets: int) -> int:
    digest = blake2b(token.encode("utf-8"), digest_size=8).digest()
    value = int.from_bytes(digest, "big")
    return value % buckets


def build_hashed_tf_vector(
    text: Optional[str],
    *,
    buckets: int = 65536,
    max_tokens: int = 128,
    stopwords: Optional[set[str]] = None,
) -> HashedVector:
    """Build a hashed TF vector.

    Returns (sparse_map, L2_norm). If input is empty, returns ({}, 0.0).
    """
    if not text:
        return ({}, 0.0)

    bucket_count = int(buckets)
    if bucket_count <= 0:
        bucket_count = 65536

    token_cap = int(max_tokens)
    if token_cap <= 0:
        token_cap = 128

    sw = stopwords if stopwords is not None else _DEFAULT_STOPWORDS

    tokens = [t for t in _tokenize(text) if len(t) > 2 and t not in sw]
    if not tokens:
        return ({}, 0.0)

    freq = Counter(tokens)
    if len(freq) > token_cap:
        freq = Counter(dict(freq.most_common(token_cap)))

    vec: Dict[int, float] = {}
    for token, weight in freq.items():
        idx = _hash_bucket(token, bucket_count)
        vec[idx] = vec.get(idx, 0.0) + float(weight)

    norm = sqrt(sum(v * v for v in vec.values()))
    if norm <= 0:
        return ({}, 0.0)

    return (vec, float(norm))


def cosine_similarity(a: HashedVector, b: HashedVector) -> float:
    """Cosine similarity between two hashed TF vectors."""
    vec_a, norm_a = a
    vec_b, norm_b = b
    if norm_a <= 0 or norm_b <= 0:
        return 0.0

    if len(vec_a) > len(vec_b):
        vec_a, vec_b = vec_b, vec_a
        norm_a, norm_b = norm_b, norm_a

    dot = 0.0
    for idx, val in vec_a.items():
        other = vec_b.get(idx)
        if other is not None:
            dot += val * other

    return float(dot / (norm_a * norm_b))


def hashed_cosine_similarity(
    text_a: Optional[str],
    text_b: Optional[str],
    *,
    buckets: int = 65536,
    max_tokens: int = 128,
    stopwords: Optional[set[str]] = None,
) -> float:
    """Convenience wrapper: vectorize both texts and compute cosine."""
    vec_a = build_hashed_tf_vector(text_a, buckets=buckets, max_tokens=max_tokens, stopwords=stopwords)
    vec_b = build_hashed_tf_vector(text_b, buckets=buckets, max_tokens=max_tokens, stopwords=stopwords)
    return cosine_similarity(vec_a, vec_b)
