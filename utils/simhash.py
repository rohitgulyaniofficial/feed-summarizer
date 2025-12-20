#!/usr/bin/env python3
"""SimHash fingerprinting helpers."""

from __future__ import annotations

from collections import Counter
from hashlib import blake2b
import re
from typing import Optional


def compute_simhash(text: Optional[str], hash_bits: int = 64) -> Optional[int]:
    """Compute a lightweight SimHash fingerprint for the provided text."""
    if not text:
        return None
    tokens = re.findall(r"\w+", text.lower())
    if not tokens:
        return None

    stopwords = {
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

    filtered_tokens = [t for t in tokens if t not in stopwords and len(t) > 2]
    if not filtered_tokens:
        return None

    freq = Counter(filtered_tokens)
    most_common_limit = 64
    if len(freq) > most_common_limit:
        freq = Counter(dict(freq.most_common(most_common_limit)))

    bits = max(8, hash_bits)
    if bits % 8 != 0:
        bits -= bits % 8

    digest_size = bits // 8
    if digest_size <= 0:
        return None

    vector = [0] * bits
    for token, weight in freq.items():
        try:
            digest = blake2b(token.encode("utf-8"), digest_size=digest_size).digest()
        except Exception:
            continue
        value = int.from_bytes(digest, "big")
        for bit in range(bits):
            if value & (1 << bit):
                vector[bit] += weight
            else:
                vector[bit] -= weight

    fingerprint = 0
    for bit, score in enumerate(vector):
        if score > 0:
            fingerprint |= 1 << bit

    return fingerprint if fingerprint != 0 else None


def hamming_distance(value_a: Optional[int], value_b: Optional[int], bits: int = 64) -> Optional[int]:
    """Compute the Hamming distance between two integer fingerprints."""
    if value_a is None or value_b is None:
        return None
    mask_bits = max(1, bits)
    mask = (1 << mask_bits) - 1
    diff = (value_a ^ value_b) & mask
    try:
        return diff.bit_count()
    except AttributeError:
        count = 0
        while diff:
            diff &= diff - 1
            count += 1
        return count
