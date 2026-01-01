"""Environment helpers for merge-report tools.

Extracted from report_merge to keep that script smaller and reusable.
"""

from __future__ import annotations

import os
from typing import Tuple


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


def _hashed_cosine_env_settings() -> Tuple[bool, float, int, int]:
    enabled = _env_bool("HASHED_COSINE_ENABLED", False)
    min_sim = _env_float("HASHED_COSINE_MIN_SIM", 0.25)
    if min_sim < 0:
        min_sim = 0.0
    if min_sim > 1:
        min_sim = 1.0
    buckets = _env_int("HASHED_COSINE_BUCKETS", 65536)
    if buckets <= 0:
        buckets = 65536
    max_tokens = _env_int("HASHED_COSINE_MAX_TOKENS", 128)
    if max_tokens <= 0:
        max_tokens = 128
    return enabled, float(min_sim), int(buckets), int(max_tokens)
