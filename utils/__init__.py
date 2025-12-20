"""Utility classes and functions for the feed processing system.

This package exposes a stable `from utils import ...` API while keeping the
implementation split into focused submodules.
"""

from __future__ import annotations

from config import config, get_logger

from utils.html import clean_html_to_markdown
from utils.int64 import INT64_MASK, decode_int64, encode_int64
from utils.rate_limiter import RateLimiter
from utils.retry import RetryHelper
from utils.simhash import compute_simhash, hamming_distance
from utils.strings import format_duration, safe_filename, truncate_string, validate_url


__all__ = [
    "RateLimiter",
    "RetryHelper",
    "clean_html_to_markdown",
    "compute_simhash",
    "config",
    "INT64_MASK",
    "decode_int64",
    "encode_int64",
    "format_duration",
    "get_logger",
    "hamming_distance",
    "safe_filename",
    "truncate_string",
    "validate_url",
]


logger = get_logger("utils")
