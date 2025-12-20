#!/usr/bin/env python3
"""SQLite-safe encoding/decoding for unsigned 64-bit integers."""

from __future__ import annotations

from typing import Optional

INT64_MASK = (1 << 64) - 1


def encode_int64(value: Optional[int]) -> Optional[int]:
    """Encode an unsigned 64-bit value into SQLite-compatible signed range."""
    if value is None:
        return None
    masked = value & INT64_MASK
    if masked >= (1 << 63):
        masked -= 1 << 64
    return masked


def decode_int64(value: Optional[int]) -> Optional[int]:
    """Decode a signed SQLite integer back into the original 64-bit value."""
    if value is None:
        return None
    return value & INT64_MASK
