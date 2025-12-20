#!/usr/bin/env python3
"""Small string helpers."""

from __future__ import annotations

import re


def validate_url(url: str) -> bool:
    if not url or not isinstance(url, str):
        return False

    stripped = url.strip()
    if not stripped:
        return False

    return stripped.startswith(("http://", "https://")) and "." in stripped


def safe_filename(filename: str, max_length: int = 255) -> str:
    if not filename:
        return "untitled"

    safe_name = re.sub(r"[<>:\"/\\|?*]", "_", filename)
    safe_name = re.sub(r"[\x00-\x1f\x7f-\x9f]", "", safe_name)
    safe_name = safe_name.strip(". ")

    if not safe_name:
        return "untitled"

    if len(safe_name) > max_length:
        safe_name = safe_name[:max_length].rstrip(". ")

    return safe_name


def format_duration(seconds: float) -> str:
    if seconds < 0:
        return "0s"

    hours = int(seconds // 3600)
    minutes = int((seconds % 3600) // 60)
    secs = int(seconds % 60)

    parts: list[str] = []
    if hours > 0:
        parts.append(f"{hours}h")
    if minutes > 0:
        parts.append(f"{minutes}m")
    if secs > 0 or not parts:
        parts.append(f"{secs}s")

    return " ".join(parts)


def truncate_string(text: str, max_length: int, suffix: str = "...") -> str:
    if not text or len(text) <= max_length:
        return text

    if len(suffix) >= max_length:
        return text[:max_length]

    return text[: max_length - len(suffix)] + suffix
