#!/usr/bin/env python3
"""Date parsing utilities for feed fetching."""

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from time import mktime, time
from typing import Any, Optional
import re

import feedparser

from config import get_logger

logger = get_logger("fetcher.dates")


def get_entry_value(entry, field: str) -> Any:
    """Safely fetch feedparser entry fields with attribute or dict access."""
    if not field or entry is None:
        return None
    try:
        value = getattr(entry, field)
    except AttributeError:
        value = None

    if value is not None:
        return value

    getter = getattr(entry, "get", None)
    if callable(getter):
        try:
            return getter(field)
        except KeyError:
            return None
    return None


def _parse_with_feedparser(date_str: str) -> Optional[int]:
    try:
        time_struct = feedparser._parse_date(date_str)
        if time_struct:
            return int(mktime(time_struct))
    except (ValueError, TypeError, AttributeError, OSError):
        return None
    return None


def _parse_with_email_utils(date_str: str) -> Optional[int]:
    try:
        dt = parsedate_to_datetime(date_str)
        if dt:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
    except (TypeError, ValueError, OverflowError):
        return None
    return None


def _parse_with_custom_formats(date_str: str) -> Optional[int]:
    custom_formats = [
        "%d %b %Y %H:%M:%S %z",
        "%d %b %Y %H:%M:%S %Z",
        "%d %b %Y %H:%M:%S",
    ]
    for fmt in custom_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp())
        except (ValueError, TypeError):
            continue
    return None


def _parse_date_string(date_str: str) -> Optional[int]:
    parsers = (
        _parse_with_feedparser,
        _parse_with_email_utils,
        _parse_with_custom_formats,
    )
    for parser in parsers:
        timestamp = parser(date_str)
        if timestamp is not None:
            return timestamp
    return None


def _date_value_to_timestamp(value: Any) -> Optional[int]:
    """Convert assorted date representations into a Unix timestamp."""
    if value in (None, ""):
        return None

    if isinstance(value, (int, float)):
        try:
            timestamp = int(value)
            if timestamp > 0:
                return timestamp
        except (ValueError, OSError):
            return None
        return None

    if isinstance(value, datetime):
        dt = value
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())

    if isinstance(value, (list, tuple)):
        try:
            return int(mktime(tuple(value)))
        except (OverflowError, ValueError, OSError, TypeError):
            return None

    if isinstance(value, str):
        return _parse_date_string(value)

    return None


def parse_date(date_str: Optional[str]) -> int:
    """Parse a date string into a Unix timestamp."""
    if not date_str:
        return int(time())

    timestamp = _parse_date_string(date_str)
    if timestamp is not None:
        return timestamp

    return int(time())


def parse_date_enhanced(entry) -> int:
    """Parse publication date with enhanced error handling for various date formats."""
    current_time = int(time())

    date_fields = [
        "published",
        "updated",
        "created",
        "modified",
        "date",
        "pubDate",
        "pubdate",
        "issued",
    ]

    for field in date_fields:
        value = get_entry_value(entry, field)
        timestamp = _date_value_to_timestamp(value)
        if timestamp:
            return timestamp

        parsed_value = get_entry_value(entry, f"{field}_parsed")
        timestamp = _date_value_to_timestamp(parsed_value)
        if timestamp:
            return timestamp

    struct_fields = [
        "published_parsed",
        "updated_parsed",
        "created_parsed",
        "modified_parsed",
        "date_parsed",
    ]
    for field in struct_fields:
        value = get_entry_value(entry, field)
        timestamp = _date_value_to_timestamp(value)
        if timestamp:
            return timestamp

    entry_id = get_entry_value(entry, "id")
    if entry_id:
        date_patterns = [
            r"(\d{4})-(\d{2})-(\d{2})",
            r"(\d{4})/(\d{2})/(\d{2})",
        ]

        for pattern in date_patterns:
            match = re.search(pattern, entry_id)
            if match:
                try:
                    year, month, day = map(int, match.groups())
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        dt = datetime(year, month, day, tzinfo=timezone.utc)
                        return int(dt.timestamp())
                except (ValueError, TypeError, OSError) as exc:
                    logger.debug("Failed to parse date components for '%s': %s", entry_id, exc)

    return current_time


def format_timestamp(timestamp: Optional[int]) -> str:
    """Return a human-readable UTC timestamp for diagnostics."""
    if timestamp in (None, ""):
        return "n/a"
    try:
        return datetime.fromtimestamp(int(timestamp), tz=timezone.utc).isoformat()
    except (OSError, OverflowError, ValueError, TypeError):
        return str(timestamp)


__all__ = [
    "get_entry_value",
    "parse_date",
    "parse_date_enhanced",
    "format_timestamp",
]
