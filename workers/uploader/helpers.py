#!/usr/bin/env python3
"""Helper utilities for Azure uploader."""

from hashlib import md5
import mimetypes
from pathlib import Path
from typing import Iterable, List, Optional, Set

from config import get_logger

logger = get_logger("uploader.helpers")


def get_local_file_hash(file_path: Path) -> Optional[str]:
    """Return hex MD5 hash for a local file, or None on failure."""
    try:
        with open(file_path, "rb") as handle:
            return md5(handle.read()).hexdigest()
    except Exception as exc:  # noqa: BLE001
        logger.error("Failed to hash file %s: %s", file_path, exc)
        return None


def get_mime_type(file_path: Path) -> str:
    """Infer MIME type with sensible fallbacks for feeds and HTML."""
    mime_type, _ = mimetypes.guess_type(str(file_path))
    if mime_type:
        return mime_type
    if file_path.suffix.lower() == ".xml":
        return "application/rss+xml"
    if file_path.suffix.lower() == ".html":
        return "text/html"
    return "application/octet-stream"


def list_local_files(local_dir: Path, recursive: bool) -> List[Path]:
    """List files under a directory respecting recursion flag."""
    if recursive:
        return [path for path in local_dir.rglob("*") if path.is_file()]
    return [path for path in local_dir.glob("*") if path.is_file()]


def build_blob_path(base_dir: Path, file_path: Path, remote_prefix: str, recursive: bool) -> str:
    """Construct blob path for a local file respecting recursion and prefix."""
    if remote_prefix:
        if recursive:
            rel_path = file_path.relative_to(base_dir).as_posix()
            return f"{remote_prefix.rstrip('/')}/{rel_path}"
        return f"{remote_prefix.rstrip('/')}/{file_path.name}"
    return file_path.name


def determine_deletions(
    remote_keys: Iterable[str],
    expected_blobs: Set[str],
    cache_prefix: Optional[str],
    recursive: bool,
) -> List[str]:
    """Return blob paths that should be removed during sync."""
    to_delete: List[str] = []
    for blob_path in remote_keys:
        if cache_prefix and not blob_path.startswith(cache_prefix):
            continue
        if not cache_prefix and "/" in blob_path:
            continue
        if cache_prefix and not recursive:
            rel = blob_path[len(cache_prefix) :]
            if "/" in rel:
                continue
        if blob_path not in expected_blobs:
            to_delete.append(blob_path)
    return to_delete
