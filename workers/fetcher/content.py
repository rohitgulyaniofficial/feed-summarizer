#!/usr/bin/env python3
"""Content helpers for feed fetching and reader mode."""

from hashlib import md5
from time import time
from typing import Optional

from readability import Document

from config import get_logger
from utils import clean_html_to_markdown
from workers.fetcher.dates import get_entry_value

logger = get_logger("fetcher.content")


def clean_html(html_content: str, base_url: Optional[str] = None) -> str:
    """Clean and convert HTML to Markdown using the shared sanitizer."""
    return clean_html_to_markdown(html_content, base_url=base_url)


def extract_content(entry) -> str:
    """Extract the content from a feed entry and convert to Markdown."""
    content = ""

    if "content" in entry and entry.content:
        for content_item in entry.content:
            if "value" in content_item:
                content = content_item.value
                break

    if not content and hasattr(entry, "summary"):
        content = entry.summary

    if not content and "description" in entry:
        content = entry.description

    if content:
        base_url = get_entry_value(entry, "link")
        markdown_content = clean_html(content, base_url=base_url)
        return markdown_content

    return "No content available"


def get_guid(entry) -> str:
    """Extract or generate a GUID for an entry."""
    if "id" in entry:
        return entry.id

    if "link" in entry:
        return md5(entry.link.encode()).hexdigest()

    if "title" in entry and "published" in entry:
        combined = f"{entry.title}{entry.published}"
        return md5(combined.encode()).hexdigest()

    return md5(str(time()).encode()).hexdigest()


def normalize_entry_identity(title: Optional[str], url: Optional[str], guid: Optional[str]) -> tuple[str, str, str]:
    """Apply consistent normalization used for storage to keep dedup logic aligned."""
    norm_title = (title or "No Title").strip()
    if not norm_title:
        norm_title = "No Title"
    norm_title = norm_title[:255]

    norm_url = (url or "").strip()
    if norm_url:
        norm_url = norm_url[:2048]

    norm_guid = (guid or "").strip()
    if norm_guid:
        norm_guid = norm_guid[:64]

    return norm_title, norm_url, norm_guid


def parse_with_readability(html_content: str, url: str) -> str | None:
    """Parse HTML content with the readability library."""
    try:
        article = Document(html_content)
        return article.summary()
    except (ImportError, ValueError, RuntimeError) as exc:
        logger.error("Error in readability parsing for %s: %s", url, exc)
        return None


__all__ = [
    "clean_html",
    "extract_content",
    "get_guid",
    "normalize_entry_identity",
    "parse_with_readability",
]
