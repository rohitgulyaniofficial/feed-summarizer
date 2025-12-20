#!/usr/bin/env python3
"""HTML sanitizing and HTML->Markdown conversion."""

from __future__ import annotations

import re
from typing import Optional
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from markdownify import markdownify as md

from config import get_logger

logger = get_logger("utils.html")


def clean_html_to_markdown(html_content: str, base_url: Optional[str] = None) -> str:
    if not html_content:
        return ""

    try:
        soup = BeautifulSoup(html_content, "html.parser")

        for tag in soup(
            [
                "script",
                "style",
                "iframe",
                "form",
                "object",
                "embed",
                "noscript",
                "frame",
                "frameset",
                "applet",
                "meta",
                "base",
                "link",
            ]
        ):
            tag.decompose()

        for tag in soup.find_all(True):
            for attr in list(tag.attrs):
                if attr.lower().startswith("on"):
                    del tag[attr]
                if attr.lower() in ["href", "src"] and tag.has_attr(attr):
                    val = str(tag[attr])
                    if val.lower().startswith("javascript:"):
                        del tag[attr]

        for img in soup.find_all("img"):
            src = img.get("src", "")
            if re.search(r"(pixel|tracker|counter|spacer|blank|trans)", src, re.I) or (
                re.search(r"\.(gif|png)$", src, re.I) and (img.get("height") in ("0", "1"))
            ):
                img.decompose()

        def _rewrite_url(value: str, attr: str) -> Optional[str]:
            if not value:
                return None
            if attr == "href" and value.startswith("mailto:"):
                return value
            if value.startswith(("http://", "https://")):
                return value
            if base_url:
                try:
                    resolved = urljoin(base_url, value)
                except Exception:
                    return None
                if resolved and resolved.startswith(("http://", "https://")):
                    return resolved
            return None

        for tag in soup.find_all(["a", "img"]):
            for attr in ["href", "src"]:
                if not tag.has_attr(attr):
                    continue
                val = str(tag[attr])
                if not val:
                    continue
                rewritten = _rewrite_url(val, attr)
                if rewritten:
                    tag[attr] = rewritten
                else:
                    if attr == "href":
                        tag[attr] = "#"
                    else:
                        del tag[attr]

        return md(str(soup), heading_style="ATX", wrap_width=0)
    except Exception as exc:
        logger.error(f"Error cleaning HTML to Markdown: {exc}")
        return html_content
