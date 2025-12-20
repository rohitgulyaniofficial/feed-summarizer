"""Helpers for working with rendered bulletin HTML files."""
from pathlib import Path
from typing import Dict, List, Optional
import re


def extract_bulletin_summary(bulletin_path: Path, max_len: int = 140) -> Optional[str]:
    """Extract a short summary snippet from a rendered bulletin HTML file."""
    if not bulletin_path.exists():
        return None
    try:
        text = bulletin_path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None

    intro_match = re.search(r'<div class="introduction">.*?<p>(.*?)</p>', text, re.DOTALL | re.IGNORECASE)
    candidate = intro_match.group(1) if intro_match else None
    if not candidate:
        summ_match = re.search(r'<div class="summary-text">(.*?)</div>', text, re.DOTALL | re.IGNORECASE)
        candidate = summ_match.group(1) if summ_match else None
    if not candidate:
        return None

    candidate = re.sub(r"<[^>]+>", " ", candidate)
    candidate = re.sub(r"\s+", " ", candidate).strip()
    if len(candidate) > max_len:
        candidate = candidate[: max_len - 1].rstrip() + "…"
    return candidate or None


def extract_bulletin_file_title(html_bulletins_dir: Path, group_name: str) -> Optional[str]:
    """Extract <h1> title from a rendered bulletin HTML file for a group."""
    path = html_bulletins_dir / f"{group_name}.html"
    if not path.exists():
        return None
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return None
    match = re.search(r"<h1[^>]*>(.*?)</h1>", text, re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    title = re.sub(r"<[^>]+>", " ", match.group(1))
    title = re.sub(r"\s+", " ", title).strip()
    return title or None


def build_recent_bulletins(html_bulletins_dir: Path, latest_titles: Dict[str, Optional[str]]) -> List[Dict[str, str]]:
    """Build recent bulletin metadata list for landing page."""
    items: List[Dict[str, str]] = []
    for group_name, title in sorted(latest_titles.items()):
        if not title:
            continue
        bulletin_file = html_bulletins_dir / f"{group_name}.html"
        summary = extract_bulletin_summary(bulletin_file) or ""
        items.append({
            "filename": bulletin_file.name,
            "title": title,
            "summary": summary,
        })
    return items


__all__ = [
    "extract_bulletin_summary",
    "extract_bulletin_file_title",
    "build_recent_bulletins",
]
