"""RSS generation helpers for publisher."""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import urljoin
import re

from bs4 import BeautifulSoup
from feedgen.feed import FeedGenerator
from markdown import markdown as md

from config import config, get_logger
from workers.publisher.merge import collect_summary_links

logger = get_logger("publisher.rss_builder")


def sanitize_xml_string(text: str) -> str:
    """Sanitize a string for XML output by removing control characters and NULL bytes."""
    if not text:
        return ""
    try:
        if isinstance(text, bytes):
            text = text.decode("utf-8", errors="ignore")
        sanitized = "".join(
            char for char in text if char in ("\t", "\n", "\r") or (ord(char) >= 32 and ord(char) != 0x7F)
        )
        return sanitized
    except Exception:
        return ""


def looks_like_html(text: str) -> bool:
    """Heuristic: does the text already contain HTML tags?"""
    if "<" in text and ">" in text:
        for tag in ("<p", "<a ", "<br", "<ul", "<ol", "<li", "<em", "<strong", "<code", "<blockquote"):
            if tag in text:
                return True
    return False


def looks_like_markdown(text: str) -> bool:
    """Detect common Markdown patterns to decide if conversion is needed."""
    if not text:
        return False
    markdown_patterns = (
        r"\[[^\]]+\]\([^\)]+\)",
        r"^#{1,6}\s+\S",
        r"(^|\n)(?:\*|-|\+)\s+\S",
        r"(^|\n)\d+\.\s+\S",
        r"`[^`]+`",
        r"\*\*[^*]+\*\*",
        r"__[^_]+__",
    )
    return any(re.search(pattern, text) for pattern in markdown_patterns)


def strip_markdown(text: str) -> str:
    """Remove basic Markdown syntax for a plain-text snippet."""
    t = text
    t = re.sub(r"\[([^\]]+)\]\(([^)]+)\)", r"\1", t)
    t = re.sub(r"!\[([^\]]*)\]\(([^)]+)\)", r"\1", t)
    t = re.sub(r"\*\*([^*]+)\*\*", r"\1", t)
    t = re.sub(r"\*([^*]+)\*", r"\1", t)
    t = re.sub(r"`([^`]+)`", r"\1", t)
    t = re.sub(r"^\s{0,3}#{1,6}\s+", "", t, flags=re.MULTILINE)
    t = re.sub(r"^\s*[-*+]\s+", "• ", t, flags=re.MULTILINE)
    t = re.sub(r"^\s*\d+\.\s+", "• ", t, flags=re.MULTILINE)
    t = re.sub(r"\s+", " ", t).strip()
    return t


def markdown_to_html(text: str) -> str:
    """Convert Markdown to HTML using python-Markdown."""
    return md(text, extensions=["extra", "sane_lists"])


def create_raw_rss(base_url: str, slug: str, feed_title: str, items: List[Dict[str, Any]]) -> str:
    """Create an RSS feed for raw items of a specific feed slug (feedgen)."""
    fg = FeedGenerator()
    fg.id(sanitize_xml_string(f"{base_url}/feeds/raw/{slug}.xml"))
    fg.title(sanitize_xml_string(feed_title or slug))
    fg.link(href=sanitize_xml_string(f"{base_url}/feeds/raw/{slug}.xml"), rel="self")
    fg.link(href=sanitize_xml_string(base_url), rel="alternate")
    fg.description(sanitize_xml_string(f"Passthrough feed for {slug}"))
    fg.language("en-us")
    fg.generator("Feed Summarizer Passthrough")

    latest_ts: Optional[int] = None
    try:
        timestamps = [int(it.get("date")) for it in items if it.get("date") is not None]
        if timestamps:
            latest_ts = max(timestamps)
    except Exception:
        latest_ts = None

    if latest_ts is not None:
        fg.lastBuildDate(datetime.fromtimestamp(latest_ts, tz=timezone.utc))
    else:
        fg.lastBuildDate(datetime.now(timezone.utc))

    for it in items:
        try:
            fe = fg.add_item()
            title = sanitize_xml_string(str(it.get("title") or it.get("url") or slug))
            link = sanitize_xml_string(str(it.get("url") or ""))
            guid_val = sanitize_xml_string(str(it.get("guid") or link or f"{slug}-{it.get('id')}"))
            fe.title(title if title else "Untitled")
            if link:
                fe.link(href=link)
            fe.guid(guid_val if guid_val else f"{slug}-{it.get('id')}", permalink=bool(guid_val and str(guid_val).startswith("http")))

            body = sanitize_xml_string(str(it.get("body") or "")).strip()
            if body:
                short_plain = strip_markdown(body)
                if len(short_plain) > 280:
                    short_plain = short_plain[:280].rsplit(" ", 1)[0] + "…"
                short_plain = sanitize_xml_string(short_plain)
                fe.description(short_plain if short_plain else " ")

                needs_conversion = not looks_like_html(body) or looks_like_markdown(body)
                html_body = body if not needs_conversion else markdown_to_html(body)
                try:
                    soup = BeautifulSoup(html_body, "html.parser")
                    for tag in soup.find_all(True):
                        for attr in ("href", "src"):
                            if tag.has_attr(attr) and isinstance(tag[attr], str):
                                cleaned = sanitize_xml_string(tag[attr].replace("\n", "").replace("\r", "").strip())
                                if link and cleaned and not cleaned.startswith(("http://", "https://", "data:")):
                                    cleaned = urljoin(link, cleaned)
                                tag[attr] = cleaned
                    html_body = str(soup)
                except Exception:
                    pass
                html_body = sanitize_xml_string(html_body)
                fe.content(html_body if html_body else " ", type="html")

            try:
                dt = datetime.fromtimestamp(int(it.get("date", 0)), tz=timezone.utc)
                fe.pubDate(dt)
            except Exception:
                pass
        except Exception as exc:
            logger.warning("Skipping problematic item %s in feed '%s': %s", it.get("id"), slug, exc)
            continue

    try:
        return fg.rss_str(pretty=True).decode("utf-8")
    except Exception as exc:
        logger.error("Failed to generate RSS XML for feed '%s': %s", slug, exc)
        try:
            return fg.rss_str(pretty=False).decode("utf-8")
        except Exception as exc2:
            logger.error("Failed to generate RSS XML without pretty printing for feed '%s': %s", slug, exc2)
            raise


def create_rss_feed(
    base_url: str,
    group_name: str,
    feed_slugs: List[str],
    bulletins: Dict[str, List[Dict[str, Any]]],
    bulletin_introductions: Optional[Dict[str, str]] = None,
    bulletin_titles: Optional[Dict[str, str]] = None,
) -> str:
    """Create RSS 2.0 XML content for a summary group (feedgen)."""
    fg = FeedGenerator()
    feed_url = f"{base_url}/feeds/{group_name}.xml"
    fg.id(feed_url)
    fg.title(f"{group_name.title()} News Bulletins")
    fg.link(href=feed_url, rel="self")
    fg.link(href=base_url, rel="alternate")
    fg.description(
        f"News bulletins for {group_name} topics, featuring AI-generated summaries from multiple sources, updated every 4 hours."
    )
    fg.language("en-us")
    fg.generator("Feed Summarizer RSS Publisher")

    latest_ts: Optional[int] = None
    try:
        for summaries in bulletins.values():
            for summary in summaries:
                ts = summary.get("published_date")
                if ts:
                    its = int(ts)
                    if latest_ts is None or its > latest_ts:
                        latest_ts = its
    except Exception:
        latest_ts = None

    if latest_ts is not None:
        fg.lastBuildDate(datetime.fromtimestamp(latest_ts, tz=timezone.utc))
    else:
        fg.lastBuildDate(datetime.now(timezone.utc))

    for session_key in sorted(bulletins.keys(), reverse=True):
        summaries = bulletins[session_key]
        if not summaries:
            continue

        fe = fg.add_item()

        introduction = bulletin_introductions.get(session_key) if bulletin_introductions else None
        if bulletin_titles and session_key in bulletin_titles and bulletin_titles[session_key]:
            title_text = bulletin_titles[session_key]
        else:
            title_text = bulletin_titles.get(session_key) if bulletin_titles else None
        fe.title(title_text)

        fe.link(href=f"{base_url}/bulletins/{group_name}.html")
        fe.guid(f"{group_name}-bulletin-{session_key}", permalink=False)

        desc_plain = (introduction or f"Bulletin for {group_name}").strip()
        if len(desc_plain) > 280:
            desc_plain = desc_plain[:280].rsplit(" ", 1)[0] + "…"
        fe.description(desc_plain)

        html_body = sanitize_xml_string(bulletins_html_content(summaries, introduction))
        fe.content(html_body, type="html")

        try:
            latest_pub_date = max(s["published_date"] for s in summaries if s.get("published_date"))
            fe.pubDate(datetime.fromtimestamp(latest_pub_date, tz=timezone.utc))
        except Exception:
            pass

    return fg.rss_str(pretty=True).decode("utf-8")


def bulletins_html_content(summaries: List[Dict[str, Any]], introduction: Optional[str]) -> str:
    """Generate minimal HTML body for RSS content: topics with list items."""
    topics = {}
    for summary in summaries:
        topic = summary.get("topic", "General")
        topics.setdefault(topic, []).append(summary)

    # Custom sort to place recurring coverage topic at the end
    recurring_topic = getattr(config, "RECURRING_COVERAGE_TOPIC", "Recurring Coverage")
    
    def topic_sort_key(topic: str) -> tuple:
        """Sort key to place recurring coverage at the end."""
        if topic == recurring_topic:
            return (1, topic)
        return (0, topic)
    
    sorted_topics = sorted(topics.keys(), key=topic_sort_key)
    for topic in topics:
        topics[topic].sort(key=lambda x: x.get("item_date", 0), reverse=True)

    parts: List[str] = []
    if introduction and introduction.strip():
        parts.append(f"<p>{introduction.strip()}</p>")

    for topic in sorted_topics:
        topic_summaries = topics[topic]
        parts.append(f"<h3>{topic}</h3>")
        parts.append("<ul>")

        for summary in topic_summaries:
            summary_text = summary.get("summary_text", "").strip()
            links = collect_summary_links(summary)
            if not summary_text:
                continue
            if links:
                link_html = []
                link_count = len(links)
                for idx, link in enumerate(links, start=1):
                    href = link.get("url")
                    if not href:
                        continue
                    label = "link" if link_count == 1 else str(idx)
                    link_html.append(f'<a href="{href}">{label}</a>')
                parts.append(f"<li>{summary_text} ({'; '.join(link_html)})</li>")
            else:
                parts.append(f"<li>{summary_text}</li>")

        parts.append("</ul>")

    return "\n".join(parts)


__all__ = [
    "sanitize_xml_string",
    "looks_like_html",
    "looks_like_markdown",
    "strip_markdown",
    "markdown_to_html",
    "create_raw_rss",
    "create_rss_feed",
    "bulletins_html_content",
]
