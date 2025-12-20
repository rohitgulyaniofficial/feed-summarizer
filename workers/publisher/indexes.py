"""HTML index generation helpers for publisher."""
from datetime import datetime, timezone
from pathlib import Path
from typing import Awaitable, Callable, Dict, List, Optional
import re

from config import get_logger
from workers.publisher.settings import load_feeds_config, load_passthrough_config
from workers.publisher.templates import env

logger = get_logger("publisher.indexes")


async def write_feeds_index(
    rss_feeds_dir: Path,
    base_url: str,
    get_latest_title: Callable[[str, int], Awaitable[Optional[str]]],
) -> None:
    """Render feeds index (RSS + passthrough) into index.html under feeds dir."""
    feeds_index_path = rss_feeds_dir / "index.html"

    rss_files = list(rss_feeds_dir.glob("*.xml"))
    raw_dir = rss_feeds_dir / "raw"
    raw_files = list(raw_dir.glob("*.xml")) if raw_dir.exists() else []
    if not rss_files and not raw_files:
        logger.warning("No RSS feeds found for index generation")
        return

    config_data = load_feeds_config()
    summaries_config = config_data.get("summaries", {}) if isinstance(config_data, dict) else {}

    feeds_info: List[Dict[str, str]] = []
    for rss_file in sorted(rss_files):
        group_name = rss_file.stem
        group_config = summaries_config.get(group_name) if isinstance(summaries_config.get(group_name), dict) else {}

        hidden = False
        try:
            hv = group_config.get("hidden") if group_config else False
            if isinstance(hv, str):
                hidden = hv.strip().lower() == "true"
            elif isinstance(hv, bool):
                hidden = hv
            if group_config.get("visible") is False:
                hidden = True
            if group_config.get("hide_from_index") is True:
                hidden = True
        except Exception:
            hidden = False
        if hidden:
            logger.info("Hiding '%s' from RSS feeds HTML index due to config flag", group_name)
            continue

        latest_title = None
        try:
            latest_title = await get_latest_title(group_name, days_back=30)
        except Exception:
            latest_title = None

        feeds_info.append(
            {
                "name": group_name,
                "title": group_config.get("title", group_name.replace("_", " ").title()),
                "description": group_config.get("description", f"{group_name} news summaries"),
                "filename": rss_file.name,
                "latest_title": latest_title,
            }
        )

    passthrough_info: List[Dict[str, str]] = []
    pt_cfg = load_passthrough_config(load_feeds_config())
    feeds_config = config_data.get("feeds", {}) if isinstance(config_data, dict) else {}
    if raw_files and pt_cfg:
        for rf in sorted(raw_files):
            slug = rf.stem
            if slug not in pt_cfg:
                continue
            opts = pt_cfg.get(slug, {}) or {}
            hidden = False
            try:
                hv = opts.get("hidden")
                if isinstance(hv, str):
                    hidden = hv.strip().lower() == "true"
                elif isinstance(hv, bool):
                    hidden = hv
                if not hidden and isinstance(feeds_config.get(slug), dict):
                    feed_cfg = feeds_config.get(slug) or {}
                    fhv = feed_cfg.get("hidden")
                    if isinstance(fhv, str):
                        hidden = fhv.strip().lower() == "true"
                    elif isinstance(fhv, bool):
                        hidden = fhv
                if opts.get("visible") is False:
                    hidden = True
                if opts.get("hide_from_index") is True:
                    hidden = True
                if isinstance(feeds_config.get(slug), dict):
                    feed_cfg = feeds_config.get(slug) or {}
                    if feed_cfg.get("visible") is False:
                        hidden = True
                    if feed_cfg.get("hide_from_index") is True:
                        hidden = True
            except Exception:
                hidden = False
            if hidden:
                logger.info("Hiding passthrough feed '%s' from RSS index due to config flag", slug)
                continue
            passthrough_info.append(
                {
                    "name": slug,
                    "title": opts.get("title")
                    or (feeds_config.get(slug, {}) or {}).get("title")
                    or slug.replace("_", " ").title(),
                    "filename": f"raw/{rf.name}",
                }
            )

    template = env.get_template("feeds_index.html")
    current_time = datetime.now(timezone.utc)
    html_content = template.render(
        feeds_info=feeds_info,
        passthrough_info=passthrough_info,
        current_time=current_time,
        base_url=base_url,
    )

    with open(feeds_index_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info("Generated RSS feeds index: %s", feeds_index_path)


async def write_bulletins_index(
    html_bulletins_dir: Path,
    base_url: str,
    get_latest_title: Callable[[str, int], Awaitable[Optional[str]]],
) -> None:
    """Render bulletins index into HTML in the bulletins directory."""
    bulletins_index_path = html_bulletins_dir / "index.html"

    html_files = list(html_bulletins_dir.glob("*.html"))
    html_files = [f for f in html_files if f.name != "index.html"]
    if not html_files:
        logger.warning("No HTML bulletins found for index generation")
        return

    config_data = load_feeds_config()
    summaries_config = config_data.get("summaries", {}) if isinstance(config_data, dict) else {}

    bulletins_info: List[Dict[str, str]] = []
    for html_file in sorted(html_files, reverse=True):
        group_name = html_file.stem
        group_config = summaries_config.get(group_name) if isinstance(summaries_config.get(group_name), dict) else {}

        hidden = False
        try:
            hv = group_config.get("hidden") if group_config else False
            if isinstance(hv, str):
                hidden = hv.strip().lower() == "true"
            elif isinstance(hv, bool):
                hidden = hv
            if group_config.get("visible") is False:
                hidden = True
            if group_config.get("hide_from_index") is True:
                hidden = True
        except Exception:
            hidden = False
        if hidden:
            logger.info("Hiding '%s' from HTML bulletins index due to config flag", group_name)
            continue

        latest_title = None
        try:
            latest_title = await get_latest_title(group_name, days_back=30)
        except Exception:
            latest_title = None

        mtime = datetime.fromtimestamp(html_file.stat().st_mtime, tz=timezone.utc)
        bulletins_info.append(
            {
                "name": group_name,
                "title": group_config.get("title", group_name.replace("_", " ").title()),
                "description": group_config.get("description", f"{group_name} news summaries"),
                "filename": html_file.name,
                "updated": mtime.strftime("%Y-%m-%d %H:%M UTC"),
                "latest_title": latest_title,
            }
        )

    template = env.get_template("bulletins_index.html")
    current_time = datetime.now(timezone.utc)
    html_content = template.render(
        bulletins_info=bulletins_info,
        current_time=current_time,
        base_url=base_url,
    )

    with open(bulletins_index_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info("Generated HTML bulletins index: %s", bulletins_index_path)


async def write_main_index(
    public_dir: Path,
    html_bulletins_dir: Path,
    rss_feeds_dir: Path,
    base_url: str,
    get_latest_title: Callable[[str, int], Awaitable[Optional[str]]],
    extract_bulletin_summary: Callable[[Path, int], Optional[str]],
) -> None:
    """Render root index.html for the public directory."""
    main_index_path = public_dir / "index.html"

    config_data = load_feeds_config()
    summaries_config = config_data.get("summaries", {}) if isinstance(config_data, dict) else {}

    bulletins_count = len([f for f in html_bulletins_dir.glob("*.html") if f.name != "index.html"])
    feeds_count = len(list(rss_feeds_dir.glob("*.xml")))

    latest_titles: Dict[str, Optional[str]] = {}
    if isinstance(summaries_config, dict):
        for group_name in summaries_config.keys():
            try:
                latest_titles[group_name] = await get_latest_title(group_name, days_back=30)
            except Exception:
                latest_titles[group_name] = None

    recent_bulletins: List[Dict[str, str]] = []
    for html_file in sorted(html_bulletins_dir.glob("*.html")):
        if html_file.name == "index.html":
            continue
        try:
            text = html_file.read_text(encoding="utf-8", errors="ignore")
        except Exception:
            text = ""
        title_match = re.search(r"<h1[^>]*>(.*?)</h1>", text, re.IGNORECASE | re.DOTALL)
        if title_match:
            raw_title = re.sub(r"<[^>]+>", " ", title_match.group(1))
            raw_title = re.sub(r"\s+", " ", raw_title).strip()
        else:
            stem = html_file.stem
            raw_title = latest_titles.get(stem) or stem.replace("_", " ").title()
        summary = extract_bulletin_summary(html_file, 140) or ""
        recent_bulletins.append(
            {"filename": html_file.name, "title": raw_title, "summary": summary}
        )

    template = env.get_template("index.html")
    current_time = datetime.now(timezone.utc)
    html_content = template.render(
        bulletins_count=bulletins_count,
        feeds_count=feeds_count,
        current_time=current_time,
        recent_bulletins=recent_bulletins,
        base_url=base_url,
    )

    with open(main_index_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    logger.info("Generated main index: %s", main_index_path)


__all__ = [
    "write_feeds_index",
    "write_bulletins_index",
    "write_main_index",
]