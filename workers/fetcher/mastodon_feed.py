#!/usr/bin/env python3
"""Mastodon list fetching helpers."""

import os
from datetime import datetime
from typing import Any, Dict, List, Optional

from aiohttp import ClientSession

from config import get_logger
from services.mastodon import fetch_list_timeline, render_status_html

logger = get_logger("fetcher.mastodon")


async def fetch_mastodon_list(
    db,
    feed_id: int,
    slug: str,
    list_url: str,
    feed_config: Dict[str, Any],
    session: Optional[ClientSession],
    proxy_url: Optional[str] = None,
) -> None:
    token = feed_config.get("token") or (lambda name: os.environ.get(name) if name else None)(feed_config.get("token_env"))
    title = feed_config.get("title") or f"Mastodon: {slug}"
    limit = int(feed_config.get("limit", 40))
    if not token:
        logger.error("Mastodon feed '%s' missing token; skipping", slug)
        return
    try:
        statuses = await fetch_list_timeline(list_url, token=token, limit=limit, session=session, proxy_url=proxy_url)
        if not statuses:
            logger.info("No statuses returned for Mastodon feed %s", slug)
            return

        await db.execute("update_feed_title", feed_id=feed_id, title=title)

        rendered = [r for r in (render_status_html(s) for s in statuses) if r]
        guids = [r["guid"] for r in rendered if r.get("guid")]
        existing = await db.execute("check_existing_guids", feed_id=feed_id, guids=guids)

        seen_batch_guids = set()

        entries_data: List[Dict[str, Any]] = []
        for rendered_status in rendered:
            guid_val = rendered_status.get("guid")
            if not guid_val:
                logger.warning("Skipping Mastodon status without guid after rendering: slug=%s", slug)
                continue
            if guid_val in seen_batch_guids:
                logger.debug("Duplicate guid in same Mastodon batch skipped: %s", guid_val)
                continue
            seen_batch_guids.add(guid_val)
            if guid_val in existing:
                continue
            title_v = (rendered_status.get("title") or "No Title")[:255]
            url_v = (rendered_status.get("url") or "")[:2048]
            guid_v = guid_val[:64]
            body_v = rendered_status["body"] or "<p>No content available</p>"
            date_v = (
                int(rendered_status["date"])
                if isinstance(rendered_status["date"], int)
                else int(datetime.now().timestamp())
            )
            entries_data.append(
                {
                    "title": title_v,
                    "url": url_v,
                    "guid": guid_v,
                    "body": body_v,
                    "date": date_v,
                }
            )

        if entries_data:
            new_items = await db.execute("save_items", feed_id=feed_id, entries_data=entries_data)
            logger.info("Saved %s NEW Mastodon entries for %s", new_items, slug)
            logger.debug(
                "Mastodon batch details for %s: %s items, guids=%s",
                slug,
                len(entries_data),
                [e.get("guid") for e in entries_data],
            )
        else:
            logger.info("No new Mastodon entries to save for %s", slug)

    except Exception as exc:
        logger.error("Error fetching Mastodon list for %s: %s", slug, exc)


__all__ = ["fetch_mastodon_list"]
