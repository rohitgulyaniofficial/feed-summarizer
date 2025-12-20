"""Passthrough RSS publishing helpers."""
from pathlib import Path
from typing import Dict, List, Optional

from config import get_logger
from workers.publisher.rss_builder import create_raw_rss
from workers.publisher.repository import (
    cache_passthrough_rss,
    get_passthrough_feed_meta,
    get_passthrough_items,
)
from workers.publisher.settings import load_feeds_config, load_passthrough_config
from utils.io import atomic_write_text

logger = get_logger("publisher.passthrough")


async def publish_passthrough_feeds(
    db,
    base_url: str,
    rss_feeds_dir: Path,
    feeds_config: Optional[Dict] = None,
    only_slugs: Optional[List[str]] = None,
) -> int:
    """Publish raw passthrough RSS feeds based on feeds.yaml passthrough config."""
    try:
        config_data = feeds_config or load_feeds_config()
        pt = load_passthrough_config(config_data)
        if not pt:
            logger.info("No passthrough feeds configured")
            return 0

        raw_dir = rss_feeds_dir / "raw"
        raw_dir.mkdir(parents=True, exist_ok=True)

        published = 0
        allowed = set(only_slugs) if only_slugs else None
        for slug, opts in pt.items():
            if allowed and slug not in allowed:
                continue
            try:
                feed_meta = await get_passthrough_feed_meta(db, slug)
                feed_title = opts.get("title") or (feed_meta.get("title") if feed_meta else slug)
                limit = int(opts.get("limit", 50))

                items = await get_passthrough_items(db, slug, limit)
                logger.info("Retrieved %d items for passthrough feed '%s'", len(items) if items else 0, slug)
                if not items:
                    logger.info("No items found for passthrough feed '%s'", slug)
                    continue

                items_sorted = sorted(items, key=lambda it: int(it.get("date") or 0), reverse=False)

                xml = create_raw_rss(base_url, slug, feed_title, items_sorted)
                logger.info("Generated RSS XML for '%s' (%d bytes)", slug, len(xml))

                out_file = raw_dir / f"{slug}.xml"
                atomic_write_text(out_file, xml, suffix=".xml")
                try:
                    await cache_passthrough_rss(db, slug, xml)
                except Exception:
                    pass
                published += 1
                logger.info("Published passthrough RSS: %s", out_file)
            except Exception as exc:
                logger.error("Error publishing passthrough feed '%s': %s", slug, exc, exc_info=True)
        return published
    except Exception as exc:
        logger.error("Passthrough publishing failed: %s", exc)
        return 0


__all__ = ["publish_passthrough_feeds"]
