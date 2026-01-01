"""Feed setup and scheduling helpers."""
from time import time
from datetime import datetime

from config import config, get_logger
from workers.fetcher.feeds import get_feed_config
from workers.fetcher.backoff import should_fetch_feed
from workers.fetcher.constants import SECONDS_PER_MINUTE

logger = get_logger("fetcher.setup")


async def setup_feed(db, slug: str, url: str) -> int | None:
    """Register feed (if missing) and return feed_id, honoring backoff."""
    await db.execute('register_feed', slug=slug, url=url)
    feed_id = await db.execute('get_feed_id', slug=slug)
    if not feed_id:
        logger.error("Could not get feed ID for %s", slug)
        return None
    if not await should_fetch_feed(db, feed_id):
        return None
    return feed_id


async def should_skip_feed_fetch(db, feed_id: int, slug: str) -> bool:
    """Check if feed should be skipped based on interval overrides and last fetched time."""
    try:
        feed_cfg = await get_feed_config(slug)
    except Exception:
        feed_cfg = {}

    interval_minutes = feed_cfg.get('interval_minutes')
    if interval_minutes is None:
        interval_minutes = feed_cfg.get('refresh_interval_minutes', config.FETCH_INTERVAL_MINUTES)
    try:
        interval_minutes = int(interval_minutes)
    except Exception:
        interval_minutes = config.FETCH_INTERVAL_MINUTES

    last_fetched_timestamp = await db.execute('get_feed_last_fetched', feed_id=feed_id)
    current_time = int(time())

    if not last_fetched_timestamp:
        return False  # do not skip

    if not config.FORCE_REFRESH_FEEDS:
        elapsed = current_time - int(last_fetched_timestamp)
        threshold = interval_minutes * SECONDS_PER_MINUTE
        if elapsed < threshold:
            try:
                last_str = datetime.fromtimestamp(int(last_fetched_timestamp)).strftime('%Y-%m-%d %H:%M:%S')
            except Exception:
                last_str = str(last_fetched_timestamp)
            logger.info(
                "Skipping %s, fetched too recently (last: %s, interval: %sm)",
                slug,
                last_str,
                interval_minutes,
            )
            return True

    return False
