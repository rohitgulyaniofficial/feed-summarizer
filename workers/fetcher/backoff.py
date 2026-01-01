"""Backoff and error-handling helpers for feed fetching."""
from time import time

from config import get_logger
from models import DatabaseQueue
from workers.fetcher.constants import HOUR_IN_SECONDS, MAX_BACKOFF_HOURS

logger = get_logger("fetcher.backoff")


def calculate_backoff_delay(error_count: int) -> float:
    """Calculate exponential backoff delay based on error count (capped)."""
    if error_count <= 0:
        return 0
    base_delay = HOUR_IN_SECONDS
    delay = base_delay * (2 ** (error_count - 1))
    max_delay = MAX_BACKOFF_HOURS * HOUR_IN_SECONDS
    return min(delay, max_delay)


async def should_fetch_feed(db: DatabaseQueue, feed_id: int) -> bool:
    """Determine if a feed should be fetched based on error history and timing."""
    try:
        error_info = await db.execute('get_feed_error_info', feed_id=feed_id)
        error_count = error_info.get('error_count', 0)

        if error_count == 0:
            return True

        last_fetched = await db.execute('get_feed_last_fetched', feed_id=feed_id)
        if not last_fetched:
            return True

        backoff_delay = calculate_backoff_delay(error_count)
        time_since_last_fetch = int(time()) - last_fetched
        should_fetch = time_since_last_fetch >= backoff_delay

        if not should_fetch:
            remaining_time = backoff_delay - time_since_last_fetch
            logger.info(
                "Feed ID %s in backoff (error count: %s), next attempt in %.1f hours",
                feed_id,
                error_count,
                remaining_time / HOUR_IN_SECONDS,
            )
        return should_fetch
    except (OSError, RuntimeError) as exc:
        logger.error("Error checking if feed %s should be fetched: %s", feed_id, exc)
        return True


async def handle_fetch_error(db: DatabaseQueue, feed_id: int, error_message: str) -> None:
    """Handle feed fetch errors by updating error tracking and last_fetched timestamp."""
    try:
        error_info = await db.execute('get_feed_error_info', feed_id=feed_id)
        current_error_count = error_info.get('error_count', 0)
        new_error_count = current_error_count + 1

        await db.execute(
            'update_feed_error',
            feed_id=feed_id,
            error_count=new_error_count,
            last_error=error_message,
        )

        backoff_delay = calculate_backoff_delay(new_error_count)
        logger.warning(
            "Feed ID %s error count increased to %s. Next attempt in %.1f hours",
            feed_id,
            new_error_count,
            backoff_delay / HOUR_IN_SECONDS,
        )
    except (OSError, RuntimeError) as exc:
        logger.error("Error updating feed error tracking for feed ID %s: %s", feed_id, exc)
