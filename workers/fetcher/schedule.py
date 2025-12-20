"""Scheduling helpers for the feed fetcher."""
from time import time
from asyncio import CancelledError, Semaphore, create_task, gather, sleep
from typing import List, Optional

from aiohttp import ClientSession

from config import config, get_logger
from workers.fetcher.constants import (
    DAILY_REPORT_THRESHOLD_SECONDS,
    HOUR_IN_SECONDS,
    MAIN_LOOP_INTERVAL_HOURS,
    MAX_BACKOFF_HOURS,
)

logger = get_logger("fetcher.schedule")


async def fetch_all_feeds(fetcher, only_slugs: Optional[List[str]] = None) -> None:
    """Fetch configured feeds concurrently with rate limiting."""
    logger.info("Starting feed fetching")

    async with ClientSession() as session:
        semaphore = Semaphore(5)

        async def fetch_with_semaphore(slug: str, url: str) -> None:
            async with semaphore:
                await fetcher.fetch_feed(slug, url, session)

        tasks = []
        for slug, url in config.FEED_SOURCES.items():
            if only_slugs is not None and slug not in only_slugs:
                continue
            tasks.append(create_task(fetch_with_semaphore(slug, url)))

        await gather(*tasks, return_exceptions=True)


async def run_daily_maintenance(fetcher) -> None:
    try:
        logger.info("Running daily database maintenance - checking for expired entries")
        expired_count = await fetcher.db.execute('expire_old_entries', expiration_days=config.ENTRY_EXPIRATION_DAYS)
        if expired_count > 0:
            logger.info("Expired %d old entries from the database", expired_count)
    except (OSError, RuntimeError) as exc:
        logger.error("Error during database maintenance: %s", exc)


async def main_async() -> None:
    """Main async function to run the feed fetcher continuously."""
    fetcher = None
    try:
        from workers.fetcher import FeedFetcher  # local import to avoid cycle
        fetcher = FeedFetcher()
        await fetcher.initialize()

        try:
            logger.info("Running database maintenance - checking for expired entries")
            expired_count = await fetcher.db.execute('expire_old_entries', expiration_days=config.ENTRY_EXPIRATION_DAYS)
            if expired_count > 0:
                logger.info("Expired %d old entries from the database", expired_count)
        except (OSError, RuntimeError) as exc:
            logger.error("Error during database maintenance: %s", exc)

        while True:
            await fetch_all_feeds(fetcher)

            if int(time()) % (MAX_BACKOFF_HOURS * HOUR_IN_SECONDS) < DAILY_REPORT_THRESHOLD_SECONDS:
                await run_daily_maintenance(fetcher)

            main_loop_interval_seconds = MAIN_LOOP_INTERVAL_HOURS * HOUR_IN_SECONDS
            logger.info("Sleeping for %s seconds until next run", main_loop_interval_seconds)
            await sleep(main_loop_interval_seconds)

    except CancelledError:
        logger.info("Feed fetcher task was cancelled")
    except (OSError, RuntimeError, ValueError) as exc:
        logger.error("Unexpected error in main async loop: %s", exc)
    finally:
        if fetcher:
            await fetcher.close()


async def main_async_single_run() -> None:
    """Run the fetcher once."""
    from workers.fetcher import FeedFetcher  # local import to avoid cycle

    fetcher = FeedFetcher()
    try:
        await fetcher.initialize()
        await fetch_all_feeds(fetcher)
        await run_daily_maintenance(fetcher)
    except CancelledError:
        logger.info("Feed fetcher task was cancelled")
    except (OSError, RuntimeError, ValueError) as exc:
        logger.error("Unexpected error in fetcher: %s", exc)
    finally:
        await fetcher.close()
