"""Entry processing helpers for feed fetcher."""
from time import time
from asyncio import Semaphore
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set

from aiohttp import ClientError, ClientSession

from config import config, get_logger
from models import DatabaseQueue
from services.telemetry import trace_span
from utils import RateLimiter
from workers.fetcher.constants import HOUR_IN_SECONDS
from workers.fetcher.content import extract_content, get_guid, normalize_entry_identity
from workers.fetcher.dates import format_timestamp, parse_date_enhanced

logger = get_logger("fetcher.entries")


async def _process_entries_with_reader_mode(
    db: DatabaseQueue,
    feed_id: int,
    slug: str,
    raw_entries: list,
    existing_guids: set[str],
    existing_urls_global: set[str],
    post_process: bool,
    reader_mode: bool,
    reader_rate_limiter: RateLimiter,
    fetch_original_content: Callable[[str, ClientSession, Optional[str]], Awaitable[Optional[str]]],
    reader_semaphore: Semaphore,
    session: Optional[ClientSession],
    proxy_url: Optional[str],
    is_initial_fetch: bool,
    initial_bootstrap_limit: int,
) -> int:
    entries_data: List[Dict[str, Any]] = []
    reader_mode_count = 0
    processed_new_items = 0
    skipped_existing_items = 0
    skipped_outside_window = 0
    bootstrap_allowed_items = 0
    existing_guid_matches = 0
    existing_url_matches = 0
    total_new_items = 0
    outside_window_examples: List[str] = []
    duplicate_examples: List[str] = []
    new_entry_examples: List[str] = []

    time_window_seconds = config.TIME_WINDOW_HOURS * HOUR_IN_SECONDS
    cutoff_timestamp = int(time()) - time_window_seconds
    logger.info(
        "%s: evaluating %d entries (cutoff=%s, window=%sh, initial_fetch=%s)",
        slug,
        len(raw_entries),
        format_timestamp(cutoff_timestamp),
        config.TIME_WINDOW_HOURS,
        is_initial_fetch,
    )
    if raw_entries:
        newest_entry = max(raw_entries, key=lambda entry: entry['pub_date'])
        oldest_entry = min(raw_entries, key=lambda entry: entry['pub_date'])
        span_hours = (newest_entry['pub_date'] - oldest_entry['pub_date']) / HOUR_IN_SECONDS
        logger.info(
            "%s: entry timespan %s → %s (≈%.1fh)",
            slug,
            format_timestamp(oldest_entry['pub_date']),
            format_timestamp(newest_entry['pub_date']),
            span_hours,
        )

    bootstrap_guids: Set[str] = set()
    if is_initial_fetch and initial_bootstrap_limit > 0:
        sorted_entries = sorted(raw_entries, key=lambda entry: entry['pub_date'], reverse=True)
        bootstrap_guids = {entry['guid'] for entry in sorted_entries[:initial_bootstrap_limit]}
        logger.debug("Bootstrap allowance prepared for %d entries on %s", len(bootstrap_guids), slug)

    for i, raw_entry in enumerate(raw_entries):
        title = raw_entry['title']
        url = raw_entry['url']
        guid = raw_entry['guid']
        body = raw_entry['body']
        pub_date = raw_entry['pub_date']

        is_recent_enough = pub_date >= cutoff_timestamp
        allow_bootstrap = is_initial_fetch and guid in bootstrap_guids
        if not is_recent_enough and not allow_bootstrap:
            skipped_outside_window += 1
            if len(outside_window_examples) < 5:
                outside_window_examples.append(f"{title[:80]} @ {format_timestamp(pub_date)}")
            continue
        if allow_bootstrap and not is_recent_enough:
            bootstrap_allowed_items += 1

        guid_known = guid in existing_guids
        url_known_global = url in existing_urls_global
        is_new_item = not (guid_known or url_known_global)
        if not is_new_item and url_known_global and not guid_known and len(duplicate_examples) < 5:
            duplicate_examples.append(f"{title[:80]} (url dup) @ {format_timestamp(pub_date)}")

        if reader_mode and is_new_item:
            try:
                await reader_rate_limiter.acquire()
                async with reader_semaphore:
                    logger.info(f"Applying reader mode for NEW entry: {title}")
                    full_content: Optional[str] = None
                    if not session:
                        logger.error(
                            "Reader mode requested for %s but no HTTP session is available; skipping full fetch for %s",
                            slug,
                            url,
                        )
                    else:
                        full_content = await fetch_original_content(url, session, proxy_url)
                    if full_content:
                        body = full_content
                        logger.info("Successfully extracted full content for: %s", title)
                        reader_mode_count += 1
            except (ClientError, OSError, ValueError, RuntimeError) as exc:
                logger.error("Error applying reader mode for %s: %s", url, exc)
        elif reader_mode and not is_new_item:
            logger.debug("Skipping reader mode for existing item: %s", title)

        if post_process:
            logger.info("Applying post-processing for entry: %s", title)

        if is_new_item:
            processed_new_items += 1
            title = title[:255] if title else "No Title"
            url = url[:2048] if url else ""
            guid = guid[:64] if guid else ""
            if not body or not body.strip():
                body = "<p>No content available</p>"

            entries_data.append(
                {
                    'title': title,
                    'url': url,
                    'guid': guid,
                    'body': body,
                    'date': pub_date,
                }
            )
            if len(new_entry_examples) < 5:
                new_entry_examples.append(f"{title[:80]} @ {format_timestamp(pub_date)}")
        else:
            skipped_existing_items += 1
            if guid_known:
                existing_guid_matches += 1
            if url_known_global:
                existing_url_matches += 1
            if len(duplicate_examples) < 5 and not url_known_global:
                duplicate_examples.append(f"{title[:80]} (guid dup) @ {format_timestamp(pub_date)}")

        if len(entries_data) >= config.SAVE_BATCH_SIZE or i == len(raw_entries) - 1:
            if entries_data:
                new_items = await db.execute('save_items', feed_id=feed_id, entries_data=entries_data)
                total_new_items += new_items
                logger.info("Saved batch of %d NEW entries from %s", len(entries_data), slug)
                entries_data = []

    if entries_data:
        new_items = await db.execute('save_items', feed_id=feed_id, entries_data=entries_data)
        total_new_items += new_items
        logger.info("Saved final batch of %d NEW entries from %s", len(entries_data), slug)
        entries_data = []

    if reader_mode:
        logger.info("Applied reader mode to %d new items from %s", reader_mode_count, slug)
    logger.info(
        "%s summary: processed=%d new_candidates=%d saved=%d existing=%d (guid=%d url=%d) outside_window=%d bootstrap=%d",
        slug,
        len(raw_entries),
        processed_new_items,
        total_new_items,
        skipped_existing_items,
        existing_guid_matches,
        existing_url_matches,
        skipped_outside_window,
        bootstrap_allowed_items,
    )
    if outside_window_examples:
        logger.info("%s outside-window samples: %s", slug, "; ".join(outside_window_examples))
    if duplicate_examples:
        logger.info("%s duplicate samples: %s", slug, "; ".join(duplicate_examples))
    if new_entry_examples:
        logger.info("%s new entry samples: %s", slug, "; ".join(new_entry_examples))
    return total_new_items


@trace_span(
    "process_feed_entries",
    tracer_name="fetcher",
    attr_from_args=lambda db, feed_id, slug, entries, **_: {
        "feed.id": int(feed_id) if feed_id else 0,
        "feed.slug": slug,
        "feed.entries.count": len(entries) if entries else 0,
    },
)
async def process_feed_entries(
    db: DatabaseQueue,
    feed_id: int,
    slug: str,
    entries: list,
    post_process: bool,
    reader_mode: bool,
    reader_rate_limiter: RateLimiter,
    fetch_original_content: Callable[[str, ClientSession, Optional[str]], Awaitable[Optional[str]]],
    session: Optional[ClientSession],
    proxy_url: Optional[str],
) -> int:
    """Process and save feed entries with reader mode support."""
    if reader_mode:
        rate_interval = 60.0 / config.READER_MODE_REQUESTS_PER_MINUTE if config.READER_MODE_REQUESTS_PER_MINUTE > 0 else 0
        logger.info(
            "Reader mode rate limit: %s requests per minute (one request every %.2f seconds)",
            config.READER_MODE_REQUESTS_PER_MINUTE,
            rate_interval,
        )

    reader_semaphore = Semaphore(config.READER_MODE_CONCURRENCY)

    raw_entries = []
    all_guids: List[str] = []
    all_urls: List[str] = []

    for entry in entries:
        if 'link' not in entry or not entry.link or not entry.link.startswith(('http://', 'https://')):
            logger.warning("Skipping entry with invalid link in %s", slug)
            continue

        title = entry.get('title', 'No Title')
        url = entry.link
        guid = get_guid(entry)
        body = extract_content(entry)
        pub_date = parse_date_enhanced(entry)
        try:
            if not isinstance(pub_date, int) or pub_date <= 0:
                pub_date = int(time())
        except Exception:
            pub_date = int(time())
        title, url, guid = normalize_entry_identity(title, url, guid)

        raw_entries.append(
            {
                'entry': entry,
                'title': title,
                'url': url,
                'guid': guid,
                'body': body,
                'pub_date': pub_date,
            }
        )
        all_guids.append(guid)
        all_urls.append(url)

    existing_guids = await db.execute('check_existing_guids', feed_id=feed_id, guids=all_guids)
    logger.info("Found %d existing items out of %d total items for %s", len(existing_guids), len(all_guids), slug)

    is_initial_fetch = len(existing_guids) == 0
    if is_initial_fetch:
        logger.info(
            "Initial fetch detected for %s - will allow up to %d most recent items regardless of time window",
            slug,
            config.INITIAL_FETCH_ITEM_LIMIT,
        )

    existing_urls_global = await db.execute('check_existing_urls', urls=all_urls)
    if existing_urls_global:
        logger.info(
            "Cross-feed dedup: %d URLs already stored globally will be treated as existing for %s",
            len(existing_urls_global),
            slug,
        )

    total_new_items = await _process_entries_with_reader_mode(
        db,
        feed_id,
        slug,
        raw_entries,
        existing_guids,
        existing_urls_global,
        post_process,
        reader_mode,
        reader_rate_limiter,
        fetch_original_content,
        reader_semaphore,
        session,
        proxy_url,
        is_initial_fetch,
        config.INITIAL_FETCH_ITEM_LIMIT,
    )

    if total_new_items > 0:
        try:
            deleted = await db.execute('prune_items_per_feed', feed_id=feed_id, max_items=config.MAX_ITEMS_PER_FEED)
            if deleted:
                logger.info("Pruned %d old items for %s (kept newest %s)", deleted, slug, config.MAX_ITEMS_PER_FEED)
        except Exception as exc:
            logger.debug("Prune skipped for %s: %s", slug, exc)

    return total_new_items
