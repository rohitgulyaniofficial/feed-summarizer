"""HTTP fetch and conditional request helpers for feeds."""
from asyncio import TimeoutError
from typing import Optional

from aiohttp import ClientError, ClientSession

from config import config, get_logger
from utils.http_cache import (
    handle_rate_limit_response,
    prepare_request_headers,
    store_response_headers,
)
from workers.fetcher.constants import HTTP_NOT_MODIFIED, HTTP_OK, HTTP_TOO_MANY_REQUESTS
from workers.fetcher.proxy import compute_timeout, format_client_error, summarize_proxy
from workers.fetcher.backoff import handle_fetch_error

logger = get_logger("fetcher.http_fetch")


async def fetch_feed_content(
    db,
    feed_id: int,
    slug: str,
    url: str,
    session: ClientSession,
    retry_helper,
    proxy_url: Optional[str] = None,
) -> bytes | None:
    """Fetch feed content with retries, conditional headers, and error handling."""
    try:
        headers = await prepare_request_headers(db, feed_id, slug)
        timeout_seconds = compute_timeout(proxy_url)
        proxy_label = summarize_proxy(proxy_url) if proxy_url else None

        for attempt in range(config.MAX_RETRIES + 1):
            try:
                request_kwargs = {
                    'headers': headers,
                    'timeout': timeout_seconds,
                    'max_redirects': config.MAX_REDIRECTS,
                }
                if proxy_url:
                    request_kwargs['proxy'] = proxy_url
                    if attempt == 0 and proxy_label:
                        logger.info("Fetching feed %s via proxy %s", slug, proxy_label)

                async with session.get(url, **request_kwargs) as response:
                    if response.status == HTTP_NOT_MODIFIED:
                        logger.info("Feed %s not modified since last fetch", slug)
                        await db.execute('update_last_fetched', feed_id=feed_id)
                        await db.execute('reset_feed_error', feed_id=feed_id)
                        return None

                    if response.status == HTTP_TOO_MANY_REQUESTS:
                        await handle_rate_limit_response(db, feed_id, slug, response)
                        return None

                    if response.status != HTTP_OK:
                        error_msg = f"HTTP {response.status}"
                        logger.error("Error fetching %s: %s", slug, error_msg)
                        await handle_fetch_error(db, feed_id, error_msg)
                        return None

                    new_etag = response.headers.get('ETag')
                    new_last_modified = response.headers.get('Last-Modified')
                    content = await response.read()
                    await store_response_headers(db, feed_id, slug, new_etag, new_last_modified)
                    return content

            except TimeoutError as exc:
                logger.warning(
                    "Timeout fetching %s (attempt %d/%d, timeout=%ss): %s",
                    slug,
                    attempt + 1,
                    config.MAX_RETRIES,
                    config.HTTP_TIMEOUT,
                    exc,
                )
                if attempt < config.MAX_RETRIES:
                    await retry_helper.sleep_for_attempt(attempt)
                    continue
                await handle_fetch_error(db, feed_id, "Timed out")
                return None
            except ClientError as exc:
                detail = format_client_error(exc)
                if attempt < config.MAX_RETRIES:
                    logger.warning(
                        "Retry %d/%d for %s due to error: %s",
                        attempt + 1,
                        config.MAX_RETRIES,
                        slug,
                        detail,
                    )
                    await retry_helper.sleep_for_attempt(attempt)
                else:
                    error_msg = f"Failed to fetch after {config.MAX_RETRIES} retries ({detail})"
                    logger.error("%s: %s", error_msg, slug)
                    await handle_fetch_error(db, feed_id, error_msg)
                    return None

    except TimeoutError as exc:
        logger.error("Timeout while fetching feed %s: %s", slug, exc)
        await handle_fetch_error(db, feed_id, "Timeout")
        return None
    except ClientError as exc:
        detail = format_client_error(exc)
        logger.error("Error fetching feed %s: %s", slug, detail)
        await handle_fetch_error(db, feed_id, f"Network error: {detail}")
        return None
    except (OSError, RuntimeError, ValueError) as exc:
        logger.error("Unexpected error processing feed %s: %s", slug, exc)
        await handle_fetch_error(db, feed_id, f"Unexpected error: {exc}")
        return None
