"""HTTP caching helpers for feed fetching.

Handles conditional request headers and response header persistence.
"""
from datetime import timezone
from email.utils import parsedate_to_datetime, format_datetime
from typing import Optional

from aiohttp import ClientResponse

from config import config, get_logger
from models import DatabaseQueue

logger = get_logger("http_cache")


def _normalize_http_date(date_value: Optional[str]) -> Optional[str]:
    """Normalize HTTP date strings to RFC 7231 format (GMT)."""
    if not date_value:
        return None
    try:
        dt = parsedate_to_datetime(date_value)
        if not dt:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        dt = dt.astimezone(timezone.utc)
        return format_datetime(dt, usegmt=True)
    except (TypeError, ValueError, OverflowError) as exc:
        logger.debug("Unable to normalize HTTP date '%s': %s", date_value, exc)
        return None


def _quote_etag(etag: str) -> str:
    """Quote ETags that are not already quoted."""
    if etag.startswith("\"") or etag.startswith('W/"'):
        return etag
    return f'"{etag}"'


async def prepare_request_headers(db: DatabaseQueue, feed_id: int, slug: str) -> dict:
    """Prepare HTTP headers for conditional requests using stored metadata."""
    etag = await db.execute('get_feed_etag', feed_id=feed_id)
    last_modified = await db.execute('get_feed_last_modified', feed_id=feed_id)

    headers = {'User-Agent': config.USER_AGENT}

    if etag:
        headers['If-None-Match'] = _quote_etag(etag)
        logger.debug("Using If-None-Match: %s for %s", headers['If-None-Match'], slug)

    if last_modified:
        normalized_last_modified = _normalize_http_date(last_modified)
        if normalized_last_modified:
            headers['If-Modified-Since'] = normalized_last_modified
            logger.debug("Using If-Modified-Since: %s for %s", normalized_last_modified, slug)
        else:
            logger.warning(
                "Invalid Last-Modified format for %s, not sending header (stored value: %s)",
                slug,
                last_modified,
            )

    return headers


async def handle_rate_limit_response(db: DatabaseQueue, feed_id: int, slug: str, response: ClientResponse) -> None:
    """Handle 429 Too Many Requests response by persisting headers and tracking errors."""
    retry_after = response.headers.get("Retry-After")
    if retry_after:
        try:
            cooldown = int(retry_after)
        except ValueError:
            cooldown = config.MIN_COOLDOWN_PERIOD
    else:
        cooldown = config.MIN_COOLDOWN_PERIOD

    logger.warning("Received 429 Too Many Requests for %s, respecting Retry-After: %s seconds", slug, cooldown)

    etag = await db.execute('get_feed_etag', feed_id=feed_id)
    last_modified = await db.execute('get_feed_last_modified', feed_id=feed_id)

    await db.execute('update_feed_headers', feed_id=feed_id, etag=etag, last_modified=last_modified)

    error_info = await db.execute('get_feed_error_info', feed_id=feed_id)
    current_error_count = error_info.get('error_count', 0)
    if current_error_count > 0:
        await db.execute(
            'update_feed_error',
            feed_id=feed_id,
            error_count=current_error_count + 1,
            last_error=f"Rate limited (429), retry after {cooldown}s",
        )
    else:
        await db.execute(
            'update_feed_error',
            feed_id=feed_id,
            error_count=1,
            last_error=f"Rate limited (429), retry after {cooldown}s",
        )


async def store_response_headers(db: DatabaseQueue, feed_id: int, slug: str, new_etag: str | None, new_last_modified: str | None) -> None:
    """Store HTTP response headers for future conditional requests."""
    normalized_last_modified = _normalize_http_date(new_last_modified) if new_last_modified else None
    if new_etag or normalized_last_modified:
        await db.execute(
            'update_feed_headers',
            feed_id=feed_id,
            etag=new_etag,
            last_modified=normalized_last_modified,
        )
        if new_etag:
            logger.debug("Stored new ETag: %s for %s", new_etag, slug)
        if normalized_last_modified:
            logger.debug("Stored new Last-Modified: %s for %s", normalized_last_modified, slug)
