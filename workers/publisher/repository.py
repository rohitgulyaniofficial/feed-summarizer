"""Repository helpers for publisher database access."""
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from config import get_logger

logger = get_logger("publisher.repository")


async def get_latest_bulletin_title(db, group_name: str, days_back: int = 30) -> Optional[str]:
    """Return the most recent non-empty stored title for a group's bulletins."""
    try:
        bulletins = await db.execute("get_bulletins_for_group", group_name=group_name, days_back=days_back)
        if bulletins:
            for bulletin in bulletins:
                try:
                    title = (bulletin.get("title") or "").strip() if isinstance(bulletin, dict) else None
                except Exception:
                    title = None
                if title:
                    return title
    except Exception as exc:
        logger.debug("Failed to read latest bulletin title for %s: %s", group_name, exc)
        return None
    return None


async def _get_cached_bulletins(db, group_name: str, days_back: int) -> Dict[str, List[Dict[str, Any]]]:
    """Fetch cached bulletins (with summaries) for a group within a window."""
    bulletins_found: Dict[str, List[Dict[str, Any]]] = {}
    bulletins = await db.execute("get_bulletins_for_group", group_name=group_name, days_back=days_back)
    for bulletin_meta in bulletins or []:
        session_key = bulletin_meta.get("session_key")
        if not session_key:
            continue
        bulletin_data = await db.execute(
            "get_bulletin",
            group_name=group_name,
            session_key=session_key,
        )
        entries = bulletin_data.get("entries") if bulletin_data else None
        if entries:
            bulletins_found[session_key] = entries
            logger.debug(
                "Found cached bulletin for %s/%s with %d entries",
                group_name,
                session_key,
                len(entries),
            )
        elif bulletin_data and bulletin_data.get("summaries"):
            bulletins_found[session_key] = bulletin_data["summaries"]
            logger.debug(
                "Found legacy cached bulletin for %s/%s with %d summaries",
                group_name,
                session_key,
                len(bulletin_data["summaries"]),
            )
    return bulletins_found


async def load_published_summaries_by_date(
    db,
    group_name: str,
    feed_slugs: List[str],
    days_back: int = 7,
) -> Dict[str, List[Dict[str, Any]]]:
    """Get published summaries grouped by publication sessions.

    Prefers cached bulletins, falls back to grouping raw summaries by publish time.
    """
    if not feed_slugs:
        return {}

    try:
        cached = await _get_cached_bulletins(db, group_name, days_back)
    except Exception as exc:
        logger.warning("Error loading cached bulletins for %s: %s", group_name, exc)
        cached = {}

    if cached:
        logger.info("Using %d cached bulletins for group '%s'", len(cached), group_name)
        return cached

    logger.info("No cached bulletins for '%s'; generating from raw summaries", group_name)
    cutoff_time = int((datetime.now(timezone.utc) - timedelta(days=days_back)).timestamp())

    summaries = await db.execute(
        "query_published_summaries_by_date",
        feed_slugs=feed_slugs,
        cutoff_time=cutoff_time,
    )

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for summary in summaries or []:
        published_ts = summary.get("published_date")
        if not published_ts:
            continue
        try:
            pub_date = datetime.fromtimestamp(published_ts, tz=timezone.utc)
            session_key = pub_date.strftime("%Y-%m-%d-%H-%M")
            grouped.setdefault(session_key, []).append(summary)
        except Exception:
            continue

    max_summaries_per_item = 25
    final_grouped: Dict[str, List[Dict[str, Any]]] = {}
    for session_key, session_summaries in grouped.items():
        if len(session_summaries) <= max_summaries_per_item:
            final_grouped[session_key] = session_summaries
        else:
            for index in range(0, len(session_summaries), max_summaries_per_item):
                chunk = session_summaries[index : index + max_summaries_per_item]
                chunk_key = f"{session_key}-{index // max_summaries_per_item + 1}"
                final_grouped[chunk_key] = chunk

    return final_grouped


async def get_passthrough_feed_meta(db, slug: str) -> Dict[str, Any]:
    """Return feed metadata for a slug (empty dict if missing)."""
    try:
        meta = await db.execute("get_feed_by_slug", slug=slug)
        return meta or {}
    except Exception as exc:
        logger.debug("Failed to load feed meta for %s: %s", slug, exc)
        return {}


async def get_passthrough_items(db, slug: str, limit: int) -> List[Dict[str, Any]]:
    """Return latest items for passthrough feed (empty list on error)."""
    try:
        items = await db.execute("query_latest_items_for_feed", slug=slug, limit=limit)
        return items or []
    except Exception as exc:
        logger.debug("Failed to load items for %s: %s", slug, exc)
        return []


async def cache_passthrough_rss(db, slug: str, xml: str) -> None:
    """Persist passthrough RSS content (best-effort)."""
    try:
        await db.execute("cache_passthrough_rss", slug=slug, xml=xml)
    except Exception as exc:
        logger.debug("Failed to cache passthrough RSS for %s: %s", slug, exc)


async def get_bulletin_metadata(db, group_name: str, session_key: str) -> Dict[str, Any]:
    """Fetch bulletin metadata (introduction/title/summaries) for a session."""
    try:
        data = await db.execute(
            "get_bulletin",
            group_name=group_name,
            session_key=session_key,
        )
        return data or {}
    except Exception as exc:
        logger.debug("No cached bulletin %s/%s: %s", group_name, session_key, exc)
        return {}


async def cache_bulletin_introduction(
    db,
    group_name: str,
    session_key: str,
    introduction: str,
    summary_ids: List[int],
    feed_slugs: List[str],
) -> None:
    """Persist introduction for a bulletin if not already cached."""
    try:
        await db.execute(
            "create_bulletin",
            group_name=group_name,
            session_key=session_key,
            introduction=introduction,
            summary_ids=summary_ids,
            feed_slugs=feed_slugs,
        )
    except Exception as exc:
        logger.debug("Failed to cache introduction for %s/%s: %s", group_name, session_key, exc)


async def update_bulletin_title(db, group_name: str, session_key: str, title: str) -> None:
    """Backfill bulletin title if absent."""
    try:
        await db.execute(
            "update_bulletin_title",
            group_name=group_name,
            session_key=session_key,
            title=title,
        )
    except Exception as exc:
        logger.debug("Failed to backfill title for %s/%s: %s", group_name, session_key, exc)


__all__ = [
    "get_latest_bulletin_title",
    "load_published_summaries_by_date",
    "get_passthrough_feed_meta",
    "get_passthrough_items",
    "cache_passthrough_rss",
    "get_bulletin_metadata",
    "cache_bulletin_introduction",
    "update_bulletin_title",
]
