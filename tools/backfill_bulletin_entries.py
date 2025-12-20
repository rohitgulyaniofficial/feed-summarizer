#!/usr/bin/env python3
"""Backfill persisted bulletin entries from existing bulletins.

This tool populates bulletin_entries for legacy bulletins so HTML and RSS can
reuse the same grouped payload without re-merging.
"""
import asyncio
from typing import Any, Dict, List

from config import config, get_logger
from models import DatabaseQueue
from workers.publisher.merge import collect_summary_links, summary_id_list

logger = get_logger("tools.backfill_bulletin_entries")


def _as_ts(value: Any) -> int:
    try:
        if hasattr(value, "timestamp"):
            return int(value.timestamp())
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
    except Exception:
        return None  # type: ignore[return-value]
    return None  # type: ignore[return-value]


def _build_entries(summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    entries: List[Dict[str, Any]] = []
    for summary in summaries:
        item_ts = _as_ts(summary.get("item_date"))
        links = summary.get("merged_links") or collect_summary_links(summary)
        entries.append(
            {
                "id": summary.get("id"),
                "summary_text": summary.get("summary_text"),
                "topic": summary.get("topic"),
                "merged_ids": summary.get("merged_ids") or summary_id_list(summary),
                "merged_links": links,
                "merged_count": summary.get("merged_count"),
                "item_date": item_ts,
                "published_date": summary.get("published_date") or item_ts,
                "item_title": summary.get("item_title") or summary.get("title"),
                "item_url": summary.get("item_url") or summary.get("url"),
                "feed_slug": summary.get("feed_slug"),
            }
        )
    return entries


async def main() -> None:
    db = DatabaseQueue(config.DATABASE_PATH)
    await db.start()
    try:
        bulletins = await db.execute("list_all_bulletins") or []
        updated = 0
        skipped = 0
        for b in bulletins:
            group_name = b.get("group_name")
            session_key = b.get("session_key")
            if not group_name or not session_key:
                continue
            bulletin = await db.execute("get_bulletin", group_name=group_name, session_key=session_key)
            if not bulletin:
                continue
            if bulletin.get("entries"):
                skipped += 1
                continue
            summaries = bulletin.get("summaries") or []
            if not summaries:
                skipped += 1
                continue
            entries = _build_entries(summaries)
            summary_ids: List[int] = []
            for s in summaries:
                summary_ids.extend(summary_id_list(s))
            await db.execute(
                "create_bulletin",
                group_name=group_name,
                session_key=session_key,
                introduction=bulletin.get("introduction") or "",
                summary_ids=summary_ids,
                feed_slugs=bulletin.get("feed_slugs") or [],
                title=bulletin.get("title"),
                entries=entries,
            )
            updated += 1
        logger.info("Backfill complete: %d updated, %d skipped", updated, skipped)
    finally:
        await db.stop()


if __name__ == "__main__":
    asyncio.run(main())
