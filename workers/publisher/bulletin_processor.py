"""Bulletin processing helper for publisher."""
from datetime import datetime, timezone
from typing import Any, Awaitable, Callable, Dict, List, Optional

from aiohttp import ClientSession

from config import config, get_logger
from workers.publisher.merge import collect_summary_links, merge_similar_summaries, summary_id_list
from workers.publisher.html_renderer import generate_bulletin_html
from workers.publisher.titles import with_session_intro_and_title
from workers.publisher.recurring import detect_recurring_coverage

logger = get_logger("publisher.bulletin_processor")


async def process_bulletin_chunk(
    *,
    group_name: str,
    feed_slugs: List[str],
    summaries: List[Dict[str, Any]],
    enable_intro: bool,
    render_html: bool,
    chunk_index: int,
    prompts: Dict[str, Any],
    db,
    html_bulletins_dir,
    generate_markdown_bulletin: Callable[[List[Dict[str, Any]]], str],
    generate_ai_introduction: Callable[[str, ClientSession], Awaitable[Optional[str]]],
    generate_ai_title: Callable[[str, ClientSession], Awaitable[Optional[str]]],
    generate_title_from_introduction: Callable[[str, str, str], str],
    mark_summaries_as_published: Callable[[List[int]], Awaitable[int]],
    ai_chat_completion: Callable[..., Awaitable[Optional[str]]],
) -> int:
    """Render/write/capture a single bulletin chunk and persist metadata."""
    if not summaries:
        return 0

    summaries = await merge_similar_summaries(
        summaries,
        prompts,
        db,
        ai_chat_completion,
    )

    # Detect recurring coverage from past week's bulletins
    recurring_days_back = int(getattr(config, "RECURRING_COVERAGE_DAYS_BACK", 7) or 7)
    recurring_ids = await detect_recurring_coverage(
        summaries,
        group_name,
        db,
        days_back=recurring_days_back,
    )
    
    # Update topics for recurring summaries
    if recurring_ids:
        recurring_topic = getattr(config, "RECURRING_COVERAGE_TOPIC", "Recurring Coverage")
        recurring_id_set = set(recurring_ids)
        for s in summaries:
            summary_id = s.get("id")
            if isinstance(summary_id, (int, str)) and int(summary_id) in recurring_id_set:
                original_topic = s.get("topic", "General")
                s["topic"] = recurring_topic
                logger.debug(
                    "Summary %s reassigned from '%s' to '%s' (recurring coverage)",
                    summary_id,
                    original_topic,
                    recurring_topic,
                )

    for s in summaries:
        try:
            d = s.get("item_date")
            if d:
                if hasattr(d, "strftime"):
                    continue
                if isinstance(d, (int, float)):
                    s["item_date"] = datetime.fromtimestamp(int(d), tz=timezone.utc)
                    continue
                if isinstance(d, str):
                    ds = d.strip()
                    ds_try = ds[:-1] if ds.endswith("Z") else ds
                    parsed = None
                    for fmt in ("%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
                        try:
                            parsed = datetime.strptime(ds_try, fmt).replace(tzinfo=timezone.utc)
                            break
                        except Exception:
                            continue
                    if not parsed:
                        try:
                            parsed = datetime.fromisoformat(ds_try)
                            if parsed.tzinfo is None:
                                parsed = parsed.replace(tzinfo=timezone.utc)
                        except Exception:
                            parsed = None
                    s["item_date"] = parsed
                else:
                    s["item_date"] = None
        except Exception:
            try:
                s["item_date"] = None
            except Exception:
                pass

    try:
        by_topic = {}
        by_feed = {}
        for s in summaries:
            t = s.get("topic") or "General"
            by_topic[t] = by_topic.get(t, 0) + 1
            f = s.get("feed_slug") or ""
            if f:
                by_feed[f] = by_feed.get(f, 0) + 1
        topic_info = ", ".join([f"{k}:{v}" for k, v in sorted(by_topic.items())])
        feed_info = ", ".join([f"{k}:{v}" for k, v in sorted(by_feed.items())])
        logger.info(
            "Bulletin '%s' chunk #%d includes %d summaries across %d topic(s): %s",
            group_name,
            chunk_index + 1,
            len(summaries),
            len(by_topic),
            topic_info,
        )
        if by_feed:
            logger.debug("Bulletin '%s' chunk #%d feed distribution: %s", group_name, chunk_index + 1, feed_info)
        if len(by_topic) == 1 and len(summaries) <= 2:
            logger.warning(
                "Bulletin '%s' chunk #%d appears small (topics=%d, items=%d).",
                group_name,
                chunk_index + 1,
                len(by_topic),
                len(summaries),
            )
    except Exception:
        pass

    session_base = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H-%M")
    provisional_session_key = session_base if chunk_index == 0 else f"{session_base}-B{chunk_index + 1}"

    introduction: Optional[str] = None
    ai_title: Optional[str] = None
    if config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
        try:
            markdown_bulletin = generate_markdown_bulletin(summaries)
            introduction, ai_title = await with_session_intro_and_title(
                markdown_bulletin=markdown_bulletin,
                summaries=summaries,
                group_name=group_name,
                session_key=provisional_session_key,
                enable_intro=enable_intro,
                prompts=prompts,
                generate_ai_introduction=generate_ai_introduction,
                generate_ai_title=generate_ai_title,
                ai_chat_completion_fn=ai_chat_completion,
                generate_title_from_introduction=generate_title_from_introduction,
                generate_title=True,
            )
            if introduction:
                logger.info(
                    "Generated AI introduction for '%s' chunk #%d (%d characters)",
                    group_name,
                    chunk_index + 1,
                    len(introduction),
                )
            if ai_title:
                logger.info(
                    "Generated AI title for '%s' chunk #%d: '%s'",
                    group_name,
                    chunk_index + 1,
                    ai_title[:120],
                )
        except Exception as exc:
            logger.error("Error generating AI intro/title for '%s' chunk #%d: %s", group_name, chunk_index + 1, exc)

    final_title = ai_title
    if not final_title:
        try:
            concat_titles = [
                (s.get("item_title") or s.get("title", "")).strip()
                for s in summaries
                if (s.get("item_title") or s.get("title"))
            ]
            if concat_titles:
                heuristic = ", ".join(concat_titles[:5])[:120]
                final_title = f"{group_name.title()}: {heuristic}".rstrip(" ,")
        except Exception:
            pass
        if not final_title:
            try:
                final_title = generate_title_from_introduction(
                    introduction or "",
                    group_name,
                    provisional_session_key,
                )
            except Exception:
                final_title = f"{group_name.title()} Bulletin"

    if render_html:
        html_content = generate_bulletin_html(
            group_name,
            feed_slugs,
            summaries,
            introduction,
            final_title,
        )
        output_file = html_bulletins_dir / f"{group_name}.html"
        with open(output_file, "w", encoding="utf-8") as f:
            f.write(html_content)
        logger.info(
            "Wrote HTML bulletin for '%s' chunk #%d with %d summaries",
            group_name,
            chunk_index + 1,
            len(summaries),
        )

    entries_payload: List[Dict[str, Any]] = []
    for summary in summaries:
        try:
            item_date_val = summary.get("item_date")
            if hasattr(item_date_val, "timestamp"):
                item_ts = int(item_date_val.timestamp())
            elif isinstance(item_date_val, (int, float)):
                item_ts = int(item_date_val)
            elif isinstance(item_date_val, str) and item_date_val.isdigit():
                item_ts = int(item_date_val)
            else:
                item_ts = None
        except Exception:
            item_ts = None

        entry_links = summary.get("merged_links") or collect_summary_links(summary)
        feed_slug = summary.get("feed_slug")
        entries_payload.append(
            {
                "id": summary.get("id"),
                "summary_text": summary.get("summary_text"),
                "topic": summary.get("topic"),
                "merged_ids": summary.get("merged_ids") or summary_id_list(summary),
                "merged_links": entry_links,
                "merged_count": summary.get("merged_count"),
                "item_date": item_ts,
                "published_date": summary.get("published_date") or item_ts,
                "item_title": summary.get("item_title") or summary.get("title"),
                "item_url": summary.get("item_url") or summary.get("url"),
                "feed_slug": feed_slug,
                "feed_label": config.FEED_LABELS.get(feed_slug, feed_slug) if feed_slug else None,
            }
        )

    summary_ids: List[int] = []
    for summary in summaries:
        summary_ids.extend(summary_id_list(summary))
    existing_bulletins = await db.execute(
        "find_bulletin_sessions_for_summaries",
        group_name=group_name,
        summary_ids=summary_ids,
    )
    if existing_bulletins:
        logger.info(
            "Summaries for group '%s' chunk #%d already exist in %d bulletin(s) - skipping creation",
            group_name,
            chunk_index + 1,
            len(existing_bulletins),
        )
        return 0

    published_count = await mark_summaries_as_published(summary_ids)
    if not summary_ids or published_count <= 0:
        logger.warning(
            "No bulletin created for '%s' chunk #%d - no summaries were marked as published",
            group_name,
            chunk_index + 1,
        )
        return 0

    session_key = provisional_session_key
    max_summaries_per_bulletin = 25
    if len(summary_ids) <= max_summaries_per_bulletin:
        await db.execute(
            "create_bulletin",
            group_name=group_name,
            session_key=session_key,
            introduction=introduction or "",
            summary_ids=summary_ids,
            feed_slugs=feed_slugs,
            title=final_title,
            entries=entries_payload,
        )
        logger.info(
            "Created bulletin record for '%s' session '%s' with %d summaries",
            group_name,
            session_key,
            len(summary_ids),
        )
    else:
        for i in range(0, len(summary_ids), max_summaries_per_bulletin):
            chunk_ids = summary_ids[i : i + max_summaries_per_bulletin]
            chunk_key = f"{session_key}-{i // max_summaries_per_bulletin + 1}"
            chunk_summaries = [
                s
                for s in summaries
                if any(source_id in chunk_ids for source_id in summary_id_list(s))
            ]
            chunk_intro = ""
            if enable_intro and config.AZURE_ENDPOINT and config.OPENAI_API_KEY and chunk_summaries:
                try:
                    chunk_markdown = generate_markdown_bulletin(chunk_summaries)
                    async with ClientSession() as session:
                        chunk_intro = await generate_ai_introduction(chunk_markdown, session) or ""
                except Exception as exc:
                    logger.error(
                        "Error generating AI introduction for '%s' chunk %s: %s",
                        group_name,
                        chunk_key,
                        exc,
                    )
            chunk_title = None
            if config.AZURE_ENDPOINT and config.OPENAI_API_KEY and chunk_summaries:
                try:
                    async with ClientSession() as session:
                        chunk_markdown = generate_markdown_bulletin(chunk_summaries)
                        chunk_title = await generate_ai_title(chunk_markdown, session)
                except Exception as exc:
                    logger.debug(
                        "AI title generation failed for chunk %s: %s",
                        chunk_key,
                        exc,
                    )
            if not chunk_title:
                try:
                    chunk_titles = [
                        (s.get("item_title") or s.get("title", "")).strip()
                        for s in chunk_summaries
                        if (s.get("item_title") or s.get("title"))
                    ]
                    if chunk_titles:
                        heuristic = ", ".join(chunk_titles[:5])[:120]
                        chunk_title = f"{group_name.title()}: {heuristic}".rstrip(" ,")
                except Exception:
                    pass
                if not chunk_title:
                    try:
                        chunk_title = generate_title_from_introduction(chunk_intro or "", group_name, chunk_key)
                    except Exception:
                        chunk_title = f"{group_name.title()} Bulletin #{chunk_key.split('-')[-1]}"
            chunk_entries: List[Dict[str, Any]] = []
            for summary in chunk_summaries:
                try:
                    item_date_val = summary.get("item_date")
                    if hasattr(item_date_val, "timestamp"):
                        item_ts = int(item_date_val.timestamp())
                    elif isinstance(item_date_val, (int, float)):
                        item_ts = int(item_date_val)
                    elif isinstance(item_date_val, str) and item_date_val.isdigit():
                        item_ts = int(item_date_val)
                    else:
                        item_ts = None
                except Exception:
                    item_ts = None
                chunk_links = summary.get("merged_links") or collect_summary_links(summary)
                feed_slug = summary.get("feed_slug")
                chunk_entries.append(
                    {
                        "id": summary.get("id"),
                        "summary_text": summary.get("summary_text"),
                        "topic": summary.get("topic"),
                        "merged_ids": summary.get("merged_ids") or summary_id_list(summary),
                        "merged_links": chunk_links,
                        "merged_count": summary.get("merged_count"),
                        "item_date": item_ts,
                        "published_date": summary.get("published_date") or item_ts,
                        "item_title": summary.get("item_title") or summary.get("title"),
                        "item_url": summary.get("item_url") or summary.get("url"),
                        "feed_slug": feed_slug,
                        "feed_label": config.FEED_LABELS.get(feed_slug, feed_slug) if feed_slug else None,
                    }
                )

            await db.execute(
                "create_bulletin",
                group_name=group_name,
                session_key=chunk_key,
                introduction=chunk_intro or "",
                summary_ids=chunk_ids,
                feed_slugs=feed_slugs,
                title=chunk_title,
                entries=chunk_entries,
            )
            logger.info(
                "Created bulletin record for '%s' session '%s' with %d summaries",
                group_name,
                chunk_key,
                len(chunk_ids),
            )

    return len(summaries)


__all__ = ["process_bulletin_chunk"]
