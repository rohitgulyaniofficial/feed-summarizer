"""RSS feed publishing pipeline helpers."""
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from config import config, get_logger
from workers.publisher.merge import summary_id_list
from workers.publisher.repository import (
    cache_bulletin_introduction,
    get_bulletin_metadata,
    update_bulletin_title,
)
from workers.publisher.rss_builder import create_rss_feed
from workers.publisher.titles import with_session_intro_and_title
from utils.io import atomic_write_text

logger = get_logger("publisher.rss_pipeline")


async def publish_group_rss(
    *,
    group_name: str,
    feed_slugs: List[str],
    retention_days: int,
    base_url: str,
    prompts: Dict[str, Any],
    db,
    enable_intro: bool,
    get_published_summaries_by_date,
    ai_chat_completion_fn,
    generate_markdown_bulletin,
    generate_ai_introduction,
    generate_ai_title,
    generate_title_from_introduction,
    rss_feeds_dir,
) -> bool:
    """Publish RSS feed for a group using provided dependencies."""
    try:
        logger.info("Publishing RSS feed for group '%s' with feeds: %s", group_name, feed_slugs)
        bulletin_window_days = max(1, int(retention_days))
        bulletins = await get_published_summaries_by_date(
            group_name,
            feed_slugs,
            days_back=bulletin_window_days,
        )

        cutoff_date = datetime.now(timezone.utc) - timedelta(days=retention_days)
        filtered_bulletins: Dict[str, List[Dict[str, Any]]] = {}
        for session_key, summaries in (bulletins or {}).items():
            try:
                if session_key.count("-") >= 4:
                    time_parts = session_key.split("-")[:5]
                    time_str = "-".join(time_parts)
                    bulletin_time = datetime.strptime(time_str, "%Y-%m-%d-%H-%M").replace(tzinfo=timezone.utc)
                else:
                    bulletin_time = datetime.strptime(session_key, "%Y-%m-%d-%H").replace(tzinfo=timezone.utc)
                if bulletin_time >= cutoff_date:
                    filtered_bulletins[session_key] = summaries
            except ValueError as exc:
                logger.warning("Could not parse session key '%s': %s", session_key, exc)
                filtered_bulletins[session_key] = summaries

        if not filtered_bulletins:
            logger.info(
                "No recent bulletins found for group '%s' within %d days",
                group_name,
                retention_days,
            )
            return True

        bulletin_introductions: Dict[str, str] = {}
        bulletin_titles: Dict[str, str] = {}

        for session_key, summaries in filtered_bulletins.items():
            try:
                bulletin_data = await get_bulletin_metadata(db, group_name, session_key)
                if enable_intro and bulletin_data and bulletin_data.get("introduction"):
                    bulletin_introductions[session_key] = bulletin_data["introduction"]
                    logger.debug(
                        "Using cached introduction for '%s' bulletin %s",
                        group_name,
                        session_key,
                    )
                if bulletin_data and bulletin_data.get("title"):
                    t = (bulletin_data.get("title") or "").strip()
                    if t:
                        bulletin_titles[session_key] = t
                        logger.debug(
                            "Using cached title for '%s' bulletin %s: '%s'",
                            group_name,
                            session_key,
                            t[:80],
                        )
            except Exception as exc:
                logger.debug("No cached bulletin data for '%s' %s: %s", group_name, session_key, exc)

        missing_introductions = set(filtered_bulletins.keys()) - set(bulletin_introductions.keys())
        if enable_intro and missing_introductions and config.AZURE_ENDPOINT and config.OPENAI_API_KEY:
            for session_key in missing_introductions:
                summaries = filtered_bulletins[session_key]
                try:
                    markdown_bulletin = generate_markdown_bulletin(summaries)
                    introduction, _ = await with_session_intro_and_title(
                        markdown_bulletin=markdown_bulletin,
                        summaries=summaries,
                        group_name=group_name,
                        session_key=session_key,
                        enable_intro=True,
                        prompts=prompts,
                        generate_ai_introduction=generate_ai_introduction,
                        generate_ai_title=generate_ai_title,
                        ai_chat_completion_fn=ai_chat_completion_fn,
                        generate_title_from_introduction=generate_title_from_introduction,
                        generate_title=False,
                    )
                    if introduction:
                        bulletin_introductions[session_key] = introduction
                        logger.info(
                            "Generated AI introduction for '%s' bulletin %s (%d characters)",
                            group_name,
                            session_key,
                            len(introduction),
                        )
                        try:
                            summary_ids: List[int] = []
                            for summary in summaries:
                                summary_ids.extend(summary_id_list(summary))
                            await cache_bulletin_introduction(
                                db,
                                group_name,
                                session_key,
                                introduction,
                                summary_ids,
                                feed_slugs,
                            )
                            logger.debug(
                                "Cached introduction for future use: '%s' session '%s'",
                                group_name,
                                session_key,
                            )
                        except Exception as cache_error:
                            logger.warning("Failed to cache introduction: %s", cache_error)
                    else:
                        logger.warning(
                            "Failed to generate AI introduction for '%s' bulletin %s",
                            group_name,
                            session_key,
                        )
                except Exception as exc:
                    logger.error(
                        "Error generating AI introduction for '%s' bulletin %s: %s",
                        group_name,
                        session_key,
                        exc,
                    )

        if enable_intro:
            cached_count = len(bulletin_introductions)
            if cached_count > 0:
                logger.info(
                    "Using introductions for '%s': %d/%d",
                    group_name,
                    cached_count,
                    len(filtered_bulletins),
                )

        for skey in filtered_bulletins.keys():
            if skey not in bulletin_titles:
                intro = bulletin_introductions.get(skey)
                fallback_title = generate_title_from_introduction(intro or "", group_name, skey)
                bulletin_titles[skey] = fallback_title
                try:
                    await update_bulletin_title(db, group_name, skey, fallback_title)
                except Exception:
                    pass

        try:
            logger.info(
                "AI titles generated for '%s': %d/%d",
                group_name,
                len(bulletin_titles),
                len(filtered_bulletins),
            )
        except Exception:
            pass

        introductions = bulletin_introductions if enable_intro else {}
        titles = bulletin_titles
        resolved_titles: Dict[str, str] = {}
        for session_key in filtered_bulletins.keys():
            if titles.get(session_key):
                resolved_titles[session_key] = titles[session_key]
            else:
                intro = introductions.get(session_key)
                resolved_titles[session_key] = generate_title_from_introduction(
                    intro or "",
                    group_name,
                    session_key,
                )

        rss_content = create_rss_feed(
            base_url,
            group_name,
            feed_slugs,
            filtered_bulletins,
            introductions,
            resolved_titles,
        )

        output_file = rss_feeds_dir / f"{group_name}.xml"
        atomic_write_text(output_file, rss_content, suffix=".xml")

        total_items = len(filtered_bulletins)
        logger.info(
            "Successfully published RSS feed with %d bulletin(s) to %s",
            total_items,
            output_file,
        )
        return True
    except Exception as exc:
        logger.error("Error publishing RSS feed for group '%s': %s", group_name, exc)
        return False


__all__ = ["publish_group_rss"]
