"""HTML bulletin orchestration helpers."""
from typing import Any, Callable, Dict, List

from config import get_logger

logger = get_logger("publisher.bulletin_orchestrator")


async def publish_html_bulletin_chunks(
    *,
    group_name: str,
    feed_slugs: List[str],
    enable_intro: bool,
    chunk_limit: int,
    per_feed_limit: int,
    max_chunks: int,
    get_latest_summaries_for_feeds: Callable[[List[str], int, int], Any],
    process_bulletin_chunk: Callable[..., Any],
) -> bool:
    """Chunked bulletin publication loop using injected dependencies."""
    try:
        logger.info("Publishing HTML bulletin for group '%s' with feeds: %s", group_name, feed_slugs)
        total_processed = 0
        chunk_index = 0

        while chunk_index < max_chunks:
            summaries = await get_latest_summaries_for_feeds(
                feed_slugs,
                limit=chunk_limit,
                per_feed_limit=per_feed_limit,
            )
            if not summaries:
                if chunk_index == 0:
                    logger.info("No unpublished summaries found for group '%s'", group_name)
                break

            processed = await process_bulletin_chunk(
                group_name=group_name,
                feed_slugs=feed_slugs,
                summaries=summaries,
                enable_intro=enable_intro,
                render_html=(chunk_index == 0),
                chunk_index=chunk_index,
            )
            total_processed += processed
            chunk_index += 1

            if processed == 0:
                logger.warning(
                    "Stopping bulletin backlog loop for '%s' because chunk #%d produced no new publications",
                    group_name,
                    chunk_index,
                )
                break
            if processed < chunk_limit:
                break

        if total_processed > 0:
            logger.info(
                "Published %d summaries for '%s' across %d chunk(s)",
                total_processed,
                group_name,
                chunk_index,
            )
        return True
    except Exception as exc:
        logger.error("Error publishing HTML bulletin for group '%s': %s", group_name, exc)
        return False


__all__ = ["publish_html_bulletin_chunks"]
