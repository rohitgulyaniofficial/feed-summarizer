#!/usr/bin/env python3
"""
RSS Publisher for feed summaries.

This module generates RSS feeds containing bulletins for each summary group
defined in feeds.yaml. Each RSS item represents a bulletin containing summaries
published within a 4-hour time window, with AI-generated introductions.
"""

from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone, timedelta
import importlib
from pathlib import Path
from aiohttp import ClientSession

# Import configuration, models, and shared utilities
from config import config, get_logger
from services.telemetry import init_telemetry, get_tracer, trace_span
from services.llm_client import chat_completion as default_ai_chat_completion
from models import DatabaseQueue
from workers.publisher.settings import (
    load_feeds_config,
    load_prompts,
    normalize_summary_group_entry,
)
from workers.publisher.templates import env
from workers.publisher.merge import synthesize_merged_summary, merge_similar_summaries
from workers.publisher.prompts import (
    generate_markdown_bulletin,
    generate_ai_introduction,
    generate_ai_title,
    generate_title_from_introduction,
)
from workers.publisher.bulletin_orchestrator import publish_html_bulletin_chunks
from workers.publisher.rss_pipeline import publish_group_rss
from workers.publisher.html_renderer import generate_bulletin_html
from workers.publisher.bulletins import (
    build_recent_bulletins as build_recent_bulletins_helper,
    extract_bulletin_file_title,
    extract_bulletin_summary,
)
from workers.publisher.bulletin_processor import process_bulletin_chunk
from workers.publisher.repository import (
    get_latest_bulletin_title,
    load_published_summaries_by_date,
)
from workers.publisher.indexes import (
    write_feeds_index,
    write_bulletins_index,
    write_main_index,
)
from workers.publisher.passthrough import publish_passthrough_feeds as publish_passthrough_feeds_helper
from workers.publisher.status_feed import write_status_feed


logger = get_logger("publisher")


def _get_ai_chat_completion():
    """Return the active chat completion function.

    This indirection lets monkeypatching of workers.publisher.ai_chat_completion
    affect calls inside core.
    """
    try:
        mod = importlib.import_module("workers.publisher")
        fn = getattr(mod, "ai_chat_completion", None)
        if callable(fn):
            return fn
    except Exception:
        pass
    return default_ai_chat_completion


class RSSPublisher:
    """Publishes RSS feeds and HTML bulletins containing summary bulletins."""
    
    def __init__(self, base_url: str = "https://example.com"):
        self.db: Optional[DatabaseQueue] = None
        # Ensure we generate content under the configured DATA_PATH/public
        self.public_dir = Path(config.PUBLIC_DIR)
        self.public_dir.mkdir(exist_ok=True)
        self.rss_feeds_dir = self.public_dir / "feeds"
        self.rss_feeds_dir.mkdir(exist_ok=True)
        self.html_bulletins_dir = self.public_dir / "bulletins"
        self.html_bulletins_dir.mkdir(exist_ok=True)
        # Replace hardcoded base_url with config.RSS_BASE_URL
        self.base_url = config.RSS_BASE_URL.rstrip('/')
        # Use retention from configuration thresholds (feeds.yaml) rather than hard-coded value
        try:
            self.retention_days = int(getattr(config, 'RETENTION_DAYS', 7))
        except Exception:
            self.retention_days = 7
        if self.retention_days < 1:
            logger.warning(f"Invalid RETENTION_DAYS={self.retention_days}; using fallback 7")
            self.retention_days = 7
        self.prompts = load_prompts()
        
    async def initialize(self):
        """Initialize the publisher with database connection."""
        self.db = DatabaseQueue(config.DATABASE_PATH)
        await self.db.start()
        try:
            logger.info(f"Publisher paths: DATA_PATH={config.DATA_PATH} PUBLIC_DIR={config.PUBLIC_DIR} DB={config.DATABASE_PATH}")
        except Exception:
            pass
        
        logger.info("Unified Publisher initialized")

    async def close(self):
        """Close connections and clean up resources."""
        if self.db:
            await self.db.stop()
        logger.info("Unified Publisher closed")

    async def _get_latest_bulletin_title(self, group_name: str, days_back: int = 30) -> Optional[str]:
        """Return the most recent non-empty stored title for a group's bulletins."""
        return await get_latest_bulletin_title(self.db, group_name, days_back)

    # ---- Helper logic for landing page recent bulletins ----
    def _extract_bulletin_summary(self, bulletin_path: Path, max_len: int = 140) -> Optional[str]:
        """Wrapper for extracting bulletin summary from HTML."""
        return extract_bulletin_summary(bulletin_path, max_len)

    def build_recent_bulletins(self, latest_titles: Dict[str, Optional[str]]) -> List[Dict[str, str]]:
        """Build recent bulletin metadata list for landing page."""
        return build_recent_bulletins_helper(self.html_bulletins_dir, latest_titles)

    # --- Consistent title retrieval for indexes ---
    def _extract_bulletin_file_title(self, group_name: str) -> Optional[str]:
        """Extract <h1> title from the rendered bulletin HTML file for a group."""
        return extract_bulletin_file_title(self.html_bulletins_dir, group_name)

    def _compute_per_feed_limit(self, feed_slugs: List[str]) -> int:
        """Compute how many summaries each feed can contribute to a single bulletin chunk."""
        if not feed_slugs:
            return max(1, config.BULLETIN_PER_FEED_LIMIT)
        dynamic = max(1, config.BULLETIN_SUMMARY_LIMIT // max(1, len(feed_slugs)))
        per_feed = min(config.BULLETIN_PER_FEED_LIMIT, dynamic)
        return max(1, per_feed)


    @trace_span("publish_passthrough_feeds", tracer_name="publisher")
    async def publish_passthrough_feeds(self, only_slugs: Optional[List[str]] = None) -> int:
        """Publish raw passthrough RSS feeds based on feeds.yaml passthrough config.

        Args:
            only_slugs: Optional list of slugs to limit publishing to.
        """
        return await publish_passthrough_feeds_helper(
            db=self.db,
            base_url=self.base_url,
            rss_feeds_dir=self.rss_feeds_dir,
            feeds_config=load_feeds_config(),
            only_slugs=only_slugs,
        )

    @trace_span(
        "get_published_summaries_by_date",
        tracer_name="publisher",
        attr_from_args=lambda self, group_name, feed_slugs, days_back=7: {
            "group.name": group_name,
            "group.feed_count": len(feed_slugs or []),
            "days_back": int(days_back),
        },
    )
    async def get_published_summaries_by_date(self, group_name: str, feed_slugs: List[str], days_back: int = 7) -> Dict[str, List[Dict[str, Any]]]:
        """Get published summaries grouped by publication sessions.
        
        First checks for existing bulletins in the database, then falls back to 
        grouping summaries by session if no bulletins exist.
        """
        return await load_published_summaries_by_date(self.db, group_name, feed_slugs, days_back)

    async def get_latest_summaries_for_feeds(self, feed_slugs: List[str], limit: int = 50, per_feed_limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get the latest unpublished summaries for specified feeds (for HTML generation)."""
        if not feed_slugs:
            return []

        summaries = await self.db.execute(
            'query_summaries_for_feeds',
            feed_slugs=feed_slugs,
            limit=limit,
            per_feed_limit=per_feed_limit,
        )
        return summaries or []

    async def _synthesize_merged_summary(
        self,
        group: List[Dict[str, Any]],
        prompt_template: str,
        use_llm: bool,
    ) -> str:
        """Backward-compatible wrapper for merge synthesis (used by tests)."""
        return await synthesize_merged_summary(
            group,
            prompt_template,
            use_llm,
            _get_ai_chat_completion(),
        )

    async def _merge_similar_summaries(self, summaries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Backward-compatible wrapper for merge logic (used by tests)."""
        return await merge_similar_summaries(
            summaries,
            self.prompts,
            self.db,
            _get_ai_chat_completion(),
        )

    async def mark_summaries_as_published(self, summary_ids: List[int]) -> int:
        """Mark summaries as published by updating the published_date in the database."""
        if not summary_ids:
            return 0
            
        try:
            # Use the database method to mark summaries as published
            marked_count = await self.db.execute('mark_summaries_as_published', summary_ids=summary_ids)
            logger.info(f"Marked {marked_count} summaries as published: {summary_ids}")
            return marked_count or 0
        except Exception as e:
            logger.error(f"Error marking summaries as published: {e}")
            return 0

    def _generate_markdown_bulletin(self, summaries: List[Dict[str, Any]]) -> str:
        """Backward-compatible wrapper for markdown bulletin generation."""
        return generate_markdown_bulletin(summaries)

    @trace_span(
        "publisher.azure_openai.intro",
        tracer_name="publisher",
        attr_from_args=lambda self, markdown_bulletin, session: {
            "prompt.length": len(markdown_bulletin or ""),
            "azure.openai.deployment": config.DEPLOYMENT_NAME,
            "azure.openai.api_version": config.OPENAI_API_VERSION,
        },
    )
    async def _generate_ai_introduction(self, markdown_bulletin: str, session: ClientSession) -> Optional[str]:
        """Backward-compatible wrapper for AI introduction generation."""
        return await generate_ai_introduction(
            markdown_bulletin,
            self.prompts,
            session,
            _get_ai_chat_completion(),
        )

    @trace_span(
        "publisher.azure_openai.title",
        tracer_name="publisher",
        attr_from_args=lambda self, markdown_bulletin, session: {
            "prompt.length": len(markdown_bulletin or ""),
            "azure.openai.deployment": config.DEPLOYMENT_NAME,
            "azure.openai.api_version": config.OPENAI_API_VERSION,
        },
    )
    async def _generate_ai_title(self, markdown_bulletin: str, session: ClientSession) -> Optional[str]:
        """Backward-compatible wrapper for AI title generation."""
        return await generate_ai_title(
            markdown_bulletin,
            self.prompts,
            session,
            _get_ai_chat_completion(),
        )

    def _generate_title_from_introduction(self, introduction: str, group_name: str, session_key: str) -> str:
        """Backward-compatible wrapper for title generation from introduction/session key."""
        return generate_title_from_introduction(introduction, group_name, session_key)

    @trace_span(
        "publish_rss_feed",
        tracer_name="publisher",
        attr_from_args=lambda self, group_name, feed_slugs: {"group.name": group_name, "group.feed_count": len(feed_slugs or [])},
    )
    async def publish_rss_feed(self, group_name: str, feed_slugs: List[str], enable_intro: bool = False) -> bool:
        """Publish an RSS feed for a summary group."""
        return await publish_group_rss(
            group_name=group_name,
            feed_slugs=feed_slugs,
            retention_days=self.retention_days,
            base_url=self.base_url,
            prompts=self.prompts,
            db=self.db,
            enable_intro=enable_intro,
            get_published_summaries_by_date=self.get_published_summaries_by_date,
            ai_chat_completion_fn=_get_ai_chat_completion(),
            generate_markdown_bulletin=self._generate_markdown_bulletin,
            generate_ai_introduction=self._generate_ai_introduction,
            generate_ai_title=self._generate_ai_title,
            generate_title_from_introduction=self._generate_title_from_introduction,
            rss_feeds_dir=self.rss_feeds_dir,
        )

    async def _write_index_html(self):
        """Backward-compatible wrapper for feeds index rendering."""
        await write_feeds_index(self.rss_feeds_dir, self.base_url, self._get_latest_bulletin_title)

    async def _write_bulletins_index_html(self):
        """Backward-compatible wrapper for bulletins index rendering."""
        await write_bulletins_index(self.html_bulletins_dir, self.base_url, self._get_latest_bulletin_title)

    async def _write_main_index_html(self):
        """Backward-compatible wrapper for main index rendering."""
        await write_main_index(
            self.public_dir,
            self.html_bulletins_dir,
            self.rss_feeds_dir,
            self.base_url,
            self._get_latest_bulletin_title,
            lambda path, max_len=140: self._extract_bulletin_summary(path, max_len),
        )

    async def cleanup_old_bulletins(self, days_to_keep: int = None) -> int:
        """Clean up old bulletins from the database.
        
        Args:
            days_to_keep: Number of days to keep. Defaults to retention_days + 1
            
        Returns:
            Number of bulletins deleted
        """
        if days_to_keep is None:
            days_to_keep = self.retention_days + 1  # Keep a bit longer than RSS retention
        
        try:
            deleted_count = await self.db.execute('delete_old_bulletins', days_to_keep=days_to_keep)
            if deleted_count > 0:
                logger.info(f"Cleaned up {deleted_count} old bulletins (older than {days_to_keep} days)")
            return deleted_count
        except Exception as e:
            logger.error(f"Error cleaning up old bulletins: {e}")
            return 0

    @trace_span("publish_status_feed", tracer_name="publisher")
    async def publish_status_feed(self) -> bool:
        """Publish the daily status feed with inline charts (once per day)."""
        status_path = self.rss_feeds_dir / "status.xml"
        now_dt = datetime.now(timezone.utc)
        try:
            if status_path.exists():
                mtime = datetime.fromtimestamp(status_path.stat().st_mtime, tz=timezone.utc)
                if mtime.date() == now_dt.date():
                    logger.info("Status feed already generated for %s", now_dt.date())
                    return True
            metrics = await self.db.execute("get_status_metrics", now_ts=int(now_dt.timestamp()))
            return write_status_feed(self.base_url, metrics or {}, status_path)
        except Exception as exc:
            logger.error("Failed to publish status feed: %s", exc)
            return False

    async def _process_bulletin_chunk(
        self,
        group_name: str,
        feed_slugs: List[str],
        summaries: List[Dict[str, Any]],
        enable_intro: bool,
        render_html: bool,
        chunk_index: int,
    ) -> int:
        """Delegate bulletin chunk processing to helper module."""
        return await process_bulletin_chunk(
            group_name=group_name,
            feed_slugs=feed_slugs,
            summaries=summaries,
            enable_intro=enable_intro,
            render_html=render_html,
            chunk_index=chunk_index,
            prompts=self.prompts,
            db=self.db,
            html_bulletins_dir=self.html_bulletins_dir,
            generate_markdown_bulletin=self._generate_markdown_bulletin,
            generate_ai_introduction=self._generate_ai_introduction,
            generate_ai_title=self._generate_ai_title,
            generate_title_from_introduction=self._generate_title_from_introduction,
            mark_summaries_as_published=self.mark_summaries_as_published,
            ai_chat_completion=_get_ai_chat_completion(),
        )

    @trace_span(
        "publish_html_bulletin",
        tracer_name="publisher",
        attr_from_args=lambda self, group_name, feed_slugs: {"group.name": group_name, "group.feed_count": len(feed_slugs or [])},
    )
    async def publish_html_bulletin(self, group_name: str, feed_slugs: List[str], enable_intro: bool = False) -> bool:
        """Publish an HTML bulletin for a summary group."""
        chunk_limit = max(1, config.BULLETIN_SUMMARY_LIMIT)
        per_feed_limit = min(chunk_limit, self._compute_per_feed_limit(feed_slugs))
        max_chunks = max(1, config.BULLETIN_MAX_CHUNKS)

        return await publish_html_bulletin_chunks(
            group_name=group_name,
            feed_slugs=feed_slugs,
            enable_intro=enable_intro,
            chunk_limit=chunk_limit,
            per_feed_limit=per_feed_limit,
            max_chunks=max_chunks,
            get_latest_summaries_for_feeds=self.get_latest_summaries_for_feeds,
            process_bulletin_chunk=self._process_bulletin_chunk,
        )

    @trace_span(
        "publish_all_rss_feeds",
        tracer_name="publisher",
        attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""},
    )
    async def publish_all_rss_feeds(self, only_slugs: Optional[List[str]] = None) -> int:
        """Publish RSS feeds for summary groups defined in feeds.yaml.

        If only_slugs is provided, publish only groups whose feed list intersects
        with the provided slugs (used for per-feed runs).
        """
        config_data = load_feeds_config()
        summaries_config = config_data.get('summaries', {})
        
        if not summaries_config:
            logger.warning("No summary groups found in feeds.yaml")
            return 0
        
        published_count = 0
        feeds_config = config_data.get('feeds', {}) if isinstance(config_data, dict) else {}
        for group_name, group_entry in summaries_config.items():
            # Support:
            # - "slug1, slug2"
            # - ["slug1", "slug2"]
            # - { feeds: "slug1, slug2"|[...], intro: bool }
            enable_intro = False
            if isinstance(group_entry, dict):
                feeds_value = group_entry.get('feeds', group_entry.get('list') or group_entry.get('sources'))
                if isinstance(feeds_value, str):
                    feed_slugs = [slug.strip() for slug in feeds_value.split(',') if slug.strip()]
                elif isinstance(feeds_value, list):
                    feed_slugs = feeds_value
                else:
                    feed_slugs = []
                enable_intro = bool(str(group_entry.get('intro', 'false')).strip().lower() == 'true' or group_entry.get('intro') is True)
            elif isinstance(group_entry, str):
                feed_slugs = [slug.strip() for slug in group_entry.split(',') if slug.strip()]
            else:
                feed_slugs = group_entry
            # Per-feed filtering
            if only_slugs:
                if not any(slug in feed_slugs for slug in only_slugs):
                    logger.debug(f"Skipping RSS for group '{group_name}' (no overlap with slugs: {only_slugs})")
                    continue
            # If group-level intro not explicitly enabled, allow per-feed opt-in to turn it on
            if not enable_intro and isinstance(feeds_config, dict):
                try:
                    for slug in feed_slugs or []:
                        fc = feeds_config.get(slug) or {}
                        iv = fc.get('intro')
                        if isinstance(iv, str):
                            ivb = iv.strip().lower() == 'true'
                        else:
                            ivb = bool(iv)
                        if ivb:
                            enable_intro = True
                            break
                except Exception:
                    pass
            
            if await self.publish_rss_feed(group_name, feed_slugs, enable_intro=enable_intro):
                published_count += 1
        
        # Generate RSS feeds index.html after publishing all feeds
        await self._write_index_html()
        
        return published_count

    @trace_span(
        "publish_all_html_bulletins",
        tracer_name="publisher",
        attr_from_args=lambda self, only_slugs=None: {"feed.only_slugs": ",".join(only_slugs) if only_slugs else ""},
    )
    async def publish_all_html_bulletins(self, only_slugs: Optional[List[str]] = None) -> int:
        """Publish HTML bulletins for summary groups defined in feeds.yaml.

        If only_slugs is provided, publish only groups whose feed list intersects
        with the provided slugs (used for per-feed runs).
        """
        config_data = load_feeds_config()
        summaries_config = config_data.get('summaries', {})
        
        if not summaries_config:
            logger.warning("No summary groups found in feeds.yaml")
            return 0
        
        published_count = 0
        feeds_config = config_data.get('feeds', {}) if isinstance(config_data, dict) else {}
        for group_name, group_entry in summaries_config.items():
            feed_slugs, enable_intro = normalize_summary_group_entry(group_entry, feeds_config)
            if only_slugs and not any(slug in feed_slugs for slug in only_slugs):
                logger.debug(f"Skipping HTML bulletin for group '{group_name}' (no overlap with slugs: {only_slugs})")
                continue
            
            if await self.publish_html_bulletin(group_name, feed_slugs, enable_intro=enable_intro):
                published_count += 1
        
        return published_count

    @trace_span("publish_all_content", tracer_name="publisher")
    async def publish_all_content(self) -> Tuple[int, int]:
        """Publish both HTML bulletins and RSS feeds for all summary groups, then generate all index files."""
        logger.info("Publishing all content (HTML bulletins and RSS feeds)...")
        
        # First publish HTML bulletins (which marks summaries as published)
        html_count = await self.publish_all_html_bulletins()
        
        # Then publish RSS feeds (which uses the published summaries)
        rss_count = await self.publish_all_rss_feeds()
        
        # Then publish passthrough feeds (raw per-feed RSS)
        pt_count = await self.publish_passthrough_feeds()

        # Publish the status feed once per day (best effort)
        await self.publish_status_feed()
        
        # Generate all index files
        await self._write_bulletins_index_html()
        await self._write_main_index_html()
        
        logger.info(f"Published {html_count} HTML bulletins, {rss_count} summary RSS feeds, {pt_count} passthrough RSS feeds")
        return html_count, rss_count







