#!/usr/bin/env python3
"""
RSS Feed fetcher and content processor.

This module fetches RSS feeds, extracts full article content using readability,
and processes items for downstream summarization. It handles rate limiting,
retry logic, and content extraction for reliable feed processing.
"""

from asyncio import TimeoutError, get_running_loop, wait_for
from aiohttp import ClientSession, ClientError

# from pypdf import PdfReader  # Removed - extract_pdf_text function is unused

from config import config, get_logger
from services.telemetry import get_tracer, init_telemetry, trace_span
from models import DatabaseQueue
from utils import RateLimiter, RetryHelper

import feedparser
from typing import List, Optional, Set
from concurrent.futures import ThreadPoolExecutor

from utils.async_utils import run_in_executor

from workers.fetcher.content import (
    clean_html,
    parse_with_readability,
)
from workers.fetcher.feeds import get_feed_config
from workers.fetcher.entries import process_feed_entries
from workers.fetcher.mastodon_feed import fetch_mastodon_list
from workers.fetcher.proxy import compute_timeout, resolve_proxy_url
from workers.fetcher.http_fetch import fetch_feed_content
from workers.fetcher.schedule import fetch_all_feeds as schedule_fetch_all_feeds
from workers.fetcher.setup import setup_feed, should_skip_feed_fetch

# Module-specific logger
logger = get_logger("fetcher")
init_telemetry("feed-summarizer-fetcher")
_tracer = get_tracer("fetcher")


class FeedFetcher:
    def __init__(self) -> None:
        self.executor = ThreadPoolExecutor()
        self.db = None  # Initialize as None, will be set in initialize()
        self.reader_rate_limiter = RateLimiter(config.READER_MODE_REQUESTS_PER_MINUTE)
        self.retry_helper = RetryHelper(max_retries=config.MAX_RETRIES, base_delay=config.RETRY_DELAY_BASE)
        self._proxy_warning_slugs: Set[str] = set()
        self._proxy_usage_logged: Set[str] = set()

    async def initialize(self) -> None:
        """Initialize the database connection."""
        self.db = DatabaseQueue(config.DATABASE_PATH)
        await self.db.start()
        logger.info("FeedFetcher initialized")

    @trace_span(
        "fetch_feed",
        tracer_name="fetcher",
        attr_from_args=lambda self, slug, url, session: {
            "feed.slug": slug,
            "feed.url": url,
        },
    )
    async def fetch_feed(self, slug: str, url: str, session: ClientSession) -> None:
        """Fetch and process a single feed asynchronously."""
        logger.info(f"Considering feed: {slug} from {url}")

        # Setup feed and validate prerequisites
        feed_id = await setup_feed(self.db, slug, url)
        if not feed_id:
            return

        # Check if we should fetch this feed based on timing and error backoff
        if await should_skip_feed_fetch(self.db, feed_id, slug):
            return

        # Get feed configuration
        feed_config = await get_feed_config(slug)
        post_process = feed_config.get("post_process", False)
        reader_mode = feed_config.get("reader_mode", False)
        feed_type = (feed_config.get("type") or "").lower()
        proxy_url = resolve_proxy_url(slug, feed_config, self._proxy_warning_slugs, self._proxy_usage_logged)
        # Attributes are recorded via decorators; avoid manual span manipulation

        self._log_feed_processing_config(slug, post_process, reader_mode)

        # Branch on feed type
        if feed_type == "mastodon":
            await fetch_mastodon_list(self.db, feed_id, slug, url, feed_config, session, proxy_url=proxy_url)
        else:
            logger.info(f"Fetching feed: {slug} from {url}")
            # Fetch the feed content (instrumented via decorator)
            content = await self._fetch_feed_content(feed_id, slug, url, session, proxy_url=proxy_url)
            if content is None:
                return
            # Parse and process the feed (instrumented via decorator)
            await self._parse_and_process_feed(feed_id, slug, content, post_process, reader_mode, session, proxy_url)

        # Update success status
        await self.db.execute("update_last_fetched", feed_id=feed_id)
        await self.db.execute("reset_feed_error", feed_id=feed_id)

    def _log_feed_processing_config(self, slug: str, post_process: bool, reader_mode: bool) -> None:
        """Log feed processing configuration."""
        if post_process:
            logger.info(f"Post-processing enabled for feed: {slug}")
        if reader_mode:
            logger.info(f"Reader mode enabled for feed: {slug}")

    @trace_span(
        "fetch_http_feed",
        tracer_name="fetcher",
        attr_from_args=lambda self, feed_id, slug, url, session: {
            "http.url": url,
            "feed.id": int(feed_id) if feed_id else 0,
            "feed.slug": slug,
        },
    )
    async def _fetch_feed_content(
        self, feed_id: int, slug: str, url: str, session: ClientSession, proxy_url: Optional[str] = None
    ) -> bytes | None:
        """Fetch feed content with retry logic via helper module."""
        return await fetch_feed_content(
            self.db,
            feed_id,
            slug,
            url,
            session,
            self.retry_helper,
            proxy_url,
        )

    @trace_span(
        "parse_and_process_feed",
        tracer_name="fetcher",
        attr_from_args=lambda self, feed_id, slug, content, post_process, reader_mode, session: {
            "feed.id": int(feed_id) if feed_id else 0,
            "feed.slug": slug,
        },
    )
    async def _parse_and_process_feed(
        self,
        feed_id: int,
        slug: str,
        content: bytes,
        post_process: bool,
        reader_mode: bool,
        session: Optional[ClientSession],
        proxy_url: Optional[str] = None,
    ) -> None:
        """Parse feed content and process entries."""
        # Set safer parsing options for feedparser
        feedparser_options = {
            "sanitize_html": True,  # Enable built-in sanitization
            "resolve_relative_uris": True,  # Resolve relative URIs
        }

        # Parse the feed - feedparser is not async, run in executor
        feed = await run_in_executor(self.executor, lambda c: feedparser.parse(c, **feedparser_options), content)

        # Check feed format and log details
        feed_type = feed.version if hasattr(feed, "version") else "Unknown"
        logger.info(f"Feed {slug} parsed as {feed_type} format")

        if feed.bozo and hasattr(feed, "bozo_exception"):
            logger.warning(f"Feed parsing warning for {slug}: {feed.bozo_exception}")
            # Check for specific XML parsing issues
            if "entities" in str(feed.bozo_exception).lower():
                logger.warning(f"Possible XML entity expansion attack attempt in {slug}")

        # Update feed title if available
        if "feed" in feed and "title" in feed.feed:
            await self.db.execute("update_feed_title", feed_id=feed_id, title=feed.feed.title)

        # Process and save entries
        if "entries" in feed and feed.entries:
            await process_feed_entries(
                self.db,
                feed_id,
                slug,
                feed.entries,
                post_process,
                reader_mode,
                self.reader_rate_limiter,
                self.fetch_original_content,
                session,
                proxy_url,
            )
        else:
            logger.warning(f"No entries found in feed {slug}")

    @trace_span(
        "fetch_all_feeds",
        tracer_name="fetcher",
        attr_from_args=lambda self, only_slugs=None: {
            "feed.only_slugs": ",".join(only_slugs) if only_slugs else "",
        },
    )
    async def fetch_all_feeds(self, only_slugs: Optional[List[str]] = None) -> None:
        """Delegate feed fetching to the scheduling helper."""
        await schedule_fetch_all_feeds(self, only_slugs)

    @trace_span(
        "reader_mode_fetch",
        tracer_name="fetcher",
        attr_from_args=lambda self, url, session: {
            "entry.url": url,
        },
    )
    async def fetch_original_content(
        self, url: str, session: ClientSession, proxy_url: Optional[str] = None
    ) -> str | None:
        """Fetch the original content from a URL using the readability library."""
        logger.info(f"Fetching original content from: {url}")
        try:
            timeout_seconds = compute_timeout(proxy_url)
            request_kwargs = {
                "headers": {"User-Agent": config.USER_AGENT},
                "timeout": timeout_seconds,
            }
            if proxy_url:
                request_kwargs["proxy"] = proxy_url
            async with session.get(url, **request_kwargs) as response:
                if response.status != 200:
                    logger.error(f"Error fetching original content from {url}: HTTP {response.status}")
                    return None

                html_content = await response.text()

                # Use readability to extract article content
                # Run in executor since readability parsing is CPU-bound
                result = await run_in_executor(self.executor, parse_with_readability, html_content, url)

                # Convert the HTML to Markdown using our clean_html function
                if result:
                    markdown_content = clean_html(result, base_url=url)
                    return markdown_content
                return None
        except (ClientError, OSError, ValueError, RuntimeError) as e:
            logger.error(f"Error fetching original content from {url}: {e}")
            return None

    async def close(self) -> None:
        """Close connections and clean up resources."""
        if self.db:
            await self.db.stop()
        if self.executor:
            logger.info("Shutting down thread pool executor...")
            try:
                # Attempt graceful shutdown with 30-second timeout using asyncio
                loop = get_running_loop()
                await wait_for(loop.run_in_executor(None, lambda: self.executor.shutdown(wait=True)), timeout=30.0)
                logger.info("Thread pool executor shut down successfully")
            except TimeoutError:
                logger.warning("Thread pool executor shutdown timed out after 30 seconds")
                # Force shutdown by not waiting for remaining threads
                self.executor.shutdown(wait=False)
                logger.warning("Forced thread pool executor shutdown (threads may still be running)")
            except Exception as e:
                logger.error(f"Error during thread pool executor shutdown: {e}")
                # Ensure executor is marked as shut down even if there's an error
                self.executor.shutdown(wait=False)
        logger.info("FeedFetcher closed")


## extract_pdf_text function removed - unused functionality
## async def extract_pdf_text(self, url: str, session: ClientSession, proxy_url: Optional[str] = None) -> Optional[str]:
##     """Download and extract text from a PDF file."""
##     try:
##         request_kwargs = {
##             "timeout": compute_timeout(proxy_url),
##         }
##         if proxy_url:
##             request_kwargs["proxy"] = proxy_url
##         async with session.get(url, **request_kwargs) as response:
##             if response.status == 200:
##                 pdf_data = await response.read()
##                 reader = PdfReader(io.BytesIO(pdf_data))
##                 text = "\n".join(page.extract_text() for page in reader.pages if page.extract_text())
##                 return text
##             else:
##                 logger.warning(f"Failed to fetch PDF {url}: HTTP {response.status}")
##     except Exception as e:
##         logger.error(f"Error extracting text from PDF {url}: {e}")
#         return None
