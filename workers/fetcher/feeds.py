#!/usr/bin/env python3
"""Feed configuration and discovery helpers for the fetcher."""

from typing import Any, Dict, Optional
from urllib.parse import urlparse

import yaml
from aiohttp import ClientError, ClientSession
from bs4 import BeautifulSoup

from config import config, get_logger

logger = get_logger("fetcher.feeds")


async def get_feed_config(slug: str) -> Dict[str, Any]:
    """Return feed configuration for the given slug with sane defaults."""
    try:
        feeds_path = config.FEEDS_CONFIG_PATH

        with open(feeds_path, "r") as f:
            feed_config_data = yaml.safe_load(f)

        if feed_config_data and "feeds" in feed_config_data:
            if slug in feed_config_data["feeds"]:
                feed_config = feed_config_data["feeds"][slug]
                result = {
                    "post_process": feed_config.get("post_process", False),
                    "reader_mode": feed_config.get("reader_mode", False),
                }
                if isinstance(feed_config, dict):
                    for key, value in feed_config.items():
                        if key not in result:
                            result[key] = value
                return result

        return {"post_process": False, "reader_mode": False}

    except (OSError, PermissionError, yaml.YAMLError, KeyError) as exc:
        logger.warning("Error loading feed config for %s: %s", slug, exc)
        return {"post_process": False, "reader_mode": False}


async def discover_feed_url(site_url: str, session: ClientSession) -> Optional[str]:
    """Discover a feed URL from a website using <link rel="alternate"> tags."""
    logger.info("Attempting to discover feed URL from: %s", site_url)
    try:
        async with session.get(site_url, headers={"User-Agent": config.USER_AGENT}, timeout=config.HTTP_TIMEOUT) as response:
            if response.status != 200:
                logger.error("Error accessing %s: HTTP %s", site_url, response.status)
                return None

            content = await response.text()
            soup = BeautifulSoup(content, "html.parser")

            feed_links = []
            for link in soup.find_all("link", rel="alternate"):
                type_attr = link.get("type", "")
                if type_attr in ("application/rss+xml", "application/atom+xml", "application/rdf+xml"):
                    href = link.get("href")
                    if not href:
                        continue
                    if not href.startswith(("http://", "https://")):
                        if href.startswith("/"):
                            parsed_url = urlparse(site_url)
                            base = f"{parsed_url.scheme}://{parsed_url.netloc}"
                            href = f"{base}{href}"
                        else:
                            href = f"{site_url.rstrip('/')}/{href}"

                    feed_links.append({"url": href, "title": link.get("title", "Unknown feed"), "type": type_attr})

            if feed_links:
                atom_feeds = [f for f in feed_links if "atom" in f["type"]]
                if atom_feeds:
                    logger.info("Discovered Atom feed: %s", atom_feeds[0]["url"])
                    return atom_feeds[0]["url"]

                logger.info("Discovered feed: %s", feed_links[0]["url"])
                return feed_links[0]["url"]

            logger.warning("No feed links found in %s", site_url)
            return None

    except (ClientError, OSError, ValueError) as exc:
        logger.error("Error discovering feed from %s: %s", site_url, exc)
        return None


__all__ = ["get_feed_config", "discover_feed_url"]
