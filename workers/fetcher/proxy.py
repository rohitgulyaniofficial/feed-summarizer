#!/usr/bin/env python3
"""Proxy and HTTP helper utilities for the fetcher."""

from typing import Dict, Optional, Set
from urllib.parse import urlparse

from aiohttp import ClientError

from config import config, get_logger

logger = get_logger("fetcher.proxy")


def resolve_proxy_url(
    slug: str,
    feed_config: Dict[str, object],
    proxy_warning_slugs: Set[str],
    proxy_usage_logged: Set[str],
) -> Optional[str]:
    """Determine the proxy URL to use for a feed, if any."""
    proxy_setting = feed_config.get("proxy") if isinstance(feed_config, dict) else None
    if proxy_setting in (None, False):
        return None

    proxy_candidate: Optional[str] = None
    if isinstance(proxy_setting, str):
        proxy_candidate = proxy_setting.strip() or None
        if not proxy_candidate:
            if slug not in proxy_warning_slugs:
                logger.warning("Feed %s enabled proxy but provided an empty URL; skipping proxy", slug)
                proxy_warning_slugs.add(slug)
    elif isinstance(proxy_setting, dict):
        dict_url = proxy_setting.get("url")
        if isinstance(dict_url, str):
            proxy_candidate = dict_url.strip() or None
            if not proxy_candidate and slug not in proxy_warning_slugs:
                logger.warning("Feed %s proxy configuration missing a valid url; skipping proxy", slug)
                proxy_warning_slugs.add(slug)
        elif proxy_setting.get("enabled") is True:
            proxy_candidate = getattr(config, "PROXY_URL", None)
        else:
            if slug not in proxy_warning_slugs:
                logger.warning("Feed %s proxy configuration is missing a url field; skipping proxy", slug)
                proxy_warning_slugs.add(slug)
    elif isinstance(proxy_setting, bool):
        if proxy_setting:
            proxy_candidate = getattr(config, "PROXY_URL", None)
            if not proxy_candidate and slug not in proxy_warning_slugs:
                logger.warning(
                    "Feed %s requested proxy routing but no proxy.url is configured in feeds.yaml",
                    slug,
                )
                proxy_warning_slugs.add(slug)
    else:
        if slug not in proxy_warning_slugs:
            logger.warning(
                "Feed %s uses unsupported proxy configuration type %s; skipping proxy",
                slug,
                type(proxy_setting).__name__,
            )
            proxy_warning_slugs.add(slug)

    if proxy_candidate:
        if slug not in proxy_usage_logged:
            logger.info("Proxy enabled for feed %s", slug)
            proxy_usage_logged.add(slug)
        return proxy_candidate

    return None


def summarize_proxy(proxy_url: Optional[str]) -> Optional[str]:
    """Provide a redacted proxy identifier for logging."""
    if not proxy_url:
        return None
    try:
        parsed = urlparse(proxy_url)
        if parsed.scheme and parsed.hostname:
            host = parsed.hostname
            if parsed.port:
                host = f"{host}:{parsed.port}"
            return f"{parsed.scheme}://{host}"
    except ValueError:
        return proxy_url
    return proxy_url


def compute_timeout(proxy_url: Optional[str]) -> int:
    """Return the HTTP timeout, scaling up when routing through a proxy."""
    base_timeout = max(int(config.HTTP_TIMEOUT), 1)
    if proxy_url:
        return base_timeout * 6
    return base_timeout


def format_client_error(error: ClientError) -> str:
    """Describe aiohttp client errors with any available status/errno."""
    parts: list[str] = [error.__class__.__name__]
    status = getattr(error, "status", None)
    if status is not None:
        parts.append(f"status={status}")
    os_error = getattr(error, "os_error", None)
    if os_error is not None:
        errno = getattr(os_error, "errno", None)
        strerror = getattr(os_error, "strerror", None)
        if errno is not None:
            parts.append(f"errno={errno}")
        if strerror:
            parts.append(str(strerror))
    message = str(error)
    if message:
        parts.append(message)
    return " ".join(parts)


__all__ = [
    "resolve_proxy_url",
    "summarize_proxy",
    "compute_timeout",
    "format_client_error",
]
