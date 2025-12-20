"""Publisher configuration helpers."""
from typing import Any, Dict, List, Tuple
import yaml

from config import config, get_logger

logger = get_logger("publisher.settings")


def load_prompts() -> Dict[str, str]:
    """Load prompts from prompt.yaml."""
    try:
        with open(config.PROMPT_CONFIG_PATH, "r") as handle:
            prompts_config = yaml.safe_load(handle) or {}
            return prompts_config
    except Exception as exc:
        logger.error("Error loading prompts from %s: %s", config.PROMPT_CONFIG_PATH, exc)
        return {}


def load_feeds_config() -> Dict[str, Any]:
    """Load the feeds.yaml configuration."""
    try:
        with open("feeds.yaml", "r") as handle:
            return yaml.safe_load(handle) or {}
    except Exception as exc:
        logger.error("Error loading feeds.yaml: %s", exc)
        return {}


def load_passthrough_config(feeds_config: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Normalize passthrough configuration from feeds.yaml contents."""
    passthrough_cfg = feeds_config.get("passthrough", {}) if isinstance(feeds_config, dict) else {}
    if isinstance(passthrough_cfg, list):
        return {slug: {"limit": 50} for slug in passthrough_cfg}
    if isinstance(passthrough_cfg, dict):
        normalized: Dict[str, Dict[str, Any]] = {}
        for slug, opts in passthrough_cfg.items():
            if isinstance(opts, dict):
                limit = int(opts.get("limit", 50))
                title = opts.get("title")
                normalized[slug] = {"limit": limit, "title": title}
            else:
                normalized[slug] = {"limit": 50}
        return normalized
    return {}


def normalize_summary_group_entry(
    group_entry: Any,
    feeds_config: Dict[str, Any],
) -> Tuple[List[str], bool]:
    """Normalize summary group configuration into feed slugs and intro flag."""
    feed_slugs: List[str] = []
    enable_intro = False

    if isinstance(group_entry, dict):
        feeds_value = group_entry.get("feeds", group_entry.get("list") or group_entry.get("sources"))
        if isinstance(feeds_value, str):
            feed_slugs = [slug.strip() for slug in feeds_value.split(",") if slug.strip()]
        elif isinstance(feeds_value, list):
            feed_slugs = feeds_value
        else:
            feed_slugs = []
        enable_intro = bool(
            str(group_entry.get("intro", "false")).strip().lower() == "true"
            or group_entry.get("intro") is True
        )
    elif isinstance(group_entry, str):
        feed_slugs = [slug.strip() for slug in group_entry.split(",") if slug.strip()]
    elif isinstance(group_entry, list):
        feed_slugs = group_entry
    else:
        feed_slugs = group_entry or []

    if not enable_intro and isinstance(feeds_config, dict):
        for slug in feed_slugs or []:
            fc = feeds_config.get(slug) or {}
            iv = fc.get("intro")
            if isinstance(iv, str):
                ivb = iv.strip().lower() == "true"
            else:
                ivb = bool(iv)
            if ivb:
                enable_intro = True
                break

    return feed_slugs, enable_intro


__all__ = [
    "load_prompts",
    "load_feeds_config",
    "load_passthrough_config",
    "normalize_summary_group_entry",
]
