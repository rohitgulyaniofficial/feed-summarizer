"""HTML rendering helpers for publisher bulletins."""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from config import config
from workers.publisher.templates import env


def generate_bulletin_html(
    summary_group: str,
    feed_slugs: List[str],
    summaries: List[Dict[str, Any]],
    introduction: Optional[str] = None,
    title_text: Optional[str] = None,
) -> str:
    """Render bulletin HTML using the Jinja2 template."""
    for summary in summaries:
        slug = summary.get("feed_slug")
        label = summary.get("feed_label")
        if not label and slug:
            label = config.FEED_LABELS.get(slug) or summary.get("feed_title")
        label = label or slug or ""
        summary["feed_label"] = label
        summary["feed_display_name"] = label

    template = env.get_template("bulletin.html")
    current_time = datetime.now(timezone.utc)
    
    # Custom sort to place recurring coverage topic at the end
    recurring_topic = getattr(config, "RECURRING_COVERAGE_TOPIC", "Recurring Coverage")
    
    def topic_sort_key(summary: Dict[str, Any]) -> tuple:
        """Sort key to place recurring coverage at the end."""
        topic = summary.get("topic", "")
        # Return (1, topic) for recurring coverage to sort after (0, topic) for others
        if topic == recurring_topic:
            return (1, topic)
        return (0, topic)
    
    sorted_topics = sorted(summaries, key=topic_sort_key)
    topic_count = len({s.get("topic", "General") for s in summaries}) if summaries else 0

    return template.render(
        summary_group=summary_group,
        feed_slugs=feed_slugs,
        summaries=summaries,
        introduction=introduction,
        title_text=title_text,
        current_time=current_time,
        sorted_topics=sorted_topics,
        topic_count=topic_count,
    )


__all__ = ["generate_bulletin_html"]
