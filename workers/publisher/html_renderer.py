"""HTML rendering helpers for publisher bulletins."""
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from workers.publisher.templates import env


def generate_bulletin_html(
    summary_group: str,
    feed_slugs: List[str],
    summaries: List[Dict[str, Any]],
    introduction: Optional[str] = None,
    title_text: Optional[str] = None,
) -> str:
    """Render bulletin HTML using the Jinja2 template."""
    template = env.get_template("bulletin.html")
    current_time = datetime.now(timezone.utc)
    sorted_topics = sorted(summaries, key=lambda x: x.get("topic", ""))
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
