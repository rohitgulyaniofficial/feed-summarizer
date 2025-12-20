"""Daily status feed with inline PNG charts."""

from __future__ import annotations

import base64
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Dict, List, Tuple

from PIL import Image, ImageDraw, ImageFont
from feedgen.feed import FeedGenerator

from config import get_logger
from workers.publisher.rss_builder import sanitize_xml_string

logger = get_logger("publisher.status_feed")

BAR_COLORS = {
    "items": (52, 120, 246),
    "summaries": (35, 152, 97),
    "published": (232, 120, 34),
    "bulletins": (121, 82, 179),
}


def _fill_missing_buckets(buckets: Dict[int, int], start: int, span: int, step: int) -> List[Tuple[int, int]]:
    """Normalize sparse buckets into a sorted list with zero-filled gaps."""
    filled: List[Tuple[int, int]] = []
    end = start + span
    cursor = (start // step) * step
    while cursor < end:
        filled.append((cursor, int(buckets.get(cursor, 0))))
        cursor += step
    return filled


def _render_multi_bar_chart(data: Dict[str, List[Tuple[int, int]]], title: str, step_label: str) -> str:
    """Render a small grouped bar chart and return base64 PNG."""
    width, height = 720, 320
    margin = 50
    img = Image.new("RGB", (width, height), (248, 249, 250))
    draw = ImageDraw.Draw(img)
    font = ImageFont.load_default()

    series_keys = list(data.keys())
    bucket_count = max(len(series) for series in data.values()) if data else 0
    if bucket_count == 0:
        buffered = BytesIO()
        img.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode("ascii")

    max_value = max((v for series in data.values() for _, v in series), default=1) or 1
    plot_width = width - 2 * margin
    plot_height = height - 2 * margin
    bar_group_width = plot_width / bucket_count
    bar_width = bar_group_width / max(len(series_keys), 1)

    draw.rectangle([margin, margin, margin + plot_width, margin + plot_height], outline=(220, 224, 228))
    title_text = title
    draw.text((margin, 10), title_text, fill=(0, 0, 0), font=font)

    # Y-axis ticks (including max) and grid line for max value
    tick_values = [0, max_value / 3, (2 * max_value) / 3, max_value]
    for tv in tick_values:
        y = margin + plot_height - (tv / max_value) * plot_height if max_value else margin + plot_height
        draw.line([(margin - 4, y), (margin, y)], fill=(120, 120, 120))
        label = f"{int(tv)}"
        bbox = draw.textbbox((0, 0), label, font=font)
        draw.text((margin - 8 - (bbox[2] - bbox[0]), y - (bbox[3] - bbox[1]) / 2), label, fill=(60, 60, 60), font=font)
        if tv == max_value and max_value > 0:
            # Horizontal line with max marker
            draw.line([(margin, y), (margin + plot_width, y)], fill=(200, 200, 200), width=1)
            marker = f"max {int(max_value)}"
            mbox = draw.textbbox((0, 0), marker, font=font)
            draw.rectangle(
                [margin + plot_width - (mbox[2] - mbox[0]) - 6, y - (mbox[3] - mbox[1]) / 2 - 2,
                 margin + plot_width, y + (mbox[3] - mbox[1]) / 2 + 2],
                fill=(248, 249, 250),
                outline=(200, 200, 200),
            )
            draw.text(
                (margin + plot_width - (mbox[2] - mbox[0]) - 4, y - (mbox[3] - mbox[1]) / 2),
                marker,
                fill=(80, 80, 80),
                font=font,
            )

    for b_index in range(bucket_count):
        x_group_start = margin + b_index * bar_group_width
        any_label_drawn = False
        for s_index, key in enumerate(series_keys):
            bucket = data[key][b_index]
            bucket_ts, value = bucket
            x0 = x_group_start + s_index * bar_width + 2
            x1 = x0 + bar_width - 4
            scaled = 0 if max_value == 0 else (value / max_value) * plot_height
            y0 = margin + plot_height - scaled
            y1 = margin + plot_height
            draw.rectangle([x0, y0, x1, y1], fill=BAR_COLORS.get(key, (120, 120, 120)))
            any_label_drawn = any_label_drawn or value > 0
        if any_label_drawn and b_index % max(1, bucket_count // 6) == 0:
            label = datetime.fromtimestamp(data[series_keys[0]][b_index][0], tz=timezone.utc).strftime(step_label)
            text_size = draw.textbbox((0, 0), label, font=font)
            draw.text(
                (
                    x_group_start + bar_group_width / 2 - (text_size[2] - text_size[0]) / 2,
                    margin + plot_height + 6,
                ),
                label,
                fill=(80, 80, 80),
                font=font,
            )

    legend_y = margin + plot_height + 22
    legend_x = margin
    for key in series_keys:
        draw.rectangle([legend_x, legend_y, legend_x + 12, legend_y + 12], fill=BAR_COLORS.get(key, (120, 120, 120)))
        draw.text((legend_x + 16, legend_y), key, fill=(0, 0, 0), font=font)
        legend_x += 80

    buffered = BytesIO()
    img.save(buffered, format="PNG")
    return base64.b64encode(buffered.getvalue()).decode("ascii")


def _format_counts(metrics: Dict[str, Any]) -> str:
    counts = metrics.get("counts", {})
    per_bulletin = metrics.get("per_bulletin", {})
    lines = [
        f"24h: {counts.get('24h', {}).get('items', 0)} items, {counts.get('24h', {}).get('summaries', 0)} summaries, {counts.get('24h', {}).get('published', 0)} published, {counts.get('24h', {}).get('bulletins', 0)} bulletins",
        f"7d:  {counts.get('7d', {}).get('items', 0)} items, {counts.get('7d', {}).get('summaries', 0)} summaries, {counts.get('7d', {}).get('published', 0)} published, {counts.get('7d', {}).get('bulletins', 0)} bulletins",
        f"Bulletin load (7d): avg {per_bulletin.get('avg', 0):.1f} summaries, max {per_bulletin.get('max', 0)}",
    ]
    return "\n".join(lines)


def build_status_feed(base_url: str, metrics: Dict[str, Any], charts: Dict[str, str]) -> str:
    now_ts = metrics.get("now", int(datetime.now(tz=timezone.utc).timestamp()))
    now_dt = datetime.fromtimestamp(now_ts, tz=timezone.utc)
    fg = FeedGenerator()
    feed_url = f"{base_url}/feeds/status.xml"
    fg.id(feed_url)
    fg.title("Feed Summarizer Status")
    fg.link(href=feed_url, rel="self")
    fg.link(href=base_url, rel="alternate")
    fg.description("Daily operational snapshot with activity counts and charts.")
    fg.language("en-us")
    fg.generator("Feed Summarizer Status Publisher")
    fg.lastBuildDate(now_dt)

    fe = fg.add_item()
    fe.title(f"Status for {now_dt.strftime('%Y-%m-%d')}")
    fe.guid(f"status-{now_dt.strftime('%Y-%m-%d')}", permalink=False)
    fe.pubDate(now_dt)

    body_parts = ["<pre>", sanitize_xml_string(_format_counts(metrics)), "</pre>"]
    if charts.get("hourly"):
        body_parts.append(
            f"<p>Last 24h</p><img alt=\"24h activity\" src=\"data:image/png;base64,{charts['hourly']}\" />"
        )
    if charts.get("daily"):
        body_parts.append(
            f"<p>Last 7d</p><img alt=\"7d activity\" src=\"data:image/png;base64,{charts['daily']}\" />"
        )
    html_body = sanitize_xml_string("".join(body_parts))
    fe.content(html_body, type="html")
    fe.description("Operational status with embedded charts.")

    return fg.rss_str(pretty=True).decode("utf-8")


def generate_status_payload(metrics: Dict[str, Any]) -> Dict[str, str]:
    now = metrics.get("now") or int(datetime.now(tz=timezone.utc).timestamp())
    hourly_buckets = {
        key: _fill_missing_buckets(series or {}, start=now - 24 * 3600, span=24 * 3600, step=3600)
        for key, series in (metrics.get("hourly") or {}).items()
    }
    daily_buckets = {
        key: _fill_missing_buckets(series or {}, start=now - 7 * 24 * 3600, span=7 * 24 * 3600, step=24 * 3600)
        for key, series in (metrics.get("daily") or {}).items()
    }

    hourly_chart = _render_multi_bar_chart(hourly_buckets, "Last 24h", "%H:%M")
    daily_chart = _render_multi_bar_chart(daily_buckets, "Last 7d", "%m-%d")

    return {"hourly": hourly_chart, "daily": daily_chart}


def write_status_feed(base_url: str, metrics: Dict[str, Any], output_path: Path) -> bool:
    try:
        charts = generate_status_payload(metrics)
        xml = build_status_feed(base_url, metrics, charts)
        output_path.write_text(xml, encoding="utf-8")
        logger.info("Wrote status feed to %s", output_path)
        return True
    except Exception as exc:
        logger.error("Failed to write status feed: %s", exc)
        return False


__all__ = ["write_status_feed", "generate_status_payload", "build_status_feed"]
