"""Daily status feed with inline charts."""

from __future__ import annotations

from html import escape
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

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
    """Render a small grouped bar chart as inline SVG (no data URLs)."""
    width, height = 720, 320
    margin = 50
    series_keys = list(data.keys())
    bucket_count = max(len(series) for series in data.values()) if data else 0
    if bucket_count == 0:
        return (
            f"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" role=\"img\" aria-label=\"{escape(title)}\">"
            "<rect width=\"100%\" height=\"100%\" fill=\"#f8f9fa\" />"
            f"<text x=\"{margin}\" y=\"{margin}\" fill=\"#666\" font-family=\"monospace\" font-size=\"12\">{escape(title)}</text>"
            "</svg>"
        )

    max_value = max((v for series in data.values() for _, v in series), default=1) or 1
    plot_width = width - 2 * margin
    plot_height = height - 2 * margin
    bar_group_width = plot_width / bucket_count
    bar_width = bar_group_width / max(len(series_keys), 1)

    parts = [
        f"<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" role=\"img\" aria-label=\"{escape(title)}\" viewBox=\"0 0 {width} {height}\">",
        "<style>text{font-family:monospace;font-size:12px;fill:#4a4a4a;} .axis{stroke:#dcdfe4;stroke-width:1;} .tick{stroke:#787878;stroke-width:1;} </style>",
        "<rect width=\"100%\" height=\"100%\" fill=\"#f8f9fa\" />",
        f"<rect x=\"{margin}\" y=\"{margin}\" width=\"{plot_width}\" height=\"{plot_height}\" fill=\"none\" stroke=\"#dce0e4\" />",
        f"<text x=\"{margin}\" y=\"22\" fill=\"#000\">{escape(title)}</text>",
    ]

    tick_values = [0, max_value / 3, (2 * max_value) / 3, max_value]
    for tv in tick_values:
        y = margin + plot_height - (tv / max_value) * plot_height if max_value else margin + plot_height
        parts.append(f"<line class=\"tick\" x1=\"{margin - 4}\" y1=\"{y}\" x2=\"{margin}\" y2=\"{y}\" />")
        label = f"{int(tv)}"
        parts.append(
            f"<text x=\"{margin - 8}\" y=\"{y + 4}\" text-anchor=\"end\">{escape(label)}</text>"
        )
        if tv == max_value and max_value > 0:
            parts.append(f"<line class=\"axis\" x1=\"{margin}\" y1=\"{y}\" x2=\"{margin + plot_width}\" y2=\"{y}\" />")
            marker = f"max {int(max_value)}"
            parts.append(
                f"<rect x=\"{margin + plot_width - 90}\" y=\"{y - 12}\" width=\"86\" height=\"16\" fill=\"#f8f9fa\" stroke=\"#c8c8c8\" />"
            )
            parts.append(
                f"<text x=\"{margin + plot_width - 47}\" y=\"{y + 1}\" text-anchor=\"middle\">{escape(marker)}</text>"
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
            color = BAR_COLORS.get(key, (120, 120, 120))
            parts.append(
                f"<rect x=\"{x0}\" y=\"{y0}\" width=\"{max(x1 - x0, 1)}\" height=\"{max(scaled, 0)}\" fill=\"rgb({color[0]},{color[1]},{color[2]})\" />"
            )
            any_label_drawn = any_label_drawn or value > 0
        if any_label_drawn and b_index % max(1, bucket_count // 6) == 0:
            label = datetime.fromtimestamp(data[series_keys[0]][b_index][0], tz=timezone.utc).strftime(step_label)
            parts.append(
                f"<text x=\"{x_group_start + bar_group_width / 2}\" y=\"{margin + plot_height + 16}\" text-anchor=\"middle\">{escape(label)}</text>"
            )

    legend_y = margin + plot_height + 26
    legend_x = margin
    for key in series_keys:
        color = BAR_COLORS.get(key, (120, 120, 120))
        parts.append(
            f"<rect x=\"{legend_x}\" y=\"{legend_y}\" width=\"12\" height=\"12\" fill=\"rgb({color[0]},{color[1]},{color[2]})\" />"
        )
        parts.append(
            f"<text x=\"{legend_x + 16}\" y=\"{legend_y + 11}\" text-anchor=\"start\">{escape(key)}</text>"
        )
        legend_x += 90

    parts.append("</svg>")
    return "".join(parts)


def _format_counts(metrics: Dict[str, Any]) -> str:
    counts = metrics.get("counts", {})
    per_bulletin = metrics.get("per_bulletin", {})
    lines = [
        f"24h: {counts.get('24h', {}).get('items', 0)} items, {counts.get('24h', {}).get('summaries', 0)} summaries, {counts.get('24h', {}).get('published', 0)} published, {counts.get('24h', {}).get('bulletins', 0)} bulletins",
        f"7d:  {counts.get('7d', {}).get('items', 0)} items, {counts.get('7d', {}).get('summaries', 0)} summaries, {counts.get('7d', {}).get('published', 0)} published, {counts.get('7d', {}).get('bulletins', 0)} bulletins",
        f"Bulletin load (7d): avg {per_bulletin.get('avg', 0):.1f} summaries, max {per_bulletin.get('max', 0)}",
    ]
    return "\n".join(lines)


def _render_metrics_table(metrics: Dict[str, Any]) -> str:
    """Render metrics as a styled HTML table."""
    counts = metrics.get("counts", {})
    counts_24h = counts.get("24h", {})
    counts_7d = counts.get("7d", {})
    per_bulletin = metrics.get("per_bulletin", {})
    
    table_html = """
<table style="border-collapse: collapse; width: 100%; margin: 20px 0; font-family: monospace; font-size: 14px;">
  <thead>
    <tr style="background-color: #f0f0f0; border-bottom: 2px solid #333;">
      <th style="padding: 10px; text-align: left; border: 1px solid #ddd;">Metric</th>
      <th style="padding: 10px; text-align: right; border: 1px solid #ddd;">Last 24h</th>
      <th style="padding: 10px; text-align: right; border: 1px solid #ddd;">Last 7d</th>
    </tr>
  </thead>
  <tbody>
    <tr style="background-color: #fff;">
      <td style="padding: 8px; border: 1px solid #ddd; color: #3478f6; font-weight: bold;">📰 Items</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{items_24h}</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{items_7d}</td>
    </tr>
    <tr style="background-color: #f9f9f9;">
      <td style="padding: 8px; border: 1px solid #ddd; color: #239861; font-weight: bold;">📝 Summaries</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{summaries_24h}</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{summaries_7d}</td>
    </tr>
    <tr style="background-color: #fff;">
      <td style="padding: 8px; border: 1px solid #ddd; color: #e87822; font-weight: bold;">📤 Published</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{published_24h}</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{published_7d}</td>
    </tr>
    <tr style="background-color: #f9f9f9;">
      <td style="padding: 8px; border: 1px solid #ddd; color: #7952b3; font-weight: bold;">📄 Bulletins</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{bulletins_24h}</td>
      <td style="padding: 8px; text-align: right; border: 1px solid #ddd;">{bulletins_7d}</td>
    </tr>
    <tr style="background-color: #fff; border-top: 2px solid #333;">
      <td style="padding: 8px; border: 1px solid #ddd; font-weight: bold;">📊 Bulletin Load (7d)</td>
      <td colspan="2" style="padding: 8px; text-align: left; border: 1px solid #ddd;">Avg: {avg_load:.1f} summaries, Max: {max_load}</td>
    </tr>
  </tbody>
</table>
""".format(
        items_24h=counts_24h.get("items", 0),
        items_7d=counts_7d.get("items", 0),
        summaries_24h=counts_24h.get("summaries", 0),
        summaries_7d=counts_7d.get("summaries", 0),
        published_24h=counts_24h.get("published", 0),
        published_7d=counts_7d.get("published", 0),
        bulletins_24h=counts_24h.get("bulletins", 0),
        bulletins_7d=counts_7d.get("bulletins", 0),
        avg_load=per_bulletin.get("avg", 0),
        max_load=per_bulletin.get("max", 0),
    )
    
    return table_html


def _render_failed_feeds_table(failed_feeds: List[Dict[str, Any]], now: int) -> str:
    if not failed_feeds:
        return "<p>No failed feeds in the last 24h.</p>"

    def _fmt_ts(ts: int) -> str:
        if not ts:
            return "—"
        return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    rows = []
    for entry in failed_feeds:
        slug = escape(str(entry.get("slug", "")))
        reason = escape(str(entry.get("last_error", ""))[:240])
        last_attempt = _fmt_ts(int(entry.get("last_fetched", 0)))
        next_attempt_ts = int(entry.get("next_attempt", 0))
        eta = max(0, next_attempt_ts - now)
        if next_attempt_ts == 0:
            next_attempt = "Immediate"
        elif eta <= 60:
            next_attempt = "now"
        elif eta < 3600:
            next_attempt = f"in {eta // 60}m"
        else:
            next_attempt = f"in {eta // 3600}h"

        rows.append(
            f"<tr style=\"background-color:#fff;\">"
            f"<td style=\"padding:8px;border:1px solid #ddd;font-weight:bold;\">{slug}</td>"
            f"<td style=\"padding:8px;border:1px solid #ddd;\">{reason}</td>"
            f"<td style=\"padding:8px;border:1px solid #ddd;white-space:nowrap;\">{last_attempt}</td>"
            f"<td style=\"padding:8px;border:1px solid #ddd;white-space:nowrap;\">{next_attempt}</td>"
            f"</tr>"
        )

    return (
        "<table style=\"border-collapse:collapse;width:100%;margin:16px 0;font-family:monospace;font-size:14px;\">"
        "<thead><tr style=\"background-color:#f0f0f0;border-bottom:2px solid #333;\">"
        "<th style=\"padding:10px;text-align:left;border:1px solid #ddd;\">Feed</th>"
        "<th style=\"padding:10px;text-align:left;border:1px solid #ddd;\">Reason</th>"
        "<th style=\"padding:10px;text-align:left;border:1px solid #ddd;\">Last attempt</th>"
        "<th style=\"padding:10px;text-align:left;border:1px solid #ddd;\">Next attempt</th>"
        "</tr></thead><tbody>" + "".join(rows) + "</tbody></table>"
    )


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

    body_parts = [
        "<h2>Metrics Summary</h2>",
        _render_metrics_table(metrics),
        "<h2>Failed Feeds (last 24h)</h2>",
        _render_failed_feeds_table(metrics.get("failed_feeds", []), now_ts),
        "<h2>Activity Details</h2>",
        "<pre>",
        sanitize_xml_string(_format_counts(metrics)),
        "</pre>",
    ]
    if charts.get("hourly"):
        body_parts.append(
            f"<div><p>Last 24h</p>{charts['hourly']}</div>"
        )
    if charts.get("daily"):
        body_parts.append(
            f"<div><p>Last 7d</p>{charts['daily']}</div>"
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
