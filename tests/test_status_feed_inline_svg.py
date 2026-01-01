from workers.publisher.status_feed import build_status_feed, generate_status_payload


def test_status_feed_uses_inline_svg():
    base_url = "https://example.com"
    now_ts = 1_700_000_000
    hour_start = now_ts - 2 * 3600
    day_start = now_ts - 2 * 24 * 3600
    metrics = {
        "now": now_ts,
        "counts": {
            "24h": {"items": 3, "summaries": 2, "published": 1, "bulletins": 1},
            "7d": {"items": 10, "summaries": 9, "published": 8, "bulletins": 3},
        },
        "per_bulletin": {"avg": 2.0, "max": 5},
        "hourly": {
            "items": {hour_start: 1, hour_start + 3600: 2},
            "summaries": {hour_start: 0, hour_start + 3600: 1},
        },
        "daily": {
            "items": {day_start: 5, day_start + 24 * 3600: 4},
            "summaries": {day_start: 2, day_start + 24 * 3600: 1},
        },
    }

    charts = generate_status_payload(metrics)
    assert all(payload.startswith("<svg") for payload in charts.values())
    feed_xml = build_status_feed(base_url, metrics, charts)
    assert "data:image" not in feed_xml
    assert "<svg" in feed_xml or "&lt;svg" in feed_xml
