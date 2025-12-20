from datetime import datetime, timezone

from workers.fetcher.dates import parse_date_enhanced


class DummyEntry(dict):
    """Dict that also exposes attributes like feedparser entries."""

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError as exc:  # pragma: no cover - mirrors feedparser behavior
            raise AttributeError(item) from exc


def test_parse_date_without_weekday():
    entry = DummyEntry(
        pubDate="17 Nov 2025 00:00:00 +0000",
        id="https://example.com/2025/11/17/post",
    )

    timestamp = parse_date_enhanced(entry)

    expected = int(datetime(2025, 11, 17, tzinfo=timezone.utc).timestamp())
    assert timestamp == expected


def test_parse_celso_style_date_with_weekday():
    entry = DummyEntry(
        pubDate="Sat, 15 Nov 2025 16:00:00 +0000",
        id="https://celso.io/posts/2025/11/15/acorn-a3020/",
    )

    timestamp = parse_date_enhanced(entry)

    expected = int(datetime(2025, 11, 15, 16, 0, tzinfo=timezone.utc).timestamp())
    assert timestamp == expected
