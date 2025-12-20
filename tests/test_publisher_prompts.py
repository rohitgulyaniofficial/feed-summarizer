import asyncio

import pytest

from workers.publisher.prompts import (
    generate_markdown_bulletin,
    generate_title_from_introduction,
)


@pytest.mark.asyncio
async def test_generate_markdown_bulletin_groups_topics():
    summaries = [
        {"topic": "Tech", "item_title": "A", "links": [{"url": "http://a"}]},
        {"topic": "Tech", "item_title": "B", "links": [{"url": "http://b"}]},
        {"topic": "Biz", "item_title": "C", "links": []},
    ]

    md = generate_markdown_bulletin(summaries)

    assert "## Tech" in md
    assert "A" in md and "B" in md
    assert "## Biz" in md


def test_generate_title_from_introduction_handles_empty_and_time():
    title = generate_title_from_introduction("", "group", "2024-01-02-03-04")
    assert "group" in title.lower() and "2024" in title

    long_intro = "This is a very long introduction that should be trimmed because it exceeds the limit." * 2
    trimmed = generate_title_from_introduction(long_intro, "group", "2024-01-02-03-04")
    assert len(trimmed) < len(long_intro)
