import asyncio
from types import SimpleNamespace
from typing import Any, Dict, List

import pytest

from workers.publisher.repository import (
    cache_bulletin_introduction,
    cache_passthrough_rss,
    get_bulletin_metadata,
    get_latest_bulletin_title,
    get_passthrough_feed_meta,
    get_passthrough_items,
    load_published_summaries_by_date,
    update_bulletin_title,
)


class FakeDB:
    def __init__(self):
        self.calls: List[Dict[str, Any]] = []
        self.results: Dict[str, Any] = {}

    async def execute(self, name: str, **kwargs):
        self.calls.append({"name": name, "kwargs": kwargs})
        return self.results.get(name)


@pytest.mark.asyncio
async def test_get_latest_bulletin_title_returns_first_non_empty():
    db = FakeDB()
    db.results["get_bulletins_for_group"] = [
        {"title": ""},
        {"title": "  Hello  "},
    ]

    title = await get_latest_bulletin_title(db, "group1", days_back=10)

    assert title == "Hello"
    assert db.calls[0]["name"] == "get_bulletins_for_group"
    assert db.calls[0]["kwargs"]["group_name"] == "group1"
    assert db.calls[0]["kwargs"]["days_back"] == 10


@pytest.mark.asyncio
async def test_load_published_summaries_prefers_cached():
    db = FakeDB()
    db.results["get_bulletins_for_group"] = [{"session_key": "s1"}]
    db.results["get_bulletin"] = {"summaries": [1, 2, 3]}

    grouped = await load_published_summaries_by_date(db, "g", ["a"], days_back=3)

    assert grouped == {"s1": [1, 2, 3]}
    assert any(call["name"] == "get_bulletin" for call in db.calls)


@pytest.mark.asyncio
async def test_load_published_summaries_falls_back_to_query():
    db = FakeDB()
    db.results["get_bulletins_for_group"] = []
    db.results["query_published_summaries_by_date"] = [
        {"published_date": 1, "id": "a"},
        {"published_date": 1, "id": "b"},
        {"published_date": 100, "id": "c"},
    ]

    grouped = await load_published_summaries_by_date(db, "g", ["a"], days_back=1)

    assert set(grouped.keys()) == {"1970-01-01-00-00", "1970-01-01-00-01"}
    assert db.calls[-1]["name"] == "query_published_summaries_by_date"


@pytest.mark.asyncio
async def test_passthrough_helpers():
    db = FakeDB()
    db.results["get_feed_by_slug"] = {"title": "Feed"}
    db.results["query_latest_items_for_feed"] = [1, 2]

    meta = await get_passthrough_feed_meta(db, "slug")
    items = await get_passthrough_items(db, "slug", 10)
    await cache_passthrough_rss(db, "slug", "<xml>")

    assert meta["title"] == "Feed"
    assert items == [1, 2]
    assert any(call["name"] == "cache_passthrough_rss" for call in db.calls)


@pytest.mark.asyncio
async def test_bulletin_metadata_cache_helpers():
    db = FakeDB()
    db.results["get_bulletin"] = {"introduction": "intro", "title": "t"}

    meta = await get_bulletin_metadata(db, "g", "s")
    await cache_bulletin_introduction(db, "g", "s", "intro", [1], ["f"])
    await update_bulletin_title(db, "g", "s", "title")

    assert meta["title"] == "t"
    names = [call["name"] for call in db.calls]
    assert "get_bulletin" in names
    assert "create_bulletin" in names
    assert "update_bulletin_title" in names
