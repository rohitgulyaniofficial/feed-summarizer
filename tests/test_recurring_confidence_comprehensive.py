#!/usr/bin/env python3
"""Comprehensive test for recurring confidence calculation."""

import pytest
from unittest.mock import patch


class _FakeDB:
    """Minimal stub to satisfy detect_recurring_coverage db.execute calls."""

    def __init__(self, results=None, should_raise: bool = False):
        self.results = results or []
        self.should_raise = should_raise

    async def execute(self, *_args, **_kwargs):  # pragma: no cover - trivial stub
        if self.should_raise:
            raise Exception("Database connection failed")
        return self.results


@pytest.mark.asyncio
async def test_recurring_empty_summaries_handling():
    """Test recurring detection with empty summaries."""
    # Use patch without capturing context manager
    with patch("config.config"):
        from workers.publisher.recurring import detect_recurring_coverage

        result = await detect_recurring_coverage([], "test_group", _FakeDB([]), days_back=7)

        assert isinstance(result, dict), "Should return dict"
        assert result["recurring_ids"] == [], "Empty summaries should return empty list"


@pytest.mark.asyncio
async def test_recurring_database_error_handling():
    """Test recurring detection with database errors."""
    # Use patch without capturing context manager
    with patch("config.config"):
        from workers.publisher.recurring import detect_recurring_coverage

        db = _FakeDB(should_raise=True)

        result = await detect_recurring_coverage([{"id": 1, "item_title": "Test", "summary_text": "x", "feed_slug": "a"}], "test_group", db, days_back=7)

        # Should return empty result on error
        assert isinstance(result, dict), "Should return dict even on error"
        assert result["recurring_ids"] == [], "Should return empty list on error"


@pytest.mark.asyncio
async def test_confidence_calculation_perfect_match():
    """Test confidence calculation with perfect match."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {
        "title": "Apple iPhone 16",
        "summary_text": "Apple announces new iPhone 16 with advanced features",
        "feed_slug": "tech-news",
        "generated_date": 2_000_000,
    }
    past = {
        "title": "Apple iPhone 16",
        "summary_text": "Apple announces new iPhone 16 with advanced features",
        "feed_slug": "business-news",
        "generated_date": 2_000_000 - 21_600,
    }

    score = _calculate_match_confidence(current, past, distance=0, current_feed="tech-news", past_feed="business-news")

    assert isinstance(score, float), "Should return float"
    assert score >= 0.8, "Perfect match should score very high"


@pytest.mark.asyncio
async def test_confidence_calculation_no_overlap():
    """Test confidence calculation with no overlap."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {"title": "Apple iPhone", "summary_text": "Apple news", "feed_slug": "tech-news"}
    past = {"title": "Google Android", "summary_text": "Google updates", "feed_slug": "business-news"}

    score = _calculate_match_confidence(current, past, distance=24, current_feed="tech-news", past_feed="business-news")

    assert isinstance(score, float), "Should return float"
    assert score <= 0.3, "No overlap should score low"


@pytest.mark.asyncio
async def test_confidence_calculation_empty_data():
    """Test confidence calculation with empty data."""
    from workers.publisher.recurring import _calculate_match_confidence

    score = _calculate_match_confidence(
        {"title": "", "summary_text": "", "feed_slug": "tech", "generated_date": 1_000},
        {"title": "", "summary_text": "", "feed_slug": "tech", "generated_date": 1_000},
        distance=24,
        current_feed="tech",
        past_feed="tech",
    )

    assert isinstance(score, float), "Should return float"
    assert score == 0.0, "Empty data should return 0.0"


@pytest.mark.asyncio
async def test_confidence_calculation_boundary_conditions():
    """Test confidence calculation at boundary conditions."""
    from workers.publisher.recurring import _calculate_match_confidence

    # Test exactly 1 title overlap
    current_1 = {"title": "Test A", "summary_text": "Content A", "feed_slug": "tech", "generated_date": 2_000_000}
    past_1 = {"title": "Test A", "summary_text": "Content A", "feed_slug": "tech", "generated_date": 2_000_000 - 21_600}

    score_1 = _calculate_match_confidence(current_1, past_1, distance=20, current_feed="tech", past_feed="tech")

    # Test exactly 2 title overlap
    current_2 = {"title": "Test AB", "summary_text": "Content AB", "feed_slug": "tech", "generated_date": 2_000_000}
    past_2 = {"title": "Test AB", "summary_text": "Content AB", "feed_slug": "tech", "generated_date": 2_000_000 - 21_600}

    score_2 = _calculate_match_confidence(current_2, past_2, distance=20, current_feed="tech", past_feed="tech")

    assert score_2 >= score_1, "More overlap should score higher"


@pytest.mark.asyncio
async def test_confidence_calculation_summary_overlap():
    """Test summary overlap scoring."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {"title": "Test", "summary_text": "Content", "feed_slug": "tech"}

    # Test different summary overlap levels
    past_2 = {"title": "Test", "summary_text": "Test", "feed_slug": "tech", "generated_date": 2_000_000 - 21_600}  # 2 overlap
    past_4 = {
        "title": "Test",
        "summary_text": "Test content",
        "feed_slug": "tech",
        "generated_date": 2_000_000 - 21_600,
    }  # 4 overlap
    past_6 = {
        "title": "Test",
        "summary_text": "Test more content",
        "feed_slug": "tech",
        "generated_date": 2_000_000 - 21_600,
    }  # 6 overlap

    score_2 = _calculate_match_confidence(current, past_2, distance=20, current_feed="tech", past_feed="tech")
    score_4 = _calculate_match_confidence(current, past_4, distance=20, current_feed="tech", past_feed="tech")
    score_6 = _calculate_match_confidence(current, past_6, distance=20, current_feed="tech", past_feed="tech")

    assert score_6 >= score_4 >= score_2, "More summary overlap should score higher"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
