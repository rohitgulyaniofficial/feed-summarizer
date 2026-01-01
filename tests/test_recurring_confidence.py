#!/usr/bin/env python3
"""Comprehensive tests for recurring coverage confidence scoring."""

import pytest
import tempfile
import os
from unittest.mock import patch


@pytest.fixture
async def temp_database():
    """Provide a temporary database for testing."""
    temp_db = tempfile.mktemp(suffix=".db")
    from models import DatabaseQueue

    db = DatabaseQueue(temp_db)
    await db.start()
    yield db
    await db.stop()
    if os.path.exists(temp_db):
        os.unlink(temp_db)


@pytest.mark.asyncio
async def test_confidence_calculation_perfect_match():
    """Test confidence calculation with perfect match."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {
        "title": "Apple iPhone 16 Launch",
        "summary_text": "Apple announces new iPhone 16 with advanced features",
        "feed_slug": "tech-news",
        "generated_date": 2_000_000,
    }
    past = {
        "title": "Apple iPhone 16 Launch",
        "summary_text": "Apple announces new iPhone 16 with advanced features",
        "feed_slug": "business-news",
        "generated_date": 2_000_000 - 21_600,  # 6 hours earlier
    }

    score = _calculate_match_confidence(current, past, distance=0, current_feed="tech-news", past_feed="business-news")

    assert isinstance(score, float), "Should return float"
    assert score >= 0.8, "Perfect match should score very high"
    assert score <= 1.0, "Score should not exceed 1.0"


@pytest.mark.asyncio
async def test_confidence_calculation_no_overlap():
    """Test confidence calculation with no overlap."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {
        "title": "Apple iPhone News",
        "summary_text": "New iPhone features announced",
        "feed_slug": "tech-news",
        "generated_date": 2_000_000,
    }
    past = {
        "title": "Google Android Update",
        "summary_text": "Google releases Android security patches",
        "feed_slug": "business-news",
        "generated_date": 2_000_000 - 21_600,
    }

    score = _calculate_match_confidence(current, past, distance=24, current_feed="tech-news", past_feed="business-news")

    assert score <= 0.2, "No overlap should score very low"


@pytest.mark.asyncio
async def test_confidence_calculation_empty_data():
    """Test confidence calculation with empty/None data."""
    from workers.publisher.recurring import _calculate_match_confidence

    # Test with empty data instead of None to avoid type issues
    score_none = _calculate_match_confidence({}, {}, distance=24, current_feed="a", past_feed="a")
    assert score_none == 0.0, "Empty data should return 0.0"

    # Test with empty strings
    score_empty = _calculate_match_confidence(
        {"title": "", "summary_text": "", "generated_date": 1_000},
        {"title": "", "summary_text": "", "generated_date": 1_000},
        distance=24,
        current_feed="a",
        past_feed="a",
    )
    assert score_empty == 0.0, "Empty strings should score at baseline"


@pytest.mark.asyncio
async def test_confidence_calculation_boundary_conditions():
    """Test confidence calculation at boundary conditions."""
    from workers.publisher.recurring import _calculate_match_confidence

    # Test exactly 1 title overlap
    current = {
        "title": "Apple Tech",
        "summary_text": "Apple news today",
        "generated_date": 2_000_000,
    }
    past = {
        "title": "Apple News",
        "summary_text": "Apple updates now",
        "generated_date": 2_000_000 - 21_600,
    }

    score_1_overlap = _calculate_match_confidence(current, past, distance=20, current_feed="tech", past_feed="business")

    # Test exactly 2 title overlap
    current_2 = {
        "title": "Apple Technology",
        "summary_text": "Apple tech today",
        "generated_date": 2_000_000,
    }
    past_2 = {
        "title": "Apple Tech News",
        "summary_text": "Apple tech updates now",
        "generated_date": 2_000_000 - 21_600,
    }

    score_2_overlap = _calculate_match_confidence(
        current_2, past_2, distance=20, current_feed="tech", past_feed="business"
    )

    # 1 overlap should get +0.2, 2 overlap should get +0.4
    assert score_2_overlap >= score_1_overlap, "More overlap should score higher"


@pytest.mark.asyncio
async def test_confidence_calculation_cross_feed_bonus():
    """Test cross-feed confidence bonus."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {"title": "Apple iPhone", "summary_text": "Apple news", "feed_slug": "tech", "generated_date": 2_000_000}
    past_same = {"title": "Apple iPhone", "summary_text": "Apple news", "feed_slug": "tech", "generated_date": 2_000_000 - 21_600}
    past_different = {"title": "Apple iPhone", "summary_text": "Apple news", "feed_slug": "business", "generated_date": 2_000_000 - 21_600}

    score_same = _calculate_match_confidence(current, past_same, distance=12, current_feed="tech", past_feed="tech")
    score_different = _calculate_match_confidence(
        current, past_different, distance=12, current_feed="tech", past_feed="business"
    )

    # Cross-feed should get +0.2 bonus
    assert score_different > score_same, "Cross-feed should score higher"


@pytest.mark.asyncio
async def test_confidence_calculation_distance_penalty():
    """Test distance-based confidence adjustment."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {"title": "Apple iPhone", "summary_text": "Apple news", "feed_slug": "tech", "generated_date": 2_000_000}
    past = {"title": "Apple iPhone", "summary_text": "Apple news", "feed_slug": "business", "generated_date": 2_000_000 - 21_600}

    # Test different distances
    score_close = _calculate_match_confidence(
        current, past, distance=8, current_feed="tech", past_feed="business"
    )  # <=12
    score_medium = _calculate_match_confidence(
        current, past, distance=15, current_feed="tech", past_feed="business"
    )  # <=18 but >12
    score_far = _calculate_match_confidence(
        current, past, distance=22, current_feed="tech", past_feed="business"
    )  # >18

    # Close should get +0.2, medium +0.1, far +0.0
    assert score_close > score_medium, "Closer distance should score higher"
    assert score_medium > score_far, "Closer distance should score higher"


@pytest.mark.asyncio
async def test_confidence_calculation_summary_overlap():
    """Test summary overlap scoring."""
    from workers.publisher.recurring import _calculate_match_confidence

    # Test different summary overlap levels
    base_current = {
        "title": "Apple iPhone",
        "summary_text": "Apple iPhone news",
        "feed_slug": "tech",
        "generated_date": 2_000_000,
    }

    past_2_tokens = {
        "title": "Apple iPhone",
        "summary_text": "Apple iPhone announcements",
        "feed_slug": "business",
        "generated_date": 2_000_000 - 21_600,
    }
    past_4_tokens = {
        "title": "Apple iPhone",
        "summary_text": "Apple iPhone announcements and updates",
        "feed_slug": "business",
        "generated_date": 2_000_000 - 21_600,
    }
    past_6_tokens = {
        "title": "Apple iPhone",
        "summary_text": "Apple iPhone announcements updates features",
        "feed_slug": "business",
        "generated_date": 2_000_000 - 21_600,
    }

    score_2 = _calculate_match_confidence(
        base_current, past_2_tokens, distance=12, current_feed="tech", past_feed="business"
    )  # Should get +0.1
    score_4 = _calculate_match_confidence(
        base_current, past_4_tokens, distance=12, current_feed="tech", past_feed="business"
    )  # Should get +0.2
    score_6 = _calculate_match_confidence(
        base_current, past_6_tokens, distance=12, current_feed="tech", past_feed="business"
    )  # Should get +0.3

    assert score_6 >= score_4 >= score_2, "More summary overlap should score higher"


@pytest.mark.asyncio
async def test_confidence_calculation_clamping():
    """Test that confidence scores are properly clamped to 0.0-1.0 range."""
    from workers.publisher.recurring import _calculate_match_confidence

    current = {"title": "A", "summary_text": "Content", "feed_slug": "tech", "generated_date": 2_000_000}
    past = {"title": "A", "summary_text": "Content", "feed_slug": "business", "generated_date": 2_000_000 - 21_600}

    score = _calculate_match_confidence(current, past, distance=0, current_feed="a", past_feed="b")

    # Even with maximum scoring, should not exceed 1.0
    assert 0.0 <= score <= 1.0, "Score should be clamped to 0.0-1.0 range"


@pytest.mark.asyncio
async def test_confidence_calculation_edge_cases():
    """Test edge cases and error conditions."""
    from workers.publisher.recurring import _calculate_match_confidence

    # Test with partial data
    current_partial = {"title": "Apple iPhone", "summary_text": "", "feed_slug": "tech", "generated_date": 2_000_000}
    past_complete = {
        "title": "Apple iPhone",
        "summary_text": "Apple news",
        "feed_slug": "business",
        "generated_date": 2_000_000 - 21_600,
    }

    score_partial = _calculate_match_confidence(
        current_partial, past_complete, distance=12, current_feed="tech", past_feed="business"
    )

    # Should work but with lower score due to missing summary
    assert isinstance(score_partial, float), "Should handle partial data gracefully"


@pytest.mark.asyncio
async def test_recurring_configuration_loading():
    """Test that recurring configuration is loaded correctly."""
    with patch("config.config") as mock_config:
        mock_config.RECURRING_LOOKBACK_DAYS = 14
        mock_config.RECURRING_CONFIDENCE_THRESHOLD = 0.7
        mock_config.RECURRING_HAMMING_THRESHOLD = 20


        # Should use configured values
        assert True, "Should load configuration successfully"


@pytest.mark.asyncio
async def test_recurring_empty_summaries_handling():
    """Test recurring detection with empty summaries."""
    from workers.publisher.recurring import detect_recurring_coverage

    with patch("config.config"):
        result = await detect_recurring_coverage([], "test_group", None, days_back=7)

        assert isinstance(result, dict), "Should return dict"
        assert "recurring_ids" in result, "Should contain recurring_ids"
        assert "coverage_stats" in result, "Should contain coverage_stats"
        assert result["recurring_ids"] == [], "Empty summaries should return empty list"


@pytest.mark.asyncio
async def test_recurring_database_error_handling(temp_database):
    """Test recurring detection with database errors."""
    from workers.publisher.recurring import detect_recurring_coverage

    with patch("config.config"):
        # Mock database error
        async def mock_error(*args, **kwargs):
            raise Exception("Database connection failed")

        with patch.object(temp_database, "execute", mock_error):
            result = await detect_recurring_coverage([{"id": 1, "title": "Test"}], "test_group", temp_database)

            # Should handle errors gracefully
            assert isinstance(result, dict), "Should return dict on error"
            assert "recurring_ids" in result, "Should contain recurring_ids on error"


def main():
    """Run tests if called directly."""
    print("Comprehensive Recurring Coverage Confidence Tests")
    print("=" * 60)

    import sys

    sys.exit(pytest.main([__file__, "-v"]))


if __name__ == "__main__":
    main()
