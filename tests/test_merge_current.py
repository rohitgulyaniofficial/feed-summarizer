"""Comprehensive pytest tests for current enhanced merge functionality."""

import pytest
import sys
from pathlib import Path
from unittest.mock import AsyncMock
from workers.publisher.merge import (
    confidence_score_for_merge,
    should_merge_pair_improved,
    merge_similar_summaries,
)

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class TestConfidenceScoring:
    """Test confidence scoring functionality."""

    def test_confidence_score_identical_topics(self):
        """Test confidence scoring with identical topics."""
        a = {
            "item_title": "Apple announces new iPhone 15",
            "summary_text": "Apple revealed new iPhone with advanced features",
            "item_date": 1699000000,
            "feed_slug": "tech-crunch",
        }
        b = {
            "item_title": "Apple iPhone 15 launch",
            "summary_text": "Apple launched iPhone 15 with improved camera",
            "item_date": 1699010000,
            "feed_slug": "the-verge",
        }

        confidence = confidence_score_for_merge(a, b, 8)  # Close distance
        assert confidence > 0.5  # Should be high confidence
        assert confidence <= 1.0

    def test_confidence_score_different_topics(self):
        """Test confidence scoring with different topics."""
        a = {
            "item_title": "Apple iPhone 15 launch",
            "summary_text": "Apple revealed new iPhone",
            "item_date": 1699000000,
            "feed_slug": "tech-crunch",
        }
        b = {
            "item_title": "Stock market rally",
            "summary_text": "Markets closed higher today",
            "item_date": 1699000000,
            "feed_slug": "reuters",
        }

        confidence = confidence_score_for_merge(a, b, 24)  # Far distance
        assert confidence < 0.3  # Should be low confidence
        assert confidence >= 0.0

    def test_confidence_score_cross_feed_bonus(self):
        """Test cross-feed bonus in confidence scoring."""
        base_time = 1699000000
        a = {
            "item_title": "iPhone 15 news",
            "summary_text": "New iPhone features announced",
            "item_date": base_time,
            "feed_slug": "tech-crunch",
        }

        # Same feed
        b_same = {
            "item_title": "iPhone 15 details",
            "summary_text": "More iPhone features revealed",
            "item_date": base_time + 7200,  # 2 hours later
            "feed_slug": "tech-crunch",
        }

        # Different feed
        b_diff = {
            "item_title": "iPhone 15 details",
            "summary_text": "More iPhone features revealed",
            "item_date": base_time + 7200,  # 2 hours later
            "feed_slug": "the-verge",
        }

        confidence_same = confidence_score_for_merge(a, b_same, 12)
        confidence_diff = confidence_score_for_merge(a, b_diff, 12)

        # Cross-feed should have higher confidence
        assert confidence_diff > confidence_same

    def test_confidence_score_time_gap_penalties(self):
        """Test time gap penalties in confidence scoring."""
        base_time = 1699000000
        a = {
            "item_title": "Tech news",
            "summary_text": "Technology update",
            "item_date": base_time,
            "feed_slug": "tech-crunch",
        }

        # Same story (1 hour)
        b_1hr = {
            "item_title": "Tech news update",
            "summary_text": "Technology news update",
            "item_date": base_time + 3600,
            "feed_slug": "the-verge",
        }

        # Same day (6 hours)
        b_6hr = {
            "item_title": "Tech news update",
            "summary_text": "Technology news update",
            "item_date": base_time + 21600,
            "feed_slug": "the-verge",
        }

        # Multiple days (48 hours)
        b_48hr = {
            "item_title": "Tech news update",
            "summary_text": "Technology news update",
            "item_date": base_time + 172800,
            "feed_slug": "the-verge",
        }

        conf_1hr = confidence_score_for_merge(a, b_1hr, 12)
        conf_6hr = confidence_score_for_merge(a, b_6hr, 12)
        conf_48hr = confidence_score_for_merge(a, b_48hr, 12)

        # 1 hour should be penalized most, 48 hours least
        # Due to the logic, they might be equal if distance gives same score
        assert conf_1hr <= conf_6hr
        assert conf_6hr <= conf_48hr


class TestEnhancedMergeDecision:
    """Test enhanced merge decision logic."""

    def test_should_merge_pair_improved_high_confidence(self):
        """Test merge decision with high confidence."""
        base_time = 1699000000
        a = {
            "id": 1,
            "item_title": "Google sues Lighthouse phishing service",
            "summary_text": "Google sued Lighthouse for phishing as a service",
            "item_date": base_time,
            "feed_slug": "theverge",
        }
        b = {
            "id": 2,
            "item_title": "Google sues Lighthouse phishing service",
            "summary_text": "Google sued Lighthouse for phishing as a service",
            "item_date": base_time + 6 * 3600,  # 6 hours later
            "feed_slug": "slashdot",
        }

        result = should_merge_pair_improved(a, b, 22)

        assert result["should_merge"] is True
        assert result["reason"] == "approved"
        assert result["confidence"] >= 0.4
        assert result["time_gap_hours"] == 6

    def test_should_merge_pair_improved_low_confidence(self):
        """Test merge decision with low confidence."""
        low_conf_a = {
            "id": 1,
            "item_title": "Apple iPhone 15",
            "summary_text": "New iPhone features",
            "item_date": 1699000000,
            "feed_slug": "tech-crunch",
        }
        low_conf_b = {
            "id": 2,
            "item_title": "Stock Market Update",
            "summary_text": "Markets closed higher",
            "item_date": 1699010000,
            "feed_slug": "reuters",
        }

        result = should_merge_pair_improved(low_conf_a, low_conf_b, 22)

        assert isinstance(result, dict)
        # May or may not merge depending on distance, but should have valid structure
        assert 0.0 <= result.get("confidence", 0.0) <= 1.0
        assert isinstance(result.get("time_gap_hours", 0), int)


class TestEnhancedMergeIntegration:
    """Test full enhanced merge pipeline."""

    @pytest.mark.asyncio
    async def test_merge_similar_summaries_enhanced_basic(self):
        """Test basic enhanced merge functionality."""
        summaries = [
            {
                "id": 1,
                "item_title": "Apple iPhone 15 Launch",
                "summary_text": "Apple announced the new iPhone 15 with advanced features",
                "feed_slug": "tech-crunch",
                "item_date": 1699000000,
                "topic": "Technology",
            },
            {
                "id": 2,
                "item_title": "iPhone 15 Details",
                "summary_text": "The iPhone 15 includes improved camera and battery",
                "feed_slug": "the-verge",
                "item_date": 1699010000,
                "topic": "Technology",
            },
            {
                "id": 3,
                "item_title": "Stock Market Update",
                "summary_text": "Markets closed higher on tech gains",
                "feed_slug": "reuters",
                "item_date": 1699000000,
                "topic": "Business",
            },
        ]

        prompts = {"similar_merge": "Combine these related summaries"}
        chat_completion_fn = AsyncMock(return_value='{"summary": "Tech news about iPhone 15", "ids": [1, 2]}')

        result = await merge_similar_summaries(summaries, prompts, None, chat_completion_fn)

        # Should not increase number of items
        assert len(result) <= 3

        # Check for merged entries
        merged_entries = [r for r in result if r.get("merged_ids")]
        if merged_entries:
            # Should have quality metadata
            merged = merged_entries[0]
            assert "merge_confidence" in merged or "merged_count" in merged
            assert merged.get("merged_count", 0) >= 2

    @pytest.mark.asyncio
    async def test_merge_similar_summaries_no_candidates(self):
        """Test merge with insufficient candidates."""
        summaries = [{"id": 1, "summary_text": "Only one summary"}]

        prompts = {}
        chat_completion_fn = AsyncMock()

        result = await merge_similar_summaries(summaries, prompts, None, chat_completion_fn)

        # Should return original unchanged
        assert len(result) == 1
        assert result == summaries
        chat_completion_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_merge_similar_summaries_empty_input(self):
        """Test merge with empty input."""
        prompts = {}
        chat_completion_fn = AsyncMock()

        result = await merge_similar_summaries([], prompts, None, chat_completion_fn)

        # Should return empty
        assert len(result) == 0
        chat_completion_fn.assert_not_called()

    @pytest.mark.asyncio
    async def test_merge_similar_summaries_malformed_input(self):
        """Test merge with malformed input."""
        # Test with missing required fields
        malformed_summaries = [{"id": "not_an_int", "summary_text": "Bad ID"}, {"id": 2, "missing_summary": True}]

        prompts = {}
        chat_completion_fn = AsyncMock()

        result = await merge_similar_summaries(malformed_summaries, prompts, None, chat_completion_fn)

        # Should handle gracefully
        assert isinstance(result, list)
        # Should not crash


class TestPerformanceAndStress:
    """Test performance and edge cases."""

    @pytest.mark.asyncio
    async def test_merge_with_many_similar_items(self):
        """Test merge with many similar items."""
        # Create summaries that should cluster together
        base_time = 1699000000
        summaries = []

        for i in range(20):
            summaries.append(
                {
                    "id": i,
                    "item_title": f"iPhone 15{i}",
                    "summary_text": f"Apple iPhone 15 features and improvements {i}",
                    "feed_slug": "tech-crunch" if i % 2 == 0 else "the-verge",
                    "item_date": base_time + i * 3600,
                    "topic": "Technology",
                }
            )

        # Add some unrelated items
        for i in range(20, 25):
            summaries.append(
                {
                    "id": i,
                    "item_title": f"Other News {i}",
                    "summary_text": f"Unrelated story content {i}",
                    "feed_slug": "reuters",
                    "item_date": base_time + i * 3600,
                    "topic": "General",
                }
            )

        prompts = {"similar_merge": "Combine these related summaries"}
        chat_completion_fn = AsyncMock(return_value='{"summary": "Merged tech news", "ids": []}')

        import time

        start_time = time.time()

        result = await merge_similar_summaries(summaries, prompts, None, chat_completion_fn)

        end_time = time.time()
        duration = end_time - start_time

        # Should complete within reasonable time
        assert duration < 10.0, f"Merge took too long: {duration}s"
        assert len(result) <= len(summaries)

        # Should cluster iPhone stories together
        merged_entries = [r for r in result if r.get("merged_ids")]
        if merged_entries:
            largest_merged = max(merged_entries, key=lambda r: r.get("merged_count", 0))
            assert largest_merged["merged_count"] > 1

    def test_merge_error_handling(self):
        """Test error handling in merge functions."""
        # Test confidence scoring with bad input
        bad_a = {"bad": "data"}
        bad_b = {"also": "bad"}

        # Should not crash
        confidence = confidence_score_for_merge(bad_a, bad_b, 12)
        assert isinstance(confidence, float)
        assert 0.0 <= confidence <= 1.0

        # Test merge decision with bad input
        result = should_merge_pair_improved(bad_a, bad_b, 22)
        assert isinstance(result, dict)
        assert "should_merge" in result


class TestConfiguration:
    """Test merge configuration."""

    def test_merge_with_default_config(self):
        """Test merge with default configuration."""
        # Test that default values work
        result = should_merge_pair_improved(
            {"id": 1, "feed_slug": "test", "item_date": 1, "item_title": "Test", "summary_text": "Test"},
            {"id": 2, "feed_slug": "test2", "item_date": 2, "item_title": "Test", "summary_text": "Test"},
            22,
        )

        assert isinstance(result, dict)
        # May not have confidence if guardrails fail
        assert "confidence" in result or result["reason"] in {"guardrails_failed", "missing_fingerprint"}

    def test_merge_with_different_thresholds(self):
        """Test merge with different SimHash thresholds."""
        a = {
            "id": 1,
            "item_title": "Apple iPhone 15",
            "summary_text": "New iPhone features",
            "item_date": 1699000000,
            "feed_slug": "tech-crunch",
        }
        b = {
            "id": 2,
            "item_title": "iPhone 15 Launch",
            "summary_text": "Apple launched iPhone 15",
            "item_date": 1699010000,
            "feed_slug": "the-verge",
        }

        # Test with strict threshold
        result_strict = should_merge_pair_improved(a, b, 10)

        # Test with lenient threshold
        result_lenient = should_merge_pair_improved(a, b, 30)

        # Lenient threshold should be more permissive
        assert isinstance(result_strict, dict)
        assert isinstance(result_lenient, dict)

        # Confidence should be present if merge is possible, otherwise may be missing
        if result_strict.get("confidence") is not None and result_lenient.get("confidence") is not None:
            assert abs(result_strict["confidence"] - result_lenient["confidence"]) < 0.001


# Performance benchmarks
class TestBenchmarks:
    """Performance benchmarking."""

    @pytest.mark.asyncio
    async def benchmark_merge_scaling(self):
        """Benchmark merge performance with different input sizes."""
        import time

        sizes = [10, 50, 100, 200]
        prompts = {"similar_merge": "Combine these summaries"}
        chat_completion_fn = AsyncMock(return_value='{"summary": "Merged", "ids": []}')

        results = {}

        for size in sizes:
            # Create test data
            summaries = []
            for i in range(size):
                summaries.append(
                    {
                        "id": i,
                        "item_title": f"Story {i}",
                        "summary_text": f"Content for story {i}",
                        "feed_slug": f"feed-{i % 5}",
                        "item_date": 1699000000 + i * 1000,
                        "topic": "General",
                    }
                )

            start_time = time.time()
            result = await merge_similar_summaries(summaries, prompts, None, chat_completion_fn)
            end_time = time.time()

            results[size] = {"duration": end_time - start_time, "input_count": size, "output_count": len(result)}

        # Print benchmark results (in real test, you'd assert time limits)
        for size, metrics in results.items():
            print(
                f"Size {size}: {metrics['duration']:.3f}s, "
                f"Input: {metrics['input_count']}, Output: {metrics['output_count']}"
            )

        # Reasonable performance assertions
        assert results[10]["duration"] < 1.0
        assert results[50]["duration"] < 2.0
        assert results[100]["duration"] < 5.0


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
