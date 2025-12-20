import pytest

from workers.publisher.merge import merge_summaries


def test_merge_summaries_merges_by_topic_sorted():
    summaries = [
        {"topic": "B", "item_title": "B item"},
        {"topic": "A", "item_title": "A item"},
        {"topic": "A", "item_title": "Another A"},
    ]

    merged = merge_summaries(summaries)

    assert list(merged.keys()) == ["A", "B"]
    assert merged["A"] == [
        {"topic": "A", "item_title": "A item"},
        {"topic": "A", "item_title": "Another A"},
    ]


def test_merge_summaries_preserves_original_order_within_topic():
    summaries = [
        {"topic": "Topic", "item_title": f"Item {i}"} for i in range(5)
    ]

    merged = merge_summaries(summaries)

    assert merged["Topic"] == summaries
