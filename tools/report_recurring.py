#!/usr/bin/env python3
"""Recurring coverage threshold evaluation tool.

This script specifically tests how different thresholds perform when
detecting recurring coverage across different bulletins and time periods.

Usage:
    python -m tools.report_recurring [--db feeds.db] [--days 7]
"""

import json
import logging
import os
import sqlite3
import time
from typing import Dict, List, Any

# Suppress config logging noise (must be set before importing config)
os.environ.setdefault("LOG_LEVEL", "ERROR")  # noqa: E402
logging.getLogger("FeedProcessor").setLevel(logging.ERROR)

from config import config, get_logger  # noqa: E402
from utils import hamming_distance  # noqa: E402
from utils.merge_policy import (  # noqa: E402
    should_merge_pair_rows,
    pair_merge_threshold_rows,
    title_token_set_from_text,
    summary_token_set_from_text,
)
from tools.standard_args import (  # noqa: E402
    create_standard_parser,
    compute_lookback,
    DEFAULT_RECURRING_DAYS_BACK,
)
from tools.data_loaders import load_published_summaries  # noqa: E402

logger = get_logger("evaluate_recurring_threshold")


def simulate_recurring_detection(summaries: List[Dict[str, Any]], threshold: int, days_back: int = 3) -> Dict[str, Any]:
    """Simulate recurring coverage detection similar to the actual implementation."""
    cutoff_time = int(time.time()) - (days_back * 24 * 60 * 60)

    # Split into past and current summaries
    past_summaries = [s for s in summaries if s["published_date"] < cutoff_time]
    current_summaries = [s for s in summaries if s["published_date"] >= cutoff_time]

    recurring_ids = []
    detection_details = []

    for current in current_summaries:
        current_fp = current["merge_fp"]
        matches = []

        for past in past_summaries:
            # Skip if same summary
            if past["id"] == current["id"]:
                continue

            # Check merge eligibility
            if not should_merge_pair_rows(current, past):
                continue

            # Calculate adaptive threshold
            pair_threshold = pair_merge_threshold_rows(current, past, threshold)

            # Calculate distance
            dist = hamming_distance(current_fp, past["merge_fp"])
            if dist is None:
                continue

            if dist <= pair_threshold:
                matches.append(
                    {
                        "past_id": past["id"],
                        "past_title": past["title"],
                        "past_summary": past.get("summary_text", ""),
                        "past_feed": past["feed_slug"],
                        "past_date": past["published_date"],
                        "distance": dist,
                        "pair_threshold": pair_threshold,
                        "time_diff_hours": (current["published_date"] - past["published_date"]) // 3600,
                    }
                )

        if matches:
            recurring_ids.append(current["id"])
            # Sort by distance (closest first)
            matches.sort(key=lambda m: (m["distance"], -m["time_diff_hours"]))
            detection_details.append(
                {
                    "current_id": current["id"],
                    "current_title": current["title"],
                    "current_summary": current.get("summary_text", ""),
                    "current_feed": current["feed_slug"],
                    "matches": matches[:3],  # Keep top 3 matches
                }
            )

    return {
        "threshold": threshold,
        "total_current": len(current_summaries),
        "total_past": len(past_summaries),
        "recurring_count": len(recurring_ids),
        "recurring_rate": len(recurring_ids) / len(current_summaries) if current_summaries else 0,
        "recurring_ids": recurring_ids,
        "detection_details": detection_details,
    }


def analyze_recurring_quality(detection_details: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Analyze the quality of recurring coverage detections."""
    if not detection_details:
        return {
            "total": 0,
            "likely_correct": 0,
            "likely_false_positive": 0,
            "accuracy_rate": 0,
            "false_positive_rate": 0,
            "time_distribution": [],
        }

    likely_correct = 0
    likely_false_positive = 0
    time_differences = []

    for detection in detection_details:
        current_title = detection["current_title"].lower()
        current_summary = detection.get("current_summary", "").lower()
        matches = detection["matches"]

        if not matches:
            continue

        # Heuristic: check if summaries have significant overlap (primary signal)
        best_match = matches[0]
        past_title = best_match["past_title"].lower()
        past_summary = best_match.get("past_summary", "").lower()
        distance = best_match["distance"]
        time_diff = best_match["time_diff_hours"]
        time_differences.append(time_diff)

        # Use summary token overlap as primary quality signal (since SimHash is on summary+title)
        summary_tokens_current = summary_token_set_from_text(current_summary)
        summary_tokens_past = summary_token_set_from_text(past_summary)
        summary_overlap = len(summary_tokens_current & summary_tokens_past) / max(
            len(summary_tokens_current), len(summary_tokens_past), 1
        )
        
        # Title overlap as secondary signal
        title_tokens_current = title_token_set_from_text(current_title)
        title_tokens_past = title_token_set_from_text(past_title)
        title_overlap = len(title_tokens_current & title_tokens_past) / max(
            len(title_tokens_current), len(title_tokens_past), 1
        )

        # Consider likely correct if:
        # - Good summary overlap (>= 0.2) - primary signal since SimHash is on summary
        # - OR good title overlap (>= 0.3) with reasonable summary overlap (>= 0.1)
        # - Distance is reasonably small (<= threshold)
        # - Time difference is reasonable (not too short, not too long)
        if (summary_overlap >= 0.2 or (title_overlap >= 0.3 and summary_overlap >= 0.1)) and distance <= 18 and 6 <= time_diff <= 168:
            likely_correct += 1
        elif summary_overlap < 0.05 and title_overlap < 0.1:
            likely_false_positive += 1
        elif distance > 25 or time_diff > 336:  # > 2 weeks
            likely_false_positive += 1
        else:
            # Uncertain cases, lean toward correct if moderate overlap
            likely_correct += 0.5

    return {
        "total": len(detection_details),
        "likely_correct": likely_correct,
        "likely_false_positive": likely_false_positive,
        "accuracy_rate": likely_correct / len(detection_details) if detection_details else 0,
        "false_positive_rate": likely_false_positive / len(detection_details) if detection_details else 0,
        "time_distribution": sorted(time_differences),
    }


def main():
    parser = create_standard_parser(
        description="Evaluate recurring coverage detection thresholds",
        with_db=True,
        with_verbosity=False,
        with_time_window=True,
        with_threshold_range=True,
    )
    parser.add_argument(
        "--days-back", type=int, default=DEFAULT_RECURRING_DAYS_BACK,
        help=f"Days back for recurring detection (default: {DEFAULT_RECURRING_DAYS_BACK})"
    )
    parser.add_argument(
        "--json", action="store_true",
        help="Output results in JSON format (machine-readable)"
    )
    parser.add_argument(
        "--no-summaries", action="store_true",
        help="Hide summary text in examples (show titles only)"
    )
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    lookback_hours, lookback_label = compute_lookback(args)
    cutoff = int(time.time()) - lookback_hours * 3600

    summaries = load_published_summaries(conn, cutoff)

    # Test different thresholds
    results = []
    for threshold in range(args.min_threshold, args.max_threshold + 1, args.threshold_step):
        detection = simulate_recurring_detection(summaries, threshold, args.days_back)
        quality = analyze_recurring_quality(detection["detection_details"])
        result = {"threshold": threshold, **detection, **quality}
        results.append(result)

    # Find best threshold
    best_threshold = args.min_threshold
    best_score = -1
    for result in results:
        accuracy = result["accuracy_rate"]
        detection_rate = result["recurring_rate"]
        if accuracy >= 0.7 and 0.05 <= detection_rate <= 0.25:
            score = accuracy + detection_rate * 0.5
        elif accuracy >= 0.8:
            score = accuracy * 0.8
        else:
            score = accuracy * 0.5
        if score > best_score:
            best_score = score
            best_threshold = result["threshold"]

    best_result = next(r for r in results if r["threshold"] == best_threshold)
    current_threshold = getattr(config, "RECURRING_HAMMING_THRESHOLD", None)

    # JSON output mode
    if getattr(args, "json", False):
        output = {
            "lookback": lookback_label,
            "sample_size": len(summaries),
            "days_back": args.days_back,
            "recommended_threshold": best_threshold,
            "current_threshold": current_threshold,
            "thresholds": [
                {
                    "threshold": r["threshold"],
                    "recurring_count": r["recurring_count"],
                    "total_current": r["total_current"],
                    "total_past": r["total_past"],
                    "recurring_rate": round(r["recurring_rate"], 4),
                    "accuracy_rate": round(r["accuracy_rate"], 4),
                    "false_positive_rate": round(r["false_positive_rate"], 4),
                    "examples": [
                        {
                            "current_id": d["current_id"],
                            "current_title": d["current_title"],
                            "current_feed": d["current_feed"],
                            "matches": [
                                {
                                    "past_id": m["past_id"],
                                    "past_title": m["past_title"],
                                    "past_feed": m["past_feed"],
                                    "distance": m["distance"],
                                    "time_diff_hours": m["time_diff_hours"],
                                }
                                for m in d["matches"][:2]
                            ],
                        }
                        for d in r["detection_details"][:5]
                    ],
                }
                for r in results
            ],
        }
        print(json.dumps(output, indent=2))
        conn.close()
        return

    # Rich text output mode
    if len(summaries) < 50:
        print(f"Warning: Only {len(summaries)} summaries found - need more data for reliable evaluation")

    print(f"Loading published summaries from last {lookback_label}...")
    print(f"Found {len(summaries)} published summaries")
    print(f"Testing thresholds from {args.min_threshold} to {args.max_threshold} (step {args.threshold_step})")
    print(f"Recurring detection window: {args.days_back} days back")
    print()

    for result in results:
        print(
            f"Threshold {result['threshold']:2d}: "
            f"{result['recurring_count']:3d}/{result['total_current']:3d} recurring "
            f"({result['recurring_rate'] * 100:4.1f}%), "
            f"accuracy {result['accuracy_rate'] * 100:3.0f}%, "
            f"false positives {result['false_positive_rate'] * 100:3.0f}%"
        )

    print()
    print("=== THRESHOLD RECOMMENDATION FOR RECURRING COVERAGE ===")
    print()

    print(f"Recommended threshold: {best_threshold}")
    print("Expected performance:")
    print(f"  - Detection rate: {best_result['recurring_rate'] * 100:.1f}% of new content flagged as recurring")
    print(f"  - Estimated accuracy: {best_result['accuracy_rate'] * 100:.1f}%")
    print(f"  - False positive rate: {best_result['false_positive_rate'] * 100:.1f}%")
    print()

    # Show examples for recommended threshold
    print("=== EXAMPLES FOR RECOMMENDED THRESHOLD ===")
    print()

    show_summaries = not getattr(args, 'no_summaries', False)
    examples = best_result["detection_details"][:5]
    for i, detection in enumerate(examples, 1):
        print(f"Example {i}: {detection['current_title'][:80]}...")
        print(f"  Current: [{detection['current_feed']}] #{detection['current_id']}")
        if show_summaries and detection.get("current_summary"):
            summary = detection["current_summary"][:200]
            if len(detection.get("current_summary", "")) > 200:
                summary += "..."
            print(f"    {summary}")

        for j, match in enumerate(detection["matches"][:2], 1):
            print(
                f"  Match {j}: distance={match['distance']}, threshold={match['pair_threshold']}, "
                f"{match['time_diff_hours']}h ago"
            )
            print(f"    [{match['past_feed']}] #{match['past_id']} {match['past_title'][:60]}...")
            if show_summaries and match.get("past_summary"):
                summary = match["past_summary"][:200]
                if len(match.get("past_summary", "")) > 200:
                    summary += "..."
                print(f"      {summary}")
        print()

    # Show time distribution
    if best_result["time_distribution"]:
        print("=== TIME DISTRIBUTION OF RECURRING COVERAGE ===")
        times = best_result["time_distribution"]
        print(f"Median time between recurring stories: {times[len(times) // 2]:.0f} hours")
        print(f"Shortest: {min(times):.0f} hours, Longest: {max(times):.0f} hours")
        print()

    # Current configuration
    current_result = next((r for r in results if r["threshold"] == current_threshold), None) if current_threshold else None

    if current_result:
        print(f"Current threshold {current_threshold}:")
        print(f"  - Detection rate: {current_result['recurring_rate'] * 100:.1f}%")
        print(f"  - Accuracy: {current_result['accuracy_rate'] * 100:.1f}%")
        print(f"  - False positives: {current_result['false_positive_rate'] * 100:.1f}%")

    print()
    print("=== RECOMMENDATIONS ===")
    print("• For recurring coverage, accuracy is more important than detection rate")
    print("• False positives can cause legitimate stories to be filtered out")
    print("• Consider your specific use case:")
    print("  - News feeds: prioritize accuracy to avoid missing breaking news")
    print("  - Technical blogs: can tolerate more false positives to reduce duplication")
    print("• Monitor the actual recurring detections after deployment")

    conn.close()


if __name__ == "__main__":
    main()
