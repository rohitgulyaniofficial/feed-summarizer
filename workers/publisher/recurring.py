"""Recurring coverage detection for bulletins."""

from time import time
from typing import Any, Dict, List, Optional

from config import config, get_logger
from utils import hamming_distance
from utils.merge_policy import (
    merge_fingerprint_from_row,
    should_merge_pair_rows,
    pair_merge_threshold_rows,
    title_token_set_from_text,
    summary_token_set_from_text,
)

logger = get_logger("publisher.recurring")


def _calculate_match_confidence(
    current: Dict[str, Any], past: Dict[str, Any], distance: int, current_feed: str, past_feed: str
) -> float:
    """Calculate confidence score for a potential recurring coverage match.

    Returns a confidence score between 0.0 and 1.0, where higher values
    indicate more likely true recurring coverage.
    """
    confidence = 0.0

    # Handle None inputs gracefully
    if not current or not past:
        return 0.0

    # Get tokens for overlap analysis
    try:
        current_title_tokens = title_token_set_from_text(current.get("title", ""))
        past_title_tokens = title_token_set_from_text(past.get("title", ""))
        current_summary_tokens = summary_token_set_from_text(current.get("summary_text", ""))
        past_summary_tokens = summary_token_set_from_text(past.get("summary_text", ""))
    except Exception:
        return 0.0

    # Title overlap (strong signal for recurring coverage)
    title_overlap = len(current_title_tokens & past_title_tokens)
    if title_overlap >= 2:
        confidence += 0.4
    elif title_overlap >= 1:
        confidence += 0.2

    # Summary overlap (medium signal)
    summary_overlap = len(current_summary_tokens & past_summary_tokens)
    if summary_overlap >= 6:
        confidence += 0.3
    elif summary_overlap >= 4:
        confidence += 0.2
    elif summary_overlap >= 2:
        confidence += 0.1

    # Cross-feed bonus (strong signal for recurring coverage)
    if current_feed != past_feed:
        confidence += 0.2

    # Distance penalty (closer is generally better)
    if distance <= 12:
        confidence += 0.2
    elif distance <= 18:
        confidence += 0.1

    # Time gap factor (prefer 6h-7d; penalize very fresh or very old inside window)
    current_ts = current.get("generated_date") or current.get("published_date") or 0
    past_ts = past.get("generated_date") or past.get("published_date") or 0
    time_gap_hours = abs((current_ts - past_ts) // 3600)

    if time_gap_hours < 2:
        confidence *= 0.2
    elif 2 <= time_gap_hours <= 72:
        confidence *= 1.0
    elif 72 < time_gap_hours <= 168:
        confidence *= 0.9
    else:
        confidence *= 0.7

    return max(0.0, min(1.0, confidence))


async def detect_recurring_coverage(
    summaries: List[Dict[str, Any]],
    group_name: str,
    db,
    days_back: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Detect summaries that match news from past N days across ALL feeds.

    Uses improved criteria optimized for recurring coverage detection:
    - Configurable tight threshold (default: 16) to reduce false positives
    - Configurable lookback period (default: 7 days) for recurring coverage
    - Configurable confidence threshold (default: 0.6) for minimum match confidence
    - Cross-feed preference to prioritize real recurring stories
    - Same-hour skip to avoid same-story duplicates

    Returns a list of summary IDs that are close matches to previously published
    news in past `days_back` days from ANY feed/group.

    Args:
        summaries: Current summaries being processed for bulletin
        group_name: Name of the bulletin group (used for logging only)
        db: Database queue instance
        days_back: Number of days to look back for previous coverage (uses config default)

    Returns:
        Dictionary with 'recurring_ids' and 'coverage_stats' keys
    """
    # Use tighter threshold for recurring coverage to reduce false positives
    # This is more conservative than bulletin duplicate detection threshold
    # Default to 16 (tighter than 24 for general duplicates)
    default_threshold = 16
    threshold = max(0, int(getattr(config, "RECURRING_HAMMING_THRESHOLD", default_threshold) or default_threshold))
    confidence_threshold = getattr(config, "RECURRING_CONFIDENCE_THRESHOLD", 0.6)
    lookback_days = days_back if days_back is not None else getattr(config, "RECURRING_LOOKBACK_DAYS", 7)
    min_gap_hours = getattr(config, "RECURRING_MIN_GAP_HOURS", 1)

    if not summaries:
        return {"recurring_ids": [], "coverage_stats": {}}

    if db is None:
        logger.warning("detect_recurring_coverage called without db; skipping detection")
        return {"recurring_ids": [], "coverage_stats": {}}

    recurring_ids: List[int] = []

    try:
        # Get ALL published summaries from past N days (across all feeds/groups)
        cutoff_time = int(time()) - (lookback_days * 24 * 60 * 60)
        past_summaries = await db.execute(
            "query_all_published_summaries_by_date",
            cutoff_time=cutoff_time,
        )

        if not past_summaries:
            logger.debug(
                "No past published summaries found within %d days (checked across all feeds)",
                lookback_days,
            )
            return {"recurring_ids": [], "coverage_stats": {}}

        logger.info(
            "Checking %d current summaries for group '%s' against %d past summaries from all feeds (using %d-day lookback, %.1f confidence threshold)",
            len(summaries),
            group_name,
            len(past_summaries),
            lookback_days,
            confidence_threshold,
        )

        # For each current summary, check if it matches any past summary
        for summary in summaries:
            summary_id = summary.get("id")
            if not isinstance(summary_id, (int, str)):
                continue

            # Normalize expected fields for fingerprinting
            if "summary_text" not in summary and "summary" in summary:
                summary["summary_text"] = summary.get("summary") or ""
            if "item_title" not in summary and "title" in summary:
                summary["item_title"] = summary.get("title") or ""
            summary.setdefault("feed_slug", "")

            # Compute merge fingerprint for current summary
            current_fp = merge_fingerprint_from_row(summary)
            if current_fp is None:
                continue

            current_feed = summary.get("feed_slug", "")
            best_match = None
            best_confidence = 0.0

            # Check against all past summaries
            for past_summary in past_summaries:
                # Skip if this is the same summary (already published)
                past_id = past_summary.get("id")
                if past_id == summary_id:
                    continue

                # Normalize past fields
                if "summary_text" not in past_summary and "summary" in past_summary:
                    past_summary["summary_text"] = past_summary.get("summary") or ""
                if "item_title" not in past_summary and "title" in past_summary:
                    past_summary["item_title"] = past_summary.get("title") or ""
                past_summary.setdefault("feed_slug", "")

                # Compute merge fingerprint for past summary
                past_fp = merge_fingerprint_from_row(past_summary)
                if past_fp is None:
                    continue

                past_feed = past_summary.get("feed_slug", "")

                # Check if they should merge based on similarity
                if not should_merge_pair_rows(summary, past_summary):
                    continue

                # Calculate Hamming distance
                dist = hamming_distance(current_fp, past_fp)
                if dist is None:
                    continue

                # Get adaptive threshold for this pair
                pair_threshold = pair_merge_threshold_rows(summary, past_summary, threshold)

                # Skip if distance exceeds threshold
                if dist > pair_threshold:
                    continue

                # Enforce minimum gap to avoid same-hour duplicates
                current_ts = summary.get("generated_date") or summary.get("published_date") or 0
                past_ts = past_summary.get("generated_date") or past_summary.get("published_date") or 0
                time_gap_hours = abs((current_ts - past_ts) // 3600)
                if time_gap_hours < min_gap_hours:
                    continue

                # Calculate confidence score for this match
                confidence = _calculate_match_confidence(summary, past_summary, dist, current_feed, past_feed)

                # Prefer cross-feed matches and higher confidence
                if (
                    best_match is None
                    or (confidence > best_confidence)
                    or (confidence == best_confidence and past_feed != current_feed)
                ):
                    best_match = past_summary
                    best_confidence = confidence

            # Mark as recurring if found good match
            if best_match and best_confidence >= confidence_threshold:  # Use configurable confidence threshold
                recurring_ids.append(int(summary_id))
                past_feed = best_match.get("feed_slug", "unknown")
                past_id = best_match.get("id")
                dist = hamming_distance(current_fp, merge_fingerprint_from_row(best_match))
                logger.info(
                    "Summary %s (group '%s') matches past summary %s from feed '%s' (distance=%d, confidence=%.2f, threshold=%.1f) - marking as recurring",
                    summary_id,
                    group_name,
                    past_id,
                    past_feed,
                    dist,
                    best_confidence,
                    confidence_threshold,
                )
                # Found a match, no need to check other past summaries

        if recurring_ids:
            logger.info(
                "Detected %d recurring summaries in group '%s' (out of %d total, checked against all feeds, using %d-day lookback, %.1f confidence threshold)",
                len(recurring_ids),
                group_name,
                len(summaries),
                lookback_days,
                confidence_threshold,
            )

        return {
            "recurring_ids": recurring_ids,
            "coverage_stats": {
                "total_checked": len(summaries),
                "total_past_summaries": len(past_summaries) if "past_summaries" in locals() else 0,
                "lookback_days": lookback_days,
                "confidence_threshold": confidence_threshold,
                "thresholds_used": threshold,
            },
        }

    except Exception as exc:
        logger.error(
            "Error detecting recurring coverage for group '%s': %s",
            group_name,
            exc,
            exc_info=True,
        )
        return {"recurring_ids": [], "coverage_stats": {}}


__all__ = ["detect_recurring_coverage"]
