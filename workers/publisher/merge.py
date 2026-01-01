"""Summary merge utilities for publisher."""

from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import json
import re

from config import config, get_logger
from utils import hamming_distance
from utils.merge_policy import (
    merge_fingerprint_from_row,
    merge_text_from_row,
    pair_merge_threshold_rows,
    should_merge_pair_rows,
    summary_token_set_from_text,
    title_token_set_from_text,
)
from utils.hashed_cosine import build_hashed_tf_vector, cosine_similarity
from utils.clustering import cluster_indices

logger = get_logger("publisher.merge")


def confidence_score_for_merge(a: Dict[str, Any], b: Dict[str, Any], distance: int) -> float:
    """Calculate confidence score for a potential merge.

    Returns confidence between 0.0 and 1.0, where higher values indicate
    higher quality merges.

    Note: We first accumulate evidence additively, then apply a multiplicative
    time-gap factor so that short-gap items are penalized meaningfully.
    """
    # Start from a small baseline so that penalties still have effect
    confidence = 0.5

    # Distance factor (closer is better)
    if distance <= 8:
        confidence += 0.3  # Very close
    elif distance <= 15:
        confidence += 0.2  # Close
    elif distance <= 22:
        confidence += 0.1  # Moderate
    else:
        confidence -= 0.1  # Far away penalty

    # Title overlap factor
    title_a = set(re.findall(r"[a-z0-9]{3,}", (a.get("item_title", "") or a.get("title", "")).lower()))
    title_b = set(re.findall(r"[a-z0-9]{3,}", (b.get("item_title", "") or b.get("title", "")).lower()))
    title_overlap = len(title_a & title_b)

    if title_overlap >= 3:
        confidence += 0.4  # Strong title match
    elif title_overlap >= 2:
        confidence += 0.2  # Good title match
    elif title_overlap >= 1:
        confidence += 0.1  # Some title match

    # Summary overlap factor
    summary_a = set(re.findall(r"[a-z0-9]{3,}", (a.get("summary_text", "")).lower()))
    summary_b = set(re.findall(r"[a-z0-9]{3,}", (b.get("summary_text", "")).lower()))
    summary_overlap = len(summary_a & summary_b)

    if summary_overlap >= 8:
        confidence += 0.3  # Strong summary match
    elif summary_overlap >= 5:
        confidence += 0.2  # Good summary match
    elif summary_overlap >= 3:
        confidence += 0.1  # Some summary match

    # Cross-feed bonus (important)
    feed_a = a.get("feed_slug", "")
    feed_b = b.get("feed_slug", "")
    if feed_a != feed_b:
        confidence += 0.2  # Cross-feed bonus

    # Time gap factor (applied at the end to scale overall confidence)
    time_gap_hours = None
    try:
        time_a = int(a.get("item_date"))
        time_b = int(b.get("item_date"))
        time_gap_hours = abs((time_a - time_b) // 3600)
    except (TypeError, ValueError, KeyError):
        time_gap_hours = None

    if time_gap_hours is not None:
        if time_gap_hours < 2:  # Likely same story
            confidence *= 0.2  # Heavy penalty
        elif 2 <= time_gap_hours <= 24:  # Same day/next day
            confidence *= 0.7  # Moderate penalty
        elif 24 < time_gap_hours <= 168:  # 1-7 days
            confidence *= 0.9  # Small penalty
        elif time_gap_hours > 168:  # More than a week
            confidence *= 0.5  # Reduced relevance

    # Normalize to 0.0-1.0 range
    return max(0.0, min(1.0, confidence))


def should_merge_pair_improved(
    a: Dict[str, Any],
    b: Dict[str, Any],
    base_threshold: int,
    cross_feed_penalty: bool = True,
    min_time_gap_hours: int = 2,
    min_confidence: float = 0.4,
) -> Dict[str, Any]:
    """Enhanced merge guardrails with cross-feed preference and confidence scoring.

    Args:
        a, b: Summary dictionaries to compare
        base_threshold: Base SimHash threshold
        cross_feed_penalty: Whether to penalize same-feed merges
        min_time_gap_hours: Minimum hours gap between items
        min_confidence: Minimum confidence score required

    Returns:
        Dict with merge decision and metadata
    """

    # Get basic merge fingerprint
    from utils.merge_policy import merge_fingerprint_from_row

    fp_a = merge_fingerprint_from_row(a)
    fp_b = merge_fingerprint_from_row(b)

    if fp_a is None or fp_b is None:
        return {"should_merge": False, "reason": "missing_fingerprint"}

    # Check basic guardrails first
    if not should_merge_pair_rows(a, b):
        return {"should_merge": False, "reason": "guardrails_failed"}

    # Calculate distance and adaptive threshold
    from utils import hamming_distance

    distance = hamming_distance(fp_a, fp_b)
    if distance is None:
        return {"should_merge": False, "reason": "distance_calculation_failed"}

    adaptive_threshold = pair_merge_threshold_rows(a, b, base_threshold)

    # Calculate confidence score
    confidence = confidence_score_for_merge(a, b, distance)

    # Cross-feed penalty
    feed_a = a.get("feed_slug", "")
    feed_b = b.get("feed_slug", "")
    if cross_feed_penalty and feed_a == feed_b:
        confidence *= 0.7  # Penalty for same-feed

    # Time gap check
    time_gap_hours = None
    try:
        time_a = int(a.get("item_date"))
        time_b = int(b.get("item_date"))
        time_gap_hours = abs((time_a - time_b) // 3600)
    except (TypeError, ValueError, KeyError):
        time_gap_hours = None

    if time_gap_hours is not None and time_gap_hours < min_time_gap_hours:
        return {
            "should_merge": False,
            "reason": "time_gap_too_short",
            "confidence": confidence,
            "distance": distance,
            "adaptive_threshold": adaptive_threshold,
            "is_cross_feed": feed_a != feed_b,
            "time_gap_hours": time_gap_hours,
        }

    # Final decision
    should_merge = distance <= adaptive_threshold and confidence >= min_confidence

    return {
        "should_merge": should_merge,
        "reason": "approved" if should_merge else "confidence_too_low",
        "confidence": confidence,
        "distance": distance,
        "adaptive_threshold": adaptive_threshold,
        "is_cross_feed": feed_a != feed_b,
        "time_gap_hours": time_gap_hours,
    }


def merge_summaries(summaries: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    """Group summaries by topic, sorting topics while preserving per-topic order."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for summary in summaries or []:
        topic = summary.get("topic") or "General"
        grouped.setdefault(topic, []).append(summary)
    return {topic: grouped[topic] for topic in sorted(grouped.keys())}


def summary_id_list(summary: Dict[str, Any]) -> List[int]:
    """Return the list of source summary IDs represented by this entry."""
    merged_ids = summary.get("merged_ids")
    if merged_ids:
        return [int(i) for i in merged_ids if isinstance(i, (int, str))]
    sid = summary.get("id")
    return [int(sid)] if isinstance(sid, (int, str)) else []


def collect_summary_links(summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return list of link descriptors for a summary (merged aware)."""
    links = summary.get("merged_links") or []
    collected: List[Dict[str, Any]] = []
    seen: set = set()
    for link in links:
        url = (link or {}).get("url")
        if not url or url in seen:
            continue
        seen.add(url)
        collected.append(
            {
                "url": url,
                "title": link.get("title") or summary.get("item_title") or summary.get("title") or "Read more",
                "feed_slug": link.get("feed_slug") or summary.get("feed_slug"),
            }
        )
    if collected:
        return collected
    fallback_url = summary.get("item_url") or summary.get("url")
    if fallback_url:
        collected.append(
            {
                "url": fallback_url,
                "title": summary.get("item_title") or summary.get("title") or "Read more",
                "feed_slug": summary.get("feed_slug"),
            }
        )
    return collected


def build_merge_links(group: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Collect deduplicated links for a merged summary group."""
    links: List[Dict[str, Any]] = []
    seen_urls: set = set()
    for member in group:
        url = member.get("item_url") or member.get("url")
        if not url or url in seen_urls:
            continue
        seen_urls.add(url)
        links.append(
            {
                "url": url,
                "title": member.get("item_title") or member.get("title") or "Read more",
                "feed_slug": member.get("feed_slug"),
            }
        )
    return links


def title_token_set(summary: Dict[str, Any]) -> Set[str]:
    """Extract a normalized set of significant tokens from a summary title."""
    raw = summary.get("item_title") or summary.get("title") or ""
    return title_token_set_from_text(str(raw))


def summary_token_set(summary: Dict[str, Any]) -> Set[str]:
    """Extract a normalized set of significant tokens from the summary text."""
    raw = summary.get("summary_text") or ""
    return summary_token_set_from_text(str(raw))


def bm25_match_query(summary: Dict[str, Any], max_tokens: int) -> str:
    """Build a conservative FTS5 MATCH query from title+summary tokens."""
    max_n = 8
    try:
        max_n = int(max_tokens)
    except Exception:
        max_n = 8
    if max_n <= 0:
        max_n = 8

    title_tokens = list(title_token_set(summary))
    summary_tokens = list(summary_token_set(summary))
    tokens = set(title_tokens + summary_tokens)
    if not tokens:
        return ""

    ranked = sorted(tokens, key=lambda t: (len(t), t), reverse=True)
    ranked = ranked[:max_n]
    parts = [f"{t}*" for t in ranked if t]
    return " OR ".join(parts)


def should_merge_pair(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    """Guardrails to reduce accidental simhash collisions."""
    return should_merge_pair_rows(a, b)


def pair_merge_threshold(a: Dict[str, Any], b: Dict[str, Any], base_threshold: int) -> int:
    """Return an adaptive SimHash threshold for a candidate pair."""
    return pair_merge_threshold_rows(a, b, base_threshold)


def merged_display_topic(group: List[Dict[str, Any]]) -> str:
    """Choose a topic label for a merged cluster."""
    raw_topics: List[str] = []
    for member in group:
        value = member.get("topic")
        if not isinstance(value, str):
            continue
        t = value.strip()
        if not t:
            continue
        if t.lower() in {"general", "misc", "unknown"}:
            continue
        raw_topics.append(t)

    if not raw_topics:
        return "Breaking News"

    unique = {t for t in raw_topics}
    if len(unique) == 1:
        return next(iter(unique))

    return "Breaking News"


def merge_fingerprint(summary: Dict[str, Any]) -> Optional[int]:
    """Compute a fingerprint used for merging."""
    return merge_fingerprint_from_row(summary)


def merge_similarity_text(summary: Dict[str, Any]) -> str:
    """Build the text used for hashed-cosine confirmation."""
    return merge_text_from_row(summary)


async def synthesize_merged_summary(
    group: List[Dict[str, Any]],
    prompt_template: str,
    use_llm: bool,
    chat_completion_fn: Callable[[List[Dict[str, str]]], Any],
) -> str:
    """Build a merged summary text for a cluster of similar summaries."""
    fallback_parts = [g.get("summary_text", "").strip() for g in group if g.get("summary_text")]
    fallback_text = "; ".join([part for part in fallback_parts if part])
    if len(fallback_text) > 1000:
        fallback_text = fallback_text[:1000]
    if not use_llm or not prompt_template:
        return fallback_text

    payload_lines = ["Summaries:"]
    for member in group:
        payload_lines.append(f"ID: {member.get('id')}")
        text = (member.get("summary_text") or "").strip()
        if text:
            payload_lines.append(f"Summary: {text[:600]}")
        payload_lines.append("")
    formatted_prompt = f"{prompt_template}\n\n" + "\n".join(payload_lines).strip()
    messages = [{"role": "user", "content": formatted_prompt}]
    try:
        response = await chat_completion_fn(messages, purpose="similar_merge")
        if not response:
            return fallback_text
        parsed = json.loads(response)
        if isinstance(parsed, list) and parsed:
            expected_ids: Set[int] = set()
            for member in group:
                try:
                    expected_ids.add(int(member.get("id")))
                except Exception:
                    continue

            best_summary: Optional[str] = None
            best_overlap = -1
            best_exact = False
            best_size = -1

            for entry in parsed:
                if not isinstance(entry, dict):
                    continue
                summary_text = entry.get("summary")
                ids_val = entry.get("ids")
                if not isinstance(summary_text, str) or not summary_text.strip():
                    continue
                if not isinstance(ids_val, list) or not ids_val:
                    continue

                ids_set: Set[int] = set()
                for raw_id in ids_val:
                    try:
                        ids_set.add(int(raw_id))
                    except Exception:
                        continue

                if not ids_set:
                    continue

                overlap = len(ids_set & expected_ids) if expected_ids else 0
                exact = bool(expected_ids) and (ids_set == expected_ids)
                size = len(ids_set)

                if exact and not best_exact:
                    best_exact = True
                    best_overlap = overlap
                    best_size = size
                    best_summary = summary_text.strip()
                    continue

                if exact == best_exact:
                    if overlap > best_overlap or (overlap == best_overlap and size > best_size):
                        best_overlap = overlap
                        best_size = size
                        best_summary = summary_text.strip()

            if best_summary:
                if expected_ids and best_overlap < len(expected_ids):
                    logger.warning(
                        "similar_merge returned partial ID coverage (expected=%s overlap=%d/%d)",
                        sorted(expected_ids),
                        best_overlap,
                        len(expected_ids),
                    )
                return best_summary
    except Exception as exc:
        ids = [member.get("id") for member in group]
        logger.warning("similar_merge prompt failed for ids %s: %s", ids, exc)
    return fallback_text


async def merge_similar_summaries(
    summaries: List[Dict[str, Any]],
    prompts: Dict[str, Any],
    db,
    chat_completion_fn: Callable[[List[Dict[str, str]]], Any],
) -> List[Dict[str, Any]]:
    """Merge highly similar summaries using enhanced SimHash with confidence scoring."""
    threshold = max(0, int(getattr(config, "SIMHASH_HAMMING_THRESHOLD", 0) or 0))
    if threshold <= 0 or not summaries:
        return summaries

    # Add fingerprints to summaries using the enhanced version
    for summary in summaries:
        try:
            summary["_merge_fp"] = merge_fingerprint_from_row(summary)
        except Exception as e:
            logger.warning("Failed to compute merge fingerprint for summary %s: %s", summary.get("id"), e)
            summary["_merge_fp"] = None

    candidates = [s for s in summaries if isinstance(s.get("_merge_fp"), int) and isinstance(s.get("id"), (int, str))]
    if len(candidates) < 2:
        return summaries

    # Enhanced options from config (defaulting to enabled for production)
    enable_confidence_scoring = bool(getattr(config, "ENHANCED_MERGE_CONFIDENCE_SCORING", True))
    enable_cross_feed_preference = bool(getattr(config, "ENHANCED_MERGE_CROSS_FEED_PREFERENCE", True))
    enable_time_gap_logic = bool(getattr(config, "ENHANCED_MERGE_TIME_GAP_LOGIC", True))
    min_confidence_threshold = float(getattr(config, "ENHANCED_MERGE_MIN_CONFIDENCE", 0.4) or 0.4)
    min_time_gap_hours = int(getattr(config, "ENHANCED_MERGE_MIN_TIME_GAP_HOURS", 2) or 2)

    # Existing optional features
    cosine_enabled = bool(getattr(config, "HASHED_COSINE_ENABLED", False))
    cosine_min = float(getattr(config, "HASHED_COSINE_MIN_SIM", 0.25) or 0.25)
    cosine_buckets = int(getattr(config, "HASHED_COSINE_BUCKETS", 65536) or 65536)
    cosine_max_tokens = int(getattr(config, "HASHED_COSINE_MAX_TOKENS", 128) or 128)

    hashed_vectors: List[Tuple[Dict[int, float], float]] = []
    if cosine_enabled:
        for s in candidates:
            try:
                text = merge_text_from_row(s)
                hashed_vectors.append(
                    build_hashed_tf_vector(text, buckets=cosine_buckets, max_tokens=cosine_max_tokens)
                )
            except Exception:
                hashed_vectors.append(({}, 0.0))

    bm25_enabled = bool(getattr(config, "BM25_MERGE_ENABLED", False))
    bm25_ratio_threshold = float(getattr(config, "BM25_MERGE_RATIO_THRESHOLD", 0.80) or 0.80)
    bm25_max_extra = int(getattr(config, "BM25_MERGE_MAX_EXTRA_DISTANCE", 6) or 6)
    bm25_max_tokens = int(getattr(config, "BM25_MERGE_MAX_QUERY_TOKENS", 8) or 8)

    linkage = str(getattr(config, "SIMHASH_MERGE_LINKAGE", "complete") or "complete").strip().lower()
    if linkage not in {"single", "complete"}:
        linkage = "complete"

    # Enhanced merge decision function
    def should_merge_enhanced(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        """Enhanced merge decision with confidence scoring."""
        if enable_confidence_scoring or enable_cross_feed_preference or enable_time_gap_logic:
            result = should_merge_pair_improved(
                a,
                b,
                threshold,
                cross_feed_penalty=enable_cross_feed_preference,
                min_time_gap_hours=min_time_gap_hours,
                min_confidence=min_confidence_threshold,
            )
            if enable_confidence_scoring:
                result["confidence"] = confidence_score_for_merge(a, b, result.get("distance", 0))
            return result
        else:
            # Fallback to basic logic
            if not should_merge_pair_rows(a, b):
                return {"should_merge": False, "reason": "guardrails_failed"}

            fp_a = a.get("_merge_fp")
            fp_b = b.get("_merge_fp")
            distance = hamming_distance(fp_a, fp_b) if fp_a is not None and fp_b is not None else None
            if distance is None:
                return {"should_merge": False, "reason": "distance_calculation_failed"}

            adaptive_threshold = pair_merge_threshold_rows(a, b, threshold)
            should_merge = distance <= adaptive_threshold

            return {
                "should_merge": should_merge,
                "reason": "approved" if should_merge else "threshold_exceeded",
                "distance": distance,
                "adaptive_threshold": adaptive_threshold,
                "confidence": 1.0 if should_merge else 0.0,
                "is_cross_feed": a.get("feed_slug", "") != b.get("feed_slug", ""),
                "time_gap_hours": abs((a.get("item_date", 0) - b.get("item_date", 0)) // 3600),
            }

    def _as_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    # Build distance and threshold matrices
    pair_dist: Dict[Tuple[int, int], int] = {}
    pair_thr: Dict[Tuple[int, int], int] = {}
    extra_edges: Set[Tuple[int, int]] = set()

    # BM25 ratios for candidates
    bm25_ratios: Dict[int, Dict[int, float]] = {}
    if bm25_enabled and db is not None:
        id_to_summary: Dict[int, Dict[str, Any]] = {}
        candidate_ids_all: List[int] = []
        for s in candidates:
            sid = _as_int(s.get("id"))
            if sid is not None:
                id_to_summary[sid] = s
                candidate_ids_all.append(sid)

        if len(candidate_ids_all) >= 2:
            for sid in candidate_ids_all:
                s = id_to_summary[sid]
                query = bm25_match_query(s, bm25_max_tokens)
                if not query:
                    continue
                candidate_ids = [x for x in candidate_ids_all if x != sid]
                try:
                    resp = await db.execute(
                        "bm25_candidates",
                        query_id=sid,
                        query_text=query,
                        topic=None,
                        candidate_ids=candidate_ids,
                        limit=10,
                    )
                except Exception:
                    continue

                self_score = None
                try:
                    self_score = resp.get("self_score") if isinstance(resp, dict) else None
                except Exception:
                    continue

                denom = abs(float(self_score)) if self_score else 1.0
                if denom <= 0:
                    continue

                out: Dict[int, float] = {}
                for row in (resp.get("candidates") or []) if isinstance(resp, dict) else []:
                    try:
                        cid = int(row.get("id"))
                        score = float(row.get("score"))
                        ratio = abs(score) / denom
                        if ratio > 1:
                            ratio = 1.0
                        out[cid] = ratio
                    except Exception:
                        continue

                if out:
                    bm25_ratios[sid] = out

    def _get_dist(i: int, j: int) -> Optional[int]:
        if i == j:
            return 0
        a, b = (i, j) if i < j else (j, i)
        if (a, b) in extra_edges:
            return 0
        return pair_dist.get((i, j))

    def _get_thr(i: int, j: int) -> Optional[int]:
        if i == j:
            return threshold
        a, b = (i, j) if i < j else (j, i)
        if (a, b) in extra_edges:
            return 0
        return pair_thr.get((i, j))

    def _leader_key(i: int) -> int:
        sid = _as_int(candidates[i].get("id"))
        return sid if sid is not None else 10**9

    # Compute all pairs with enhanced logic
    for i in range(len(candidates)):
        for j in range(i + 1, len(candidates)):
            a = candidates[i]
            b = candidates[j]

            # Check enhanced guardrails
            merge_decision = should_merge_enhanced(a, b)
            if not merge_decision["should_merge"]:
                continue

            # Build matrices
            distance = hamming_distance(a.get("_merge_fp", 0), b.get("_merge_fp", 0))
            if distance is None:
                continue
            pair_dist[(i, j)] = int(distance)
            pair_thr[(i, j)] = merge_decision.get("adaptive_threshold", threshold)

            # BM25 extra edges
            if bm25_enabled and merge_decision["should_merge"]:
                sid_a = _as_int(a.get("id"))
                sid_b = _as_int(b.get("id"))

                if sid_a is not None and sid_b is not None:
                    ra = bm25_ratios.get(sid_a, {}).get(sid_b, 0.0)
                    rb = bm25_ratios.get(sid_b, {}).get(sid_a, 0.0)

                    if (
                        ra >= bm25_ratio_threshold
                        and rb >= bm25_ratio_threshold
                        and distance <= merge_decision.get("adaptive_threshold", threshold) + bm25_max_extra
                    ):
                        extra_edges.add((i, j))

            # Confidence vectors for cosine similarity
            if cosine_enabled and i < len(hashed_vectors) and j < len(hashed_vectors):
                cos = cosine_similarity(hashed_vectors[i], hashed_vectors[j])
                if cos < cosine_min:
                    continue

    # Enhanced clustering
    clusters_idx = cluster_indices(len(candidates), linkage, _get_dist, _get_thr, leader_key=_leader_key)

    merge_groups: List[List[Dict[str, Any]]] = [[candidates[i] for i in cluster] for cluster in clusters_idx]

    if not merge_groups:
        return summaries

    # Process clusters with enhanced synthesis
    aggregated_entries: Dict[int, Dict[str, Any]] = {}
    membership: Dict[int, int] = {}

    for group in merge_groups:
        leader = min(group, key=lambda g: _as_int(g.get("id")) or 10**9)
        leader_id = _as_int(leader.get("id"))
        if leader_id is None:
            continue

        merged_ids = [_as_int(member.get("id")) for member in group]
        merged_ids = [mid for mid in merged_ids if mid is not None]

        merged_text = await synthesize_merged_summary(
            group,
            prompts.get("similar_merge") or "",
            use_llm=enable_confidence_scoring,  # Use LLM more with confidence scoring
            chat_completion_fn=chat_completion_fn,
        )

        # Create enhanced merged entry
        merged_entry = dict(leader)
        merged_entry["summary_text"] = merged_text
        merged_entry["topic"] = merged_display_topic(group)
        merged_entry["merged_ids"] = merged_ids
        merged_entry["merged_links"] = build_merge_links(group)
        merged_entry["merged_count"] = len(group)

        # Add quality metadata
        if enable_confidence_scoring:
            quality_scores = []
            for member in group:
                if member.get("id") is not None:
                    result = should_merge_enhanced(leader, member)
                    quality_scores.append(result.get("confidence", 0.0))

            if quality_scores:
                avg_quality = sum(quality_scores) / len(quality_scores)
                merged_entry["merge_confidence"] = avg_quality
                variance = sum((s - avg_quality) ** 2 for s in quality_scores) / len(quality_scores)
                merged_entry["merge_quality_variance"] = max(0.0, variance)

        aggregated_entries[leader_id] = merged_entry
        for member in group:
            sid = _as_int(member.get("id"))
            if sid is not None:
                membership[sid] = leader_id

    logger.info(
        "Enhanced merge: %d candidates, %d clusters, confidence_scoring=%s, cross_feed_preference=%s, time_gap_logic=%s",
        len(candidates),
        len(merge_groups),
        enable_confidence_scoring,
        enable_cross_feed_preference,
        enable_time_gap_logic,
    )

    # Filter and return results
    merged_output: List[Dict[str, Any]] = []
    for summary in summaries:
        sid = _as_int(summary.get("id"))
        if sid is not None and sid in aggregated_entries:
            merged_output.append(aggregated_entries[sid])
        else:
            merged_output.append(summary)

    return merged_output


__all__ = [
    "merge_summaries",
    "summary_id_list",
    "collect_summary_links",
    "build_merge_links",
    "bm25_match_query",
    "synthesize_merged_summary",
    "merge_similar_summaries",
]
