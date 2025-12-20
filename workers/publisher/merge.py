"""Summary merge utilities for publisher."""
from typing import Any, Callable, Dict, List, Optional, Set, Tuple
import json

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
                "title": link.get("title")
                or summary.get("item_title")
                or summary.get("title")
                or "Read more",
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
                "title": summary.get("item_title")
                or summary.get("title")
                or "Read more",
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
                "title": member.get("item_title")
                or member.get("title")
                or "Read more",
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
    """Merge highly similar summaries using SimHash + optional LLM prompt."""
    threshold = max(0, int(getattr(config, "SIMHASH_HAMMING_THRESHOLD", 0) or 0))
    if threshold <= 0 or not summaries:
        return summaries

    for summary in summaries:
        try:
            summary["_merge_fp"] = merge_fingerprint(summary)
        except Exception:
            summary["_merge_fp"] = None

    candidates = [s for s in summaries if isinstance(s.get("_merge_fp"), int) and isinstance(s.get("id"), (int, str))]
    if len(candidates) < 2:
        return summaries

    cosine_enabled = bool(getattr(config, "HASHED_COSINE_ENABLED", False))
    cosine_min = float(getattr(config, "HASHED_COSINE_MIN_SIM", 0.25) or 0.25)
    cosine_buckets = int(getattr(config, "HASHED_COSINE_BUCKETS", 65536) or 65536)
    cosine_max_tokens = int(getattr(config, "HASHED_COSINE_MAX_TOKENS", 128) or 128)

    hashed_vectors: List[Tuple[Dict[int, float], float]] = []
    if cosine_enabled:
        for s in candidates:
            try:
                text = merge_similarity_text(s)
                hashed_vectors.append(build_hashed_tf_vector(text, buckets=cosine_buckets, max_tokens=cosine_max_tokens))
            except Exception:
                hashed_vectors.append(({}, 0.0))

    bm25_enabled = bool(getattr(config, "BM25_MERGE_ENABLED", False))
    bm25_ratio_threshold = float(getattr(config, "BM25_MERGE_RATIO_THRESHOLD", 0.80) or 0.80)
    bm25_max_extra = int(getattr(config, "BM25_MERGE_MAX_EXTRA_DISTANCE", 6) or 6)
    bm25_max_tokens = int(getattr(config, "BM25_MERGE_MAX_QUERY_TOKENS", 8) or 8)

    linkage = str(getattr(config, "SIMHASH_MERGE_LINKAGE", "complete") or "complete").strip().lower()
    if linkage not in {"single", "complete"}:
        linkage = "complete"

    def _as_int(value: Any) -> Optional[int]:
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    index_by_id: Dict[int, int] = {}
    for idx, summary in enumerate(summaries):
        sid = _as_int(summary.get("id"))
        if sid is None:
            continue
        index_by_id[sid] = idx

    bm25_ratios: Dict[int, Dict[int, float]] = {}
    if bm25_enabled and db is not None:
        id_to_summary: Dict[int, Dict[str, Any]] = {}
        candidate_ids_all: List[int] = []
        for s in candidates:
            sid = _as_int(s.get("id"))
            if sid is None:
                continue
            id_to_summary[sid] = s
            candidate_ids_all.append(sid)

        if len(candidate_ids_all) >= 2:
            for sid in candidate_ids_all:
                s = id_to_summary.get(sid)
                if not s:
                    continue
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
                    self_score = None
                if not isinstance(self_score, (int, float)) or self_score == 0:
                    continue

                denom = abs(float(self_score))
                if denom <= 0:
                    continue

                out: Dict[int, float] = {}
                for row in (resp.get("candidates") or []) if isinstance(resp, dict) else []:
                    try:
                        cid = int(row.get("id"))
                        score = float(row.get("score"))
                    except Exception:
                        continue
                    ratio = abs(score) / denom
                    if ratio > 1:
                        ratio = 1.0
                    out[cid] = ratio
                if out:
                    bm25_ratios[sid] = out

    pair_dist: Dict[Tuple[int, int], int] = {}
    pair_thr: Dict[Tuple[int, int], int] = {}
    for i in range(len(candidates)):
        a = candidates[i]
        for j in range(i + 1, len(candidates)):
            b = candidates[j]
            if not should_merge_pair(a, b):
                continue

            if cosine_enabled:
                cos = cosine_similarity(hashed_vectors[i], hashed_vectors[j])
                if cos < cosine_min:
                    continue
            dist = hamming_distance(a.get("_merge_fp"), b.get("_merge_fp"))
            if dist is None:
                continue
            pair_dist[(i, j)] = int(dist)
            pair_thr[(i, j)] = pair_merge_threshold(a, b, threshold)

    def _get_dist(i: int, j: int) -> Optional[int]:
        if i == j:
            return 0
        if i < j:
            return pair_dist.get((i, j))
        return pair_dist.get((j, i))

    def _get_thr(i: int, j: int) -> Optional[int]:
        if i == j:
            return threshold
        if i < j:
            return pair_thr.get((i, j))
        return pair_thr.get((j, i))

    def _leader_key(i: int) -> int:
        sid = _as_int(candidates[i].get("id"))
        if sid is None:
            return 10**9
        return int(index_by_id.get(sid, 10**9))

    if linkage == "single":
        extra_edges: Set[Tuple[int, int]] = set()
        if bm25_enabled:
            for i in range(len(candidates)):
                for j in range(i + 1, len(candidates)):
                    dist = _get_dist(i, j)
                    thr = _get_thr(i, j) or threshold
                    if dist is not None and dist <= thr:
                        continue
                    if dist is not None and dist > (thr + max(0, bm25_max_extra)):
                        continue
                    sid_a = _as_int(candidates[i].get("id"))
                    sid_b = _as_int(candidates[j].get("id"))
                    if sid_a is None or sid_b is None:
                        continue
                    ra = bm25_ratios.get(sid_a, {}).get(sid_b, 0.0)
                    rb = bm25_ratios.get(sid_b, {}).get(sid_a, 0.0)
                    if ra >= bm25_ratio_threshold and rb >= bm25_ratio_threshold:
                        if cosine_enabled:
                            cos = cosine_similarity(hashed_vectors[i], hashed_vectors[j])
                            if cos < cosine_min:
                                continue
                        extra_edges.add((i, j))

        def get_dist(i: int, j: int) -> Optional[int]:
            if i == j:
                return 0
            a, b = (i, j) if i < j else (j, i)
            if (a, b) in extra_edges:
                return 0
            return _get_dist(i, j)

        def get_thr(i: int, j: int) -> Optional[int]:
            if i == j:
                return threshold
            a, b = (i, j) if i < j else (j, i)
            if (a, b) in extra_edges:
                return 0
            return _get_thr(i, j)

        clusters_idx = cluster_indices(
            len(candidates),
            "single",
            get_dist,
            get_thr,
            leader_key=_leader_key,
        )
    else:
        clusters_idx = cluster_indices(
            len(candidates),
            "complete",
            _get_dist,
            _get_thr,
            leader_key=_leader_key,
        )

    merge_groups: List[List[Dict[str, Any]]] = [[candidates[i] for i in cluster] for cluster in clusters_idx]

    if not merge_groups:
        return summaries

    prompt_template = (prompts.get("similar_merge") or "").strip()
    use_llm = bool(prompt_template and config.AZURE_ENDPOINT and config.OPENAI_API_KEY)

    merge_groups.sort(key=lambda g: min(index_by_id.get(_as_int(x.get("id")) or -1, 10**9) for x in g))

    aggregated_entries: Dict[int, Dict[str, Any]] = {}
    membership: Dict[int, int] = {}
    for group in merge_groups:
        leader = min(group, key=lambda g: index_by_id.get(_as_int(g.get("id")) or -1, 10**9))
        leader_id = _as_int(leader.get("id"))
        if leader_id is None:
            continue
        merged_ids = [
            _as_int(member.get("id")) for member in group if _as_int(member.get("id")) is not None
        ]
        merged_text = await synthesize_merged_summary(group, prompt_template, use_llm, chat_completion_fn)
        merged_entry = dict(leader)
        merged_entry["summary_text"] = merged_text or leader.get("summary_text")
        merged_entry["topic"] = merged_display_topic(group)
        merged_entry["merged_ids"] = merged_ids
        merged_entry["merged_links"] = build_merge_links(group)
        merged_entry["merged_count"] = len(group)
        aggregated_entries[leader_id] = merged_entry
        for member in group:
            sid = _as_int(member.get("id"))
            if sid is not None:
                membership[sid] = leader_id
        logger.info(
            "Merged %d summaries into leader %s (threshold=%d linkage=%s)",
            len(group),
            leader_id,
            threshold,
            linkage,
        )

    merged_output: List[Dict[str, Any]] = []
    for summary in summaries:
        sid = _as_int(summary.get("id"))
        if sid is not None and sid in membership:
            leader_id = membership[sid]
            if sid != leader_id:
                continue
            merged_output.append(aggregated_entries.get(leader_id, summary))
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
