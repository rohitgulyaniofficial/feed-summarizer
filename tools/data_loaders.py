#!/usr/bin/env python3
"""Shared data loading utilities for analysis tools.

This module centralizes SQL queries and data transformation logic
used across multiple analysis tools, ensuring consistency and reducing duplication.
"""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from utils import decode_int64
from utils.merge_policy import (
    merge_fingerprint_from_row,
    title_token_set_from_text,
    summary_token_set_from_text,
)


# Type aliases
SummaryRow = Dict[str, Any]
BulletinKey = Tuple[str, str, int]  # (group_name, session_key, created_date)# =============================================================================
# Schema Helpers
# =============================================================================

def has_merge_simhash_column(conn: sqlite3.Connection) -> bool:
    """Check if summaries table has merge_simhash column."""
    try:
        rows = conn.execute("PRAGMA table_info(summaries)").fetchall()
        names = {r[1] for r in rows}
        return "merge_simhash" in names
    except Exception:
        return False


# =============================================================================
# Row Processing
# =============================================================================

def _process_summary_row(
    row: sqlite3.Row,
    *,
    include_tokens: bool = False,
    include_bulletin_info: bool = False,
) -> Optional[SummaryRow]:
    """Transform a database row into a standardized summary dict.
    
    Args:
        row: SQLite row with summary data
        include_tokens: Whether to include pre-computed token sets
        include_bulletin_info: Whether to include bulletin-related fields
        
    Returns:
        Processed summary dict, or None if fingerprint cannot be computed
    """
    title = str(row["item_title"] or "")
    summary_text = str(row["summary_text"] or "")
    
    # Get stored merge fingerprint or compute it
    stored_fp = None
    merge_simhash = row["merge_simhash"] if "merge_simhash" in row.keys() else None
    if merge_simhash is not None:
        stored_fp = decode_int64(merge_simhash) if isinstance(merge_simhash, int) else None
    
    merge_fp = (
        stored_fp
        if stored_fp is not None
        else merge_fingerprint_from_row({"item_title": title, "summary_text": summary_text})
    )
    
    if merge_fp is None:
        return None
    
    result: SummaryRow = {
        "id": int(row["id"] if "id" in row.keys() else row["summary_id"]),
        "feed_slug": str(row["feed_slug"] or ""),
        "title": title,
        "url": str(row["url"] if "url" in row.keys() else row.get("item_url", "") or ""),
        "summary_text": summary_text,
        "merge_fp": merge_fp,
        "generated_date": int(row["generated_date"] or 0) if "generated_date" in row.keys() else 0,
        "published_date": int(row["published_date"] or 0) if "published_date" in row.keys() else 0,
    }
    
    if include_tokens:
        result["title_tokens"] = title_token_set_from_text(title)
        result["summary_tokens"] = summary_token_set_from_text(summary_text)
    
    if include_bulletin_info:
        result["group_name"] = str(row["group_name"] or "") if "group_name" in row.keys() else ""
        result["session_key"] = str(row["session_key"] or "") if "session_key" in row.keys() else ""
        if "bulletin_date" in row.keys():
            result["bulletin_date"] = int(row["bulletin_date"] or 0)
    
    return result


# =============================================================================
# Data Loaders
# =============================================================================

def load_published_summaries(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    *,
    limit: Optional[int] = None,
    include_tokens: bool = False,
    include_bulletin_info: bool = True,
) -> List[SummaryRow]:
    """Load published summaries for analysis.
    
    Args:
        conn: Database connection
        cutoff_epoch: Unix timestamp cutoff (load summaries generated after this)
        limit: Maximum number of summaries to load (None for no limit)
        include_tokens: Whether to include pre-computed token sets
        include_bulletin_info: Whether to include bulletin-related fields
        
    Returns:
        List of processed summary dicts
    """
    conn.row_factory = sqlite3.Row
    
    limit_clause = f"LIMIT {int(limit)}" if limit else ""
    
    sql = f"""
        SELECT s.id, s.summary_text, s.merge_simhash, s.generated_date, s.published_date,
               i.title AS item_title, i.url, f.slug AS feed_slug,
               b.group_name, b.session_key, b.created_date AS bulletin_date
        FROM summaries s
        JOIN items i ON i.id = s.id
        JOIN feeds f ON f.id = i.feed_id
        LEFT JOIN bulletin_summaries bs ON bs.summary_id = s.id
        LEFT JOIN bulletins b ON b.id = bs.bulletin_id
        WHERE s.generated_date >= ?
          AND s.summary_text IS NOT NULL AND s.summary_text != ''
          AND s.published_date IS NOT NULL
        ORDER BY s.published_date DESC
        {limit_clause}
    """
    
    rows = conn.execute(sql, (cutoff_epoch,)).fetchall()
    summaries = []
    
    for row in rows:
        processed = _process_summary_row(
            row,
            include_tokens=include_tokens,
            include_bulletin_info=include_bulletin_info,
        )
        if processed is not None:
            summaries.append(processed)
    
    return summaries


def load_bulletin_summaries(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    *,
    include_tokens: bool = True,
) -> Dict[BulletinKey, List[SummaryRow]]:
    """Load summaries grouped by bulletin for merge analysis.
    
    Args:
        conn: Database connection
        cutoff_epoch: Unix timestamp cutoff (load bulletins created after this)
        include_tokens: Whether to include pre-computed token sets
        
    Returns:
        Dict mapping (group_name, session_key, created_date) to list of summaries
    """
    conn.row_factory = sqlite3.Row
    has_merge = has_merge_simhash_column(conn)
    
    merge_select = "s.merge_simhash," if has_merge else ""
    
    sql = f"""
        SELECT b.group_name, b.session_key, b.created_date,
               s.id AS summary_id, s.summary_text, {merge_select}
               i.title AS item_title, i.url AS item_url,
               f.slug AS feed_slug
        FROM bulletins b
        JOIN bulletin_summaries bs ON bs.bulletin_id = b.id
        JOIN summaries s ON s.id = bs.summary_id
        JOIN items i ON i.id = s.id
        JOIN feeds f ON f.id = i.feed_id
        WHERE b.created_date >= ?
          AND s.summary_text IS NOT NULL AND s.summary_text != ''
        ORDER BY b.created_date DESC
    """
    
    rows = conn.execute(sql, (cutoff_epoch,)).fetchall()
    
    by_bulletin: Dict[BulletinKey, List[SummaryRow]] = defaultdict(list)
    
    for row in rows:
        title = str(row["item_title"] or "")
        summary_text = str(row["summary_text"] or "")
        
        # Get fingerprint
        stored_fp = None
        if has_merge:
            stored_merge = row["merge_simhash"]
            stored_fp = decode_int64(stored_merge) if isinstance(stored_merge, int) else None
        
        merge_fp = stored_fp if stored_fp is not None else merge_fingerprint_from_row(
            {"item_title": title, "summary_text": summary_text}
        )
        
        if merge_fp is None:
            continue
        
        summary: SummaryRow = {
            "id": int(row["summary_id"]),
            "feed_slug": str(row["feed_slug"] or ""),
            "title": title,
            "url": str(row["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": merge_fp,
        }
        
        if include_tokens:
            summary["title_tokens"] = title_token_set_from_text(title)
            summary["summary_tokens"] = summary_token_set_from_text(summary_text)
        
        key: BulletinKey = (
            str(row["group_name"]),
            str(row["session_key"]),
            int(row["created_date"]),
        )
        by_bulletin[key].append(summary)
    
    return by_bulletin


def load_keyword_matches(
    conn: sqlite3.Connection,
    cutoff_epoch: int,
    keyword: str,
    limit: int,
    *,
    include_tokens: bool = True,
) -> List[SummaryRow]:
    """Load summaries matching a keyword search.
    
    Args:
        conn: Database connection
        cutoff_epoch: Unix timestamp cutoff
        keyword: Keyword to search in title/summary_text
        limit: Maximum results to return
        include_tokens: Whether to include pre-computed token sets
        
    Returns:
        List of matching summary dicts
    """
    conn.row_factory = sqlite3.Row
    needle = f"%{(keyword or '').strip()}%"
    has_merge = has_merge_simhash_column(conn)
    
    merge_select = "s.merge_simhash AS merge_simhash," if has_merge else "NULL AS merge_simhash,"
    
    sql = f"""
        SELECT s.id AS summary_id,
               {merge_select}
               s.summary_text AS summary_text,
               s.generated_date AS generated_date,
               i.title AS item_title,
               i.url AS item_url,
               f.slug AS feed_slug
        FROM summaries s
        JOIN items i ON i.id = s.id
        JOIN feeds f ON f.id = i.feed_id
        WHERE s.generated_date >= ?
          AND s.summary_text IS NOT NULL AND s.summary_text != ''
          AND (i.title LIKE ? OR s.summary_text LIKE ?)
        ORDER BY s.generated_date DESC
        LIMIT ?
    """
    
    rows = conn.execute(sql, (cutoff_epoch, needle, needle, int(limit))).fetchall()
    
    matches: List[SummaryRow] = []
    for row in rows:
        title = str(row["item_title"] or "")
        summary_text = str(row["summary_text"] or "")
        
        stored_fp = None
        if row["merge_simhash"] is not None:
            stored_fp = decode_int64(row["merge_simhash"]) if isinstance(row["merge_simhash"], int) else None
        
        merge_fp = stored_fp if stored_fp is not None else merge_fingerprint_from_row(
            {"item_title": title, "summary_text": summary_text}
        )
        
        if merge_fp is None:
            continue
        
        summary: SummaryRow = {
            "id": int(row["summary_id"]),
            "feed_slug": str(row["feed_slug"] or ""),
            "title": title,
            "url": str(row["item_url"] or ""),
            "summary_text": summary_text,
            "merge_fp": merge_fp,
            "generated_date": int(row["generated_date"] or 0),
        }
        
        if include_tokens:
            summary["title_tokens"] = title_token_set_from_text(title)
            summary["summary_tokens"] = summary_token_set_from_text(summary_text)
        
        matches.append(summary)
    
    return matches


# =============================================================================
# Analysis Helpers
# =============================================================================

def analyze_merge_at_threshold(
    by_bulletin: Dict[BulletinKey, List[SummaryRow]],
    threshold: int,
    cluster_func,
) -> Dict[str, Any]:
    """Analyze merge behavior at a specific threshold.
    
    Args:
        by_bulletin: Bulletin summaries grouped by key
        threshold: SimHash hamming distance threshold
        cluster_func: Function(items, threshold) -> List[List[SummaryRow]]
        
    Returns:
        Dict with merge statistics and cluster details
    """
    total_bulletins = len(by_bulletin)
    total_before = sum(len(items) for items in by_bulletin.values())
    total_after = 0
    total_clusters = 0
    total_merged_items = 0
    cross_feed_clusters = 0
    same_feed_clusters = 0
    session_details = []
    
    for key, items in by_bulletin.items():
        before = len(items)
        clusters = cluster_func(items, threshold)
        cluster_count = len(clusters)
        merged_items = sum(len(c) for c in clusters)
        after = before - (merged_items - cluster_count)
        
        total_after += after
        total_clusters += cluster_count
        total_merged_items += merged_items
        
        reduction = before - after
        if reduction > 0:
            group_name, session_key, created_date = key
            cluster_details = []
            for cluster in clusters:
                feeds = set(item.get("feed_slug", "") for item in cluster)
                is_cross_feed = len(feeds) > 1
                if is_cross_feed:
                    cross_feed_clusters += 1
                else:
                    same_feed_clusters += 1
                    
                cluster_details.append({
                    "size": len(cluster),
                    "cross_feed": is_cross_feed,
                    "feeds": list(feeds),
                    "items": [
                        {"id": item.get("id"), "feed": item.get("feed_slug"), "title": item.get("title", "")}
                        for item in cluster
                    ],
                })
            session_details.append({
                "session": f"{group_name}/{session_key}",
                "before": before,
                "after": after,
                "reduction": reduction,
                "clusters": cluster_details,
            })
    
    reduction_pct = ((total_before - total_after) / total_before * 100) if total_before > 0 else 0
    quality_pct = (cross_feed_clusters / total_clusters * 100) if total_clusters > 0 else 0
    
    return {
        "threshold": threshold,
        "bulletins_scanned": total_bulletins,
        "items_before": total_before,
        "items_after": total_after,
        "net_reduction": total_before - total_after,
        "reduction_percent": round(reduction_pct, 2),
        "merged_clusters": total_clusters,
        "merged_items": total_merged_items,
        "cross_feed_clusters": cross_feed_clusters,
        "same_feed_clusters": same_feed_clusters,
        "quality_percent": round(quality_pct, 1),
        "sessions": sorted(session_details, key=lambda s: -s["reduction"])[:20],
    }
