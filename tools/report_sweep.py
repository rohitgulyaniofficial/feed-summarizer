#!/usr/bin/env python3
"""Threshold sweep tool for comprehensive merge and recurring analysis.

This tool performs threshold sweeps for both merge deduplication and recurring
coverage detection, providing unified analysis and recommendations.

Usage:
    python -m tools.report_sweep --db feeds.db --days 14
    python -m tools.report_sweep --db feeds.db --days 14 --json
    python -m tools.report_sweep --db feeds.db --days 14 --mode merge
    python -m tools.report_sweep --db feeds.db --days 14 --mode recurring
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count
from typing import Any, Dict, List, Optional, Sequence

# Suppress config logging noise (must be set before importing config)
os.environ.setdefault("LOG_LEVEL", "ERROR")  # noqa: E402
logging.getLogger("FeedProcessor").setLevel(logging.ERROR)

from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn  # noqa: E402

from config import config  # noqa: E402
from utils import hamming_distance  # noqa: E402
from utils.clustering import cluster_indices  # noqa: E402
from utils.merge_policy import (  # noqa: E402
    pair_merge_threshold_rows,
    should_merge_pair_rows,
    title_token_set_from_text,
)
from tools.standard_args import (  # noqa: E402
    create_standard_parser,
    compute_lookback,
    add_threshold_range_args,
)
from tools.data_loaders import (  # noqa: E402
    load_bulletin_summaries,
    load_published_summaries,
    analyze_merge_at_threshold,
    SummaryRow,
)

console = Console()


# =============================================================================
# Clustering (reused from report_merge logic)
# =============================================================================

def _build_clusters(items: Sequence[SummaryRow], threshold: int) -> List[List[SummaryRow]]:
    """Cluster items by SimHash distance using complete linkage."""
    candidates = [
        it for it in items
        if isinstance(it.get("id"), int) and isinstance(it.get("merge_fp"), int)
    ]
    if len(candidates) < 2 or threshold <= 0:
        return []

    def get_dist(i: int, j: int) -> Optional[int]:
        a = candidates[i].get("merge_fp")
        b = candidates[j].get("merge_fp")
        if not isinstance(a, int) or not isinstance(b, int):
            return None
        return hamming_distance(a, b)

    def get_thr(i: int, j: int) -> Optional[int]:
        a, b = candidates[i], candidates[j]
        if not should_merge_pair_rows(a, b):
            return None
        return int(pair_merge_threshold_rows(a, b, threshold))

    clusters_idx = cluster_indices(len(candidates), "complete", get_dist, get_thr)
    return [[candidates[i] for i in cluster] for cluster in clusters_idx]


# =============================================================================
# Recurring Analysis
# =============================================================================

def analyze_recurring_at_threshold(
    summaries: List[SummaryRow],
    threshold: int,
    days_back: int = 3,
) -> Dict[str, Any]:
    """Analyze recurring detection behavior at a specific threshold."""
    cutoff_time = int(time.time()) - (days_back * 24 * 60 * 60)
    
    past_summaries = [s for s in summaries if s.get("published_date", 0) < cutoff_time]
    current_summaries = [s for s in summaries if s.get("published_date", 0) >= cutoff_time]
    
    if not current_summaries or not past_summaries:
        return {
            "threshold": threshold,
            "total_current": len(current_summaries),
            "total_past": len(past_summaries),
            "recurring_count": 0,
            "recurring_rate": 0,
            "cross_feed_matches": 0,
            "same_feed_matches": 0,
            "examples": [],
        }
    
    recurring_ids = []
    cross_feed_matches = 0
    same_feed_matches = 0
    examples = []
    
    for current in current_summaries:
        current_fp = current.get("merge_fp")
        if current_fp is None:
            continue
            
        best_match = None
        best_dist = threshold + 1
        
        for past in past_summaries:
            if past["id"] == current["id"]:
                continue
            if not should_merge_pair_rows(current, past):
                continue
                
            pair_threshold = pair_merge_threshold_rows(current, past, threshold)
            dist = hamming_distance(current_fp, past.get("merge_fp"))
            
            if dist is not None and dist <= pair_threshold and dist < best_dist:
                best_dist = dist
                best_match = past
        
        if best_match:
            recurring_ids.append(current["id"])
            is_cross_feed = current.get("feed_slug") != best_match.get("feed_slug")
            if is_cross_feed:
                cross_feed_matches += 1
            else:
                same_feed_matches += 1
            
            if len(examples) < 5:
                time_diff = (current.get("published_date", 0) - best_match.get("published_date", 0)) // 3600
                title_tokens_a = title_token_set_from_text(current.get("title", ""))
                title_tokens_b = title_token_set_from_text(best_match.get("title", ""))
                title_overlap = len(title_tokens_a & title_tokens_b) / max(len(title_tokens_a), len(title_tokens_b), 1)
                
                examples.append({
                    "current_id": current["id"],
                    "current_title": current.get("title", ""),
                    "current_feed": current.get("feed_slug", ""),
                    "past_id": best_match["id"],
                    "past_title": best_match.get("title", ""),
                    "past_feed": best_match.get("feed_slug", ""),
                    "distance": best_dist,
                    "time_diff_hours": time_diff,
                    "title_overlap": round(title_overlap, 3),
                    "cross_feed": is_cross_feed,
                })
    
    recurring_rate = len(recurring_ids) / len(current_summaries) if current_summaries else 0
    quality_pct = (cross_feed_matches / len(recurring_ids) * 100) if recurring_ids else 0
    
    return {
        "threshold": threshold,
        "total_current": len(current_summaries),
        "total_past": len(past_summaries),
        "recurring_count": len(recurring_ids),
        "recurring_rate": round(recurring_rate, 4),
        "cross_feed_matches": cross_feed_matches,
        "same_feed_matches": same_feed_matches,
        "quality_percent": round(quality_pct, 1),
        "examples": examples,
    }


# =============================================================================
# Sweep Runner
# =============================================================================

def _merge_worker(args: tuple) -> Dict[str, Any]:
    """Worker function for parallel merge analysis."""
    by_bulletin, threshold = args
    return analyze_merge_at_threshold(by_bulletin, threshold, _build_clusters)


def _recurring_worker(args: tuple) -> Dict[str, Any]:
    """Worker function for parallel recurring analysis."""
    summaries, threshold, days_back = args
    return analyze_recurring_at_threshold(summaries, threshold, days_back)


def run_merge_sweep(
    conn: sqlite3.Connection,
    cutoff: int,
    thresholds: List[int],
    verbose: bool = False,
    parallel: bool = True,
) -> List[Dict[str, Any]]:
    """Run merge threshold sweep."""
    if verbose:
        console.print("[dim]Loading bulletin summaries...[/dim]")
    by_bulletin = load_bulletin_summaries(conn, cutoff)
    if verbose:
        total_items = sum(len(v) for v in by_bulletin.values())
        console.print(f"[dim]Loaded {len(by_bulletin)} bulletins, {total_items} items[/dim]")
    
    if parallel and len(thresholds) > 1:
        workers = min(cpu_count(), len(thresholds))
        if verbose:
            console.print(f"[dim]Running merge sweep with {workers} workers...[/dim]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            disable=not verbose,
        ) as progress:
            task = progress.add_task("Merge sweep", total=len(thresholds))
            results = []
            
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_merge_worker, (by_bulletin, t)): t
                    for t in thresholds
                }
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    progress.advance(task)
            
            # Sort by threshold
            results.sort(key=lambda r: r["threshold"])
    else:
        results = []
        for i, threshold in enumerate(thresholds):
            if verbose:
                console.print(f"[dim]Testing threshold {threshold} ({i+1}/{len(thresholds)})...[/dim]")
            result = analyze_merge_at_threshold(by_bulletin, threshold, _build_clusters)
            results.append(result)
    
    return results


def run_recurring_sweep(
    conn: sqlite3.Connection,
    cutoff: int,
    thresholds: List[int],
    days_back: int = 3,
    verbose: bool = False,
    parallel: bool = True,
) -> List[Dict[str, Any]]:
    """Run recurring threshold sweep."""
    if verbose:
        console.print("[dim]Loading published summaries...[/dim]")
    summaries = load_published_summaries(conn, cutoff, include_bulletin_info=True)
    if verbose:
        console.print(f"[dim]Loaded {len(summaries)} summaries[/dim]")
    
    if parallel and len(thresholds) > 1:
        workers = min(cpu_count(), len(thresholds))
        if verbose:
            console.print(f"[dim]Running recurring sweep with {workers} workers...[/dim]")
        
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
            disable=not verbose,
        ) as progress:
            task = progress.add_task("Recurring sweep", total=len(thresholds))
            results = []
            
            with ProcessPoolExecutor(max_workers=workers) as executor:
                futures = {
                    executor.submit(_recurring_worker, (summaries, t, days_back)): t
                    for t in thresholds
                }
                for future in as_completed(futures):
                    result = future.result()
                    results.append(result)
                    progress.advance(task)
            
            # Sort by threshold
            results.sort(key=lambda r: r["threshold"])
    else:
        results = []
        for i, threshold in enumerate(thresholds):
            if verbose:
                console.print(f"[dim]Testing threshold {threshold} ({i+1}/{len(thresholds)})...[/dim]")
            result = analyze_recurring_at_threshold(summaries, threshold, days_back)
            results.append(result)
    
    return results


def find_best_merge_threshold(results: List[Dict[str, Any]]) -> int:
    """Find recommended merge threshold balancing reduction and quality."""
    best_threshold = results[0]["threshold"] if results else 20
    best_score = -1
    
    for r in results:
        # Score: balance between reduction and quality (cross-feed %)
        reduction = r.get("reduction_percent", 0)
        quality = r.get("quality_percent", 0)
        
        # Prefer thresholds with quality >= 70% and reasonable reduction
        if quality >= 70:
            score = quality * 0.6 + reduction * 0.4
        elif quality >= 60:
            score = quality * 0.5 + reduction * 0.3
        else:
            score = quality * 0.3  # Penalize low quality
        
        if score > best_score:
            best_score = score
            best_threshold = r["threshold"]
    
    return best_threshold


def find_best_recurring_threshold(results: List[Dict[str, Any]]) -> int:
    """Find recommended recurring threshold balancing early detection and precision.
    
    For recurring coverage, we want to:
    - Detect stories that were covered in the previous few days
    - Be conservative (lower threshold = stricter matching, fewer false positives)
    - Both cross-feed and same-feed matches are valid recurring coverage:
      - Cross-feed: same story from different sources
      - Same-feed: follow-up, update, or continuing coverage
    - Target a reasonable detection rate (5-20% for a 3-day window)
    """
    best_threshold = results[0]["threshold"] if results else 16
    best_score = -1
    
    for r in results:
        rate = r.get("recurring_rate", 0)
        threshold = r.get("threshold", 0)
        recurring_count = r.get("recurring_count", 0)
        
        # For recurring, lower thresholds are more conservative (stricter matching)
        # We want to catch real recurring stories without too many false positives
        
        # Penalize very low detection rates (not useful) and very high rates (too noisy)
        if rate < 0.01 or recurring_count < 3:
            rate_score = 0.2  # Too few detections to be useful
        elif rate <= 0.08:
            rate_score = 1.0  # Sweet spot: 1-8% detection
        elif rate <= 0.15:
            rate_score = 0.9  # Good: 8-15%
        elif rate <= 0.25:
            rate_score = 0.6  # Getting aggressive: 15-25%
        elif rate <= 0.40:
            rate_score = 0.3  # Too aggressive: 25-40%
        else:
            rate_score = 0.1  # Way too aggressive: >40%
        
        # For recurring detection, both cross-feed and same-feed are valid
        # We don't penalize same-feed as much as for merge deduplication
        # Just ensure we have meaningful detections
        detection_score = 1.0 if recurring_count >= 5 else 0.7 if recurring_count >= 2 else 0.3
        
        # Prefer lower (more conservative) thresholds when rates are similar
        # Threshold bonus: 14=1.0, 16=0.95, 18=0.9, 20=0.85, etc.
        threshold_bonus = max(0.5, 1.0 - (threshold - 14) * 0.025)
        
        score = rate_score * 0.5 + detection_score * 0.2 + threshold_bonus * 0.3
        
        if score > best_score:
            best_score = score
            best_threshold = threshold
    
    return best_threshold


# =============================================================================
# Output Formatters
# =============================================================================

def print_merge_sweep_rich(results: List[Dict[str, Any]], recommended: int, current: int):
    """Print merge sweep results with Rich formatting."""
    console.print(Panel.fit("Merge Threshold Sweep", style="bold cyan"))
    console.print()
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Threshold", justify="right", style="cyan")
    table.add_column("Reduction", justify="right")
    table.add_column("Clusters", justify="right")
    table.add_column("Cross-Feed", justify="right", style="green")
    table.add_column("Same-Feed", justify="right", style="red")
    table.add_column("Quality", justify="right")
    
    for r in results:
        quality_style = "green" if r["quality_percent"] >= 70 else "yellow" if r["quality_percent"] >= 60 else "red"
        marker = " ◀" if r["threshold"] == current else ""
        table.add_row(
            f"{r['threshold']}{marker}",
            f"{r['net_reduction']} ({r['reduction_percent']:.1f}%)",
            str(r["merged_clusters"]),
            str(r["cross_feed_clusters"]),
            str(r["same_feed_clusters"]),
            f"[{quality_style}]{r['quality_percent']:.0f}%[/{quality_style}]",
        )
    
    console.print(table)
    console.print()
    
    console.print(f"[cyan]Current threshold:[/cyan] {current}")
    console.print(f"[cyan]Recommended:[/cyan] [bold yellow]{recommended}[/bold yellow]")
    
    if current != recommended:
        console.print(f"\n[yellow]⚠ Consider changing SIMHASH_HAMMING_THRESHOLD from {current} to {recommended}[/yellow]")
    else:
        console.print("\n[green]✓ Current threshold matches recommendation[/green]")


def print_recurring_sweep_rich(results: List[Dict[str, Any]], recommended: int, current: int):
    """Print recurring sweep results with Rich formatting."""
    console.print(Panel.fit("Recurring Threshold Sweep", style="bold magenta"))
    console.print()
    
    table = Table(show_header=True, header_style="bold")
    table.add_column("Threshold", justify="right", style="cyan")
    table.add_column("Detected", justify="right")
    table.add_column("Rate", justify="right")
    table.add_column("Cross-Feed", justify="right", style="green")
    table.add_column("Same-Feed", justify="right", style="red")
    table.add_column("Quality", justify="right")
    
    for r in results:
        quality_style = "green" if r["quality_percent"] >= 70 else "yellow" if r["quality_percent"] >= 50 else "red"
        marker = " ◀" if r["threshold"] == current else ""
        table.add_row(
            f"{r['threshold']}{marker}",
            f"{r['recurring_count']}/{r['total_current']}",
            f"{r['recurring_rate']*100:.1f}%",
            str(r["cross_feed_matches"]),
            str(r["same_feed_matches"]),
            f"[{quality_style}]{r['quality_percent']:.0f}%[/{quality_style}]" if r["recurring_count"] > 0 else "[dim]N/A[/dim]",
        )
    
    console.print(table)
    console.print()
    
    console.print(f"[cyan]Current threshold:[/cyan] {current}")
    console.print(f"[cyan]Recommended:[/cyan] [bold yellow]{recommended}[/bold yellow]")
    
    if current != recommended:
        console.print(f"\n[yellow]⚠ Consider changing RECURRING_HAMMING_THRESHOLD from {current} to {recommended}[/yellow]")
    else:
        console.print("\n[green]✓ Current threshold matches recommendation[/green]")


# =============================================================================
# Main
# =============================================================================

def main():
    parser = create_standard_parser(
        description="Threshold sweep for merge and recurring analysis",
        with_db=True,
        with_verbosity=False,
        with_time_window=True,
        with_json=True,
    )
    add_threshold_range_args(parser)
    parser.add_argument(
        "--mode",
        choices=["both", "merge", "recurring"],
        default="both",
        help="Which threshold to sweep (default: both)",
    )
    parser.add_argument(
        "--days-back",
        type=int,
        default=3,
        help="Days back for recurring detection window (default: 3)",
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Show progress during sweep",
    )
    parser.add_argument(
        "--no-parallel",
        action="store_true",
        help="Disable parallel processing",
    )
    args = parser.parse_args()
    
    conn = sqlite3.connect(args.db)
    lookback_hours, lookback_label = compute_lookback(args)
    cutoff = int(time.time()) - lookback_hours * 3600
    
    thresholds = list(range(args.min_threshold, args.max_threshold + 1, args.threshold_step))
    
    current_merge = config.SIMHASH_HAMMING_THRESHOLD
    current_recurring = config.RECURRING_HAMMING_THRESHOLD
    
    output = {
        "lookback": lookback_label,
        "current_merge_threshold": current_merge,
        "current_recurring_threshold": current_recurring,
    }
    
    verbose = args.verbose and not getattr(args, "json", False)
    parallel = not args.no_parallel
    
    # Run sweeps based on mode
    if args.mode in ("both", "merge"):
        merge_results = run_merge_sweep(conn, cutoff, thresholds, verbose=verbose, parallel=parallel)
        recommended_merge = find_best_merge_threshold(merge_results)
        output["merge"] = {
            "recommended": recommended_merge,
            "thresholds": merge_results,
        }
    
    if args.mode in ("both", "recurring"):
        recurring_results = run_recurring_sweep(conn, cutoff, thresholds, args.days_back, verbose=verbose, parallel=parallel)
        recommended_recurring = find_best_recurring_threshold(recurring_results)
        output["recurring"] = {
            "recommended": recommended_recurring,
            "days_back": args.days_back,
            "thresholds": recurring_results,
        }
    
    # Output
    if getattr(args, "json", False):
        print(json.dumps(output, indent=2))
        return
    
    # Rich output
    console.print(Panel.fit(f"Threshold Sweep Analysis (last {lookback_label})", style="bold blue"))
    console.print()
    
    if "merge" in output:
        print_merge_sweep_rich(
            output["merge"]["thresholds"],
            output["merge"]["recommended"],
            current_merge,
        )
        console.print()
    
    if "recurring" in output:
        print_recurring_sweep_rich(
            output["recurring"]["thresholds"],
            output["recurring"]["recommended"],
            current_recurring,
        )
        console.print()
    
    # Summary recommendations
    console.print(Panel.fit("Summary", style="bold green"))
    console.print()
    
    changes_needed = []
    if "merge" in output and output["merge"]["recommended"] != current_merge:
        changes_needed.append(f"SIMHASH_HAMMING_THRESHOLD: {current_merge} → {output['merge']['recommended']}")
    if "recurring" in output and output["recurring"]["recommended"] != current_recurring:
        changes_needed.append(f"RECURRING_HAMMING_THRESHOLD: {current_recurring} → {output['recurring']['recommended']}")
    
    if changes_needed:
        console.print("[yellow]Recommended changes:[/yellow]")
        for change in changes_needed:
            console.print(f"  • {change}")
    else:
        console.print("[green]✓ All thresholds are optimal[/green]")
    
    conn.close()


if __name__ == "__main__":
    main()
