#!/usr/bin/env python3
"""Comprehensive threshold analysis tool for recurring coverage."""

import json
import logging
import os
import sqlite3
import time
from typing import Dict, List, Any, Sequence
from concurrent.futures import ProcessPoolExecutor, as_completed
from multiprocessing import cpu_count

# Suppress config logging noise (must be set before importing config)
os.environ.setdefault("LOG_LEVEL", "ERROR")  # noqa: E402
logging.getLogger("FeedProcessor").setLevel(logging.ERROR)

from rich.console import Console  # noqa: E402
from rich.table import Table  # noqa: E402
from rich.panel import Panel  # noqa: E402
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn  # noqa: E402

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
    DEFAULT_MIN_THRESHOLD,
    DEFAULT_MAX_THRESHOLD,
    DEFAULT_THRESHOLD_STEP,
    DEFAULT_EXAMPLES_PER_THRESHOLD,
)
from tools.data_loaders import load_published_summaries  # noqa: E402

logger = get_logger("threshold_recommendation")
console = Console()


def _analyze_threshold_worker(threshold: int, summaries: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Worker function for parallel threshold analysis (no progress tracking)."""
    if len(summaries) < 2:
        return {"threshold": threshold, "pairs": 0, "examples": [], "total_pairs": 0, "true_positives": 0, "accuracy": 0}

    pairs_found = []

    for i in range(len(summaries)):
        for j in range(i + 1, len(summaries)):
            a = summaries[i]
            b = summaries[j]

            # Skip if from same bulletin session within 1 hour
            if (
                a["group_name"] == b["group_name"]
                and abs(a["published_date"] - b["published_date"]) < 3600
            ):
                continue

            # Check merge eligibility
            if not should_merge_pair_rows(a, b):
                continue

            # Calculate adaptive threshold
            pair_threshold = pair_merge_threshold_rows(a, b, threshold)

            # Calculate distance
            dist = hamming_distance(a["merge_fp"], b["merge_fp"])
            if dist is None:
                continue

            if dist <= pair_threshold:
                time_diff = abs(a["published_date"] - b["published_date"]) // 3600

                # Use summary token overlap as primary quality signal (SimHash is on summary+title)
                summary_tokens_a = summary_token_set_from_text(a.get("summary_text", ""))
                summary_tokens_b = summary_token_set_from_text(b.get("summary_text", ""))
                summary_overlap = len(summary_tokens_a & summary_tokens_b) / max(
                    len(summary_tokens_a), len(summary_tokens_b), 1
                )

                # Title overlap as secondary signal
                title_tokens_a = title_token_set_from_text(a["title"])
                title_tokens_b = title_token_set_from_text(b["title"])
                title_overlap = len(title_tokens_a & title_tokens_b) / max(
                    len(title_tokens_a), len(title_tokens_b), 1
                )

                # Heuristic for likely true positive:
                # - Good summary overlap (>=20%) is primary signal since SimHash uses summaries
                # - OR strong title overlap (>=30%) with some summary overlap (>=10%)
                # - Cross-feed pairs are more likely true positives (same story, different sources)
                # - Time gap 6-240h suggests recurring coverage, not duplicate publish
                cross_feed = a["feed_slug"] != b["feed_slug"]
                likely_true_positive = (
                    (summary_overlap >= 0.2 or (title_overlap >= 0.3 and summary_overlap >= 0.1))
                    and cross_feed
                    and 6 <= time_diff <= 240
                )

                pairs_found.append(
                    {
                        "distance": dist,
                        "pair_threshold": pair_threshold,
                        "time_diff_hours": time_diff,
                        "title_overlap": title_overlap,
                        "summary_overlap": summary_overlap,
                        "likely_true_positive": likely_true_positive,
                        "cross_feed": cross_feed,
                        "title_a": a["title"],
                        "title_b": b["title"],
                        "summary_a": a.get("summary_text", ""),
                        "summary_b": b.get("summary_text", ""),
                        "feed_a": a["feed_slug"],
                        "feed_b": b["feed_slug"],
                    }
                )

    pairs_found.sort(key=lambda p: (p["distance"], p["time_diff_hours"]))

    true_positives = sum(1 for p in pairs_found if p["likely_true_positive"])
    accuracy = true_positives / len(pairs_found) if pairs_found else 0

    return {
        "threshold": threshold,
        "total_pairs": len(pairs_found),
        "true_positives": true_positives,
        "accuracy": accuracy,
        "examples": pairs_found[:10],
    }


def _parse_thresholds(arg: str) -> Sequence[int]:
    if not arg:
        return list(range(DEFAULT_MIN_THRESHOLD, DEFAULT_MAX_THRESHOLD + 2, DEFAULT_THRESHOLD_STEP))
    vals: List[int] = []
    for part in arg.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            vals.append(int(part))
        except ValueError:
            raise SystemExit(f"Invalid threshold value: {part}")
    return vals or list(range(DEFAULT_MIN_THRESHOLD, DEFAULT_MAX_THRESHOLD + 2, DEFAULT_THRESHOLD_STEP))


def main():
    parser = create_standard_parser(
        description="Comprehensive threshold analysis and recommendation",
        with_db=True,
        with_verbosity=False,
        with_time_window=True,
        with_samples=True,
    )
    parser.add_argument(
        "--thresholds",
        help=f"Comma-separated thresholds to test (default: {DEFAULT_MIN_THRESHOLD}-{DEFAULT_MAX_THRESHOLD} step {DEFAULT_THRESHOLD_STEP})"
    )
    parser.add_argument(
        "--examples", type=int, default=DEFAULT_EXAMPLES_PER_THRESHOLD,
        help=f"How many example pairs to print per threshold (default: {DEFAULT_EXAMPLES_PER_THRESHOLD})"
    )
    parser.add_argument(
        "--profile",
        choices=["conservative", "balanced", "aggressive"],
        default="balanced",
        help="Risk tolerance profile",
    )
    parser.add_argument("--workers", type=int, default=None, help="Number of parallel workers (default: CPU count)")
    parser.add_argument("--json", action="store_true", help="Output results in JSON format (machine-readable)")
    parser.add_argument("--no-summaries", action="store_true", help="Hide summary text in examples (show titles only)")
    args = parser.parse_args()

    conn = sqlite3.connect(args.db)
    lookback_hours, lookback_label = compute_lookback(args)
    cutoff = int(time.time()) - lookback_hours * 3600

    # Load data (common to both output formats)
    summaries = load_published_summaries(conn, cutoff, limit=args.limit, include_bulletin_info=True)
    thresholds = _parse_thresholds(args.thresholds)
    max_workers = args.workers or cpu_count()

    # Run analyses in parallel (common to both output formats)
    analyses = []
    if getattr(args, "json", False):
        # Silent execution for JSON mode
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            future_to_threshold = {
                executor.submit(_analyze_threshold_worker, th, summaries): th 
                for th in thresholds
            }
            for future in as_completed(future_to_threshold):
                try:
                    analysis = future.result()
                    analyses.append(analysis)
                except Exception:
                    pass
        analyses.sort(key=lambda a: a["threshold"])
        
        profile = args.profile.lower()
        recommended_threshold = {
            "conservative": 12,
            "balanced": 16,
            "aggressive": 24,
        }.get(profile, 16)
        current_threshold = getattr(config, 'RECURRING_HAMMING_THRESHOLD', None)
        
        output = {
            "lookback": lookback_label,
            "sample_size": len(summaries),
            "profile": profile,
            "recommended_threshold": recommended_threshold,
            "current_threshold": current_threshold,
            "thresholds": [
                {
                    "threshold": a["threshold"],
                    "pairs_found": a["total_pairs"],
                    "true_positives": a["true_positives"],
                    "accuracy": round(a["accuracy"], 4),
                    "examples": [
                        {
                            "distance": ex["distance"],
                            "time_diff_hours": ex["time_diff_hours"],
                            "title_overlap": round(ex["title_overlap"], 3),
                            "likely_true_positive": ex["likely_true_positive"],
                            "cross_feed": ex.get("cross_feed", False),
                            "title_a": ex["title_a"],
                            "title_b": ex["title_b"],
                            "feed_a": ex["feed_a"],
                            "feed_b": ex["feed_b"],
                        }
                        for ex in a.get("examples", [])[:args.examples]
                    ],
                }
                for a in analyses
            ],
        }
        print(json.dumps(output, indent=2))
        return

    # Rich output mode
    console.print(Panel.fit("Comprehensive Threshold Analysis for Recurring Coverage Detection", 
                            style="bold blue"))
    console.print()

    console.print(f"[cyan]Loading sample data from last {lookback_label}...[/cyan]")

    if len(summaries) < 50:
        console.print(f"[yellow]Warning: Only {len(summaries)} summaries found - need more data for reliable analysis[/yellow]")

    console.print(f"[green]Sample size: {len(summaries)} summaries[/green]")
    console.print()

    # Calculate total comparisons for progress tracking
    total_comparisons = len(summaries) * (len(summaries) - 1) // 2
    
    console.print(f"[cyan]Analyzing {len(thresholds)} thresholds ({total_comparisons:,} pair comparisons per threshold)...[/cyan]")
    console.print(f"[cyan]Using {max_workers} parallel workers[/cyan]")
    console.print()
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        overall_task = progress.add_task(
            "[cyan]Overall progress", 
            total=len(thresholds)
        )
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit all threshold analyses
            future_to_threshold = {
                executor.submit(_analyze_threshold_worker, th, summaries): th 
                for th in thresholds
            }
            
            # Collect results as they complete
            for future in as_completed(future_to_threshold):
                threshold = future_to_threshold[future]
                try:
                    analysis = future.result()
                    analyses.append(analysis)
                    progress.update(
                        overall_task, 
                        advance=1,
                        description=f"[cyan]Completed threshold {threshold}"
                    )
                except Exception as exc:
                    console.print(f"[red]Threshold {threshold} generated an exception: {exc}[/red]")
    
    # Sort analyses by threshold
    analyses.sort(key=lambda a: a["threshold"])

    console.print()
    
    # Create table for threshold sweep
    table = Table(title="Threshold Sweep Results", show_header=True, header_style="bold magenta")
    table.add_column("Threshold", style="cyan", justify="right")
    table.add_column("Pairs Found", style="yellow", justify="right")
    table.add_column("True Positives", style="green", justify="right")
    table.add_column("Accuracy", style="blue", justify="right")
    
    for analysis in analyses:
        accuracy_color = "green" if analysis['accuracy'] > 0.7 else "yellow" if analysis['accuracy'] > 0.4 else "red"
        table.add_row(
            str(analysis['threshold']),
            str(analysis['total_pairs']),
            str(analysis['true_positives']),
            f"[{accuracy_color}]{analysis['accuracy']:.1%}[/{accuracy_color}]"
        )
    
    console.print(table)

    if args.examples > 0:
        console.print()
        console.print("[bold]Example Pairs by Threshold:[/bold]")
        show_summaries = not getattr(args, 'no_summaries', False)
        for analysis in analyses:
            examples = analysis.get("examples", [])[: args.examples]
            if not examples:
                continue
            console.print(f"\n[cyan]Threshold {analysis['threshold']}:[/cyan] {len(examples)} example(s)")
            for i, match in enumerate(examples, 1):
                confidence_color = "green" if match["likely_true_positive"] else "red"
                console.print(
                    f"  [dim]#{i}:[/dim] dist=[yellow]{match['distance']}[/yellow], "
                    f"gap=[yellow]{match['time_diff_hours']}h[/yellow], "
                    f"title_overlap=[yellow]{match['title_overlap']:.2f}[/yellow], "
                    f"conf=[{confidence_color}]{'HIGH' if match['likely_true_positive'] else 'LOW'}[/{confidence_color}]"
                )
                console.print(f"     [dim]A:[/dim] {match['title_a']} [dim]({match['feed_a']})[/dim]")
                if show_summaries and match.get("summary_a"):
                    summary = match["summary_a"][:150]
                    if len(match.get("summary_a", "")) > 150:
                        summary += "..."
                    console.print(f"        [dim italic]{summary}[/dim italic]")
                console.print(f"     [dim]B:[/dim] {match['title_b']} [dim]({match['feed_b']})[/dim]")
                if show_summaries and match.get("summary_b"):
                    summary = match["summary_b"][:150]
                    if len(match.get("summary_b", "")) > 150:
                        summary += "..."
                    console.print(f"        [dim italic]{summary}[/dim italic]")

    console.print()
    console.print(Panel.fit("Recommendations by Risk Profile", style="bold green"))
    console.print()

    profile = args.profile.lower()
    recommended_threshold = {
        "conservative": 12,
        "balanced": 16,
        "aggressive": 24,
    }.get(profile, 16)

    recommended = next((a for a in analyses if a["threshold"] == recommended_threshold), None)

    console.print(f"[cyan]Profile:[/cyan] [bold]{profile.upper()}[/bold]")
    console.print(f"[cyan]Recommended threshold:[/cyan] [bold yellow]{recommended_threshold}[/bold yellow]")
    if recommended:
        console.print(f"[cyan]Expected accuracy:[/cyan] {recommended['accuracy'] * 100:.1f}%")
        console.print(f"[cyan]Expected matches:[/cyan] {recommended['total_pairs']} per sample")

    console.print()
    console.print("[bold]Example Matches for Recommended Threshold:[/bold]")
    console.print()
    if recommended and recommended.get("examples"):
        for match in recommended["examples"][: args.examples]:
            confidence_color = "green" if match["likely_true_positive"] else "red"
            console.print(
                f"[yellow]Distance:[/yellow] {match['distance']}, "
                f"[yellow]Time Diff:[/yellow] {match['time_diff_hours']}h, "
                f"[yellow]Title Overlap:[/yellow] {match['title_overlap']:.2f}, "
                f"[{confidence_color}]Confidence: {'HIGH' if match['likely_true_positive'] else 'LOW'}[/{confidence_color}]"
            )
            console.print(f"  [dim]A:[/dim] {match['title_a']} [dim]({match['feed_a']})[/dim]")
            if show_summaries and match.get("summary_a"):
                summary = match["summary_a"][:150]
                if len(match.get("summary_a", "")) > 150:
                    summary += "..."
                console.print(f"     [dim italic]{summary}[/dim italic]")
            console.print(f"  [dim]B:[/dim] {match['title_b']} [dim]({match['feed_b']})[/dim]")
            if show_summaries and match.get("summary_b"):
                summary = match["summary_b"][:150]
                if len(match.get("summary_b", "")) > 150:
                    summary += "..."
                console.print(f"     [dim italic]{summary}[/dim italic]")
            console.print()
    else:
        console.print("[red]No examples available for the recommended threshold.[/red]")

    console.print()
    console.print(Panel.fit("Current Configuration", style="bold cyan"))
    current_threshold = getattr(config, 'RECURRING_HAMMING_THRESHOLD', None)
    console.print(f"[cyan]Current threshold:[/cyan] {current_threshold if current_threshold is not None else 'N/A'}")
    current = next((a for a in analyses if a["threshold"] == getattr(config, "RECURRING_HAMMING_THRESHOLD", -1)), None)
    if current:
        console.print(f"[cyan]Accuracy:[/cyan] {current['accuracy'] * 100:.1f}%")
        console.print(f"[cyan]Matches:[/cyan] {current['total_pairs']} per sample")

    console.print()
    console.print("[bold yellow]CHANGE RECOMMENDATION:[/bold yellow]")
    if current_threshold is not None:
        if current_threshold != recommended_threshold:
            console.print(f"  [red]Current: {current_threshold}[/red] → [green]Recommended: {recommended_threshold}[/green]")
        else:
            console.print(f"  [green]✓ Current threshold ({current_threshold}) matches recommendation[/green]")
    else:
        console.print(f"  [yellow]Recommended threshold: {recommended_threshold}[/yellow]")

    console.print()
    console.print(Panel.fit("Final Guidance", style="bold magenta"))
    console.print()
    
    guidance = """
[bold cyan]THRESHOLD SELECTION FACTORS:[/bold cyan]
• [green]Lower thresholds (12-18):[/green] Higher accuracy, fewer matches, conservative
• [yellow]Medium thresholds (20-26):[/yellow] Balanced accuracy and coverage
• [red]Higher thresholds (28+):[/red] More coverage, lower accuracy, aggressive

[bold cyan]RECOMMENDATIONS BY USE CASE:[/bold cyan]
• [green]News/Journalism:[/green] Use conservative (12-18) to avoid missing breaking news
• [yellow]Technical Blogs:[/yellow] Use balanced (20-24) to reduce duplication
• [red]Social Media/Feeds:[/red] Use aggressive (26+) to maximize deduplication

[bold cyan]MONITORING ADVICE:[/bold cyan]
• After changing threshold, monitor for 1-2 weeks
• Check if legitimate stories are being filtered out
• Adjust based on your specific content mix and user feedback

[bold cyan]SAMPLE QUALITY NOTES:[/bold cyan]
• Based on {summaries} summaries from last {days} days
• More data leads to more reliable recommendations
• Consider running analysis after major content source changes
"""
    console.print(guidance.format(summaries=len(summaries), days=args.days))


if __name__ == "__main__":
    # Suppress all logging from config module
    logging.getLogger('FeedProcessor').setLevel(logging.CRITICAL)
    main()
