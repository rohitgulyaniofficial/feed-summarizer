"""Standard command line argument patterns for tools directory scripts.

This module provides common argument definitions and setup patterns
to ensure consistency across all tools. Defaults are read from the main
config module for full consistency with the application.
"""

import argparse
from typing import Optional, List

# Import validated settings from main config (ensures .env and secrets loading)
from config import config


# =============================================================================
# Defaults from main config (validated, environment-aware)
# =============================================================================

# Database
DEFAULT_DATABASE_PATH = config.DATABASE_PATH

# Thresholds from config (already validated)
DEFAULT_SIMHASH_THRESHOLD = config.SIMHASH_HAMMING_THRESHOLD
DEFAULT_RECURRING_THRESHOLD = config.RECURRING_HAMMING_THRESHOLD
DEFAULT_BM25_MAX_EXTRA_DISTANCE = config.BM25_MERGE_MAX_EXTRA_DISTANCE

# Time windows (tools-specific, not in main config)
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_LOOKBACK_HOURS = 168  # 7 days in hours

# Threshold ranges for analysis tools (tools-specific)
DEFAULT_MIN_THRESHOLD = 12
DEFAULT_MAX_THRESHOLD = 32
DEFAULT_THRESHOLD_STEP = 2

# Sample/limit sizes (tools-specific)
DEFAULT_SAMPLE_LIMIT = 200
DEFAULT_EXAMPLES_PER_THRESHOLD = 5

# Recurring detection (tools-specific)
DEFAULT_RECURRING_DAYS_BACK = 3


# =============================================================================
# Argument Helpers
# =============================================================================

def add_database_arg(parser: argparse.ArgumentParser, default: Optional[str] = None) -> None:
    """Add database path argument.

    Args:
        parser: ArgumentParser instance
        default: Default database path (uses DEFAULT_DATABASE_PATH if None)
    """
    db_default = default if default is not None else DEFAULT_DATABASE_PATH
    parser.add_argument("--db", default=db_default, help="Path to SQLite database (default: %(default)s)")


def add_verbosity_args(parser: argparse.ArgumentParser) -> None:
    """Add verbose and quiet arguments.

    Args:
        parser: ArgumentParser instance
    """
    verbosity_group = parser.add_mutually_exclusive_group()
    verbosity_group.add_argument("--verbose", "-v", action="store_true", help="Enable verbose output")
    verbosity_group.add_argument("--quiet", "-q", action="store_true", help="Suppress non-error output")


def add_time_window_args(parser: argparse.ArgumentParser, default_days: Optional[int] = None) -> None:
    """Add time window arguments (days/hours).

    Args:
        parser: ArgumentParser instance
        default_days: Default number of days (uses DEFAULT_LOOKBACK_DAYS if None)
    """
    days_default = default_days if default_days is not None else DEFAULT_LOOKBACK_DAYS
    time_group = parser.add_mutually_exclusive_group()
    time_group.add_argument(
        "--days", type=int, default=days_default, help="Lookback window in days (default: %(default)s)"
    )
    time_group.add_argument("--hours", type=int, help="Lookback window in hours")


def add_threshold_arg(parser: argparse.ArgumentParser, default: Optional[int] = None) -> None:
    """Add SimHash threshold argument.

    Args:
        parser: ArgumentParser instance
        default: Default threshold value (uses DEFAULT_SIMHASH_THRESHOLD if None)
    """
    thr_default = default if default is not None else DEFAULT_SIMHASH_THRESHOLD
    parser.add_argument(
        "--threshold",
        type=int,
        default=thr_default,
        help=f"SimHash hamming distance threshold (default: {thr_default})",
    )


def add_threshold_range_args(parser: argparse.ArgumentParser) -> None:
    """Add threshold range arguments for analysis tools.

    Args:
        parser: ArgumentParser instance
    """
    parser.add_argument(
        "--min-threshold", type=int, default=DEFAULT_MIN_THRESHOLD,
        help=f"Minimum threshold to test (default: {DEFAULT_MIN_THRESHOLD})"
    )
    parser.add_argument(
        "--max-threshold", type=int, default=DEFAULT_MAX_THRESHOLD,
        help=f"Maximum threshold to test (default: {DEFAULT_MAX_THRESHOLD})"
    )
    parser.add_argument(
        "--threshold-step", type=int, default=DEFAULT_THRESHOLD_STEP,
        help=f"Threshold step size (default: {DEFAULT_THRESHOLD_STEP})"
    )


def add_output_arg(parser: argparse.ArgumentParser, help_text: str = "Output file") -> None:
    """Add output file argument.

    Args:
        parser: ArgumentParser instance
        help_text: Help text description
    """
    parser.add_argument("--output", "-o", help=help_text)


def add_json_arg(parser: argparse.ArgumentParser) -> None:
    """Add JSON output format argument.

    Args:
        parser: ArgumentParser instance
    """
    parser.add_argument(
        "--json",
        action="store_true",
        help="Output results in JSON format (machine-readable)"
    )


def add_sample_arg(parser: argparse.ArgumentParser, default: Optional[int] = None) -> None:
    """Add sample size argument.

    Args:
        parser: ArgumentParser instance
        default: Default sample size (uses DEFAULT_SAMPLE_LIMIT if None)
    """
    sample_default = default if default is not None else DEFAULT_SAMPLE_LIMIT
    parser.add_argument(
        "--limit", type=int, default=sample_default, help=f"Sample/limit size (default: {sample_default})"
    )


def add_list_arg(parser: argparse.ArgumentParser, name: str, help_text: str) -> None:
    """Add comma-separated list argument.

    Args:
        parser: ArgumentParser instance
        name: Argument name (without --)
        help_text: Help text description
    """
    parser.add_argument(f"--{name}", help=f"Comma-separated {help_text}")


def compute_lookback(args: argparse.Namespace) -> tuple:
    """Compute lookback hours and label from args.
    
    Args:
        args: Parsed arguments with days/hours attributes
        
    Returns:
        Tuple of (lookback_hours, lookback_label)
    """
    if getattr(args, "hours", None) is not None:
        lookback_hours = int(args.hours)
        lookback_label = f"{lookback_hours}h"
    else:
        days = getattr(args, "days", DEFAULT_LOOKBACK_DAYS)
        lookback_hours = int(days) * 24
        lookback_label = f"{days}d"
    return lookback_hours, lookback_label


def create_standard_parser(
    description: str,
    with_db: bool = True,
    with_verbosity: bool = True,
    with_time_window: bool = False,
    with_threshold: bool = False,
    with_threshold_range: bool = False,
    with_output: bool = False,
    with_samples: bool = False,
    with_json: bool = False,
) -> argparse.ArgumentParser:
    """Create parser with standard arguments.

    Args:
        description: Script description
        with_db: Include database argument
        with_verbosity: Include verbosity arguments
        with_time_window: Include time window arguments
        with_threshold: Include threshold argument
        with_threshold_range: Include threshold range arguments (min/max/step)
        with_output: Include output argument
        with_samples: Include sample/limit argument
        with_json: Include JSON output argument

    Returns:
        Configured ArgumentParser
    """
    parser = argparse.ArgumentParser(description=description)

    if with_db:
        add_database_arg(parser)

    if with_verbosity:
        add_verbosity_args(parser)

    if with_time_window:
        add_time_window_args(parser)

    if with_threshold:
        add_threshold_arg(parser)

    if with_threshold_range:
        add_threshold_range_args(parser)

    if with_output:
        add_output_arg(parser)

    if with_samples:
        add_sample_arg(parser)

    if with_json:
        add_json_arg(parser)

    return parser


def parse_comma_separated_int(value: str, name: str) -> List[int]:
    """Parse comma-separated integer list.

    Args:
        value: Comma-separated string
        name: Parameter name for error messages

    Returns:
        List of integers

    Raises:
        SystemExit: If parsing fails
    """
    if not value:
        return []

    try:
        return [int(x.strip()) for x in value.split(",") if x.strip()]
    except ValueError:
        raise SystemExit(f"Invalid {name}: {value} (must be comma-separated integers)")


def parse_comma_separated_float(value: str, name: str) -> List[float]:
    """Parse comma-separated float list.

    Args:
        value: Comma-separated string
        name: Parameter name for error messages

    Returns:
        List of floats

    Raises:
        SystemExit: If parsing fails
    """
    if not value:
        return []

    try:
        return [float(x.strip()) for x in value.split(",") if x.strip()]
    except ValueError:
        raise SystemExit(f"Invalid {name}: {value} (must be comma-separated numbers)")


def validate_args(args: argparse.Namespace) -> None:
    """Validate common argument combinations.

    Args:
        args: Parsed arguments

    Raises:
        SystemExit: If validation fails
    """
    if args.verbose and args.quiet:
        raise SystemExit("Cannot specify both --verbose and --quiet")

    if hasattr(args, "threshold") and args.threshold is not None:
        if args.threshold < 0:
            raise SystemExit("Threshold must be non-negative")

    if hasattr(args, "samples") and args.samples < 0:
        raise SystemExit("Sample count must be non-negative")

    if hasattr(args, "days") and args.days and hasattr(args, "hours") and args.hours:
        raise SystemExit("Cannot specify both --days and --hours")


# Common argument presets for different script types


def create_analysis_parser(description: str) -> argparse.ArgumentParser:
    """Create parser for analysis scripts."""
    return create_standard_parser(
        description, with_db=True, with_verbosity=True, with_time_window=True, with_threshold=True, with_samples=True, with_json=True
    )


def create_migration_parser(description: str) -> argparse.ArgumentParser:
    """Create parser for migration scripts."""
    return create_standard_parser(description, with_db=True, with_verbosity=True)


def create_report_parser(description: str) -> argparse.ArgumentParser:
    """Create parser for reporting scripts."""
    return create_standard_parser(
        description, with_db=True, with_verbosity=True, with_time_window=True, with_output=True, with_json=True
    )
