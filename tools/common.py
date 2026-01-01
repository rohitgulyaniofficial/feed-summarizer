"""Common utilities for tools directory scripts.

This module provides standardized error handling, database operations,
logging setup, and other shared functionality for all tools.
"""

import logging
import sqlite3
import sys
from pathlib import Path
from typing import Optional, Any, List
from contextlib import contextmanager

from config import get_logger


def validate_database_path(db_path: str) -> Path:
    """Validate that database file exists and is readable.

    Args:
        db_path: Path to SQLite database file

    Returns:
        Path object for the database

    Raises:
        SystemExit: If database doesn't exist or isn't readable
    """
    path = Path(db_path)
    if not path.exists():
        raise SystemExit(f"Database not found: {db_path}")
    if not path.is_file():
        raise SystemExit(f"Database path is not a file: {db_path}")
    return path


@contextmanager
def safe_database_connection(db_path: str, row_factory: bool = True):
    """Context manager for safe database connections.

    Args:
        db_path: Path to SQLite database
        row_factory: Whether to set sqlite3.Row as row factory

    Yields:
        sqlite3.Connection object

    Raises:
        SystemExit: If connection fails
    """
    validate_database_path(db_path)

    try:
        conn = sqlite3.connect(db_path)
        if row_factory:
            conn.row_factory = sqlite3.Row
        yield conn
    except sqlite3.Error as e:
        raise SystemExit(f"Database connection failed: {e}")
    finally:
        if "conn" in locals():
            conn.close()


def setup_script_logging(script_name: str, verbose: bool = False, quiet: bool = False) -> logging.Logger:
    """Setup consistent logging for scripts.

    Args:
        script_name: Name of the script for logger
        verbose: Enable debug output
        quiet: Suppress non-error output

    Returns:
        Configured logger instance
    """
    if quiet:
        level = logging.WARNING
    elif verbose:
        level = logging.DEBUG
    else:
        level = logging.INFO

    logger = get_logger(script_name)
    logger.setLevel(level)

    return logger


def print_table(headers: List[str], rows: List[List[Any]], sort_by_col: Optional[int] = None) -> None:
    """Print formatted table to stdout.

    Args:
        headers: Column headers
        rows: Table rows (list of lists)
        sort_by_col: Optional column index to sort by
    """
    if not rows:
        print("No data to display")
        return

    if sort_by_col is not None:
        rows = sorted(rows, key=lambda x: x[sort_by_col])

    # Calculate column widths
    col_widths = []
    for i, header in enumerate(headers):
        max_width = len(header)
        for row in rows:
            if i < len(row):
                max_width = max(max_width, len(str(row[i])))
        col_widths.append(max_width)

    # Print headers
    header_row = " | ".join(f"{header:<{col_widths[i]}}" for i, header in enumerate(headers))
    print(header_row)
    print("-" * len(header_row))

    # Print rows
    for row in rows:
        formatted_row = []
        for i, cell in enumerate(row):
            if i < len(col_widths):
                formatted_row.append(f"{str(cell):<{col_widths[i]}}")
        print(" | ".join(formatted_row))


class ProgressTracker:
    """Simple progress tracker for long-running operations."""

    def __init__(self, total: int, operation: str = "Processing", logger: Optional[logging.Logger] = None):
        self.total = total
        self.current = 0
        self.operation = operation
        self.logger = logger or logging.getLogger(__name__)
        self.last_reported = 0
        self.report_interval = max(1, total // 20)  # Report every 5%

    def update(self, increment: int = 1) -> None:
        """Update progress counter.

        Args:
            increment: Amount to increment counter by
        """
        self.current += increment

        if self.current - self.last_reported >= self.report_interval or self.current == self.total:
            pct = (self.current / self.total) * 100
            self.logger.info(f"{self.operation}: {self.current:,}/{self.total:,} ({pct:.1f}%)")
            self.last_reported = self.current

    def finish(self) -> None:
        """Mark operation as complete."""
        self.logger.info(f"{self.operation}: Complete ({self.total:,} items processed)")


def handle_script_error(error: Exception, logger: Optional[logging.Logger] = None) -> int:
    """Handle script errors consistently.

    Args:
        error: Exception that occurred
        logger: Logger instance for error reporting

    Returns:
        Exit code (1 for error)
    """
    if logger:
        logger.error(f"Script failed: {error}")
    else:
        print(f"Error: {error}", file=sys.stderr)
    return 1


def safe_int_parse(value: str, name: str, default: Optional[int] = None) -> int:
    """Safely parse integer from command line argument.

    Args:
        value: String value to parse
        name: Parameter name for error messages
        default: Default value if parsing fails

    Returns:
        Parsed integer value

    Raises:
        SystemExit: If parsing fails and no default provided
    """
    try:
        return int(value)
    except ValueError:
        if default is not None:
            return default
        raise SystemExit(f"Invalid {name}: {value} (must be an integer)")


def validate_positive_int(value: int, name: str) -> int:
    """Validate that integer is positive.

    Args:
        value: Integer value to validate
        name: Parameter name for error messages

    Returns:
        Validated integer

    Raises:
        SystemExit: If value is not positive
    """
    if value <= 0:
        raise SystemExit(f"{name} must be positive: {value}")
    return value
