"""Database access layer.

This package replaces the historical single-file `models.py` module.

Public API remains stable:
    from models import DatabaseQueue

Note: our test suite imports every Python file by *file path* (not by package
name). To keep those import checks happy, this module avoids eager relative
imports and instead exposes `DatabaseQueue` lazily.
"""

from __future__ import annotations

from typing import Any

__all__ = ["DatabaseQueue"]


def __getattr__(name: str) -> Any:
    if name != "DatabaseQueue":
        raise AttributeError(name)
    # Imported lazily to avoid import recursion when repo tools load files
    # via importlib by path.
    from models.queue import DatabaseQueue

    return DatabaseQueue

