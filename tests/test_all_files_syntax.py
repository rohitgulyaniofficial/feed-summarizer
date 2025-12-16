import py_compile
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def iter_python_files(root: Path):
    """Yield all project .py files that should be syntax-checked.

    Skips common non-source locations like virtualenvs, cache dirs and dotfolders.
    """
    skip_dirs = {"__pycache__", ".venv", ".git", ".pytest_cache", ".pyscn"}

    for path in root.rglob("*.py"):
        # Skip files under unwanted directories
        parts = set(path.parts)
        if parts & skip_dirs:
            continue
        yield path


@pytest.mark.parametrize("path", sorted(iter_python_files(PROJECT_ROOT)))
def test_all_python_files_compile_without_syntax_errors(path: Path):
    """Ensure every Python source file can be compiled.

    This implicitly validates there are no syntax errors anywhere in the project.
    """
    # py_compile.compile will raise a PyCompileError on syntax issues
    py_compile.compile(str(path), doraise=True)
