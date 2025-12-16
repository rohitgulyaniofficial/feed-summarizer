import os
import py_compile
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

# Exclude non-source or generated trees.
EXCLUDE_DIR_PARTS = {
    ".venv",
    "__pycache__",
    "public",
    "tor",
    "lib",
    "publisher",  # generated output folder in this repo
}


def _iter_python_files() -> list[Path]:
    files: list[Path] = []
    for p in REPO_ROOT.rglob("*.py"):
        if any(part in EXCLUDE_DIR_PARTS for part in p.parts):
            continue
        # Skip editor/temporary files
        if p.name.startswith("."):
            continue
        files.append(p)
    return sorted(files)


def test_all_python_files_compile() -> None:
    """Ensure every Python file is syntactically valid.

    This catches SyntaxError/IndentationError across the whole repo.
    """
    for p in _iter_python_files():
        py_compile.compile(str(p), doraise=True)


def test_all_python_files_import_without_name_errors() -> None:
    """Import every module file in a subprocess.

    This catches import-time failures (e.g., missing typing imports used
    in annotations) without polluting the test runner process.

    Note: we deliberately execute by file path (not package name) to cover
    standalone scripts as well.
    """
    code = (
        "import importlib.util, sys\n"
        "path = sys.argv[1]\n"
        "spec = importlib.util.spec_from_file_location('repo_import_check', path)\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
    )

    base_env = os.environ.copy()
    # Keep imports deterministic and avoid noisy warnings breaking CI output.
    base_env.setdefault("PYTHONWARNINGS", "ignore")

    for p in _iter_python_files():
        # Tests are already imported by pytest; skip to avoid double-loading fixtures.
        if p.parts[-2] == "tests":
            continue

        subprocess.run(
            [sys.executable, "-c", code, str(p)],
            check=True,
            timeout=30,
            env=base_env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
