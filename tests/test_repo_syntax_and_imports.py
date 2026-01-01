import os
import py_compile
import subprocess
import sys
from multiprocessing import Pool
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


def _import_file_subprocess(file_path: Path) -> tuple[Path, bool, str]:
    """Import a single file in a subprocess. Returns (path, success, error_msg)."""
    code = (
        "import importlib.util, sys\n"
        "path = sys.argv[1]\n"
        "spec = importlib.util.spec_from_file_location('repo_import_check', path)\n"
        "mod = importlib.util.module_from_spec(spec)\n"
        "spec.loader.exec_module(mod)\n"
    )

    base_env = os.environ.copy()
    base_env.setdefault("PYTHONWARNINGS", "ignore")

    try:
        subprocess.run(
            [sys.executable, "-c", code, str(file_path)],
            check=True,
            timeout=30,
            env=base_env,
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        # Return only success status to avoid unused variable
        return (file_path, True, "")
    except subprocess.CalledProcessError as e:
        return (file_path, False, f"Exit code {e.returncode}: {e.stderr}")
    except subprocess.TimeoutExpired:
        return (file_path, False, "Timeout after 30 seconds")
    except Exception as e:
        return (file_path, False, str(e))


def test_all_python_files_import_without_name_errors() -> None:
    """Import every module file in a subprocess using parallel workers.

    This catches import-time failures (e.g., missing typing imports used
    in annotations) without polluting the test runner process.

    Note: we deliberately execute by file path (not package name) to cover
    standalone scripts as well.
    """
    files_to_test = []
    for p in _iter_python_files():
        # Tests are already imported by pytest; skip to avoid double-loading fixtures.
        if len(p.parts) >= 2 and p.parts[-2] == "tests":
            continue
        files_to_test.append(p)

    # Use multiprocessing pool for parallel imports
    with Pool(processes=os.cpu_count()) as pool:
        results = pool.map(_import_file_subprocess, files_to_test)

    # Check for failures
    failures = [(path, error) for path, success, error in results if not success]
    if failures:
        error_msg = "\n".join(f"{path}: {error}" for path, error in failures)
        raise AssertionError(f"Failed to import {len(failures)} file(s):\n{error_msg}")
