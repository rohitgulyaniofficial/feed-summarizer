"""IO helpers reused across components."""
from pathlib import Path
import os
import shutil
import tempfile
from typing import Optional

from config import get_logger

logger = get_logger("utils.io")


def atomic_write_text(target_path: Path, content: str, encoding: str = "utf-8", suffix: Optional[str] = None) -> None:
    """Atomically write text to the given path.

    Writes to a temporary file in the target directory, fsyncs, then moves
    into place to avoid partially written outputs.
    """
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_suffix = suffix if suffix is not None else target_path.suffix or ".tmp"
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding=encoding,
        suffix=tmp_suffix,
        dir=target_path.parent,
        delete=False,
    ) as tmp:
        tmp.write(content)
        tmp.flush()
        os.fsync(tmp.fileno())
        temp_path = Path(tmp.name)
    shutil.move(str(temp_path), target_path)
    logger.debug("Wrote %s atomically", target_path)


__all__ = ["atomic_write_text"]
