#!/usr/bin/env python3
"""Render current status feed charts to SVG files for local iteration."""

from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path
from time import time
from typing import Any, Dict

from config import config, get_logger
from models import DatabaseQueue
from workers.publisher.status_feed import generate_status_payload

logger = get_logger("tools.render_status_charts")


async def _fetch_metrics(db: DatabaseQueue) -> Dict[str, Any]:
    """Fetch status metrics from the database (best effort)."""
    try:
        return await db.execute("get_status_metrics", now_ts=int(time())) or {}
    except Exception as exc:  # pragma: no cover - debug helper
        logger.error("Failed to fetch status metrics: %s", exc)
        return {}


def _write_charts(charts: Dict[str, str], output_dir: Path) -> int:
    """Persist inline chart payloads to disk for preview."""
    output_dir.mkdir(parents=True, exist_ok=True)
    written = 0
    for name, payload in charts.items():
        if not payload:
            continue
        try:
            if payload.lstrip().startswith("<svg"):
                target = output_dir / f"status_{name}.svg"
                target.write_text(payload, encoding="utf-8")
                written += 1
                logger.info("Wrote %s", target)
            else:
                logger.warning("Payload for %s is not SVG; skipping", name)
        except Exception as exc:  # pragma: no cover - debug helper
            logger.error("Failed to write chart %s: %s", name, exc)
    return written


async def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path(config.PUBLIC_DIR) / "feeds" / "status_charts",
        help="Directory to write SVG charts (default: PUBLIC_DIR/feeds/status_charts)",
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=Path(config.DATABASE_PATH),
        help="Path to the SQLite database (default: config.DATABASE_PATH)",
    )
    args = parser.parse_args(argv)

    db = DatabaseQueue(str(args.db_path))
    await db.start()
    try:
        metrics = await _fetch_metrics(db)
    finally:
        await db.stop()

    charts = generate_status_payload(metrics)
    written = _write_charts(charts, args.output_dir)
    if written == 0:
        logger.warning("No charts were written (metrics may be empty)")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
