#!/usr/bin/env python3
"""Public entrypoints for Azure uploads."""

from pathlib import Path
from typing import Dict, Optional, Tuple

from config import config, get_logger
from workers.uploader.client import AzureStorageUploader
from workers.uploader.sync import print_sync_summary, sync_public_directory

logger = get_logger("uploader")


async def upload_public_directory(
    public_dir: Path,
    *,
    force: bool = False,
    sync_delete: Optional[bool] = None,
) -> Optional[Dict[str, Tuple[int, int, int]]]:
    """Upload a public directory using a short-lived uploader instance."""
    uploader = AzureStorageUploader()
    if not uploader.enabled:
        logger.debug("Azure upload skipped - not configured")
        return None

    await uploader.initialize()
    if sync_delete is None:
        sync_delete = bool(config.AZURE_UPLOAD_SYNC_DELETE)

    try:
        results = await sync_public_directory(uploader, public_dir, force=force, sync=bool(sync_delete))
        print_sync_summary(results)
        return results
    finally:
        await uploader.close()
