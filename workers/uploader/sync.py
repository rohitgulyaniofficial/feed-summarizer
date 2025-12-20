#!/usr/bin/env python3
"""Directory and public sync routines for Azure uploads."""

from pathlib import Path
from typing import Dict, Optional, Tuple

from config import get_logger
from workers.uploader.client import AzureStorageUploader
from workers.uploader.helpers import build_blob_path, determine_deletions, list_local_files

logger = get_logger("uploader")


async def upload_directory(
    uploader: AzureStorageUploader,
    local_dir: Path,
    remote_prefix: str = "",
    force: bool = False,
    sync: bool = False,
    recursive: bool = False,
) -> Tuple[int, int, int]:
    """Upload all files in a directory to Azure storage with optional sync."""
    if not uploader.enabled:
        return 0, 0, 0

    if not local_dir.exists() or not local_dir.is_dir():
        logger.warning("Directory does not exist: %s", local_dir)
        return 0, 0, 0

    cache_prefix = remote_prefix.rstrip("/") + "/" if remote_prefix else None
    await uploader._refresh_blob_cache(cache_prefix)  # noqa: SLF001

    local_files = list_local_files(local_dir, recursive)
    logger.info("Found %d local files in %s", len(local_files), local_dir)

    uploaded = 0
    skipped = 0
    deleted = 0
    expected_blobs = set()

    for file_path in local_files:
        blob_path = build_blob_path(local_dir, file_path, remote_prefix, recursive)
        expected_blobs.add(blob_path)
        if await uploader.upload_file(file_path, blob_path, force=force):
            uploaded += 1
        else:
            skipped += 1

    if sync:
        remote_blobs_to_delete = determine_deletions(
            uploader.iter_cached_blob_keys(),
            expected_blobs,
            cache_prefix,
            recursive,
        )
        logger.info("Found %d remote blobs to delete", len(remote_blobs_to_delete))
        for blob_path in remote_blobs_to_delete:
            if await uploader.delete_blob(blob_path):
                deleted += 1

    return uploaded, skipped, deleted


async def sync_public_directory(
    uploader: AzureStorageUploader,
    public_dir: Path,
    force: bool = False,
    sync: bool = False,
) -> Dict[str, Tuple[int, int, int]]:
    """Sync the entire public directory to Azure storage."""
    if not uploader.enabled:
        return {}

    results: Dict[str, Tuple[int, int, int]] = {}
    try:
        logger.info(
            "Sync root: local='%s' -> remote container='%s' (account='%s')",
            public_dir,
            uploader.container,
            uploader.storage_account,
        )
    except Exception:  # noqa: BLE001
        pass

    feeds_dir = public_dir / "feeds"
    if feeds_dir.exists():
        logger.info("Syncing RSS feeds...")
        results["feeds"] = await upload_directory(
            uploader,
            feeds_dir,
            "feeds",
            force=force,
            sync=sync,
            recursive=True,
        )

    bulletins_dir = public_dir / "bulletins"
    if bulletins_dir.exists():
        logger.info("Syncing HTML bulletins...")
        results["bulletins"] = await upload_directory(
            uploader,
            bulletins_dir,
            "bulletins",
            force=force,
            sync=sync,
        )

    index_file = public_dir / "index.html"
    if index_file.exists():
        logger.info("Syncing main index...")
        await uploader._refresh_blob_cache()  # noqa: SLF001
        success = await uploader.upload_file(index_file, "index.html", force=force)
        results["index"] = (1 if success else 0, 0 if success else 1, 0)

    return results


def print_sync_summary(results: Dict[str, Tuple[int, int, int]]) -> None:
    """Print a summary of sync results."""
    if not results:
        logger.info("No sync operations performed")
        return

    total_uploaded = 0
    total_skipped = 0
    total_deleted = 0

    logger.info("📊 Azure Storage Sync Summary:")
    logger.info("=" * 50)

    for category, (uploaded, skipped, deleted) in results.items():
        total_uploaded += uploaded
        total_skipped += skipped
        total_deleted += deleted

        logger.info("%s:", category.upper())
        logger.info("  ✅ Uploaded: %s", uploaded)
        logger.info("  ⏭️  Skipped:  %s", skipped)
        logger.info("  🗑️  Deleted:  %s", deleted)

    logger.info("=" * 50)
    logger.info("TOTALS - Uploaded: %s, Skipped: %s, Deleted: %s", total_uploaded, total_skipped, total_deleted)
    logger.info("=" * 50)
