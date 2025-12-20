#!/usr/bin/env python3
"""Azure Storage uploader client primitives (direct SDK usage)."""

from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import md5
from pathlib import Path
from typing import Dict, Iterable, Optional

try:
    from azure.core.exceptions import AzureError
    from azure.storage.blob import ContentSettings
    from azure.storage.blob.aio import BlobServiceClient
    AZURE_STORAGE_AVAILABLE = True
except ImportError:  # pragma: no cover - optional dependency
    AzureError = ContentSettings = BlobServiceClient = None  # type: ignore
    AZURE_STORAGE_AVAILABLE = False

from config import config, get_logger
from workers.uploader.helpers import get_local_file_hash, get_mime_type

logger = get_logger("uploader")


@dataclass
class BlobResponse:
    ok: bool
    status: int


class AzureStorageUploader:
    """Manages uploads to Azure Blob Storage for RSS feeds and HTML bulletins."""

    def __init__(self, storage_account: Optional[str] = None, storage_key: Optional[str] = None, container: Optional[str] = None):
        self.storage_account = storage_account or config.AZURE_STORAGE_ACCOUNT
        self.storage_key = storage_key or config.AZURE_STORAGE_KEY
        self.container = container or config.AZURE_STORAGE_CONTAINER

        self._service: Optional[BlobServiceClient] = None
        self.enabled = bool(self.storage_account and self.storage_key and AZURE_STORAGE_AVAILABLE)
        self._blob_cache: Dict[str, Dict] = {}

        if not AZURE_STORAGE_AVAILABLE:
            logger.info("Azure storage upload disabled - azure-storage-blob not installed")
        elif not self.enabled:
            logger.info("Azure storage upload disabled - missing storage account or key configuration")

    async def initialize(self) -> None:
        if not self.enabled:
            return

        try:
            self._service = BlobServiceClient(
                account_url=f"https://{self.storage_account}.blob.core.windows.net",
                credential=self.storage_key,
            )
            logger.info("Azure storage uploader initialized for account '%s', container '%s'", self.storage_account, self.container)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to initialize Azure blob client: %s", exc)
            self.enabled = False

    async def close(self) -> None:
        if self._service:
            await self._service.close()
            self._service = None

    async def _refresh_blob_cache(self, prefix: Optional[str] = None) -> None:
        if not self.enabled or not self._service:
            return

        try:
            logger.info(
                "Refreshing blob cache for container '%s'%s",
                self.container,
                f" with prefix '{prefix}'" if prefix else "",
            )
            self._blob_cache.clear()

            blob_count = 0
            container_client = self._service.get_container_client(self.container)
            async for blob in container_client.list_blobs():
                blob_name = blob["name"] if isinstance(blob, dict) else getattr(blob, "name", None)
                if not blob_name:
                    continue
                if prefix and not blob_name.startswith(prefix):
                    continue

                content_settings = getattr(blob, "content_settings", None)
                blob_dict = {
                    "name": blob_name,
                    "content-length": getattr(blob, "size", 0) or 0,
                    "last-modified": getattr(blob, "last_modified", None),
                    "content-md5": getattr(content_settings, "content_md5", None) if content_settings else None,
                    "content-type": getattr(content_settings, "content_type", None) if content_settings else None,
                    "cache-control": getattr(content_settings, "cache_control", None) if content_settings else None,
                }
                self._blob_cache[blob_name] = blob_dict
                blob_count += 1

            logger.info("Cached %d blob(s) from Azure storage", blob_count)
        except Exception as exc:  # noqa: BLE001
            logger.error("Failed to refresh blob cache: %s", exc)
            self._blob_cache.clear()

    def iter_cached_blob_keys(self) -> Iterable[str]:
        return list(self._blob_cache.keys())

    def _get_cached_blob_info(self, blob_path: str) -> Optional[Dict]:
        return self._blob_cache.get(blob_path)

    async def _get_remote_blob_info(self, blob_path: str) -> Optional[Dict]:
        if not self.enabled or not self._service:
            return None

        cached_info = self._get_cached_blob_info(blob_path)
        if cached_info is not None:
            return cached_info

        try:
            container_client = self._service.get_container_client(self.container)
            async for blob in container_client.list_blobs(name_starts_with=blob_path):
                blob_name = blob["name"] if isinstance(blob, dict) else getattr(blob, "name", None)
                if blob_name == blob_path:
                    content_settings = getattr(blob, "content_settings", None)
                    blob_dict = {
                        "name": blob_name,
                        "content-length": getattr(blob, "size", 0) or 0,
                        "last-modified": getattr(blob, "last_modified", None),
                        "content-md5": getattr(content_settings, "content_md5", None) if content_settings else None,
                    }
                    self._blob_cache[blob_path] = blob_dict
                    return blob_dict
            return None
        except Exception as exc:  # noqa: BLE001
            logger.debug("Could not get remote blob info for %s: %s", blob_path, exc)
            return None

    def _should_upload_file(self, local_path: Path, remote_blob_info: Optional[Dict]) -> bool:
        if not local_path.exists():
            return False

        if remote_blob_info is None:
            return True

        local_stat = local_path.stat()
        local_size = local_stat.st_size
        local_mtime = datetime.fromtimestamp(local_stat.st_mtime, tz=timezone.utc)

        remote_size = remote_blob_info.get("content-length", 0)
        remote_mtime = remote_blob_info.get("last-modified")
        remote_md5 = remote_blob_info.get("content-md5")

        if remote_md5 is not None:
            local_hash = get_local_file_hash(local_path)
            if local_hash is not None:
                remote_hash = remote_md5.hex() if isinstance(remote_md5, bytes) else str(remote_md5)
                if local_hash == remote_hash:
                    logger.debug("Skipping upload of %s - content hash matches remote", local_path.name)
                    return False

        if local_size != remote_size:
            logger.debug("Size difference for %s: local=%s, remote=%s", local_path.name, local_size, remote_size)
            return True

        if remote_mtime and local_mtime > remote_mtime:
            logger.debug("Local file newer for %s: local=%s, remote=%s", local_path.name, local_mtime, remote_mtime)
            return True

        return False

    async def upload_file(self, local_path: Path, blob_path: str, force: bool = False) -> bool:
        if not self.enabled or not self._service:
            return False

        if not local_path.exists():
            logger.warning("Local file does not exist: %s", local_path)
            return False

        try:
            if not force:
                remote_info = await self._get_remote_blob_info(blob_path)
                if not self._should_upload_file(local_path, remote_info):
                    logger.debug("Skipping upload of %s - no changes detected", blob_path)
                    return False

            mime_type = get_mime_type(local_path)
            cache_control = "public, max-age=300"

            logger.info("Uploading %s to %s", local_path.name, blob_path)

            with open(local_path, "rb") as handle:
                data = handle.read()

            content_md5 = md5(data).digest()
            blob_client = self._service.get_container_client(self.container).get_blob_client(blob_path)
            settings = ContentSettings(
                content_type=mime_type or "application/octet-stream",
                cache_control=cache_control,
                content_md5=content_md5,
            )

            try:
                await blob_client.upload_blob(data, overwrite=True, content_settings=settings)
                logger.info("✅ Successfully uploaded %s", blob_path)
                return True
            except AzureError as exc:
                status = getattr(getattr(exc, "response", None), "status_code", 500)
                logger.error("❌ Failed to upload %s: HTTP %s (%s)", blob_path, status, exc)
                return False

        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Error uploading %s: %s", local_path.name, exc)
            return False

    async def delete_blob(self, blob_path: str) -> bool:
        if not self.enabled or not self._service:
            return False

        try:
            logger.info("Deleting blob %s", blob_path)
            blob_client = self._service.get_container_client(self.container).get_blob_client(blob_path)
            await blob_client.delete_blob(delete_snapshots="include")
            logger.info("✅ Successfully deleted %s", blob_path)
            self._blob_cache.pop(blob_path, None)
            return True

        except AzureError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", 500)
            logger.error("❌ Failed to delete %s: HTTP %s (%s)", blob_path, status, exc)
            return False
        except Exception as exc:  # noqa: BLE001
            logger.error("❌ Error deleting %s: %s", blob_path, exc)
            return False
