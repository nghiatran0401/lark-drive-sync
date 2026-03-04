from __future__ import annotations

import asyncio
import contextlib
import csv
import random
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Iterable, Protocol

from .models import AccountConfig, DriveObject, TransferResult


class DriveTreeClient(Protocol):
    def list_objects_recursive(self, account: AccountConfig) -> Iterable[DriveObject]:
        """Yield drive objects under account root."""

    def stream_bytes(self, account_id: str, object_id: str, chunk_size: int = 1024 * 1024) -> Iterable[bytes]:
        """Yield file bytes as chunks."""


class LarkFolderClient(Protocol):
    def create_folder(self, name: str, parent_lark_folder_id: str) -> tuple[str, str]:
        """Create destination folder and return object id/url."""

    def upload_file_to_folder(
        self,
        object_name: str,
        parent_lark_folder_id: str,
        chunks: Iterable[bytes],
        *,
        content_type: str | None = None,
        resume_token: str | None = None,
    ) -> TransferResult:
        """Upload file bytes into a specific Lark folder."""


@dataclass(frozen=True)
class SyncStats:
    discovered: int
    folders_created: int
    files_uploaded: int
    bytes_uploaded: int
    files_failed: int
    files_skipped_google_native: int
    files_skipped_zero_byte: int
    files_skipped_already_mapped: int
    folders_skipped_already_mapped: int


class MappingCsvWriter:
    def __init__(self, output_path: str) -> None:
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._mapped_google_ids: set[str] = set()
        if not self.output_path.exists() or self.output_path.stat().st_size == 0:
            with self.output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["account_id", "object_type", "google_object_id", "google_url", "lark_object_id", "lark_url"])
        self._load_existing_mappings()

    def _load_existing_mappings(self) -> None:
        with self.output_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                google_object_id = (row.get("google_object_id") or "").strip()
                if google_object_id:
                    self._mapped_google_ids.add(google_object_id)

    def is_mapped(self, google_object_id: str) -> bool:
        return google_object_id in self._mapped_google_ids

    def load_folder_map_for_account(self, account_id: str) -> dict[str, str]:
        folder_map: dict[str, str] = {}
        with self.output_path.open("r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if (row.get("account_id") or "").strip() != account_id:
                    continue
                if (row.get("object_type") or "").strip().lower() != "folder":
                    continue
                google_id = (row.get("google_object_id") or "").strip()
                lark_id = (row.get("lark_object_id") or "").strip()
                if google_id and lark_id:
                    folder_map[google_id] = lark_id
        return folder_map

    async def append(
        self,
        *,
        account_id: str,
        object_type: str,
        google_object_id: str,
        google_url: str,
        lark_object_id: str,
        lark_url: str,
    ) -> bool:
        async with self._lock:
            if google_object_id in self._mapped_google_ids:
                return False
            with self.output_path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([account_id, object_type, google_object_id, google_url, lark_object_id, lark_url])
            self._mapped_google_ids.add(google_object_id)
            return True


class FailedCsvWriter:
    def __init__(self, output_path: str) -> None:
        self.output_path = Path(output_path)
        self.output_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = asyncio.Lock()
        self._seen: set[tuple[str, str]] = set()
        if not self.output_path.exists() or self.output_path.stat().st_size == 0:
            with self.output_path.open("w", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["account_id", "google_object_id", "google_url", "reason"])

    async def append(self, *, account_id: str, google_object_id: str, google_url: str, reason: str) -> None:
        key = (google_object_id, reason)
        async with self._lock:
            if key in self._seen:
                return
            with self.output_path.open("a", newline="", encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow([account_id, google_object_id, google_url, reason])
            self._seen.add(key)


class SimpleSyncEngine:
    """Content-only Drive->Lark sync with pipelined async workers."""

    def __init__(
        self,
        drive_client: DriveTreeClient,
        lark_client: LarkFolderClient,
        *,
        concurrency: int,
        chunk_size: int,
        mapping_writer: MappingCsvWriter,
        failed_writer: FailedCsvWriter | None = None,
        progress_hook: Callable[[str], None] | None = None,
    ) -> None:
        self.drive_client = drive_client
        self.lark_client = lark_client
        self.concurrency = max(1, concurrency)
        self.chunk_size = max(64 * 1024, chunk_size)
        self.mapping_writer = mapping_writer
        self.failed_writer = failed_writer
        self.progress_hook = progress_hook

    async def sync_account(self, account: AccountConfig, root_lark_folder_id: str) -> SyncStats:
        folder_map: dict[str, str] = {account.root_folder_id: root_lark_folder_id}
        existing_folders = self.mapping_writer.load_folder_map_for_account(account.account_id)
        if existing_folders:
            folder_map.update(existing_folders)
            self._log(f"bootstrap: preloaded_folder_mappings={len(existing_folders)}")
        failed_folders: set[str] = set()
        discovered_folder_ids: set[str] = {account.root_folder_id}
        folder_queue: asyncio.Queue[DriveObject | None] = asyncio.Queue(maxsize=self.concurrency * 20)
        file_queue: asyncio.Queue[DriveObject | None] = asyncio.Queue(maxsize=self.concurrency * 50)
        discovery_done = asyncio.Event()

        counters = {
            "discovered": 0,
            "folders_created": 0,
            "files_uploaded": 0,
            "bytes_uploaded": 0,
            "files_failed": 0,
            "files_skipped_google_native": 0,
            "files_skipped_zero_byte": 0,
            "files_skipped_already_mapped": 0,
            "folders_skipped_already_mapped": 0,
        }
        counter_lock = asyncio.Lock()
        folder_map_lock = asyncio.Lock()
        parent_wait_retries: dict[str, int] = {}
        max_parent_wait_retries = 400

        async def inc(key: str, n: int = 1) -> int:
            async with counter_lock:
                counters[key] += n
                return counters[key]

        async def producer() -> None:
            try:
                for obj in self.drive_client.list_objects_recursive(account):
                    scanned = await inc("discovered")
                    if scanned == 1 or scanned % 100 == 0:
                        self._log(f"discovery: scanned={scanned}")
                    if self.mapping_writer.is_mapped(obj.object_id):
                        if obj.is_folder:
                            skipped = await inc("folders_skipped_already_mapped")
                            if skipped == 1 or skipped % 50 == 0:
                                self._log(f"folders: skipped_already_mapped={skipped}")
                        else:
                            skipped = await inc("files_skipped_already_mapped")
                            if skipped == 1 or skipped % 50 == 0:
                                self._log(f"files: skipped_already_mapped={skipped}")
                        continue
                    if obj.is_folder:
                        discovered_folder_ids.add(obj.object_id)
                        await folder_queue.put(obj)
                    elif obj.is_google_native:
                        skipped = await inc("files_skipped_google_native")
                        if skipped == 1 or skipped % 50 == 0:
                            self._log(f"files: skipped_google_native={skipped}")
                    elif obj.size_bytes <= 0:
                        skipped = await inc("files_skipped_zero_byte")
                        if skipped == 1 or skipped % 50 == 0:
                            self._log(f"files: skipped_zero_byte={skipped}")
                        if self.failed_writer:
                            await self.failed_writer.append(
                                account_id=account.account_id,
                                google_object_id=obj.object_id,
                                google_url=obj.web_view_link,
                                reason="zero_byte_source",
                            )
                    else:
                        await file_queue.put(obj)
            finally:
                discovery_done.set()
                self._log("discovery complete")

        async def folder_worker() -> None:
            while True:
                folder = await folder_queue.get()
                if folder is None:
                    folder_queue.task_done()
                    return
                try:
                    parent_drive_id = folder.parent_id or account.root_folder_id
                    async with folder_map_lock:
                        parent_lark_id = folder_map.get(parent_drive_id)
                    if parent_lark_id is None:
                        retry_count = parent_wait_retries.get(folder.object_id, 0) + 1
                        parent_wait_retries[folder.object_id] = retry_count
                        if discovery_done.is_set():
                            if parent_drive_id in failed_folders:
                                self._log(
                                    f"folder parent failed: drive_folder_id={folder.object_id} "
                                    f"parent_drive_id={parent_drive_id}; placing under root"
                                )
                                parent_lark_id = root_lark_folder_id
                            elif parent_drive_id not in discovered_folder_ids:
                                # Parent folder does not exist in discovered set; avoid deadlock.
                                self._log(
                                    f"folder parent missing: drive_folder_id={folder.object_id} "
                                    f"parent_drive_id={parent_drive_id}; placing under root"
                                )
                                parent_lark_id = root_lark_folder_id
                            elif retry_count >= max_parent_wait_retries:
                                self._log(
                                    f"folder parent unresolved timeout: drive_folder_id={folder.object_id} "
                                    f"parent_drive_id={parent_drive_id}; placing under root"
                                )
                                parent_lark_id = root_lark_folder_id
                        if parent_lark_id is not None:
                            # parent_lark_id resolved via fallback path above.
                            pass
                        else:
                            await asyncio.sleep(0.05)
                            await folder_queue.put(folder)
                            continue

                    lark_id, lark_url = await asyncio.to_thread(
                        self.lark_client.create_folder,
                        folder.name,
                        parent_lark_id,
                    )
                    async with folder_map_lock:
                        folder_map[folder.object_id] = lark_id
                    created = await inc("folders_created")
                    if created == 1 or created % 25 == 0:
                        self._log(f"folders: created={created}")
                    await self.mapping_writer.append(
                        account_id=account.account_id,
                        object_type="folder",
                        google_object_id=folder.object_id,
                        google_url=folder.web_view_link,
                        lark_object_id=lark_id,
                        lark_url=lark_url,
                    )
                except Exception as exc:  # noqa: BLE001
                    self._log(
                        f"folder create error: drive_folder_id={folder.object_id} "
                        f"error={type(exc).__name__}: {exc}"
                    )
                    failed_folders.add(folder.object_id)
                finally:
                    folder_queue.task_done()

        async def file_worker() -> None:
            while True:
                file_obj = await file_queue.get()
                if file_obj is None:
                    file_queue.task_done()
                    return
                try:
                    parent_drive_id = file_obj.parent_id or account.root_folder_id
                    async with folder_map_lock:
                        parent_lark_id = folder_map.get(parent_drive_id)
                    if parent_lark_id is None:
                        if parent_drive_id in failed_folders:
                            failed = await inc("files_failed")
                            if failed == 1 or failed % 10 == 0:
                                self._log(
                                    f"files: uploaded={counters['files_uploaded']} failed={failed}"
                                )
                            continue
                        if discovery_done.is_set() and folder_queue.empty():
                            # Fallback if parent folder wasn't represented as an object.
                            parent_lark_id = root_lark_folder_id
                        else:
                            await asyncio.sleep(0.05)
                            await file_queue.put(file_obj)
                            continue

                    bytes_uploaded = await self._upload_one_file(account, file_obj, parent_lark_id)
                    if bytes_uploaded is not None:
                        uploaded = await inc("files_uploaded")
                        total_bytes = await inc("bytes_uploaded", bytes_uploaded)
                        if uploaded == 1 or uploaded % 25 == 0:
                            self._log(
                                f"files: uploaded={uploaded} failed={counters['files_failed']} "
                                f"bytes_uploaded_mb={total_bytes / (1024 * 1024):.1f}"
                            )
                    else:
                        failed = await inc("files_failed")
                        if failed == 1 or failed % 10 == 0:
                            self._log(f"files: uploaded={counters['files_uploaded']} failed={failed}")
                finally:
                    file_queue.task_done()

        folder_workers = [asyncio.create_task(folder_worker()) for _ in range(max(2, min(self.concurrency, 8)))]
        file_workers = [asyncio.create_task(file_worker()) for _ in range(self.concurrency)]
        producer_task = asyncio.create_task(producer())
        heartbeat_stop = asyncio.Event()

        async def heartbeat() -> None:
            while not heartbeat_stop.is_set():
                await asyncio.sleep(30)
                if heartbeat_stop.is_set():
                    break
                self._log(
                    "heartbeat: "
                    f"folder_q={folder_queue.qsize()} file_q={file_queue.qsize()} "
                    f"discovered={counters['discovered']} folders_created={counters['folders_created']} "
                    f"files_uploaded={counters['files_uploaded']} "
                    f"bytes_uploaded_mb={counters['bytes_uploaded'] / (1024 * 1024):.1f} "
                    f"files_failed={counters['files_failed']}"
                )

        heartbeat_task = asyncio.create_task(heartbeat())

        try:
            await producer_task
            await folder_queue.join()
            for _ in folder_workers:
                await folder_queue.put(None)
            await asyncio.gather(*folder_workers)

            await file_queue.join()
            for _ in file_workers:
                await file_queue.put(None)
            await asyncio.gather(*file_workers)
        finally:
            heartbeat_stop.set()
            heartbeat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await heartbeat_task

        self._log(
            "sync summary: "
            f"discovered={counters['discovered']} folders_created={counters['folders_created']} "
            f"files_uploaded={counters['files_uploaded']} "
            f"bytes_uploaded_mb={counters['bytes_uploaded'] / (1024 * 1024):.1f} "
            f"files_failed={counters['files_failed']} "
            f"files_skipped_google_native={counters['files_skipped_google_native']} "
            f"files_skipped_zero_byte={counters['files_skipped_zero_byte']} "
            f"files_skipped_already_mapped={counters['files_skipped_already_mapped']} "
            f"folders_skipped_already_mapped={counters['folders_skipped_already_mapped']}"
        )
        return SyncStats(
            discovered=counters["discovered"],
            folders_created=counters["folders_created"],
            files_uploaded=counters["files_uploaded"],
            bytes_uploaded=counters["bytes_uploaded"],
            files_failed=counters["files_failed"],
            files_skipped_google_native=counters["files_skipped_google_native"],
            files_skipped_zero_byte=counters["files_skipped_zero_byte"],
            files_skipped_already_mapped=counters["files_skipped_already_mapped"],
            folders_skipped_already_mapped=counters["folders_skipped_already_mapped"],
        )

    async def _upload_one_file(
        self,
        account: AccountConfig,
        file_obj: DriveObject,
        parent_lark_id: str,
    ) -> int | None:
        def _upload() -> TransferResult:
            return self.lark_client.upload_file_to_folder(
                file_obj.name,
                parent_lark_id,
                self.drive_client.stream_bytes(
                    account.account_id,
                    file_obj.object_id,
                    chunk_size=self.chunk_size,
                ),
                content_type=file_obj.mime_type,
                resume_token=f"{account.account_id}:{file_obj.object_id}",
            )

        max_attempts = 4
        for attempt in range(1, max_attempts + 1):
            try:
                result = await asyncio.to_thread(_upload)
                await self.mapping_writer.append(
                    account_id=account.account_id,
                    object_type="file",
                    google_object_id=file_obj.object_id,
                    google_url=file_obj.web_view_link,
                    lark_object_id=result.lark_object_id,
                    lark_url=result.lark_url,
                )
                return result.bytes_copied
            except Exception as exc:  # noqa: BLE001
                reason = f"{type(exc).__name__}:{exc}"
                lowered = str(exc).lower()
                if "cannotdownloadabusivefile" in lowered:
                    self._log(
                        f"upload skip abusive: drive_file_id={file_obj.object_id} "
                        "reason=cannotDownloadAbusiveFile"
                    )
                    if self.failed_writer:
                        await self.failed_writer.append(
                            account_id=account.account_id,
                            google_object_id=file_obj.object_id,
                            google_url=file_obj.web_view_link,
                            reason="google_abusive_file_blocked_download",
                        )
                    return None

                if attempt < max_attempts and self._is_retryable_upload_error(exc):
                    delay = min(8.0, 0.4 * (2 ** (attempt - 1))) + random.uniform(0.0, 0.25)
                    self._log(
                        f"upload retry: drive_file_id={file_obj.object_id} "
                        f"attempt={attempt}/{max_attempts} delay_s={delay:.2f} "
                        f"error={type(exc).__name__}: {exc}"
                    )
                    await asyncio.sleep(delay)
                    continue

                self._log(f"upload error: drive_file_id={file_obj.object_id} error={reason}")
                if self.failed_writer:
                    await self.failed_writer.append(
                        account_id=account.account_id,
                        google_object_id=file_obj.object_id,
                        google_url=file_obj.web_view_link,
                        reason=reason,
                    )
                return None

        return None

    @staticmethod
    def _is_retryable_upload_error(exc: Exception) -> bool:
        text = f"{type(exc).__name__}: {exc}".lower()
        retryable_markers = (
            "timed out",
            "timeout",
            "broken pipe",
            "remote end closed connection",
            "remote disconnected",
            "connection reset",
            "temporarily unavailable",
            "http error 429",
            "http error 500",
            "http error 502",
            "http error 503",
            "http error 504",
            "lark retryable",
        )
        return any(marker in text for marker in retryable_markers)

    def _log(self, message: str) -> None:
        if self.progress_hook:
            self.progress_hook(message)


