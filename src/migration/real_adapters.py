from __future__ import annotations

import json
import math
import os
import random
import threading
import time
import zlib
from datetime import datetime, timezone
from typing import Callable, Iterable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .config import (
    RealIntegrationConfig,
    fetch_google_access_token_from_refresh,
    fetch_lark_tenant_access_token,
)
from .models import AccountConfig, DriveObject, TransferResult


class LarkRetryableError(RuntimeError):
    """Retryable Lark API application-level error."""


class LarkApiError(RuntimeError):
    """Non-retryable Lark API application-level error with code metadata."""

    def __init__(self, code: int, msg: str, log_id: str | None, url: str) -> None:
        self.code = code
        self.msg = msg
        self.log_id = log_id
        self.url = url
        super().__init__(f"Lark API error code={code} msg={msg} log_id={log_id} url={url}")


class AuthTokenError(RuntimeError):
    """Raised when an upstream API rejects auth credentials."""


def _http_json(
    method: str,
    url: str,
    token: str,
    payload: dict | None = None,
    retries: int = 5,
    token_refresher: Callable[[], str] | None = None,
) -> dict:
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
    auth_token = token
    refreshed = False
    attempt = 0
    while True:
        headers = {"Authorization": f"Bearer {auth_token}", "Content-Type": "application/json"}
        req = Request(url=url, data=data, headers=headers, method=method)
        try:
            with urlopen(req, timeout=30) as resp:  # noqa: S310
                body = resp.read().decode("utf-8")
            if not body:
                return {}
            parsed = json.loads(body)
            _raise_on_lark_api_error(parsed, url=url)
            return parsed
        except LarkApiError as exc:
            if (
                exc.code == 99991663
                and "larksuite.com" in url
                and token_refresher is not None
                and not refreshed
            ):
                auth_token = token_refresher()
                refreshed = True
                continue
            raise
        except LarkRetryableError:
            if attempt >= retries:
                raise
        except HTTPError as exc:
            if exc.code == 401:
                if "googleapis.com" in url:
                    if token_refresher is not None and not refreshed:
                        auth_token = token_refresher()
                        refreshed = True
                        continue
                    raise AuthTokenError(
                        "Google Drive API returned 401 Unauthorized. "
                        "Set GOOGLE_ACCESS_TOKEN, or configure GOOGLE_CLIENT_ID + "
                        "GOOGLE_CLIENT_SECRET + GOOGLE_REFRESH_TOKEN for auto-refresh."
                    ) from exc
                if "larksuite.com" in url:
                    raise AuthTokenError(
                        "Lark API returned 401 Unauthorized. "
                        "Refresh token settings (LARK_ACCESS_TOKEN or LARK_APP_ID/LARK_APP_SECRET) and re-run."
                    ) from exc
            err = _parse_lark_http_error(exc, url)
            if err is not None:
                if isinstance(err, LarkRetryableError) and attempt < retries:
                    pass
                else:
                    raise err from exc
            elif attempt >= retries or exc.code not in {408, 429, 500, 502, 503, 504}:
                details = _read_http_error_body(exc)
                raise RuntimeError(f"Lark/Drive API HTTP {exc.code} {method} {url}: {details}") from exc
        except URLError:
            if attempt >= retries:
                raise
        attempt += 1
        time.sleep(min(8.0, (2**attempt) * 0.25) + random.uniform(0.0, 0.2))


def _http_multipart(
    url: str,
    token: str,
    fields: dict[str, str],
    file_field_name: str,
    file_name: str,
    file_bytes: bytes,
    content_type: str,
    retries: int = 5,
    token_refresher: Callable[[], str] | None = None,
) -> dict:
    boundary = "----larkdriveboundary7MA4YWxkTrZu0gW"
    body = bytearray()
    for key, value in fields.items():
        body.extend(f"--{boundary}\r\n".encode("utf-8"))
        body.extend(f'Content-Disposition: form-data; name="{key}"\r\n\r\n'.encode("utf-8"))
        body.extend(f"{value}\r\n".encode("utf-8"))
    body.extend(f"--{boundary}\r\n".encode("utf-8"))
    body.extend(
        (
            f'Content-Disposition: form-data; name="{file_field_name}"; filename="{file_name}"\r\n'
            f"Content-Type: {content_type}\r\n\r\n"
        ).encode("utf-8")
    )
    body.extend(file_bytes)
    body.extend(b"\r\n")
    body.extend(f"--{boundary}--\r\n".encode("utf-8"))

    auth_token = token
    refreshed = False
    attempt = 0
    while True:
        headers = {
            "Authorization": f"Bearer {auth_token}",
            "Content-Type": f"multipart/form-data; boundary={boundary}",
        }
        req = Request(url=url, data=bytes(body), headers=headers, method="POST")
        try:
            with urlopen(req, timeout=120) as resp:  # noqa: S310
                payload = json.loads(resp.read().decode("utf-8"))
            _raise_on_lark_api_error(payload, url=url)
            return payload
        except LarkApiError as exc:
            if (
                exc.code == 99991663
                and "larksuite.com" in url
                and token_refresher is not None
                and not refreshed
            ):
                auth_token = token_refresher()
                refreshed = True
                continue
            raise
        except LarkRetryableError:
            if attempt >= retries:
                raise
        except HTTPError as exc:
            if exc.code == 401:
                if "googleapis.com" in url:
                    raise AuthTokenError(
                        "Google Drive API returned 401 Unauthorized. "
                        "Refresh GOOGLE_ACCESS_TOKEN in .env and re-run."
                    ) from exc
                if "larksuite.com" in url:
                    if token_refresher is not None and not refreshed:
                        auth_token = token_refresher()
                        refreshed = True
                        continue
                    raise AuthTokenError(
                        "Lark API returned 401 Unauthorized. "
                        "Refresh token settings (LARK_ACCESS_TOKEN or LARK_APP_ID/LARK_APP_SECRET) and re-run."
                    ) from exc
            err = _parse_lark_http_error(exc, url)
            if err is not None:
                if isinstance(err, LarkRetryableError) and attempt < retries:
                    pass
                else:
                    raise err from exc
            elif attempt >= retries or exc.code not in {408, 429, 500, 502, 503, 504}:
                details = _read_http_error_body(exc)
                raise RuntimeError(f"Lark API HTTP {exc.code} POST {url}: {details}") from exc
        except URLError:
            if attempt >= retries:
                raise
        attempt += 1
        time.sleep(min(8.0, (2**attempt) * 0.25) + random.uniform(0.0, 0.2))


def _raise_on_lark_api_error(payload: dict, *, url: str) -> None:
    code = payload.get("code")
    if code in (None, 0):
        return
    msg = payload.get("msg", "unknown error")
    log_id = payload.get("error", {}).get("log_id")
    if code in {99991400, 1061045}:
        raise LarkRetryableError(f"Lark retryable code={code} msg={msg} log_id={log_id} url={url}")
    raise LarkApiError(code=code, msg=msg, log_id=log_id, url=url)


def _read_http_error_body(exc: HTTPError) -> str:
    try:
        body = exc.read().decode("utf-8", errors="replace")
    except Exception:  # noqa: BLE001
        body = "<unable to read body>"
    return body[:400]


def _parse_lark_http_error(exc: HTTPError, url: str) -> Exception | None:
    try:
        body = exc.read().decode("utf-8", errors="replace")
        payload = json.loads(body)
    except Exception:  # noqa: BLE001
        return None
    code = payload.get("code")
    if code in (None, 0):
        return None
    msg = payload.get("msg", "unknown error")
    log_id = payload.get("error", {}).get("log_id")
    if code in {99991400, 1061045}:
        return LarkRetryableError(f"Lark retryable code={code} msg={msg} log_id={log_id} url={url}")
    return LarkApiError(code=code, msg=msg, log_id=log_id, url=url)


class GoogleDriveApiClient:
    """Google Drive API integration for discovery and streaming content reads.

    Notes:
    - Uses account-scoped credentials represented by `credential_ref` only as metadata.
    - Supports either direct access token or automatic refresh-token flow.
    """

    def __init__(self, config: RealIntegrationConfig) -> None:
        self.cfg = config
        self._google_access_token = config.google_access_token
        self._google_token_lock = threading.Lock()

    def _refresh_google_access_token(self) -> str:
        with self._google_token_lock:
            client_id = self.cfg.google_client_id
            client_secret = self.cfg.google_client_secret
            refresh_token = self.cfg.google_refresh_token
            if not (client_id and client_secret and refresh_token):
                raise AuthTokenError(
                    "Google access token expired and refresh credentials are missing. "
                    "Set GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET + GOOGLE_REFRESH_TOKEN."
                )
            try:
                token = fetch_google_access_token_from_refresh(client_id, client_secret, refresh_token)
            except ValueError as exc:
                raise AuthTokenError(str(exc)) from exc
            self._google_access_token = token
            return token

    def list_objects_recursive(self, account: AccountConfig) -> Iterable[DriveObject]:
        queue = [account.root_folder_id]
        while queue:
            parent_id = queue.pop(0)
            page_token: str | None = None
            while True:
                params = {
                    "q": f"'{parent_id}' in parents and trashed = false",
                    "fields": "nextPageToken,files(id,name,mimeType,md5Checksum,size,modifiedTime,owners(emailAddress),webViewLink)",
                    "supportsAllDrives": "true",
                    "includeItemsFromAllDrives": "true",
                    "pageSize": "1000",
                }
                if page_token:
                    params["pageToken"] = page_token
                url = f"{self.cfg.google_api_base_url}/files?{urlencode(params)}"
                payload = _http_json(
                    "GET",
                    url,
                    self._google_access_token,
                    token_refresher=self._refresh_google_access_token,
                )
                for item in payload.get("files", []):
                    mime_type = item.get("mimeType", "")
                    is_folder = mime_type == "application/vnd.google-apps.folder"
                    obj_id = item["id"]
                    if is_folder:
                        queue.append(obj_id)
                    yield DriveObject(
                        account_id=account.account_id,
                        object_id=obj_id,
                        parent_id=parent_id,
                        name=item.get("name", obj_id),
                        mime_type=mime_type,
                        checksum=item.get("md5Checksum"),
                        size_bytes=int(item.get("size", 0) or 0),
                        modified_time=_parse_rfc3339(item.get("modifiedTime")),
                        owner_principal=_owner_email(item),
                        web_view_link=item.get("webViewLink", f"https://drive.google.com/file/d/{obj_id}/view"),
                        is_folder=is_folder,
                        is_google_native=mime_type.startswith("application/vnd.google-apps."),
                    )
                page_token = payload.get("nextPageToken")
                if not page_token:
                    break

    def stream_bytes(self, account_id: str, object_id: str, chunk_size: int = 1024 * 1024) -> Iterable[bytes]:
        del account_id
        url = f"{self.cfg.google_api_base_url}/files/{object_id}?alt=media&supportsAllDrives=true"
        refreshed = False
        while True:
            req = Request(url=url, headers={"Authorization": f"Bearer {self._google_access_token}"}, method="GET")
            try:
                with urlopen(req, timeout=120) as resp:  # noqa: S310
                    while True:
                        chunk = resp.read(chunk_size)
                        if not chunk:
                            return
                        yield chunk
            except HTTPError as exc:
                if exc.code == 401 and not refreshed:
                    self._refresh_google_access_token()
                    refreshed = True
                    continue
                if exc.code == 401:
                    raise AuthTokenError(
                        "Google Drive API returned 401 Unauthorized during download. "
                        "Set GOOGLE_ACCESS_TOKEN, or configure GOOGLE_CLIENT_ID + "
                        "GOOGLE_CLIENT_SECRET + GOOGLE_REFRESH_TOKEN for auto-refresh."
                    ) from exc
                details = _read_http_error_body(exc)
                raise RuntimeError(f"Google Drive API HTTP {exc.code} GET {url}: {details}") from exc

class LarkApiClient:
    """Lark API integration for folder creation and file upload.

    This is a concrete skeleton with placeholder endpoint paths that can be adjusted
    to the exact tenant API contract during integration testing.
    """

    def __init__(self, config: RealIntegrationConfig) -> None:
        self.cfg = config
        # upload_part endpoint explicitly discourages concurrency; serialize multipart flows.
        self._multipart_lock = threading.Lock()
        self._lark_access_token = config.lark_access_token
        self._lark_token_lock = threading.Lock()
        # Large one-shot upload_all calls are more likely to timeout on unstable links.
        # Route larger files directly to multipart flow for better resilience.
        threshold_mb = int(os.getenv("LARK_MULTIPART_THRESHOLD_MB", "32"))
        self._multipart_threshold_bytes = max(1, threshold_mb) * 1024 * 1024

    def _refresh_lark_access_token(self) -> str:
        with self._lark_token_lock:
            app_id = self.cfg.lark_app_id
            app_secret = self.cfg.lark_app_secret
            if not (app_id and app_secret):
                raise AuthTokenError(
                    "Lark token expired and app credentials are missing. "
                    "Set LARK_APP_ID + LARK_APP_SECRET for auto-refresh."
                )
            try:
                token = fetch_lark_tenant_access_token(self.cfg.lark_api_base_url, app_id, app_secret)
            except ValueError as exc:
                raise AuthTokenError(str(exc)) from exc
            self._lark_access_token = token
            return token

    def _build_file_url(self, file_token: str) -> str:
        return f"{self.cfg.lark_web_base_url}/file/{file_token}"

    def _build_folder_url(self, folder_token: str) -> str:
        return f"{self.cfg.lark_web_base_url}/drive/folder/{folder_token}"

    def upload_resumable(
        self,
        object_name: str,
        chunks: Iterable[bytes],
        *,
        resume_token: str | None = None,
        content_type: str | None = None,
    ) -> TransferResult:
        data = b"".join(chunks)
        # Endpoint path may vary by tenant/app scopes; keep isolated here for easy swap.
        url = f"{self.cfg.lark_api_base_url}/drive/v1/files/upload_all"
        fields = {
            "file_name": object_name,
            "size": str(len(data)),
            "parent_type": "explorer",
            "parent_node": resume_token or "",
        }
        result = _http_multipart(
            url=url,
            token=self._lark_access_token,
            fields=fields,
            file_field_name="file",
            file_name=object_name,
            file_bytes=data,
            content_type=content_type or "application/octet-stream",
            token_refresher=self._refresh_lark_access_token,
        )
        obj = result.get("data", {})
        lark_object_id = obj.get("file_token") or obj.get("id") or f"lark-{resume_token or object_name}"
        lark_url = obj.get("url") or self._build_file_url(lark_object_id)
        return TransferResult(
            object_id=resume_token or object_name,
            lark_object_id=lark_object_id,
            lark_url=lark_url,
            bytes_copied=len(data),
            checksum=None,
        )

    def create_folder(self, name: str, parent_lark_folder_id: str) -> tuple[str, str]:
        url = f"{self.cfg.lark_api_base_url}/drive/v1/files/create_folder"
        payload = {
            "name": name,
            "folder_token": parent_lark_folder_id,
        }
        result = _http_json(
            "POST",
            url,
            self._lark_access_token,
            payload=payload,
            token_refresher=self._refresh_lark_access_token,
        )
        obj = result.get("data", {})
        folder_id = obj.get("token") or obj.get("folder_token") or obj.get("id")
        if not folder_id:
            raise ValueError("Lark folder creation did not return folder identifier")
        folder_url = obj.get("url") or self._build_folder_url(folder_id)
        return folder_id, folder_url

    def upload_file_to_folder(
        self,
        object_name: str,
        parent_lark_folder_id: str,
        chunks: Iterable[bytes],
        *,
        content_type: str | None = None,
        resume_token: str | None = None,
    ) -> TransferResult:
        data = b"".join(chunks)
        if len(data) >= self._multipart_threshold_bytes:
            result = self._multipart_upload_file(
                object_name=object_name,
                parent_lark_folder_id=parent_lark_folder_id,
                data=data,
            )
            obj = result.get("data", {})
            lark_object_id = obj.get("file_token") or obj.get("id") or f"lark-{object_name}"
            lark_url = obj.get("url") or self._build_file_url(lark_object_id)
            return TransferResult(
                object_id=object_name,
                lark_object_id=lark_object_id,
                lark_url=lark_url,
                bytes_copied=len(data),
                checksum=None,
            )

        url = f"{self.cfg.lark_api_base_url}/drive/v1/files/upload_all"
        primary_fields = {
            "file_name": object_name,
            "size": str(len(data)),
            "parent_type": "explorer",
            "parent_node": parent_lark_folder_id,
        }
        try:
            result = _http_multipart(
                url=url,
                token=self._lark_access_token,
                fields=primary_fields,
                file_field_name="file",
                file_name=object_name,
                file_bytes=data,
                content_type=content_type or "application/octet-stream",
                token_refresher=self._refresh_lark_access_token,
            )
        except LarkApiError as exc:
            if exc.code != 1061002:
                raise
            # Fallback 1: stricter tenants that expect parent_token.
            try:
                result = _http_multipart(
                    url=url,
                    token=self._lark_access_token,
                    fields={
                        "file_name": object_name,
                        "size": str(len(data)),
                        "parent_token": parent_lark_folder_id,
                    },
                    file_field_name="file",
                    file_name=object_name,
                    file_bytes=data,
                    content_type=content_type or "application/octet-stream",
                    token_refresher=self._refresh_lark_access_token,
                )
            except LarkApiError as exc2:
                if exc2.code != 1061002:
                    raise
                # Fallback 2: multipart block upload flow for files rejected by upload_all.
                result = self._multipart_upload_file(
                    object_name=object_name,
                    parent_lark_folder_id=parent_lark_folder_id,
                    data=data,
                )
        obj = result.get("data", {})
        lark_object_id = obj.get("file_token") or obj.get("id") or f"lark-{object_name}"
        lark_url = obj.get("url") or self._build_file_url(lark_object_id)
        return TransferResult(
            object_id=object_name,
            lark_object_id=lark_object_id,
            lark_url=lark_url,
            bytes_copied=len(data),
            checksum=None,
        )

    def _multipart_upload_file(self, object_name: str, parent_lark_folder_id: str, data: bytes) -> dict:
        with self._multipart_lock:
            block_size = 4 * 1024 * 1024
            total_size = len(data)
            block_num = max(1, math.ceil(total_size / block_size))

            prepare = _http_json(
                "POST",
                f"{self.cfg.lark_api_base_url}/drive/v1/files/upload_prepare",
                self._lark_access_token,
                payload={
                    "file_name": object_name,
                    "parent_type": "explorer",
                    "parent_node": parent_lark_folder_id,
                    "size": total_size,
                },
                token_refresher=self._refresh_lark_access_token,
            )
            upload_id = (prepare.get("data") or {}).get("upload_id")
            prepared_block_num = int((prepare.get("data") or {}).get("block_num") or block_num)
            if not upload_id:
                raise RuntimeError("Multipart upload_prepare missing upload_id")

            for seq in range(prepared_block_num):
                start = seq * block_size
                end = min(start + block_size, total_size)
                chunk = data[start:end]
                if not chunk and total_size == 0:
                    break
                if not chunk:
                    raise RuntimeError("Multipart chunk range produced empty chunk unexpectedly")
                checksum = str(zlib.adler32(chunk) & 0xFFFFFFFF)
                _http_multipart(
                    url=f"{self.cfg.lark_api_base_url}/drive/v1/files/upload_part",
                    token=self._lark_access_token,
                    fields={
                        "upload_id": upload_id,
                        "seq": str(seq),
                        "size": str(len(chunk)),
                        "checksum": checksum,
                    },
                    file_field_name="file",
                    file_name=f"{object_name}.part{seq}",
                    file_bytes=chunk,
                    content_type="application/octet-stream",
                    token_refresher=self._refresh_lark_access_token,
                )
                # Keep under the endpoint's low QPS guidance.
                time.sleep(0.22)

            finish = _http_json(
                "POST",
                f"{self.cfg.lark_api_base_url}/drive/v1/files/upload_finish",
                self._lark_access_token,
                payload={
                    "upload_id": upload_id,
                    "block_num": prepared_block_num,
                },
                token_refresher=self._refresh_lark_access_token,
            )
            return finish

def _parse_rfc3339(value: str | None) -> datetime:
    if not value:
        return datetime.now(tz=timezone.utc)
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)


def _owner_email(item: dict) -> str:
    owners = item.get("owners", [])
    if not owners:
        return "unknown-owner@example.com"
    return owners[0].get("emailAddress", "unknown-owner@example.com")

