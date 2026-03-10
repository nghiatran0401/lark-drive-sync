from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import AccountConfig


def load_dotenv_if_present(path: str = ".env") -> None:
    """Best-effort .env loader without external dependencies."""
    env_path = Path(path)
    if not env_path.exists():
        return
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip("'").strip('"')
        os.environ[key] = value


@dataclass(frozen=True)
class RealIntegrationConfig:
    google_api_base_url: str
    lark_api_base_url: str
    lark_web_base_url: str
    google_access_token: str
    google_client_id: str
    google_client_secret: str
    google_refresh_token: str
    lark_access_token: str
    lark_user_refresh_token: str
    lark_token_mode: str
    lark_app_id: str
    lark_app_secret: str


def load_real_integration_config() -> RealIntegrationConfig:
    lark_base = os.getenv("LARK_API_BASE_URL", "https://open.larksuite.com/open-apis")
    lark_web_base = os.getenv("LARK_WEB_BASE_URL", "https://larksuite.com")
    google_client_id = os.getenv("GOOGLE_CLIENT_ID", "").strip()
    google_client_secret = os.getenv("GOOGLE_CLIENT_SECRET", "").strip()
    google_refresh_token = os.getenv("GOOGLE_REFRESH_TOKEN", "").strip()
    google_access_token = _resolve_google_access_token(
        access_token=os.getenv("GOOGLE_ACCESS_TOKEN", "").strip(),
        client_id=google_client_id,
        client_secret=google_client_secret,
        refresh_token=google_refresh_token,
    )
    lark_app_id = os.getenv("LARK_APP_ID", "").strip()
    lark_app_secret = os.getenv("LARK_APP_SECRET", "").strip()
    lark_user_refresh_token = os.getenv("LARK_USER_REFRESH_TOKEN", "").strip()
    token_mode = os.getenv("LARK_TOKEN_MODE", "auto").strip().lower()
    lark_access_token = _resolve_lark_access_token(lark_base, token_mode=token_mode)
    return RealIntegrationConfig(
        google_api_base_url=os.getenv("GOOGLE_API_BASE_URL", "https://www.googleapis.com/drive/v3"),
        lark_api_base_url=lark_base,
        lark_web_base_url=lark_web_base.rstrip("/"),
        google_access_token=google_access_token,
        google_client_id=google_client_id,
        google_client_secret=google_client_secret,
        google_refresh_token=google_refresh_token,
        lark_access_token=lark_access_token,
        lark_user_refresh_token=lark_user_refresh_token,
        lark_token_mode=token_mode,
        lark_app_id=lark_app_id,
        lark_app_secret=lark_app_secret,
    )


def load_single_account_from_env() -> AccountConfig:
    account_id = os.getenv("DRIVE_ACCOUNT_ID", "").strip()
    root_folder_id = os.getenv("DRIVE_ROOT_FOLDER_ID", "").strip()
    credential_ref = os.getenv("DRIVE_CREDENTIAL_REF", "env://DRIVE_ACCOUNT").strip()
    if not account_id or not root_folder_id:
        raise ValueError(
            "Missing single-drive config. Set DRIVE_ACCOUNT_ID and DRIVE_ROOT_FOLDER_ID in .env."
        )
    return AccountConfig(
        account_id=account_id,
        root_folder_id=root_folder_id,
        credential_ref=credential_ref,
    )


def load_single_lark_root_folder_from_env() -> str:
    lark_root_folder_id = os.getenv("LARK_ROOT_FOLDER_ID", "").strip()
    if not lark_root_folder_id:
        raise ValueError("Missing LARK_ROOT_FOLDER_ID in .env.")
    return lark_root_folder_id


def _resolve_lark_access_token(lark_api_base_url: str, *, token_mode: str) -> str:
    app_id = os.getenv("LARK_APP_ID", "").strip()
    app_secret = os.getenv("LARK_APP_SECRET", "").strip()
    user_token = os.getenv("LARK_USER_ACCESS_TOKEN", "").strip()
    env_token = os.getenv("LARK_ACCESS_TOKEN", "").strip()

    if token_mode not in {"auto", "user", "tenant"}:
        raise ValueError("Invalid LARK_TOKEN_MODE. Use one of: auto, user, tenant.")

    if token_mode == "user":
        if user_token:
            return user_token
        raise ValueError("LARK_TOKEN_MODE=user requires LARK_USER_ACCESS_TOKEN.")

    if token_mode == "tenant":
        if app_id and app_secret:
            return _fetch_tenant_token(lark_api_base_url, app_id, app_secret)
        if env_token:
            return env_token
        raise ValueError("LARK_TOKEN_MODE=tenant requires LARK_APP_ID+LARK_APP_SECRET or LARK_ACCESS_TOKEN.")

    # auto mode: prefer user token, then explicit env token, then tenant token from app creds.
    if user_token:
        return user_token
    if env_token:
        return env_token
    if app_id and app_secret:
        return _fetch_tenant_token(lark_api_base_url, app_id, app_secret)
    raise ValueError(
        "Missing LARK token configuration. Set LARK_USER_ACCESS_TOKEN, "
        "or LARK_ACCESS_TOKEN, or LARK_APP_ID + LARK_APP_SECRET."
    )


def _resolve_google_access_token(
    *,
    access_token: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> str:
    if access_token:
        return access_token
    if client_id and client_secret and refresh_token:
        return fetch_google_access_token_from_refresh(client_id, client_secret, refresh_token)
    raise ValueError(
        "Missing Google auth configuration. Set GOOGLE_ACCESS_TOKEN, or "
        "GOOGLE_CLIENT_ID + GOOGLE_CLIENT_SECRET + GOOGLE_REFRESH_TOKEN."
    )


def fetch_google_access_token_from_refresh(
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> str:
    payload = urlencode(
        {
            "client_id": client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
            "grant_type": "refresh_token",
        }
    ).encode("utf-8")
    req = Request(
        url="https://oauth2.googleapis.com/token",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=30) as resp:  # noqa: S310
            body = json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        details = ""
        try:
            details = exc.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            details = "<unable to read body>"
        raise ValueError(
            "Google token refresh failed. Verify GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, "
            "and GOOGLE_REFRESH_TOKEN are from the same OAuth client and refresh token is valid. "
            f"HTTP {exc.code} response: {details[:400]}"
        ) from exc
    except URLError as exc:
        raise ValueError(f"Google token refresh network error: {exc}") from exc
    token = (body.get("access_token") or "").strip()
    if token:
        return token
    raise ValueError(f"Failed to obtain Google access token via refresh token. Response: {body}")


def _fetch_tenant_token(lark_api_base_url: str, app_id: str, app_secret: str) -> str:
    url = f"{lark_api_base_url}/auth/v3/tenant_access_token/internal"
    payload = json.dumps({"app_id": app_id, "app_secret": app_secret}).encode("utf-8")
    req = Request(url=url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=30) as resp:  # noqa: S310
        body = json.loads(resp.read().decode("utf-8"))
    token = body.get("tenant_access_token", "")
    if token:
        return token
    raise ValueError(
        f"Failed to obtain Lark tenant access token via app credentials. Response: {body}"
    )


def fetch_lark_tenant_access_token(lark_api_base_url: str, app_id: str, app_secret: str) -> str:
    return _fetch_tenant_token(lark_api_base_url, app_id, app_secret)


def fetch_lark_user_access_token_from_refresh(
    lark_api_base_url: str,
    client_id: str,
    client_secret: str,
    refresh_token: str,
) -> str:
    url = f"{lark_api_base_url}/authen/v2/oauth/token"
    payload = json.dumps(
        {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": client_id,
            "client_secret": client_secret,
        }
    ).encode("utf-8")
    req = Request(url=url, data=payload, headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(req, timeout=30) as resp:  # noqa: S310
        body = json.loads(resp.read().decode("utf-8"))
    token = (body.get("access_token") or "").strip()
    if token:
        return token
    raise ValueError(f"Failed to obtain Lark user access token via refresh token. Response: {body}")

