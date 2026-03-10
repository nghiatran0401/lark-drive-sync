from __future__ import annotations

import argparse
import json
import os
import secrets
import urllib.parse
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from .config import load_dotenv_if_present


def _open_base_url() -> str:
    configured = os.getenv("LARK_AUTH_BASE_URL", "").strip()
    if configured:
        return configured.rstrip("/")
    api_base = os.getenv("LARK_API_BASE_URL", "https://open.larksuite.com/open-apis").strip()
    if "/open-apis" in api_base:
        return api_base.split("/open-apis", 1)[0].rstrip("/")
    return "https://open.larksuite.com"


def _app_id() -> str:
    value = os.getenv("LARK_APP_ID", "").strip()
    if not value:
        raise ValueError("Missing LARK_APP_ID in environment/.env.")
    return value


def _app_secret() -> str:
    value = os.getenv("LARK_APP_SECRET", "").strip()
    if not value:
        raise ValueError("Missing LARK_APP_SECRET in environment/.env.")
    return value


def _post_json(url: str, payload: dict[str, str]) -> dict:
    body = json.dumps(payload).encode("utf-8")
    req = Request(
        url=url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(req, timeout=30) as resp:  # noqa: S310
            return json.loads(resp.read().decode("utf-8"))
    except HTTPError as exc:
        details = ""
        try:
            details = exc.read().decode("utf-8", errors="replace")
        except Exception:  # noqa: BLE001
            details = "<unable to read response body>"
        raise RuntimeError(f"HTTP {exc.code} {url}: {details}") from exc
    except URLError as exc:
        raise RuntimeError(f"Network error calling {url}: {exc}") from exc


def _build_auth_url(redirect_uri: str, scope: str, state: str | None) -> str:
    query = {
        "app_id": _app_id(),
        "redirect_uri": redirect_uri,
        "scope": scope,
        "state": state or secrets.token_urlsafe(16),
    }
    return f"{_open_base_url()}/open-apis/authen/v1/index?{urllib.parse.urlencode(query)}"


def _exchange_code(code: str, redirect_uri: str) -> dict:
    url = f"{_open_base_url()}/open-apis/authen/v2/oauth/token"
    payload = {
        "client_id": _app_id(),
        "client_secret": _app_secret(),
        "grant_type": "authorization_code",
        "code": code,
        "redirect_uri": redirect_uri,
    }
    return _post_json(url, payload)


def _refresh_token(refresh_token: str) -> dict:
    url = f"{_open_base_url()}/open-apis/authen/v2/oauth/token"
    payload = {
        "client_id": _app_id(),
        "client_secret": _app_secret(),
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }
    return _post_json(url, payload)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Lark user OAuth helper")
    sub = parser.add_subparsers(dest="command", required=True)

    auth = sub.add_parser("auth-url", help="Generate user authorization URL")
    auth.add_argument("--redirect-uri", required=True, help="OAuth redirect URI configured in Lark app")
    auth.add_argument(
        "--scope",
        default="offline_access drive:drive drive:file drive:file:upload",
        help="Requested scopes (space-separated or as required by your app config)",
    )
    auth.add_argument("--state", default="", help="Optional state string")

    exchange = sub.add_parser("exchange-code", help="Exchange auth code for user token")
    exchange.add_argument("--code", required=True, help="Authorization code from callback URL")
    exchange.add_argument(
        "--redirect-uri",
        default=os.getenv("LARK_OAUTH_REDIRECT_URI", "").strip(),
        help="OAuth redirect URI used during authorization (or set LARK_OAUTH_REDIRECT_URI)",
    )

    refresh = sub.add_parser("refresh-token", help="Refresh user access token")
    refresh.add_argument(
        "--refresh-token",
        default=os.getenv("LARK_USER_REFRESH_TOKEN", "").strip(),
        help="Refresh token (or set LARK_USER_REFRESH_TOKEN in .env)",
    )

    return parser.parse_args()


def _print_env_hints(response: dict) -> None:
    if not isinstance(response, dict):
        return
    data = response.get("data", {}) if isinstance(response.get("data"), dict) else {}
    access_token = (
        response.get("access_token")
        or data.get("access_token")
        or data.get("user_access_token")
        or ""
    ).strip()
    refresh_token = (response.get("refresh_token") or data.get("refresh_token") or "").strip()
    if access_token:
        print("\nAdd/update in .env:")
        print("LARK_TOKEN_MODE=user")
        print(f"LARK_USER_ACCESS_TOKEN={access_token}")
        if refresh_token:
            print(f"LARK_USER_REFRESH_TOKEN={refresh_token}")


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()

    if args.command == "auth-url":
        url = _build_auth_url(args.redirect_uri, args.scope, args.state or None)
        print(url)
        return

    if args.command == "exchange-code":
        redirect_uri = (args.redirect_uri or "").strip()
        if not redirect_uri:
            raise ValueError("Missing redirect URI. Pass --redirect-uri or set LARK_OAUTH_REDIRECT_URI.")
        response = _exchange_code(args.code, redirect_uri)
        print(json.dumps(response, ensure_ascii=False, indent=2))
        _print_env_hints(response)
        return

    if args.command == "refresh-token":
        token = (args.refresh_token or "").strip()
        if not token:
            raise ValueError("Missing refresh token. Pass --refresh-token or set LARK_USER_REFRESH_TOKEN.")
        response = _refresh_token(token)
        print(json.dumps(response, ensure_ascii=False, indent=2))
        _print_env_hints(response)
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
