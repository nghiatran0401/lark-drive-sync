# Drive to Lark Simple Sync

This repository provides a single-purpose sync tool:

- Copy folders/files from one Google Drive source to Lark
- Preserve folder structure under configured Lark roots
- Upload files concurrently with pipelined workers
- Write mapping CSV rows during sync

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
drive-migrate --concurrency 16 --mapping-out reports/mappings.csv
```

```
bash vps_sync.sh prepare
bash vps_sync.sh start 4
bash vps_sync.sh status
bash vps_sync.sh logs

bash vps_sync.sh stop
```

## Required Environment Variables

- One Google auth option:
  - `GOOGLE_ACCESS_TOKEN`, or
  - `GOOGLE_CLIENT_ID` + `GOOGLE_CLIENT_SECRET` + `GOOGLE_REFRESH_TOKEN` (recommended for long runs)
- One Lark auth option:
  - `LARK_USER_ACCESS_TOKEN` (OAuth user token), or
  - `LARK_ACCESS_TOKEN`, or
  - `LARK_APP_ID` + `LARK_APP_SECRET`
- `DRIVE_ACCOUNT_ID`
- `DRIVE_ROOT_FOLDER_ID`
- `LARK_ROOT_FOLDER_ID`

Update these three values each time to run a different drive migration.

## Optional Environment Variables

- `GOOGLE_API_BASE_URL` (default: `https://www.googleapis.com/drive/v3`)
- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` / `GOOGLE_REFRESH_TOKEN` (for automatic access-token refresh)
- `LARK_API_BASE_URL` (default: `https://open.larksuite.com/open-apis`)
- `LARK_WEB_BASE_URL` (default: `https://larksuite.com`, set to your tenant domain)
- `SIMPLE_SYNC_CONCURRENCY` (default: `8`)
- `SIMPLE_SYNC_CHUNK_SIZE` (default: `4194304`)
- `SIMPLE_SYNC_MAPPING_OUT` (default: `reports/mappings.csv`)
- `SIMPLE_SYNC_FAILED_OUT` (default: `reports/failed_items.csv`)

## Mapping CSV Format

Rows are appended after each folder/file is created:

`account_id,object_type,google_object_id,google_url,lark_object_id,lark_url`

## Runtime Notes

- Discovery, folder creation, and file upload run concurrently.
- Progress logs are printed during run with `[progress]` prefix.
- Google-native docs (Sheets/Docs/etc.) are skipped in content-only mode.
- Zero-byte files are skipped and written to failed report CSV.
- With refresh credentials configured, Google access token refresh is automatic during runtime.
- For Lark app auth, make sure required scopes are granted:
  - `drive:drive`
  - `drive:file`
  - `drive:file:upload`
- You can control token selection with `LARK_TOKEN_MODE`:
  - `auto` (default): user token -> access token -> tenant token
  - `user`: require `LARK_USER_ACCESS_TOKEN`
  - `tenant`: require app credentials (or `LARK_ACCESS_TOKEN`)
