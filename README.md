# Drive to Lark Simple Sync

This repository provides a single-purpose sync tool:

- Copy folders/files from one Google Drive source to Lark
- Preserve folder structure under configured Lark roots
- Upload files concurrently with pipelined workers
- Write mapping CSV rows during sync
- Keep migration artifacts separated per Google Drive profile

## Quick Start

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .
DRIVE_PROFILE=nuoiemmedia drive-migrate --concurrency 16
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

## Per-Drive Workspace Architecture

- Common code remains in `src/` and shared scripts stay at root.
- Each drive uses its own profile slug via `DRIVE_PROFILE` (recommended) or `DRIVE_ACCOUNT_ID`.
- Default output paths are auto-isolated to:
  - `reports/drives/<profile>/mappings.csv`
  - `reports/drives/<profile>/failed_items.csv`
  - `reports/drives/<profile>/run.log`
- `vps_sync.sh` also uses profile-scoped PID files: `.sync-<profile>.pid`

Example:

```bash
# Drive 1
export DRIVE_PROFILE=nuoiemmedia
bash vps_sync.sh start 8

# Drive 2
export DRIVE_PROFILE=sucmanh2000
bash vps_sync.sh start 8
```

## Optional Environment Variables

- `GOOGLE_API_BASE_URL` (default: `https://www.googleapis.com/drive/v3`)
- `GOOGLE_CLIENT_ID` / `GOOGLE_CLIENT_SECRET` / `GOOGLE_REFRESH_TOKEN` (for automatic access-token refresh)
- `LARK_API_BASE_URL` (default: `https://open.larksuite.com/open-apis`)
- `LARK_WEB_BASE_URL` (default: `https://larksuite.com`, set to your tenant domain)
- `SIMPLE_SYNC_CONCURRENCY` (default: `8`)
- `SIMPLE_SYNC_CHUNK_SIZE` (default: `4194304`)
- `SIMPLE_SYNC_MAPPING_OUT` (default: `reports/drives/<profile>/mappings.csv`)
- `SIMPLE_SYNC_FAILED_OUT` (default: `reports/drives/<profile>/failed_items.csv`)
- `DRIVE_PROFILE` (default fallback: `DRIVE_ACCOUNT_ID`)

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

## Post-Migration Cleanup (Google Drive)

Use this flow only after sync summary is stable and unresolved failures are reviewed.

1) Build unresolved failed list:

```bash
python3 - <<'PY'
import csv
from pathlib import Path
import os

raw_profile = (os.getenv("DRIVE_PROFILE") or os.getenv("DRIVE_ACCOUNT_ID") or "default-drive").strip().lower()
profile = "".join(c if c.isalnum() or c in "._-" else "-" for c in raw_profile).strip("-") or "default-drive"
base = Path("reports") / "drives" / profile
mapping = Path(os.getenv("SIMPLE_SYNC_MAPPING_OUT", str(base / "mappings.csv")))
failed = Path(os.getenv("SIMPLE_SYNC_FAILED_OUT", str(base / "failed_items.csv")))
unresolved = base / "unresolved_failed_items.csv"
m=set()
for r in csv.DictReader(mapping.open("r", encoding="utf-8", newline="")):
    gid=(r.get("google_object_id") or "").strip()
    if gid:
        m.add(gid)
rows=[]
for r in csv.DictReader(failed.open("r", encoding="utf-8", newline="")):
    gid=(r.get("google_object_id") or "").strip()
    if gid and gid not in m:
        rows.append(r)
with unresolved.open("w", encoding="utf-8", newline="") as f:
    w=csv.DictWriter(f, fieldnames=["account_id","google_object_id","google_url","reason"])
    w.writeheader(); w.writerows(rows)
print("wrote", unresolved, "rows", len(rows))
PY
```

2) Export safe delete candidates (exclude unresolved failures):

```bash
PYTHONPATH=src python3 -m migration.export_delete_candidates \
  --mapping reports/drives/<profile>/mappings.csv \
  --unresolved reports/drives/<profile>/unresolved_failed_items.csv \
  --out-candidates reports/drives/<profile>/delete_candidates.csv \
  --out-exclusions reports/drives/<profile>/delete_exclusions.csv
```

3) Dry-run delete batch:

```bash
PYTHONPATH=src python3 -m migration.trash_google_batches \
  --input reports/drives/<profile>/delete_candidates.csv \
  --offset 0 \
  --batch-size 500 \
  --dry-run
```

4) Optional strict verification gate before trash:

```bash
PYTHONPATH=src python3 -m migration.verify_before_trash \
  --input reports/drives/<profile>/delete_candidates.csv \
  --verified-out reports/drives/<profile>/verified_ok.csv \
  --failed-out reports/drives/<profile>/verification_failed.csv
```

If used, run trash batches against `reports/drives/<profile>/verified_ok.csv`.

5) Execute batch (move to Trash):

```bash
PYTHONPATH=src python3 -m migration.trash_google_batches \
  --input reports/drives/<profile>/verified_ok.csv \
  --offset 0 \
  --batch-size 500
```

6) Repeat with increasing `--offset` in phases. Keep `reports/drives/<profile>/delete_batches_log.csv` as the audit trail.
