from __future__ import annotations

import argparse
import csv
from datetime import datetime, timezone
from pathlib import Path

from .config import load_dotenv_if_present, load_real_integration_config
from .real_adapters import GoogleDriveApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Trash Google Drive items in safe batches.")
    parser.add_argument(
        "--input",
        default="reports/delete_candidates.csv",
        help="Delete candidates CSV path",
    )
    parser.add_argument(
        "--offset",
        type=int,
        default=0,
        help="Start offset in the candidates CSV",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Number of rows to process in this run",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Plan only, no trash operations",
    )
    parser.add_argument(
        "--out-log",
        default="reports/delete_batches_log.csv",
        help="Output CSV log path",
    )
    return parser.parse_args()


def _load_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Candidates file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _append_log(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    write_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "ts_utc",
                "mode",
                "google_object_id",
                "object_type",
                "status",
                "error",
            ],
        )
        if write_header:
            writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    load_dotenv_if_present()

    all_rows = _load_rows(Path(args.input))
    start = max(0, args.offset)
    end = min(len(all_rows), start + max(1, args.batch_size))
    batch = all_rows[start:end]

    if not batch:
        print("No rows selected for this batch.")
        return

    mode = "dry_run" if args.dry_run else "execute"
    now = datetime.now(tz=timezone.utc).isoformat()

    logs: list[dict[str, str]] = []
    if args.dry_run:
        for row in batch:
            logs.append(
                {
                    "ts_utc": now,
                    "mode": mode,
                    "google_object_id": (row.get("google_object_id") or "").strip(),
                    "object_type": (row.get("object_type") or "").strip(),
                    "status": "planned",
                    "error": "",
                }
            )
        _append_log(Path(args.out_log), logs)
        print(f"Dry-run planned rows: {len(batch)} (offset={start}, end={end})")
        return

    cfg = load_real_integration_config()
    drive = GoogleDriveApiClient(cfg)

    success = 0
    failed = 0
    for row in batch:
        gid = (row.get("google_object_id") or "").strip()
        obj_type = (row.get("object_type") or "").strip()
        if not gid:
            failed += 1
            logs.append(
                {
                    "ts_utc": now,
                    "mode": mode,
                    "google_object_id": "",
                    "object_type": obj_type,
                    "status": "failed",
                    "error": "missing_google_object_id",
                }
            )
            continue
        try:
            drive.trash_object(gid)
            success += 1
            logs.append(
                {
                    "ts_utc": now,
                    "mode": mode,
                    "google_object_id": gid,
                    "object_type": obj_type,
                    "status": "trashed",
                    "error": "",
                }
            )
        except Exception as exc:  # noqa: BLE001
            failed += 1
            logs.append(
                {
                    "ts_utc": now,
                    "mode": mode,
                    "google_object_id": gid,
                    "object_type": obj_type,
                    "status": "failed",
                    "error": f"{type(exc).__name__}:{exc}",
                }
            )

    _append_log(Path(args.out_log), logs)
    print(
        f"Batch complete offset={start} end={end} "
        f"success={success} failed={failed} log={args.out_log}"
    )


if __name__ == "__main__":
    main()
