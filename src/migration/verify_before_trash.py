from __future__ import annotations

import argparse
import csv
from pathlib import Path

from .config import load_dotenv_if_present, load_real_integration_config
from .real_adapters import GoogleDriveApiClient, LarkApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Verify mapped files against Lark metadata before trashing Google files."
    )
    parser.add_argument("--input", default="reports/delete_candidates.csv", help="Delete candidates CSV")
    parser.add_argument("--verified-out", default="reports/verified_ok.csv", help="Verified output CSV")
    parser.add_argument("--failed-out", default="reports/verification_failed.csv", help="Failed output CSV")
    parser.add_argument("--offset", type=int, default=0, help="Start offset for candidate processing")
    parser.add_argument("--limit", type=int, default=0, help="Process limit (0 = all)")
    parser.add_argument(
        "--allow-name-mismatch",
        action="store_true",
        default=False,
        help="Do not fail when filename mismatch is detected",
    )
    parser.add_argument(
        "--allow-size-mismatch",
        action="store_true",
        default=False,
        help="Do not fail when size mismatch is detected",
    )
    return parser.parse_args()


def _load_candidates(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Candidates file not found: {path}")
    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def _to_int(value: str) -> int | None:
    v = (value or "").strip()
    if not v:
        return None
    try:
        return int(v)
    except ValueError:
        return None


def _pick_name(meta: dict) -> str:
    for key in ("name", "file_name", "title"):
        v = (meta.get(key) or "").strip()
        if v:
            return v
    return ""


def _pick_size(meta: dict) -> int | None:
    for key in ("size", "file_size", "bytes"):
        val = meta.get(key)
        if val is None:
            continue
        try:
            return int(val)
        except (TypeError, ValueError):
            continue
    return None


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    load_dotenv_if_present()
    cfg = load_real_integration_config()
    drive = GoogleDriveApiClient(cfg)
    lark = LarkApiClient(cfg)

    rows = _load_candidates(Path(args.input))
    start = max(0, args.offset)
    end = len(rows) if args.limit <= 0 else min(len(rows), start + args.limit)
    selected = rows[start:end]
    print(f"[progress] verify-start total_selected={len(selected)} offset={start} end={end}", flush=True)

    verified: list[dict[str, str]] = []
    failed: list[dict[str, str]] = []

    for idx, row in enumerate(selected, start=1):
        object_type = (row.get("object_type") or "").strip().lower()
        google_id = (row.get("google_object_id") or "").strip()
        lark_id = (row.get("lark_object_id") or "").strip()
        google_name = ""
        lark_name = ""
        google_size: int | None = None
        lark_size: int | None = None

        if idx == 1 or idx % 200 == 0:
            print(f"[progress] verify: processed={idx}/{len(selected)}", flush=True)

        if object_type != "file":
            failed.append(
                {
                    "google_object_id": google_id,
                    "lark_object_id": lark_id,
                    "reason": "non_file_candidate",
                    "google_name": "",
                    "lark_name": "",
                    "google_size": "",
                    "lark_size": "",
                }
            )
            continue

        try:
            g = drive.get_object_metadata(google_id)
            l = lark.get_file_metadata(lark_id)
            google_name = (g.get("name") or "").strip()
            lark_name = _pick_name(l)
            google_size = _to_int(str(g.get("size") or ""))
            lark_size = _pick_size(l)

            reasons: list[str] = []
            if google_name and lark_name and google_name != lark_name and not args.allow_name_mismatch:
                reasons.append("name_mismatch")
            if (
                google_size is not None
                and lark_size is not None
                and google_size != lark_size
                and not args.allow_size_mismatch
            ):
                reasons.append("size_mismatch")

            if reasons:
                failed.append(
                    {
                        "google_object_id": google_id,
                        "lark_object_id": lark_id,
                        "reason": ";".join(reasons),
                        "google_name": google_name,
                        "lark_name": lark_name,
                        "google_size": str(google_size or ""),
                        "lark_size": str(lark_size or ""),
                    }
                )
                continue

            verified.append(row)
        except Exception as exc:  # noqa: BLE001
            failed.append(
                {
                    "google_object_id": google_id,
                    "lark_object_id": lark_id,
                    "reason": f"{type(exc).__name__}:{exc}",
                    "google_name": google_name,
                    "lark_name": lark_name,
                    "google_size": str(google_size or ""),
                    "lark_size": str(lark_size or ""),
                }
            )

    _write_csv(
        Path(args.verified_out),
        ["account_id", "object_type", "google_object_id", "google_url", "lark_object_id", "lark_url"],
        verified,
    )
    _write_csv(
        Path(args.failed_out),
        ["google_object_id", "lark_object_id", "reason", "google_name", "lark_name", "google_size", "lark_size"],
        failed,
    )
    print(
        f"[progress] verify-done verified={len(verified)} failed={len(failed)} "
        f"verified_out={args.verified_out} failed_out={args.failed_out}",
        flush=True,
    )


if __name__ == "__main__":
    main()
