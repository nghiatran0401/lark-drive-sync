from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Callable

from .config import load_dotenv_if_present, load_single_account_from_env, load_real_integration_config
from .models import AccountConfig
from .paths import default_report_file
from .real_adapters import GoogleDriveApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan top-level-folder batch migration status for current DRIVE_ROOT_FOLDER_ID."
    )
    parser.add_argument(
        "--mapping",
        default=default_report_file("mappings.csv"),
        help="Mappings CSV path",
    )
    parser.add_argument(
        "--out",
        default=default_report_file("top_level_batches.csv"),
        help="Output CSV path",
    )
    return parser.parse_args()


def _load_mapped_ids(mapping_path: Path, account_id: str) -> set[str]:
    if not mapping_path.exists():
        return set()
    mapped: set[str] = set()
    with mapping_path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("account_id") or "").strip() != account_id:
                continue
            gid = (row.get("google_object_id") or "").strip()
            if gid:
                mapped.add(gid)
    return mapped


def build_top_level_batch_rows(
    *,
    account: AccountConfig,
    source: GoogleDriveApiClient,
    mapped_ids: set[str],
    progress_hook: Callable[[str], None] | None = None,
) -> list[dict[str, str]]:
    nodes: dict[str, tuple[str, str, bool, bool, int]] = {}
    # object_id -> (parent_id, name, is_folder, is_google_native, size_bytes)
    top_level_folders: dict[str, str] = {}

    scanned = 0
    for obj in source.list_objects_recursive(account):
        scanned += 1
        if progress_hook and (scanned == 1 or scanned % 2000 == 0):
            progress_hook(f"scanned={scanned}")
        parent = (obj.parent_id or "").strip()
        nodes[obj.object_id] = (parent, obj.name, obj.is_folder, obj.is_google_native, obj.size_bytes)
        if obj.is_folder and parent == account.root_folder_id:
            top_level_folders[obj.object_id] = obj.name

    def top_level_folder_id(object_id: str) -> str | None:
        current = object_id
        seen: set[str] = set()
        while current and current not in seen:
            seen.add(current)
            node = nodes.get(current)
            if node is None:
                return None
            parent, _, _, _, _ = node
            if parent == account.root_folder_id:
                return current
            current = parent
        return None

    totals: dict[str, int] = {fid: 0 for fid in top_level_folders}
    mapped: dict[str, int] = {fid: 0 for fid in top_level_folders}

    for oid, (_, _, is_folder, is_google_native, size_bytes) in nodes.items():
        if is_folder:
            continue
        if is_google_native or size_bytes <= 0:
            continue
        top_id = top_level_folder_id(oid)
        if not top_id:
            continue
        totals[top_id] = totals.get(top_id, 0) + 1
        if oid in mapped_ids:
            mapped[top_id] = mapped.get(top_id, 0) + 1

    out_rows: list[dict[str, str]] = []
    for fid, name in sorted(top_level_folders.items(), key=lambda kv: kv[1].lower()):
        total = totals.get(fid, 0)
        done = mapped.get(fid, 0)
        remaining = max(0, total - done)
        status = "done" if total > 0 and remaining == 0 else ("not_started" if done == 0 else "in_progress")
        out_rows.append(
            {
                "top_folder_id": fid,
                "top_folder_name": name,
                "total_files": str(total),
                "mapped_files": str(done),
                "remaining_files": str(remaining),
                "status": status,
            }
        )
    return out_rows


def pick_next_top_level_folder(rows: list[dict[str, str]]) -> dict[str, str] | None:
    candidates = [r for r in rows if int(r.get("remaining_files", "0") or "0") > 0]
    if not candidates:
        return None
    # Prefer folders with the largest remainder; tie-break by name for deterministic runs.
    candidates.sort(key=lambda r: (-int(r["remaining_files"]), r["top_folder_name"].lower()))
    return candidates[0]


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    account = load_single_account_from_env()
    cfg = load_real_integration_config()
    source = GoogleDriveApiClient(cfg)
    mapped_ids = _load_mapped_ids(Path(args.mapping), account.account_id)
    out_rows = build_top_level_batch_rows(
        account=account,
        source=source,
        mapped_ids=mapped_ids,
        progress_hook=lambda m: print(f"[progress] {m}", flush=True),
    )

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "top_folder_id",
                "top_folder_name",
                "total_files",
                "mapped_files",
                "remaining_files",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(out_rows)

    print(f"top-level batch plan written: {out_path} rows={len(out_rows)}")
    next_row = pick_next_top_level_folder(out_rows)
    if next_row:
        print(
            "next_top_folder: "
            f"{next_row['top_folder_name']} ({next_row['top_folder_id']}) "
            f"remaining={next_row['remaining_files']}"
        )
    else:
        print("next_top_folder: none (all done)")
    print(
        "Tip: run one batch via "
        "`python -m migration.cli --drive-root-folder-id <top_folder_id> --no-folder-bootstrap`"
    )


if __name__ == "__main__":
    main()
