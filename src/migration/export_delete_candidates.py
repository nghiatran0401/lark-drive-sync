from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict, deque
from pathlib import Path

from .config import load_dotenv_if_present, load_real_integration_config, load_single_account_from_env
from .models import DriveObject
from .real_adapters import GoogleDriveApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build safe Google delete candidates from migration reports."
    )
    parser.add_argument(
        "--mapping",
        default=os.getenv("SIMPLE_SYNC_MAPPING_OUT", "reports/mappings.csv"),
        help="Mappings CSV path",
    )
    parser.add_argument(
        "--unresolved",
        default="reports/unresolved_failed_items.csv",
        help="Unresolved failed items CSV path",
    )
    parser.add_argument(
        "--out-candidates",
        default="reports/delete_candidates.csv",
        help="Output CSV for safe delete candidates",
    )
    parser.add_argument(
        "--out-exclusions",
        default="reports/delete_exclusions.csv",
        help="Output CSV for excluded/protected IDs",
    )
    parser.add_argument(
        "--out-colored",
        default="reports/colored_folders.csv",
        help="Output CSV for colored folders discovered in Drive",
    )
    parser.add_argument(
        "--include-colored-protection",
        action="store_true",
        default=True,
        help="Protect colored folders and all descendants (default: enabled)",
    )
    parser.add_argument(
        "--no-colored-protection",
        dest="include_colored_protection",
        action="store_false",
        help="Disable colored-folder protection",
    )
    return parser.parse_args()


def _load_mapping_rows(path: Path) -> dict[str, dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"Mapping CSV not found: {path}")
    rows: dict[str, dict[str, str]] = {}
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = (row.get("google_object_id") or "").strip()
            if not gid:
                continue
            rows[gid] = {
                "account_id": (row.get("account_id") or "").strip(),
                "object_type": (row.get("object_type") or "").strip(),
                "google_object_id": gid,
                "google_url": (row.get("google_url") or "").strip(),
                "lark_object_id": (row.get("lark_object_id") or "").strip(),
                "lark_url": (row.get("lark_url") or "").strip(),
            }
    return rows


def _load_unresolved_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    ids: set[str] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            gid = (row.get("google_object_id") or "").strip()
            if gid:
                ids.add(gid)
    return ids


def _crawl_drive_tree(source: GoogleDriveApiClient) -> tuple[dict[str, DriveObject], dict[str, list[str]]]:
    account = load_single_account_from_env()
    objects: dict[str, DriveObject] = {}
    children: dict[str, list[str]] = defaultdict(list)
    for obj in source.list_objects_recursive(account):
        objects[obj.object_id] = obj
        parent = (obj.parent_id or "").strip()
        if parent:
            children[parent].append(obj.object_id)
    return objects, children


def _collect_colored_subtree(
    objects: dict[str, DriveObject],
    children: dict[str, list[str]],
) -> tuple[set[str], list[dict[str, str]]]:
    colored_roots = [obj for obj in objects.values() if obj.is_folder and (obj.folder_color_rgb or "").strip()]
    protected_ids: set[str] = set()
    q: deque[str] = deque()
    for obj in colored_roots:
        q.append(obj.object_id)
    while q:
        current = q.popleft()
        if current in protected_ids:
            continue
        protected_ids.add(current)
        for child in children.get(current, []):
            q.append(child)

    colored_rows: list[dict[str, str]] = []
    for obj in colored_roots:
        colored_rows.append(
            {
                "google_folder_id": obj.object_id,
                "google_folder_name": obj.name,
                "google_folder_color_rgb": (obj.folder_color_rgb or "").strip(),
                "google_folder_url": obj.web_view_link,
            }
        )
    colored_rows.sort(key=lambda r: (r["google_folder_name"].lower(), r["google_folder_id"]))
    return protected_ids, colored_rows


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    args = parse_args()
    load_dotenv_if_present()

    mapping_rows = _load_mapping_rows(Path(args.mapping))
    unresolved_ids = _load_unresolved_ids(Path(args.unresolved))

    protected_ids = set(unresolved_ids)
    exclusion_rows: list[dict[str, str]] = [
        {"google_object_id": gid, "reason": "unresolved_failed_item"} for gid in sorted(unresolved_ids)
    ]

    colored_rows: list[dict[str, str]] = []
    if args.include_colored_protection:
        cfg = load_real_integration_config()
        source = GoogleDriveApiClient(cfg)
        objects, children = _crawl_drive_tree(source)
        colored_protected_ids, colored_rows = _collect_colored_subtree(objects, children)
        for gid in sorted(colored_protected_ids):
            if gid not in protected_ids:
                exclusion_rows.append({"google_object_id": gid, "reason": "colored_folder_protected"})
        protected_ids.update(colored_protected_ids)

    candidates: list[dict[str, str]] = []
    for gid, row in mapping_rows.items():
        if gid in protected_ids:
            continue
        candidates.append(row)
    candidates.sort(key=lambda r: (r["object_type"], r["google_object_id"]))

    _write_csv(
        Path(args.out_candidates),
        ["account_id", "object_type", "google_object_id", "google_url", "lark_object_id", "lark_url"],
        candidates,
    )
    _write_csv(Path(args.out_exclusions), ["google_object_id", "reason"], exclusion_rows)
    _write_csv(
        Path(args.out_colored),
        ["google_folder_id", "google_folder_name", "google_folder_color_rgb", "google_folder_url"],
        colored_rows,
    )

    print(f"delete candidates written: {args.out_candidates} ({len(candidates)} rows)")
    print(f"delete exclusions written: {args.out_exclusions} ({len(exclusion_rows)} rows)")
    print(f"colored folders written: {args.out_colored} ({len(colored_rows)} rows)")


if __name__ == "__main__":
    main()
