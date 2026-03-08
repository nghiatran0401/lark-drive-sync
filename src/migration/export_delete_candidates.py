from __future__ import annotations

import argparse
import csv
import os
import unicodedata
from collections import defaultdict, deque
from pathlib import Path

from .config import load_dotenv_if_present, load_real_integration_config, load_single_account_from_env
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
        help="Output CSV for excluded IDs (currently unresolved failures only)",
    )
    parser.add_argument(
        "--protect-folder-name",
        action="append",
        default=[],
        help="Folder name to protect (exclude folder and all descendants). Can repeat.",
    )
    parser.add_argument(
        "--protect-folder-names-file",
        default="reports/protected_folder_names.txt",
        help="Optional text file with one protected folder name per line.",
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


def _write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _normalize_name(value: str) -> str:
    return " ".join(unicodedata.normalize("NFC", value).strip().casefold().split())


def _load_protected_names(args: argparse.Namespace) -> set[str]:
    names = {_normalize_name(n) for n in (args.protect_folder_name or []) if n.strip()}
    file_path = Path(args.protect_folder_names_file)
    if file_path.exists():
        for raw in file_path.read_text(encoding="utf-8").splitlines():
            name = raw.strip()
            if not name or name.startswith("#"):
                continue
            names.add(_normalize_name(name))
    return names


def _collect_protected_subtree_ids(protected_names: set[str]) -> tuple[set[str], int]:
    if not protected_names:
        return set(), 0

    cfg = load_real_integration_config()
    account = load_single_account_from_env()
    source = GoogleDriveApiClient(cfg)

    objects: dict[str, tuple[str, bool]] = {}
    children: dict[str, list[str]] = defaultdict(list)
    scanned = 0
    for obj in source.list_objects_recursive(account):
        scanned += 1
        if scanned == 1 or scanned % 2000 == 0:
            print(f"[progress] protected-scan: scanned={scanned}", flush=True)
        objects[obj.object_id] = (obj.name, obj.is_folder)
        parent = (obj.parent_id or "").strip()
        if parent:
            children[parent].append(obj.object_id)
    print(f"[progress] protected-scan complete: scanned={scanned}", flush=True)

    roots: list[str] = []
    for obj_id, (name, is_folder) in objects.items():
        if not is_folder:
            continue
        if _normalize_name(name) in protected_names:
            roots.append(obj_id)

    protected_ids: set[str] = set()
    q: deque[str] = deque(roots)
    while q:
        current = q.popleft()
        if current in protected_ids:
            continue
        protected_ids.add(current)
        for child in children.get(current, []):
            q.append(child)
    return protected_ids, len(roots)


def main() -> None:
    args = parse_args()
    load_dotenv_if_present()

    mapping_rows = _load_mapping_rows(Path(args.mapping))
    unresolved_ids = _load_unresolved_ids(Path(args.unresolved))
    protected_name_set = _load_protected_names(args)

    protected_ids = set(unresolved_ids)
    exclusion_rows: list[dict[str, str]] = [
        {"google_object_id": gid, "reason": "unresolved_failed_item"} for gid in sorted(unresolved_ids)
    ]

    protected_subtree_ids, protected_root_count = _collect_protected_subtree_ids(protected_name_set)
    if protected_subtree_ids:
        for gid in sorted(protected_subtree_ids):
            if gid not in protected_ids:
                exclusion_rows.append({"google_object_id": gid, "reason": "manual_protected_folder"})
        protected_ids.update(protected_subtree_ids)

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

    print(f"delete candidates written: {args.out_candidates} ({len(candidates)} rows)")
    print(f"delete exclusions written: {args.out_exclusions} ({len(exclusion_rows)} rows)")
    if protected_name_set:
        print(
            f"manual protected names matched={protected_root_count} "
            f"protected_subtree_ids={len(protected_subtree_ids)}"
        )


if __name__ == "__main__":
    main()
