from __future__ import annotations

import argparse
import csv
import os
from pathlib import Path

from .config import load_dotenv_if_present, load_real_integration_config, load_single_account_from_env
from .real_adapters import GoogleDriveApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Report Google Drive folders that have colors.")
    parser.add_argument(
        "--out",
        default=os.getenv("COLORED_FOLDERS_OUT", "reports/colored_folders.csv"),
        help="CSV output path",
    )
    parser.add_argument(
        "--mapping",
        default=os.getenv("SIMPLE_SYNC_MAPPING_OUT", "reports/mappings.csv"),
        help="Mappings CSV path used to flag migrated folders",
    )
    return parser.parse_args()


def _load_folder_mapping(mapping_csv_path: Path) -> dict[str, tuple[str, str]]:
    if not mapping_csv_path.exists():
        return {}
    folder_map: dict[str, tuple[str, str]] = {}
    with mapping_csv_path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if (row.get("object_type") or "").strip().lower() != "folder":
                continue
            google_id = (row.get("google_object_id") or "").strip()
            lark_id = (row.get("lark_object_id") or "").strip()
            lark_url = (row.get("lark_url") or "").strip()
            if google_id:
                folder_map[google_id] = (lark_id, lark_url)
    return folder_map


def main() -> None:
    args = parse_args()
    load_dotenv_if_present()
    account = load_single_account_from_env()
    cfg = load_real_integration_config()
    source = GoogleDriveApiClient(cfg)

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    folder_mapping = _load_folder_mapping(Path(args.mapping))

    rows: list[dict[str, str]] = []
    for obj in source.list_objects_recursive(account):
        if not obj.is_folder:
            continue
        color_rgb = (obj.folder_color_rgb or "").strip()
        if not color_rgb:
            continue
        mapped = folder_mapping.get(obj.object_id)
        rows.append(
            {
                "google_folder_id": obj.object_id,
                "google_folder_name": obj.name,
                "google_folder_color_rgb": color_rgb,
                "google_folder_url": obj.web_view_link,
                "migrated_to_lark": "yes" if mapped else "no",
                "lark_folder_id": mapped[0] if mapped else "",
                "lark_folder_url": mapped[1] if mapped else "",
            }
        )

    rows.sort(key=lambda r: (r["google_folder_name"].lower(), r["google_folder_id"]))

    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "google_folder_id",
                "google_folder_name",
                "google_folder_color_rgb",
                "google_folder_url",
                "migrated_to_lark",
                "lark_folder_id",
                "lark_folder_url",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Colored folders report written: {output_path}")
    print(f"Total colored folders: {len(rows)}")
    print(f"Migrated colored folders: {sum(1 for r in rows if r['migrated_to_lark'] == 'yes')}")


if __name__ == "__main__":
    main()
