from __future__ import annotations

import argparse
import asyncio
import os
from dataclasses import replace
from pathlib import Path

from .config import (
    load_dotenv_if_present,
    load_single_account_from_env,
    load_single_lark_root_folder_from_env,
    load_real_integration_config,
)
from .plan_top_level_batches import build_top_level_batch_rows, pick_next_top_level_folder, _load_mapped_ids
from .paths import default_report_file
from .real_adapters import AuthTokenError, GoogleDriveApiClient, LarkApiClient
from .simple_sync import FailedCsvWriter, MappingCsvWriter, SimpleSyncEngine


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Drive to Lark simple sync CLI")
    parser.add_argument(
        "--concurrency",
        type=int,
        default=int(os.getenv("SIMPLE_SYNC_CONCURRENCY", "8")),
        help="Simple-sync file upload concurrency",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=int(os.getenv("SIMPLE_SYNC_CHUNK_SIZE", str(4 * 1024 * 1024))),
        help="Simple-sync chunk size in bytes",
    )
    parser.add_argument(
        "--mapping-out",
        default=os.getenv("SIMPLE_SYNC_MAPPING_OUT", default_report_file("mappings.csv")),
        help="Mapping CSV output path",
    )
    parser.add_argument(
        "--failed-out",
        default=os.getenv("SIMPLE_SYNC_FAILED_OUT", default_report_file("failed_items.csv")),
        help="Failed/skipped items CSV output path",
    )
    parser.add_argument(
        "--drive-root-folder-id",
        default="",
        help="Optional override for DRIVE_ROOT_FOLDER_ID (useful for top-level batch runs)",
    )
    parser.add_argument(
        "--no-folder-bootstrap",
        action="store_true",
        default=False,
        help="Disable preloading folder mappings from CSV for safer destination-root switches",
    )
    parser.add_argument(
        "--auto-next-top-folder",
        action="store_true",
        default=False,
        help="Automatically pick next unfinished top-level Google folder from current mappings",
    )
    return parser.parse_args()


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    account = load_single_account_from_env()
    cfg = load_real_integration_config()
    if args.auto_next_top_folder and args.drive_root_folder_id.strip():
        raise ValueError("Use either --auto-next-top-folder or --drive-root-folder-id, not both.")
    if args.auto_next_top_folder:
        source_for_plan = GoogleDriveApiClient(cfg)
        mapped_ids = _load_mapped_ids(Path(args.mapping_out), account.account_id)
        rows = build_top_level_batch_rows(
            account=account,
            source=source_for_plan,
            mapped_ids=mapped_ids,
            progress_hook=lambda m: print(f"[progress] auto-plan: {m}", flush=True),
        )
        next_row = pick_next_top_level_folder(rows)
        if not next_row:
            print("[progress] auto-plan: all top-level folders completed. nothing to sync.", flush=True)
            return
        account = replace(account, root_folder_id=next_row["top_folder_id"])
        print(
            "[progress] auto-plan: selected "
            f"{next_row['top_folder_name']} ({account.root_folder_id}) "
            f"remaining={next_row['remaining_files']}",
            flush=True,
        )
    elif args.drive_root_folder_id.strip():
        account = replace(account, root_folder_id=args.drive_root_folder_id.strip())
        print(f"[progress] override: drive_root_folder_id={account.root_folder_id}", flush=True)
    lark_root_folder_id = load_single_lark_root_folder_from_env()
    source = GoogleDriveApiClient(cfg)
    lark = LarkApiClient(cfg)

    mapping_writer = MappingCsvWriter(args.mapping_out)
    failed_writer = FailedCsvWriter(args.failed_out)

    def progress(message: str) -> None:
        print(f"[progress] {message}", flush=True)

    engine = SimpleSyncEngine(
        drive_client=source,
        lark_client=lark,
        concurrency=args.concurrency,
        chunk_size=args.chunk_size,
        mapping_writer=mapping_writer,
        failed_writer=failed_writer,
        progress_hook=progress,
        bootstrap_folder_mappings=(
            False if args.auto_next_top_folder else (not args.no_folder_bootstrap)
        ),
    )
    try:
        stats = asyncio.run(engine.sync_account(account, lark_root_folder_id))
    except AuthTokenError as exc:
        print(f"Authentication error: {exc}")
        raise SystemExit(2) from exc
    print(
        "Simple sync completed "
        f"discovered={stats.discovered} folders_created={stats.folders_created} "
        f"files_uploaded={stats.files_uploaded} "
        f"bytes_uploaded_mb={stats.bytes_uploaded / (1024 * 1024):.1f} "
        f"files_failed={stats.files_failed} "
        f"files_skipped_google_native={stats.files_skipped_google_native} "
        f"files_skipped_zero_byte={stats.files_skipped_zero_byte} "
        f"mappings={args.mapping_out}"
    )


if __name__ == "__main__":
    main()

