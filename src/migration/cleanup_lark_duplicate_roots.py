from __future__ import annotations

import argparse
import csv
from collections import deque
from pathlib import Path

from .config import load_dotenv_if_present, load_real_integration_config
from .paths import default_report_file
from .real_adapters import LarkApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Safely clean duplicate top-level folders across two Lark roots."
    )
    parser.add_argument("--root-a", required=True, help="Older/first Lark root folder token")
    parser.add_argument("--root-b", required=True, help="Newer/second Lark root folder token")
    parser.add_argument(
        "--out",
        default=default_report_file("lark_duplicate_cleanup_report.csv"),
        help="Output dry-run/apply report CSV",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Apply safe deletions (default: dry-run only)",
    )
    return parser.parse_args()


def _child_token(item: dict) -> str:
    return (
        (item.get("token") or "")
        or (item.get("obj_token") or "")
        or (item.get("file_token") or "")
        or (item.get("node_token") or "")
    ).strip()


def _child_name(item: dict) -> str:
    return (item.get("name") or item.get("title") or "").strip()


def _child_type(item: dict) -> str:
    t = (item.get("type") or item.get("obj_type") or item.get("node_type") or "").strip().lower()
    if t:
        return t
    if item.get("is_folder") is True:
        return "folder"
    return "file"


def _is_folder(item: dict) -> bool:
    t = _child_type(item)
    return "folder" in t


def _list_top_level_folders(client: LarkApiClient, root_token: str) -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for child in client.list_folder_children(root_token):
        if not _is_folder(child):
            continue
        name = _child_name(child)
        token = _child_token(child)
        if not name or not token:
            continue
        out.setdefault(name, []).append(token)
    return out


def _count_subtree_files(client: LarkApiClient, folder_token: str) -> int:
    count = 0
    queue: deque[str] = deque([folder_token])
    while queue:
        current = queue.popleft()
        children = client.list_folder_children(current)
        for child in children:
            token = _child_token(child)
            if _is_folder(child):
                if token:
                    queue.append(token)
            else:
                count += 1
    return count


def _collect_subfolders_bottom_up(client: LarkApiClient, root_folder_token: str) -> list[str]:
    postorder: list[str] = []
    stack: list[tuple[str, bool]] = [(root_folder_token, False)]
    while stack:
        token, expanded = stack.pop()
        if expanded:
            postorder.append(token)
            continue
        stack.append((token, True))
        children = client.list_folder_children(token)
        for child in children:
            child_token = _child_token(child)
            if _is_folder(child) and child_token:
                stack.append((child_token, False))
            elif child_token:
                raise RuntimeError(f"Folder tree is not empty-file tree; found file token={child_token}")
    return postorder


def _delete_empty_folder_tree(client: LarkApiClient, root_folder_token: str) -> int:
    order = _collect_subfolders_bottom_up(client, root_folder_token)
    deleted = 0
    for token in order:
        client.delete_drive_node(token, node_type="folder")
        deleted += 1
    return deleted


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    cfg = load_real_integration_config()
    client = LarkApiClient(cfg)

    folders_a = _list_top_level_folders(client, args.root_a)
    folders_b = _list_top_level_folders(client, args.root_b)

    duplicates = sorted(set(folders_a).intersection(folders_b))
    report_rows: list[dict[str, str]] = []
    safe_delete_tokens: list[str] = []

    print(
        f"[progress] top-level folders: root_a={len(folders_a)} root_b={len(folders_b)} duplicates={len(duplicates)}",
        flush=True,
    )

    for idx, name in enumerate(duplicates, start=1):
        tokens_a = folders_a[name]
        tokens_b = folders_b[name]
        counts_a = [(tok, _count_subtree_files(client, tok)) for tok in tokens_a]
        counts_b = [(tok, _count_subtree_files(client, tok)) for tok in tokens_b]
        files_a = sum(c for _, c in counts_a)
        files_b = sum(c for _, c in counts_b)
        action = "keep_both"
        delete_side = ""
        delete_tokens: list[str] = []
        reason = "both sides have files or both empty"
        if files_a == 0 and files_b > 0:
            action = "delete_a"
            delete_side = "a"
            delete_tokens = [tok for tok, c in counts_a if c == 0]
            reason = "duplicate folder: root_a subtree has zero files"
        elif files_b == 0 and files_a > 0:
            action = "delete_b"
            delete_side = "b"
            delete_tokens = [tok for tok, c in counts_b if c == 0]
            reason = "duplicate folder: root_b subtree has zero files"
        if delete_tokens:
            safe_delete_tokens.extend(delete_tokens)
        report_rows.append(
            {
                "folder_name": name,
                "root_a_token": "|".join(tokens_a),
                "root_b_token": "|".join(tokens_b),
                "root_a_file_count": str(files_a),
                "root_b_file_count": str(files_b),
                "action": action,
                "delete_side": delete_side,
                "delete_token": "|".join(delete_tokens),
                "reason": reason,
            }
        )
        if idx == 1 or idx % 10 == 0:
            print(f"[progress] assessed_duplicate_folders={idx}/{len(duplicates)}", flush=True)

    deleted = 0
    failed = 0
    if args.apply and safe_delete_tokens:
        for token in safe_delete_tokens:
            # Re-check invariant immediately before delete.
            try:
                files_now = _count_subtree_files(client, token)
                if files_now != 0:
                    failed += 1
                    continue
                deleted += _delete_empty_folder_tree(client, token)
            except Exception:  # noqa: BLE001
                failed += 1

    out_path = Path(args.out)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "folder_name",
                "root_a_token",
                "root_b_token",
                "root_a_file_count",
                "root_b_file_count",
                "action",
                "delete_side",
                "delete_token",
                "reason",
            ],
        )
        writer.writeheader()
        writer.writerows(report_rows)

    print(f"report={out_path}")
    print(f"duplicates={len(duplicates)} safe_delete_candidates={len(safe_delete_tokens)}")
    if args.apply:
        print(f"apply_result deleted={deleted} failed={failed}")
    else:
        print("dry_run_only=true")


if __name__ == "__main__":
    main()
