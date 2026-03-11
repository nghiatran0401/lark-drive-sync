from __future__ import annotations

import argparse
import time

from .config import load_dotenv_if_present, load_real_integration_config
from .real_adapters import LarkApiClient


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Wipe all contents inside one or more Lark root folders.")
    parser.add_argument("--root", action="append", required=True, help="Lark root folder token (repeatable)")
    parser.add_argument("--max-passes", type=int, default=12, help="Max wipe passes per root")
    parser.add_argument("--sleep-seconds", type=float, default=2.0, help="Sleep between passes")
    return parser.parse_args()


def _child_token(item: dict) -> str:
    return (
        (item.get("token") or "")
        or (item.get("obj_token") or "")
        or (item.get("file_token") or "")
        or (item.get("node_token") or "")
    ).strip()


def _child_type(item: dict) -> str:
    return (item.get("type") or item.get("obj_type") or item.get("node_type") or "file").strip().lower() or "file"


def _is_folder(item: dict) -> bool:
    return "folder" in _child_type(item)


def _delete_folder_tree(client: LarkApiClient, folder_token: str) -> int:
    # Postorder traversal: delete files first, then folders.
    deleted = 0
    stack: list[tuple[str, bool]] = [(folder_token, False)]
    postorder_folders: list[str] = []
    file_nodes: list[tuple[str, str]] = []

    while stack:
        token, expanded = stack.pop()
        if expanded:
            postorder_folders.append(token)
            continue
        stack.append((token, True))
        for child in client.list_folder_children(token):
            ctoken = _child_token(child)
            if not ctoken:
                continue
            ctype = _child_type(child)
            if _is_folder(child):
                stack.append((ctoken, False))
            else:
                file_nodes.append((ctoken, ctype))

    for token, ctype in file_nodes:
        if client.delete_drive_node(token, node_type=ctype) is None:
            pass
        deleted += 1

    for token in postorder_folders:
        if token == folder_token:
            continue
        if client.delete_drive_node(token, node_type="folder") is None:
            pass
        deleted += 1

    return deleted


def _wipe_root_once(client: LarkApiClient, root_token: str) -> tuple[int, int]:
    children = client.list_folder_children(root_token)
    deleted_ops = 0
    for child in children:
        token = _child_token(child)
        if not token:
            continue
        ctype = _child_type(child)
        if _is_folder(child):
            deleted_ops += _delete_folder_tree(client, token)
            client.delete_drive_node(token, node_type="folder")
            deleted_ops += 1
        else:
            client.delete_drive_node(token, node_type=ctype)
            deleted_ops += 1
    remaining = len(client.list_folder_children(root_token))
    return deleted_ops, remaining


def main() -> None:
    load_dotenv_if_present()
    args = parse_args()
    cfg = load_real_integration_config()
    client = LarkApiClient(cfg)

    for root in args.root:
        print(f"[progress] wipe start root={root}", flush=True)
        for i in range(1, max(1, args.max_passes) + 1):
            before = len(client.list_folder_children(root))
            print(f"[progress] root={root} pass={i} top_children_before={before}", flush=True)
            if before == 0:
                break
            deleted_ops, after = _wipe_root_once(client, root)
            print(
                f"[progress] root={root} pass={i} deleted_ops={deleted_ops} top_children_after={after}",
                flush=True,
            )
            if after == 0:
                break
            time.sleep(max(0.0, args.sleep_seconds))
        final = len(client.list_folder_children(root))
        print(f"[progress] wipe done root={root} top_children_final={final}", flush=True)


if __name__ == "__main__":
    main()
