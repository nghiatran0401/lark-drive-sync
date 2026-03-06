from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class AccountConfig:
    account_id: str
    root_folder_id: str
    credential_ref: str


@dataclass(frozen=True)
class DriveObject:
    account_id: str
    object_id: str
    parent_id: Optional[str]
    name: str
    mime_type: str
    checksum: Optional[str]
    size_bytes: int
    modified_time: datetime
    owner_principal: str
    web_view_link: str
    is_folder: bool
    is_google_native: bool
    folder_color_rgb: Optional[str] = None


@dataclass(frozen=True)
class TransferResult:
    object_id: str
    lark_object_id: str
    lark_url: str
    bytes_copied: int
    checksum: Optional[str]

