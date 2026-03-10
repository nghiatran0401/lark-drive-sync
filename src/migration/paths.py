from __future__ import annotations

import os
import re


def drive_profile_slug() -> str:
    raw = (os.getenv("DRIVE_PROFILE", "") or os.getenv("DRIVE_ACCOUNT_ID", "default-drive")).strip()
    slug = re.sub(r"[^a-zA-Z0-9._-]+", "-", raw).strip("-").lower()
    return slug or "default-drive"


def default_reports_dir() -> str:
    return f"reports/drives/{drive_profile_slug()}"


def default_report_file(filename: str) -> str:
    return f"{default_reports_dir()}/{filename}"
