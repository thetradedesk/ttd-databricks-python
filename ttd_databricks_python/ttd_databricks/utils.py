"""Shared utility functions for the TTD Databricks SDK."""

from __future__ import annotations

import re
from typing import Optional


def extract_item_number(message: Optional[str]) -> Optional[int]:
    """Extract 1-based item number from an API error message (e.g. 'Invalid DAID, item #2' → 2)."""
    if not message:
        return None
    match = re.search(r"item #(\d+)", message, re.IGNORECASE)
    return int(match.group(1)) if match else None
