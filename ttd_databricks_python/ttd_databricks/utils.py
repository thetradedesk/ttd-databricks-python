"""Shared utility functions for the TTD Databricks SDK."""
import re


def extract_item_number(message: str) -> int | None:
    """Extract 1-based item number from an API error message (e.g. 'Invalid DAID, item #2' → 2)."""
    if not message:
        return None
    match = re.search(r"item #(\d+)", message, re.IGNORECASE)
    return int(match.group(1)) if match else None
