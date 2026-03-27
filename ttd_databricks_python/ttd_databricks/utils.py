"""Shared utility functions for the TTD Databricks SDK."""

from __future__ import annotations

import re
from typing import Any, Optional


def extract_item_number(message: Optional[str]) -> Optional[int]:
    """Extract 1-based item number from an API error message (e.g. 'Invalid DAID, item #2' → 2)."""
    if not message:
        return None
    match = re.search(r"item #(\d+)", message, re.IGNORECASE)
    return int(match.group(1)) if match else None


def parse_failed_lines(failed_lines: list[Any], row_count: int) -> list[dict[str, Any]]:
    """Map API failed_lines to per-row result dicts with success, error_code, error_message.

    Rows with a parseable item number get their specific error.
    Rows without one fall back to the unattributable error (if any).
    Rows with no error are marked as success.
    """
    from ttd_data.types import UNSET

    failed_item_mapping: dict[int, dict[str, Optional[str]]] = {}
    has_unattributable = False
    unattributable_error_code: Optional[str] = None
    unattributable_error_message: Optional[str] = None

    for line in failed_lines:
        message = line.message if line.message is not UNSET else None
        error_code = line.error_code.value if (line.error_code and line.error_code is not UNSET) else None
        item_number = extract_item_number(message)
        if item_number is not None:
            failed_item_mapping[item_number] = {"error_code": error_code, "error_message": message}
        else:
            has_unattributable = True
            # Last unattributable error wins — multiple unattributable errors are not accumulated.
            unattributable_error_code = error_code
            unattributable_error_message = message

    results: list[dict[str, Any]] = []
    for i in range(1, row_count + 1):
        if i in failed_item_mapping:
            err = failed_item_mapping[i]
            results.append({"success": False, "error_code": err["error_code"], "error_message": err["error_message"]})
        elif has_unattributable:
            results.append(
                {
                    "success": False,
                    "error_code": unattributable_error_code,
                    "error_message": unattributable_error_message,
                }
            )
        else:
            results.append({"success": True, "error_code": None, "error_message": None})

    return results
