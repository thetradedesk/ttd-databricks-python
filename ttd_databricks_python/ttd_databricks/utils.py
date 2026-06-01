"""Shared utility functions for the TTD Databricks SDK."""

from __future__ import annotations

from typing import Any, Optional

from ttd_databricks_python.ttd_databricks.schemas import UID2_RESOLUTIONS_COLUMN


def empty_resolution_value() -> dict[str, list[Any]]:
    """Return a fresh empty `uid2_resolutions` value for failure paths."""
    return {UID2_RESOLUTIONS_COLUMN: []}


def parse_failed_lines(failed_lines: list[Any], row_count: int) -> list[dict[str, Any]]:
    """Map API failed_lines to per-row result dicts with success, error_code, error_message.

    Rows with an item_number get their specific error.
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
        item_number = (
            int(line.item_number) if (line.item_number is not UNSET and line.item_number is not None) else None
        )
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


def _resolution_to_dict(resolution: Any, submitted_id: Optional[str]) -> dict[str, Any]:
    return {
        "submitted_id": submitted_id,
        "current_uid2": resolution.current_raw_uid,
        "previous_uid2": resolution.previous_raw_uid,
        "refresh_from": resolution.refresh_from,
        "unmapped_reason": resolution.unmapped_reason,
    }


def attach_resolutions(
    results: list[dict[str, Any]],
    raw_pii_ids_per_row: list[list[str]],
    identity_resolutions: dict[str, Any],
) -> None:
    """Merge per-row UID2 resolutions into each result dict as `uid2_resolutions: array<struct>`.

    `raw_pii_ids_per_row[i]` is the list of raw PII identifiers for row i (empty if none).
    Mutates `results` in place.
    """
    for result, raws in zip(results, raw_pii_ids_per_row, strict=True):
        entries: list[dict[str, Any]] = []
        for raw in raws:
            resolution = identity_resolutions.get(raw)
            if resolution is not None:
                entries.append(_resolution_to_dict(resolution, submitted_id=raw))
        result[UID2_RESOLUTIONS_COLUMN] = entries
