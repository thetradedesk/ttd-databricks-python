"""Shared helpers used by per-endpoint handlers."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, cast

from ttd_databricks_python.ttd_databricks.id_types import is_raw_pii_id_type

if TYPE_CHECKING:
    from ttd_data.uid2 import UID2Resolution


def collect_item_level_raw_pii_ids(items_data: list[dict[str, Any]]) -> list[list[str]]:
    """Per-row raw PII identifiers for item-level-id endpoints."""
    return [[d["id_value"]] if is_raw_pii_id_type(d["id_type"]) else [] for d in items_data]


def extract_response_data(response: Any, server_response_attr: str) -> tuple[list[Any], dict[str, UID2Resolution]]:
    """Pull `(failed_lines, identity_resolutions)` from a successful SDK response.

    Raises AttributeError on missing `server_response_attr` to surface handler typos.
    """
    from ttd_data.types import UNSET

    identity_resolutions = getattr(response, "identity_resolutions", {}) or {}
    failed_lines: list[Any] = []
    server_response = getattr(response, server_response_attr)
    if server_response is not None:
        fl = server_response.failed_lines
        if fl is not UNSET and fl is not None:
            failed_lines = cast(list[Any], fl)
    return failed_lines, identity_resolutions


def extract_failed_lines_from_error(exc: Any) -> list[Any]:
    """Pull `failed_lines` from a 400 response error; `[]` means unrecoverable — caller should re-raise."""
    from ttd_data.types import UNSET

    fl = exc.data.failed_lines
    if fl is UNSET or fl is None or not fl:
        return []
    return cast(list[Any], fl)
