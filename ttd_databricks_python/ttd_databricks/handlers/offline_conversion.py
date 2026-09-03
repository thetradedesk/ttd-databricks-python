"""API handler for the /providerapi/offlineconversion endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from ttd_databricks_python.ttd_databricks.constants import TTD_DATABRICKS_SDK_ORIGIN_ID
from ttd_databricks_python.ttd_databricks.contexts import OfflineConversionContext
from ttd_databricks_python.ttd_databricks.handlers._common import (
    ServerResponseAttr,
    extract_failed_lines_from_error,
    extract_response_data,
)
from ttd_databricks_python.ttd_databricks.id_types import RAW_PII_ID_TYPES

if TYPE_CHECKING:
    from ttd_data import DataClient
    from ttd_data.models import OfflineConversionDataItem
    from ttd_data.uid2 import UID2Resolution

__all__ = ["build_items", "call_api", "collect_raw_pii_ids_per_row"]


def _user_id_type(type_name: str) -> Any:
    """Map a user-facing user_ids[].type name to a `ttd_data.uid2.UserIdType` member.

    The enum subclasses `str` for wire compatibility with `List[List[str]]`.
    """
    from ttd_data import UserIdType

    mapping = {
        "TDID": UserIdType.TDID,
        "DAID": UserIdType.DAID,
        "UID2": UserIdType.UID2,
        "UID2TOKEN": UserIdType.UID2_TOKEN,
        "EUID": UserIdType.EUID,
        "EUIDTOKEN": UserIdType.EUID_TOKEN,
        "RAMPID": UserIdType.RAMP_ID,
        "EMAIL": UserIdType.EMAIL,
        "PHONE": UserIdType.PHONE,
        "HASHEDEMAIL": UserIdType.HASHED_EMAIL,
        "HASHEDPHONE": UserIdType.HASHED_PHONE,
    }
    key = type_name.upper()
    if key not in mapping:
        valid = ", ".join(sorted(mapping))
        raise ValueError(f"Unknown user_ids type {type_name!r}. Must be one of: {valid}.")
    return mapping[key]


def build_items(items_data: list[dict[str, Any]]) -> list[OfflineConversionDataItem]:
    """Convert list of row dicts to OfflineConversionDataItem SDK objects."""
    from ttd_data.models import (
        OfflineConversionDataItem,
        RealTimeConversionEventLineItem,
        RealTimeConversionEventsPrivacySetting,
    )

    from ttd_databricks_python.ttd_databricks.schemas.offline_conversion import ITEM_OPTIONAL_FIELDS

    items = []
    for row in items_data:
        kwargs: dict[str, Any] = {
            "tracking_tag_id": row["tracking_tag_id"],
            "timestamp_utc": row["timestamp_utc"],
        }

        raw_user_ids = row.get("user_ids")
        if raw_user_ids is not None and len(raw_user_ids) > 0:
            kwargs["user_id_array"] = [[_user_id_type(user_id["type"]), user_id["id"]] for user_id in raw_user_ids]

        for field in ITEM_OPTIONAL_FIELDS:
            value = row.get(field)
            if value is not None:
                kwargs[field] = value

        raw_line_items = row.get("line_items")
        if raw_line_items is not None and len(raw_line_items) > 0:
            kwargs["line_items"] = [
                RealTimeConversionEventLineItem(
                    **{k: v for k, v in (li if isinstance(li, dict) else li.asDict()).items() if v is not None}
                )
                for li in raw_line_items
            ]

        raw_privacy_settings = row.get("privacy_settings")
        if raw_privacy_settings is not None and len(raw_privacy_settings) > 0:
            kwargs["privacy_settings"] = [
                RealTimeConversionEventsPrivacySetting(
                    **{k: v for k, v in (ps if isinstance(ps, dict) else ps.asDict()).items() if v is not None}
                )
                for ps in raw_privacy_settings
            ]

        items.append(OfflineConversionDataItem(**kwargs))
    return items


def collect_raw_pii_ids_per_row(items_data: list[dict[str, Any]]) -> list[list[str]]:
    """Per-row raw PII identifiers from `user_ids`, in submission order.

    Output aligns positionally with the row's `uid2_resolutions` array.
    """
    out: list[list[str]] = []
    for row in items_data:
        raw_user_ids = row.get("user_ids")
        if raw_user_ids is None:
            raw_user_ids = []
        out.append(
            [entry["id"] for entry in raw_user_ids if entry["type"] and entry["type"].upper() in RAW_PII_ID_TYPES]
        )
    return out


def call_api(
    client: DataClient,
    context: OfflineConversionContext,
    items: list[OfflineConversionDataItem],
    data_load_trace_id: Optional[str] = None,
) -> tuple[list[Any], dict[str, UID2Resolution]]:
    """Call ingest_offline_conversion_data. Returns (failed_lines, identity_resolutions).

    Raises OfflineConversionDataServerResponseError on 400 responses without failed_lines.
    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.errors import OfflineConversionDataServerResponseError
    from ttd_data.models import DataOrigin, DataOriginType
    from ttd_data.types import UNSET

    sdk_origin = DataOrigin(id=TTD_DATABRICKS_SDK_ORIGIN_ID, type=DataOriginType.INTEGRATION)
    data_origins = (context.data_origins or []) + [sdk_origin]

    has_user_id_array = any(item.user_id_array is not UNSET and item.user_id_array is not None for item in items)

    try:
        response = client.offline_conversion.ingest_offline_conversion_data(
            data_provider_id=context.data_provider_id,
            user_id_array_metadata_format=["type", "id"] if has_user_id_array else UNSET,
            items=items,
            data_load_trace_id=data_load_trace_id if data_load_trace_id is not None else UNSET,
            data_origins=data_origins,
            server_url=context.base_url_override,
        )
        return extract_response_data(response, ServerResponseAttr.OFFLINE_CONVERSION_DATA)
    except OfflineConversionDataServerResponseError as exc:
        failed_lines = extract_failed_lines_from_error(exc)
        if not failed_lines:
            raise
        return failed_lines, {}
