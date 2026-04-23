"""API handler for the /providerapi/offlineconversion endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

from ttd_databricks_python.ttd_databricks.constants import TTD_DATABRICKS_SDK_ORIGIN_ID
from ttd_databricks_python.ttd_databricks.contexts import OfflineConversionContext

if TYPE_CHECKING:
    from ttd_data import DataClient
    from ttd_data.models import OfflineConversionDataItem

# Maps user_ids[].type → string type code used in UserIdArray.
# Keys are uppercased so that lookup is case-insensitive.
_USER_ID_TYPE_CODE: dict[str, str] = {
    "TDID": "0",
    "DAID": "1",
    "UID2": "2",
    "UID2TOKEN": "3",
    "EUID": "4",
    "EUIDTOKEN": "5",
    "RAMPID": "6",
}


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
        if raw_user_ids:
            kwargs["user_id_array"] = [
                [_USER_ID_TYPE_CODE[user_id["type"].upper()], user_id["id"]] for user_id in raw_user_ids
            ]

        for field in ITEM_OPTIONAL_FIELDS:
            value = row.get(field)
            if value is not None:
                kwargs[field] = value

        raw_line_items = row.get("line_items")
        if raw_line_items:
            kwargs["line_items"] = [
                RealTimeConversionEventLineItem(
                    **{k: v for k, v in (li if isinstance(li, dict) else li.asDict()).items() if v is not None}
                )
                for li in raw_line_items
            ]

        raw_privacy_settings = row.get("privacy_settings")
        if raw_privacy_settings:
            kwargs["privacy_settings"] = [
                RealTimeConversionEventsPrivacySetting(
                    **{k: v for k, v in (ps if isinstance(ps, dict) else ps.asDict()).items() if v is not None}
                )
                for ps in raw_privacy_settings
            ]

        items.append(OfflineConversionDataItem(**kwargs))
    return items


def call_api(
    client: DataClient,
    context: OfflineConversionContext,
    items: list[OfflineConversionDataItem],
    api_token: str,
    data_load_trace_id: Optional[str] = None,
) -> list[Any]:
    """Call ingest_offline_conversion_data. Returns failed_lines (may be empty).

    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.models import DataOrigin, DataOriginType
    from ttd_data.types import UNSET

    sdk_origin = DataOrigin(id=TTD_DATABRICKS_SDK_ORIGIN_ID, type=DataOriginType.INTEGRATION)
    data_origins = (context.data_origins or []) + [sdk_origin]

    has_user_id_array = any(item.user_id_array is not UNSET and item.user_id_array is not None for item in items)

    failed_lines: list[Any] = []
    response = client.offline_conversion.ingest_offline_conversion_data(
        ttd_auth=api_token,
        data_provider_id=context.data_provider_id,
        user_id_array_metadata_format=["type", "id"] if has_user_id_array else UNSET,
        items=items,
        data_load_trace_id=data_load_trace_id if data_load_trace_id is not None else UNSET,
        data_origins=data_origins,
        server_url=context.base_url_override,
    )
    server_response = response.offline_conversion_data_server_response
    if server_response is not None:
        fl = server_response.failed_lines
        if fl is not UNSET and fl is not None:
            failed_lines = cast(list[Any], fl)
    return failed_lines
