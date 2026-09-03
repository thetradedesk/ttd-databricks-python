"""API handler for the /data/advertiser endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from ttd_databricks_python.ttd_databricks.constants import TTD_DATABRICKS_SDK_ORIGIN_ID
from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext
from ttd_databricks_python.ttd_databricks.handlers._common import (
    ServerResponseAttr,
    extract_failed_lines_from_error,
    extract_response_data,
)
from ttd_databricks_python.ttd_databricks.handlers._common import (
    collect_item_level_raw_pii_ids as collect_raw_pii_ids_per_row,
)
from ttd_databricks_python.ttd_databricks.id_types import normalize_id_type

if TYPE_CHECKING:
    from ttd_data import DataClient
    from ttd_data.models import AdvertiserDataItem
    from ttd_data.uid2 import UID2Resolution

__all__ = ["build_items", "call_api", "collect_raw_pii_ids_per_row"]


def build_items(items_data: list[dict[str, Any]]) -> list[AdvertiserDataItem]:
    """Convert list of row dicts to AdvertiserDataItem SDK objects."""
    from ttd_data.models import AdvertiserData, AdvertiserDataItem

    from ttd_databricks_python.ttd_databricks.schemas.advertiser import DATA_OPTIONAL_FIELDS, ITEM_OPTIONAL_FIELDS

    items = []
    for d in items_data:
        adv_data_kwargs = {"name": d["segment_name"]}
        for field in DATA_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                adv_data_kwargs[field] = d[field]

        adv_item_kwargs = {
            normalize_id_type(d["id_type"]): d["id_value"],
            "data": [AdvertiserData(**adv_data_kwargs)],
        }
        for field in ITEM_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                adv_item_kwargs[field] = d[field]

        items.append(AdvertiserDataItem(**adv_item_kwargs))
    return items


def call_api(
    client: DataClient,
    context: AdvertiserContext,
    items: list[AdvertiserDataItem],
    data_load_trace_id: Optional[str] = None,
) -> tuple[list[Any], dict[str, UID2Resolution]]:
    """Call ingest_advertiser_data. Returns (failed_lines, identity_resolutions).

    Raises AdvertiserDataServerResponseError on 400 responses without failed_lines.
    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.errors import AdvertiserDataServerResponseError
    from ttd_data.models import DataOrigin, DataOriginType
    from ttd_data.types import UNSET

    sdk_origin = DataOrigin(id=TTD_DATABRICKS_SDK_ORIGIN_ID, type=DataOriginType.INTEGRATION)
    data_origins = (context.data_origins or []) + [sdk_origin]

    try:
        response = client.advertiser.ingest_advertiser_data(
            advertiser_id=context.advertiser_id,
            data_provider_id=context.data_provider_id if context.data_provider_id is not None else UNSET,
            items=items,
            data_load_trace_id=data_load_trace_id if data_load_trace_id is not None else UNSET,
            data_origins=data_origins,
            server_url=context.base_url_override,
        )
        return extract_response_data(response, ServerResponseAttr.ADVERTISER_DATA)
    except AdvertiserDataServerResponseError as exc:
        failed_lines = extract_failed_lines_from_error(exc)
        if not failed_lines:
            raise
        return failed_lines, {}
