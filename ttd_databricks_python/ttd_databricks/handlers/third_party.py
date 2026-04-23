"""API handler for the /data/thirdparty endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

from ttd_databricks_python.ttd_databricks.constants import TTD_DATABRICKS_SDK_ORIGIN_ID
from ttd_databricks_python.ttd_databricks.contexts import ThirdPartyContext
from ttd_databricks_python.ttd_databricks.id_types import normalize_id_type

if TYPE_CHECKING:
    from ttd_data import DataClient
    from ttd_data.models import ThirdPartyDataItem


def build_items(items_data: list[dict[str, Any]]) -> list[ThirdPartyDataItem]:
    """Convert list of row dicts to ThirdPartyDataItem SDK objects."""
    from ttd_data.models import ThirdPartyData, ThirdPartyDataItem

    from ttd_databricks_python.ttd_databricks.schemas.third_party import DATA_OPTIONAL_FIELDS, ITEM_OPTIONAL_FIELDS

    items = []
    for d in items_data:
        tp_data_kwargs = {"name": d["segment_name"]}
        for field in DATA_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                tp_data_kwargs[field] = d[field]

        tp_item_kwargs = {
            normalize_id_type(d["id_type"]): d["id_value"],
            "data": [ThirdPartyData(**tp_data_kwargs)],
        }
        for field in ITEM_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                tp_item_kwargs[field] = d[field]

        items.append(ThirdPartyDataItem(**tp_item_kwargs))
    return items


def call_api(
    client: DataClient,
    context: ThirdPartyContext,
    items: list[ThirdPartyDataItem],
    api_token: str,
    data_load_trace_id: Optional[str] = None,
) -> list[Any]:
    """Call ingest_third_party_data. Returns failed_lines (may be empty).

    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.errors import ThirdPartyDataServerResponseError
    from ttd_data.models import DataOrigin, DataOriginType
    from ttd_data.types import UNSET

    sdk_origin = DataOrigin(id=TTD_DATABRICKS_SDK_ORIGIN_ID, type=DataOriginType.INTEGRATION)
    data_origins = (context.data_origins or []) + [sdk_origin]

    failed_lines: list[Any] = []
    response = client.third_party.ingest_third_party_data(
        ttd_auth=api_token,
        data_provider_id=context.data_provider_id,
        items=items,
        is_user_id_already_hashed=context.is_user_id_already_hashed,
        data_load_trace_id=data_load_trace_id if data_load_trace_id is not None else UNSET,
        data_origins=data_origins,
        server_url=context.base_url_override,
    )
    server_response = response.third_party_data_server_response
    if server_response is not None:
        fl = server_response.failed_lines
        if fl is not UNSET and fl is not None:
            failed_lines = cast(list[Any], fl)
    return failed_lines
