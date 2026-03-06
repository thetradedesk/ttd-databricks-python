"""API handler for the /data/advertiser endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext

if TYPE_CHECKING:
    from ttd_data import DataClient
    from ttd_data.models import AdvertiserDataItem


def build_items(items_data: list[dict[str, Any]]) -> list[AdvertiserDataItem]:
    """Convert list of row dicts to AdvertiserDataItem SDK objects."""
    from ttd_data.models import AdvertiserDataItem, AdvertiserData
    from ttd_databricks_python.ttd_databricks.schemas.advertiser import DATA_OPTIONAL_FIELDS, ITEM_OPTIONAL_FIELDS

    items = []
    for d in items_data:
        adv_data_kwargs = {"name": d["segment_name"]}
        for field in DATA_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                adv_data_kwargs[field] = d[field]

        adv_item_kwargs = {
            d["id_type"]: d["id_value"],
            "data": [AdvertiserData(**adv_data_kwargs)],
        }
        for field in ITEM_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                adv_item_kwargs[field] = d[field]

        items.append(AdvertiserDataItem(**adv_item_kwargs))
    return items


def call_api(client: DataClient, context: AdvertiserContext, items: list[AdvertiserDataItem], api_token: str) -> list[Any]:
    """Call ingest_advertiser_data. Returns failed_lines (may be empty).

    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.errors import AdvertiserDataServerResponseError
    from ttd_data.types import UNSET
    failed_lines = []
    try:
        response = client.advertiser.ingest_advertiser_data(
            advertiser_id=context.advertiser_id,
            ttd_auth=api_token,
            data_provider_id=context.data_provider_id,
            items=items,
            server_url=context.base_url_override,
        )
        server_response = response.advertiser_data_server_response
        if server_response is not None:
            fl = server_response.failed_lines
            if fl is not UNSET and fl is not None:
                failed_lines = fl
    except AdvertiserDataServerResponseError as exc:
        fl = exc.data.failed_lines
        if fl is not UNSET and fl is not None:
            failed_lines = fl
    return failed_lines
