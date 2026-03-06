"""API handler for the /data/thirdparty endpoint."""


def build_items(items_data: list) -> list:
    """Convert list of row dicts to ThirdPartyDataItem SDK objects."""
    from ttd_data.models import ThirdPartyDataItem, ThirdPartyData
    from ttd_databricks_python.ttd_databricks.schemas.third_party import DATA_OPTIONAL_FIELDS, ITEM_OPTIONAL_FIELDS

    items = []
    for d in items_data:
        tp_data_kwargs = {"name": d["segment_name"]}
        for field in DATA_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                tp_data_kwargs[field] = d[field]

        tp_item_kwargs = {
            d["id_type"]: d["id_value"],
            "data": [ThirdPartyData(**tp_data_kwargs)],
        }
        for field in ITEM_OPTIONAL_FIELDS:
            if d.get(field) is not None:
                tp_item_kwargs[field] = d[field]

        items.append(ThirdPartyDataItem(**tp_item_kwargs))
    return items


def call_api(client, context, items: list, api_token: str) -> list:
    """Call ingest_third_party_data. Returns failed_lines (may be empty).

    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.errors import ThirdPartyDataServerResponseError
    from ttd_data.types import UNSET
    failed_lines = []
    try:
        response = client.third_party.ingest_third_party_data(
            ttd_auth=api_token,
            data_provider_id=context.data_provider_id,
            items=items,
            is_user_id_already_hashed=context.is_user_id_already_hashed,
            server_url=context.base_url_override,
        )
        server_response = response.third_party_data_server_response
        if server_response is not None:
            fl = server_response.failed_lines
            if fl is not UNSET and fl is not None:
                failed_lines = fl
    except ThirdPartyDataServerResponseError as exc:
        fl = exc.data.failed_lines
        if fl is not UNSET and fl is not None:
            failed_lines = fl
    return failed_lines
