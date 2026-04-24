"""API handler for the /data/deletion-optout/merchant endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional, cast

from ttd_databricks_python.ttd_databricks.contexts import DeletionOptOutMerchantContext
from ttd_databricks_python.ttd_databricks.id_types import normalize_id_type

if TYPE_CHECKING:
    from ttd_data import DataClient
    from ttd_data.models import PartnerDsrDataItem


def build_items(items_data: list[dict[str, Any]]) -> list[PartnerDsrDataItem]:
    """Convert list of row dicts to PartnerDsrDataItem SDK objects."""
    from ttd_data.models import PartnerDsrDataItem

    items = []
    for d in items_data:
        items.append(PartnerDsrDataItem(**{normalize_id_type(d["id_type"]): d["id_value"]}))
    return items


def call_api(
    client: DataClient,
    context: DeletionOptOutMerchantContext,
    items: list[PartnerDsrDataItem],
    api_token: str,
    data_load_trace_id: Optional[str] = None,
) -> list[Any]:
    """Call data_subject_request_merchant_data. Returns failed_lines (may be empty).

    Raises MerchantDsrResponseError on 400 responses without failed_lines.
    Raises APIError / NoResponseError on unrecoverable errors — caller is
    responsible for converting these to the appropriate exception type.
    """
    from ttd_data.errors import MerchantDsrResponseError
    from ttd_data.types import UNSET

    failed_lines: list[Any] = []
    try:
        response = client.deletion_opt_out.data_subject_request_merchant_data(
            ttd_auth=api_token,
            merchant_id=context.merchant_id,
            items=items,
            data_load_trace_id=data_load_trace_id if data_load_trace_id is not None else UNSET,
            request_type=context.request_type,
            server_url=context.base_url_override,
        )
        server_response = response.merchant_dsr_response
        if server_response is not None:
            fl = server_response.failed_lines
            if fl is not UNSET and fl is not None:
                failed_lines = cast(list[Any], fl)
    except MerchantDsrResponseError as exc:
        fl = exc.data.failed_lines
        if fl is UNSET or fl is None or not fl:
            raise
        failed_lines = cast(list[Any], fl)
    return failed_lines
