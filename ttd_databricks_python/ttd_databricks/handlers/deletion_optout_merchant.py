"""API handler for the /data/deletion-optout/merchant endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Optional

from ttd_databricks_python.ttd_databricks.contexts import DeletionOptOutMerchantContext
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
    from ttd_data.models import PartnerDsrDataItem
    from ttd_data.uid2 import UID2Resolution

__all__ = ["build_items", "call_api", "collect_raw_pii_ids_per_row"]


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
    data_load_trace_id: Optional[str] = None,
) -> tuple[list[Any], dict[str, UID2Resolution]]:
    """Call data_subject_request_merchant_data.

    Returns `(failed_lines, identity_resolutions)`.
    """
    from ttd_data.errors import MerchantDsrResponseError
    from ttd_data.types import UNSET

    try:
        response = client.deletion_opt_out.data_subject_request_merchant_data(
            merchant_id=context.merchant_id,
            items=items,
            data_load_trace_id=data_load_trace_id if data_load_trace_id is not None else UNSET,
            request_type=context.request_type,
            server_url=context.base_url_override,
        )
        return extract_response_data(response, ServerResponseAttr.MERCHANT_DSR)
    except MerchantDsrResponseError as exc:
        failed_lines = extract_failed_lines_from_error(exc)
        if not failed_lines:
            raise
        return failed_lines, {}
