"""Schema and field definitions for the /data/deletion-optout/advertiser endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

def input_schema() -> StructType:
    """
    Schema for the /data/deletion-optout/advertiser endpoint input table.

    Each DataFrame row represents one identity to be deleted or opted out
    for the specified advertiser.

    Mandatory columns (not nullable):
      id_type  → which PartnerDsrDataItem identity field this row uses.
                 Must be one of: tdid, daid, uid2, uid2_token, ramp_id, core_id,
                 euid, euid_token, id5, net_id, first_id, merkury_id, iqvia_ppid.
      id_value → the identifier value for the given id_type.
    """
    from pyspark.sql.types import StructType, StructField, StringType

    return StructType([
        # Mandatory
        StructField("id_type", StringType(), False),
        StructField("id_value", StringType(), False),
    ])
