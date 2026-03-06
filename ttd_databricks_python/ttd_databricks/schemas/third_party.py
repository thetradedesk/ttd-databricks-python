"""Schema and field definitions for the /data/thirdparty endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

# Fields passed to ThirdPartyData
DATA_OPTIONAL_FIELDS: frozenset[str] = frozenset({
    "timestamp_utc", "ttl_in_minutes",
})

# Fields passed to ThirdPartyDataItem
ITEM_OPTIONAL_FIELDS: frozenset[str] = frozenset({
    "cookie_mapping_partner_id",
})


def input_schema() -> StructType:
    """
    Schema for the /data/thirdparty endpoint input table.

    Each DataFrame row represents one audience membership for a single identity.

    Mandatory columns (not nullable):
      id_type      → which ThirdPartyDataItem identity field this row uses.
                     Must be one of: tdid, daid, uid2, uid2_token, ramp_id, core_id,
                     euid, euid_token, id5, net_id, first_id, merkury_id, iqvia_ppid.
      id_value     → the identifier value for the given id_type.
      segment_name → ThirdPartyData.name (audience segment / data element name).

    Optional columns (nullable):
      cookie_mapping_partner_id → ThirdPartyDataItem.CookieMappingPartnerId
      timestamp_utc             → ThirdPartyData.TimestampUtc
      ttl_in_minutes            → ThirdPartyData.TtlInMinutes
    """
    from pyspark.sql.types import (
        StructType, StructField,
        StringType, TimestampType, IntegerType,
    )
    return StructType([
        # Mandatory
        StructField("id_type", StringType(), False),
        StructField("id_value", StringType(), False),
        StructField("segment_name", StringType(), False),
        # Optional
        StructField("cookie_mapping_partner_id", StringType(), True),
        StructField("timestamp_utc", TimestampType(), True),
        StructField("ttl_in_minutes", IntegerType(), True),
    ])
