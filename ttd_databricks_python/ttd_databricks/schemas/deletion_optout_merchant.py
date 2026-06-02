"""Schema and field definitions for the /data/deletion-optout/merchant endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType


def input_schema() -> StructType:
    """
    Schema for the /data/deletion-optout/merchant endpoint input table.

    Each DataFrame row represents one identity to be deleted or opted out
    for the specified merchant (via product catalog).

    Mandatory columns (not nullable):
      id_type  → which PartnerDsrDataItem identity field this row uses.
                 Must be one of: TDID, DAID, UID2, UID2Token, EUID, EUIDToken,
                 RampID, ID5, netID, FirstId, CoreID, MerkuryID, IqviaPPID,
                 Email, Phone, HashedEmail, HashedPhone.
                 Email/Phone/HashedEmail/HashedPhone require `uid2_config`
                 to be set on the `TtdDatabricksClient`. They are resolved
                 to a UID2/EUID by the UID2/EUID operator set in `uid2_config`.
                 The raw PII identifier does not get sent to the TTD Data API.
                 The raw PII identifier to UID2/EUID mapping is stored in the
                 `uid2_resolutions` column of the output table.
      id_value → the identifier value for the given id_type.
    """
    from pyspark.sql.types import StringType, StructField, StructType

    return StructType(
        [
            # Mandatory
            StructField("id_type", StringType(), False),
            StructField("id_value", StringType(), False),
        ]
    )
