"""Schema and field definitions for the /providerapi/offlineconversion endpoint."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql.types import StructType

# Fields passed directly to OfflineConversionDataItem (no nested data model)
ITEM_OPTIONAL_FIELDS: frozenset[str] = frozenset(
    {
        "data_provider_user_id",
        "cookie_mapping_partner_id",
        "order_id",
        "impression_id",
        "value",
        "value_currency",
        "country",
        "region",
        "metro",
        "city",
        "merchant_id",
        "event_name",
        "td1",
        "td2",
        "td3",
        "td4",
        "td5",
        "td6",
        "td7",
        "td8",
        "td9",
        "td10",
    }
)


def input_schema() -> StructType:
    """
    Schema for the /providerapi/offlineconversion endpoint input table.

    Each DataFrame row represents one offline conversion event.

    Mandatory columns (not nullable):
      tracking_tag_id → OfflineConversionDataItem.TrackingTagId
      timestamp_utc   → OfflineConversionDataItem.TimestampUtc

    Optional columns (nullable):
      user_ids        → OfflineConversionDataItem.UserIdArray in the API request.
                        Required only if impression_id is not provided.
                        Array of structs with fields:
                          type — identity type name.
                                 Must be one of: TDID, DAID, UID2, UID2Token,
                                 EUID, EUIDToken, RampID.
                                 Converted to integer code (0–6) in the request.
                          id   — identity value string.
                        UserIdArrayMetadataFormat is hardcoded to ["type", "id"].
                        Up to 20 IDs per row; multiple IDs of the same type are allowed.
      data_provider_user_id     → OfflineConversionDataItem.DataProviderUserId
      cookie_mapping_partner_id → OfflineConversionDataItem.CookieMappingPartnerId
      order_id                  → OfflineConversionDataItem.OrderId
      impression_id             → OfflineConversionDataItem.ImpressionId
      value                     → OfflineConversionDataItem.Value
      value_currency            → OfflineConversionDataItem.ValueCurrency
      country                   → OfflineConversionDataItem.Country
      region                    → OfflineConversionDataItem.Region
      metro                     → OfflineConversionDataItem.Metro
      city                      → OfflineConversionDataItem.City
      merchant_id               → OfflineConversionDataItem.MerchantId
      event_name                → OfflineConversionDataItem.EventName
      td1 … td10                → OfflineConversionDataItem.TD1 … TD10
      line_items                → OfflineConversionDataItem.LineItems
                                   Array of structs with fields: item_code, name, qty, price, cat
      privacy_settings          → OfflineConversionDataItem.PrivacySettings
                                   Array of structs with fields: privacy_type, is_applicable, consent_string
    """
    from pyspark.sql.types import (
        ArrayType,
        MapType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    return StructType(
        [
            # Mandatory
            StructField("tracking_tag_id", StringType(), False),
            StructField("timestamp_utc", TimestampType(), False),
            # Optional
            # user_ids, line_items and privacy_settings use MapType so the schema
            # matches what Spark infers from Python list-of-dicts. StructType would
            # require users to provide an explicit schema when writing, causing
            # DELTA_FAILED_TO_MERGE_FIELDS on saveAsTable.
            StructField("user_ids", ArrayType(MapType(StringType(), StringType())), True),
            StructField("data_provider_user_id", StringType(), True),
            StructField("cookie_mapping_partner_id", StringType(), True),
            StructField("order_id", StringType(), True),
            StructField("impression_id", StringType(), True),
            StructField("value", StringType(), True),
            StructField("value_currency", StringType(), True),
            StructField("country", StringType(), True),
            StructField("region", StringType(), True),
            StructField("metro", StringType(), True),
            StructField("city", StringType(), True),
            StructField("merchant_id", StringType(), True),
            StructField("event_name", StringType(), True),
            StructField("td1", StringType(), True),
            StructField("td2", StringType(), True),
            StructField("td3", StringType(), True),
            StructField("td4", StringType(), True),
            StructField("td5", StringType(), True),
            StructField("td6", StringType(), True),
            StructField("td7", StringType(), True),
            StructField("td8", StringType(), True),
            StructField("td9", StringType(), True),
            StructField("td10", StringType(), True),
            StructField("line_items", ArrayType(MapType(StringType(), StringType())), True),
            StructField("privacy_settings", ArrayType(MapType(StringType(), StringType())), True),
        ]
    )
