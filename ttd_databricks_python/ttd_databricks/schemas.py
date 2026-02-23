"""Schema definitions and validation utilities for TTD endpoints."""

from enum import Enum
from typing import List

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint


class SchemaType(Enum):
    INPUT = "input"
    OUTPUT = "output"


# ---------------------------------------------------------------------------
# Endpoint-specific input schema functions
# Each function returns a StructType with the mandatory columns for that endpoint.
# pyspark is imported lazily inside each function — safe for non-Databricks environments.
# ---------------------------------------------------------------------------

def _advertiser_input_schema():
    """
    Mandatory columns for the /data/advertiser endpoint.

    Each DataFrame row represents one audience membership:
      tdid      → AdvertiserDataItem.tdid        (TTD user identifier)
      segment_name → AdvertiserData.name          (audience segment / data element name)
                     also used in error responses as AdvertiserDataServerResponseLine.data_name

    TODO: Extend beyond the minimal case to support additional AdvertiserDataItem identity fields
      (daid, uid2, uid2_token, ramp_id, core_id, euid, euid_token, id5, net_id, first_id,
      merkury_id, iqvia_ppid, cookie_mapping_partner_id) and optional AdvertiserData fields
      (timestamp_utc, ttl_in_minutes, base_bid_cpm, base_bid_cpm_metadata, bid_factor).
    """
    from pyspark.sql.types import StructType, StructField, StringType
    return StructType([
        StructField("tdid", StringType(), True),
        StructField("segment_name", StringType(), True),
    ])


_ENDPOINT_SCHEMA_FACTORIES: dict = {
    TTDEndpoint.ADVERTISER: _advertiser_input_schema,
}

_STATUS_COLUMNS = [
    ("success", "boolean"),
    ("error_code", "string"),
    ("error_message", "string"),
    ("processed_timestamp", "timestamp"),
]


# ---------------------------------------------------------------------------
# Public schema helpers
# ---------------------------------------------------------------------------

def get_ttd_input_schema(endpoint: TTDEndpoint):
    """
    Returns the TTD input schema (StructType) for the given endpoint.

    The schema defines all mandatory columns. Client DataFrames may contain
    additional columns — these are preserved in output but ignored during API submission.

    Args:
        endpoint: TTDEndpoint enum specifying which endpoint's schema to return.

    Returns:
        pyspark.sql.types.StructType with mandatory columns for the endpoint.

    Raises:
        TTDConfigurationError: If no schema is defined for the endpoint.
    """
    from ttd_databricks_python.ttd_databricks.exceptions import TTDConfigurationError

    if endpoint not in _ENDPOINT_SCHEMA_FACTORIES:
        raise TTDConfigurationError(f"No input schema defined for endpoint: {endpoint}")

    return _ENDPOINT_SCHEMA_FACTORIES[endpoint]()


def get_mandatory_column_names(endpoint: TTDEndpoint) -> List[str]:
    """
    Returns the list of mandatory column names for the given endpoint.

    Derived from the endpoint's StructType schema.

    Raises:
        TTDConfigurationError: If no schema is defined for the endpoint.
    """
    return [field.name for field in get_ttd_input_schema(endpoint).fields]


def validate_ttd_schema(
    df,
    endpoint: TTDEndpoint,
    schema_type: SchemaType = SchemaType.INPUT,
) -> None:
    """
    Validates that a DataFrame contains all required columns for the endpoint.

    - df: DataFrame to validate.
    - endpoint: Determines the required mandatory columns.
    - schema_type:
        INPUT  — validates mandatory TTD columns only.
        OUTPUT — validates mandatory TTD columns + status columns
                 (success, error_code, error_message, processed_timestamp).

    Raises TTDSchemaValidationError if any required columns are missing.
    Extra columns in the DataFrame are ignored.
    """
    from ttd_databricks_python.ttd_databricks.exceptions import TTDSchemaValidationError

    required = get_mandatory_column_names(endpoint)
    if schema_type == SchemaType.OUTPUT:
        required = required + [name for name, _ in _STATUS_COLUMNS]

    actual = set(df.schema.fieldNames())
    missing = [col_name for col_name in required if col_name not in actual]

    if missing:
        raise TTDSchemaValidationError(
            missing_columns=missing,
            schema_type=schema_type.value,
            endpoint_name=endpoint.display_name,
        )


def get_output_schema(input_schema):
    """
    Builds the output schema: input schema fields + status columns.

    Args:
        input_schema: pyspark.sql.types.StructType — typically the input table schema.

    Returns:
        pyspark.sql.types.StructType with all input fields plus:
        success (boolean), error_code (string), error_message (string),
        processed_timestamp (timestamp).
    """
    from pyspark.sql.types import StructType, StructField, StringType, BooleanType, TimestampType

    type_map = {
        "boolean": BooleanType(),
        "string": StringType(),
        "timestamp": TimestampType(),
    }
    status_fields = [StructField(name, type_map[dtype], True) for name, dtype in _STATUS_COLUMNS]
    return StructType(input_schema.fields + status_fields)


def get_metadata_schema():
    """
    Returns the metadata table schema for tracking batch processing runs.

    Columns: last_processed_date (timestamp), run_timestamp (timestamp),
             records_processed (long).
    """
    from pyspark.sql.types import StructType, StructField, TimestampType, LongType

    return StructType([
        StructField("last_processed_date", TimestampType(), True),
        StructField("run_timestamp", TimestampType(), True),
        StructField("records_processed", LongType(), True),
    ])
