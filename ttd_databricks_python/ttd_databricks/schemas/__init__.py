"""Schema utilities for TTD endpoints."""

from __future__ import annotations

import importlib
from enum import Enum
from typing import TYPE_CHECKING

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class SchemaType(Enum):
    INPUT = "input"
    OUTPUT = "output"


# Uniform column shape across all endpoints. See `attach_resolutions` in utils.py
# for the per-endpoint array semantics.
UID2_RESOLUTIONS_COLUMN = "uid2_resolutions"


# Columns appended to every endpoint's output schema after the input fields.
# Order is load-bearing: existing output Delta tables expect this exact column order.
_EXTRA_OUTPUT_COLS: list[tuple[str, DataType]] = [
    ("success", BooleanType()),
    ("error_code", StringType()),
    ("error_message", StringType()),
    ("processed_timestamp", TimestampType()),
    (
        UID2_RESOLUTIONS_COLUMN,
        ArrayType(
            StructType(
                [
                    StructField("submitted_id", StringType(), True),
                    StructField("current_uid2", StringType(), True),
                    StructField("previous_uid2", StringType(), True),
                    StructField("refresh_from", TimestampType(), True),
                    StructField("unmapped_reason", StringType(), True),
                ]
            ),
            True,
        ),
    ),
]


def get_ttd_input_schema(endpoint: TTDEndpoint) -> StructType:
    """
    Returns the TTD input schema (StructType) for the given endpoint.

    The schema defines all columns (mandatory and optional) for the endpoint.
    Non-nullable columns must be present in the DataFrame. Nullable columns are
    filled automatically if omitted. Extra columns are preserved in output but
    ignored during API submission.

    Args:
        endpoint: TTDEndpoint enum specifying which endpoint's schema to return.

    Returns:
        pyspark.sql.types.StructType with all columns (mandatory and optional) for the endpoint.
    """
    return importlib.import_module(endpoint.schema_module).input_schema()  # type: ignore[no-any-return]


def get_required_column_names(endpoint: TTDEndpoint) -> list[str]:
    """
    Returns the list of required (non-nullable) column names for the given endpoint.

    Nullable columns are optional — they may be omitted from the DataFrame entirely
    and will be filled with null by the client before submission.
    """
    return [field.name for field in get_ttd_input_schema(endpoint).fields if not field.nullable]


def validate_ttd_schema(
    df: DataFrame,
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
                 (success, error_code, error_message, processed_timestamp, uid2_resolutions).

    Raises TTDSchemaValidationError if any required columns are missing.
    Extra columns in the DataFrame are ignored.
    """
    from ttd_databricks_python.ttd_databricks.exceptions import TTDSchemaValidationError

    required_columns = get_required_column_names(endpoint)
    if schema_type == SchemaType.OUTPUT:
        required_columns = required_columns + [name for name, _ in _EXTRA_OUTPUT_COLS]

    provided_columns = set(df.schema.fieldNames())
    missing = [col_name for col_name in required_columns if col_name not in provided_columns]

    if missing:
        raise TTDSchemaValidationError(
            missing_columns=missing,
            schema_type=schema_type.value,
            endpoint_name=endpoint.display_name,
        )


def get_output_schema(input_schema: StructType) -> StructType:
    """
    Builds the output schema: input schema fields + status columns + `uid2_resolutions`.

    Args:
        input_schema: pyspark.sql.types.StructType — typically the input table schema.

    Returns:
        pyspark.sql.types.StructType with all input fields plus:
        success (boolean), error_code (string), error_message (string),
        processed_timestamp (timestamp), uid2_resolutions (array<struct>).
    """
    extra_fields = [StructField(name, dtype, True) for name, dtype in _EXTRA_OUTPUT_COLS]
    return StructType(input_schema.fields + extra_fields)


def get_metadata_schema() -> StructType:
    """
    Returns the metadata table schema for tracking batch processing runs.

    Columns: last_processed_date (timestamp), run_timestamp (timestamp),
             records_processed (long).
    """
    from pyspark.sql.types import LongType

    return StructType(
        [
            StructField("last_processed_date", TimestampType(), True),
            StructField("run_timestamp", TimestampType(), True),
            StructField("records_processed", LongType(), True),
        ]
    )
