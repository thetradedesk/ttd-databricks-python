"""Unit tests for ttd_databricks.schemas (validate_ttd_schema, get_required_column_names).

DataFrame is mocked — no SparkSession needed.
"""

import pytest

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint
from ttd_databricks_python.ttd_databricks.exceptions import TTDSchemaValidationError
from ttd_databricks_python.ttd_databricks.schemas import SchemaType, get_required_column_names, validate_ttd_schema


class _MockDataFrame:
    """Minimal stand-in for a Spark DataFrame — only needs schema.fieldNames()."""

    class _Schema:
        def __init__(self, names: list[str]) -> None:
            self._names = names

        def fieldNames(self) -> list[str]:
            return self._names

    def __init__(self, column_names: list[str]) -> None:
        self.schema = self._Schema(column_names)


# --------------------------------------------------------------------------- #
# get_required_column_names — schema contracts per endpoint                    #
# --------------------------------------------------------------------------- #


def test_advertiser_requires_id_type_id_value_segment_name():
    required = get_required_column_names(TTDEndpoint.ADVERTISER)
    assert {"id_type", "id_value", "segment_name"}.issubset(required)


def test_advertiser_optional_columns_are_not_required():
    # Nullable columns must not appear in required list — callers can omit them
    required = get_required_column_names(TTDEndpoint.ADVERTISER)
    assert "cookie_mapping_partner_id" not in required
    assert "ttl_in_minutes" not in required
    assert "timestamp_utc" not in required


def test_deletion_optout_advertiser_requires_only_id_fields():
    # Deletion endpoints have no segment_name — different schema from data endpoints
    required = get_required_column_names(TTDEndpoint.DELETION_OPTOUT_ADVERTISER)
    assert {"id_type", "id_value"}.issubset(required)
    assert "segment_name" not in required


def test_offline_conversion_requires_different_mandatory_columns():
    # Offline conversion uses tracking_tag_id/timestamp_utc, not id_type/id_value
    required = get_required_column_names(TTDEndpoint.OFFLINE_CONVERSION)
    assert {"tracking_tag_id", "timestamp_utc"}.issubset(required)
    assert "id_type" not in required


# --------------------------------------------------------------------------- #
# validate_ttd_schema — input schema                                            #
# --------------------------------------------------------------------------- #


def test_validation_passes_with_all_required_columns():
    df = _MockDataFrame(["id_type", "id_value", "segment_name"])
    validate_ttd_schema(df, TTDEndpoint.ADVERTISER)  # must not raise


def test_validation_passes_when_extra_columns_present():
    # Extra/custom columns are preserved in output — validation must not reject them
    df = _MockDataFrame(["id_type", "id_value", "segment_name", "custom_col"])
    validate_ttd_schema(df, TTDEndpoint.ADVERTISER)


def test_validation_raises_when_mandatory_column_missing():
    df = _MockDataFrame(["id_value", "segment_name"])  # missing id_type
    with pytest.raises(TTDSchemaValidationError) as exc_info:
        validate_ttd_schema(df, TTDEndpoint.ADVERTISER)
    assert "id_type" in exc_info.value.missing_columns


def test_validation_reports_all_missing_columns_at_once():
    # All errors reported in one exception — callers shouldn't have to fix one at a time
    df = _MockDataFrame(["segment_name"])  # missing id_type and id_value
    with pytest.raises(TTDSchemaValidationError) as exc_info:
        validate_ttd_schema(df, TTDEndpoint.ADVERTISER)
    assert "id_type" in exc_info.value.missing_columns
    assert "id_value" in exc_info.value.missing_columns


def test_validation_error_includes_endpoint_name_for_debuggability():
    df = _MockDataFrame([])
    with pytest.raises(TTDSchemaValidationError) as exc_info:
        validate_ttd_schema(df, TTDEndpoint.ADVERTISER)
    assert exc_info.value.endpoint_name == TTDEndpoint.ADVERTISER.display_name


# --------------------------------------------------------------------------- #
# validate_ttd_schema — output schema                                           #
# --------------------------------------------------------------------------- #


_STATUS_COLS = ["success", "error_code", "error_message", "processed_timestamp"]


def test_output_validation_passes_with_input_and_status_columns():
    df = _MockDataFrame(["id_type", "id_value", "segment_name"] + _STATUS_COLS)
    validate_ttd_schema(df, TTDEndpoint.ADVERTISER, SchemaType.OUTPUT)


def test_output_validation_raises_when_status_columns_missing():
    # A DataFrame missing status columns fails output validation even if input cols are present
    df = _MockDataFrame(["id_type", "id_value", "segment_name"])
    with pytest.raises(TTDSchemaValidationError) as exc_info:
        validate_ttd_schema(df, TTDEndpoint.ADVERTISER, SchemaType.OUTPUT)
    for col in _STATUS_COLS:
        assert col in exc_info.value.missing_columns
