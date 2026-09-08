"""Unit tests for TtdDatabricksClient.push_data().

Uses a local SparkSession. Handler module is patched so no real API calls are made.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext
from ttd_databricks_python.ttd_databricks.exceptions import TTDSchemaValidationError
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient

pytestmark = pytest.mark.spark

_CONTEXT = AdvertiserContext(advertiser_id="adv123")
_REQUIRED_SCHEMA = StructType(
    [
        StructField("id_type", StringType(), False),
        StructField("id_value", StringType(), False),
        StructField("segment_name", StringType(), False),
    ]
)
_STATUS_COLS = ["success", "error_code", "error_message", "processed_timestamp"]


def _make_client(spark: SparkSession) -> TtdDatabricksClient:
    return TtdDatabricksClient(
        data_api_client=MagicMock(spec=DataClient),
        spark=spark,
    )


def _make_handler(failed_lines: list | None = None) -> MagicMock:
    mock_handler = MagicMock()
    mock_handler.build_items.side_effect = lambda rows: [MagicMock() for _ in rows]
    mock_handler.collect_raw_pii_ids_per_row.side_effect = lambda rows: [[] for _ in rows]
    mock_handler.call_api.return_value = (failed_lines or [], {})
    return mock_handler


# --------------------------------------------------------------------------- #
# Happy path                                                                    #
# --------------------------------------------------------------------------- #


def test_all_rows_succeed_output_has_success_true(spark: SparkSession) -> None:
    data = [("TDID", "abc123", "seg1"), ("UID2", "def456", "seg2")]
    df = spark.createDataFrame(data, _REQUIRED_SCHEMA)

    with patch("importlib.import_module", return_value=_make_handler()):
        result = _make_client(spark).push_data(df, _CONTEXT)

    rows = result.collect()
    assert len(rows) == 2
    assert all(r["success"] is True for r in rows)
    assert all(r["error_code"] is None for r in rows)
    assert all(r["error_message"] is None for r in rows)


# --------------------------------------------------------------------------- #
# Output schema contract                                                        #
# --------------------------------------------------------------------------- #


def test_output_dataframe_has_status_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame([("TDID", "abc123", "seg1")], _REQUIRED_SCHEMA)

    with patch("importlib.import_module", return_value=_make_handler()):
        result = _make_client(spark).push_data(df, _CONTEXT)

    field_names = result.schema.fieldNames()
    for col in _STATUS_COLS:
        assert col in field_names


def test_empty_dataframe_returns_empty_output_with_correct_schema(spark: SparkSession) -> None:
    # _call_api is never reached for an empty DF — no patch needed
    empty_df = spark.createDataFrame([], _REQUIRED_SCHEMA)

    result = _make_client(spark).push_data(empty_df, _CONTEXT)

    assert result.count() == 0
    for col in _STATUS_COLS:
        assert col in result.schema.fieldNames()


def test_extra_columns_preserved_in_output(spark: SparkSession) -> None:
    schema = StructType(
        [
            *_REQUIRED_SCHEMA.fields,
            StructField("custom_col", StringType(), True),
        ]
    )
    df = spark.createDataFrame([("TDID", "abc123", "seg1", "my_value")], schema)

    with patch("importlib.import_module", return_value=_make_handler()):
        result = _make_client(spark).push_data(df, _CONTEXT)

    assert "custom_col" in result.schema.fieldNames()
    assert result.collect()[0]["custom_col"] == "my_value"


# --------------------------------------------------------------------------- #
# Error handling                                                                #
# --------------------------------------------------------------------------- #


def test_partial_failure_maps_error_to_correct_row(spark: SparkSession) -> None:
    # Two rows in one batch; item #1 fails, item #2 succeeds
    data = [("TDID", "abc123", "seg1"), ("TDID", "def456", "seg2")]
    df = spark.createDataFrame(data, _REQUIRED_SCHEMA)

    failed_line = MagicMock()
    failed_line.message = "Validation failed for item #1"
    failed_line.error_code.value = "INVALID_ID"

    with patch("importlib.import_module", return_value=_make_handler([failed_line])):
        result = _make_client(spark).push_data(df, _CONTEXT)

    by_id = {r["id_value"]: r for r in result.collect()}
    assert by_id["abc123"]["success"] is False
    assert by_id["abc123"]["error_code"] == "INVALID_ID"
    assert by_id["def456"]["success"] is True


def test_400_error_fails_only_its_own_batch_and_continues(spark: SparkSession) -> None:
    # batch_size=1 -> 3 separate API calls: first succeeds, second hits a 400 (fails just
    # that batch), third still runs and succeeds.
    import httpx
    from ttd_data.errors import DataError

    data = [("TDID", "ok", "seg1"), ("TDID", "bad-request", "seg2"), ("TDID", "later-succeeds", "seg3")]
    df = spark.createDataFrame(data, _REQUIRED_SCHEMA)

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = 400
    raw.text = "invalid segment"
    raw.headers = httpx.Headers({})
    client_error = DataError("bad request", raw)

    mock_handler = _make_handler()
    mock_handler.call_api.side_effect = [([], {}), client_error, ([], {})]

    with patch("importlib.import_module", return_value=mock_handler):
        result = _make_client(spark).push_data(df, _CONTEXT, batch_size=1)

    by_id = {r["id_value"]: r for r in result.collect()}
    assert len(by_id) == 3
    assert by_id["ok"]["success"] is True
    assert by_id["bad-request"]["success"] is False
    assert by_id["bad-request"]["error_code"] == "Bad Request"
    assert by_id["later-succeeds"]["success"] is True  # later batch still ran


@pytest.mark.parametrize("status_code", [401, 403])
def test_401_or_403_aborts_remaining_batches(spark: SparkSession, status_code: int) -> None:
    # batch_size=1 -> 3 separate API calls: first succeeds, second hits a 401/403 (stops
    # the run), third is never attempted. The successful row survives, the rejected row
    # keeps the server's status, and only the untried row is ABORTED.
    import httpx
    from ttd_data.errors import DataError

    data = [("TDID", "ok", "seg1"), ("TDID", "unauthorized", "seg2"), ("TDID", "never-attempted", "seg3")]
    df = spark.createDataFrame(data, _REQUIRED_SCHEMA)

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = status_code
    raw.text = "not authorized"
    raw.headers = httpx.Headers({})
    client_error = DataError("auth error", raw)

    mock_handler = _make_handler()
    mock_handler.call_api.side_effect = [([], {}), client_error]

    with patch("importlib.import_module", return_value=mock_handler):
        result = _make_client(spark).push_data(df, _CONTEXT, batch_size=1)

    by_id = {r["id_value"]: r for r in result.collect()}
    assert len(by_id) == 3
    assert by_id["ok"]["success"] is True  # earlier batch's result is not discarded
    assert by_id["unauthorized"]["success"] is False
    assert "not authorized" in by_id["unauthorized"]["error_message"]  # sent and rejected
    assert by_id["never-attempted"]["success"] is False
    assert by_id["never-attempted"]["error_code"] == "ABORTED"  # never sent


def test_missing_required_column_raises_schema_validation_error(spark: SparkSession) -> None:
    schema = StructType(
        [
            StructField("id_type", StringType(), False),
            StructField("id_value", StringType(), False),
            # missing: segment_name
        ]
    )
    df = spark.createDataFrame([("TDID", "abc123")], schema)

    with pytest.raises(TTDSchemaValidationError) as exc_info:
        _make_client(spark).push_data(df, _CONTEXT)

    assert "segment_name" in exc_info.value.missing_columns
