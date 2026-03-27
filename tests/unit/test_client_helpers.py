"""Unit tests for TtdDatabricksClient private helpers."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row, SparkSession
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.exceptions import TTDConfigurationError
from ttd_databricks_python.ttd_databricks.schemas import get_metadata_schema
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient


def _make_client(**kwargs) -> TtdDatabricksClient:  # type: ignore[no-untyped-def]
    return TtdDatabricksClient(
        data_api_client=MagicMock(spec=DataClient),
        api_token="test-token",
        **kwargs,
    )


# --------------------------------------------------------------------------- #
# _get_spark                                                                    #
# --------------------------------------------------------------------------- #


def test_get_spark_raises_config_error_when_no_active_session() -> None:
    # Client created without an explicit SparkSession; no active session on the thread.
    # Callers running outside Databricks must get a clear error, not an AttributeError.
    client = _make_client()
    with patch("pyspark.sql.SparkSession.getActiveSession", return_value=None):
        with pytest.raises(TTDConfigurationError):
            client._get_spark()


# --------------------------------------------------------------------------- #
# _get_last_processed_date                                                      #
# --------------------------------------------------------------------------- #


def test_get_last_processed_date_reraises_unexpected_exception() -> None:
    # Genuine errors (permissions, corrupt table) must not be silently swallowed.
    mock_spark = MagicMock()
    mock_spark.table.side_effect = Exception("Permission denied on table ttd_metadata")

    with pytest.raises(Exception, match="Permission denied"):
        _make_client()._get_last_processed_date(mock_spark, "ttd_metadata", override=None)


def test_get_last_processed_date_returns_latest_date(spark: SparkSession) -> None:
    # The incremental filter must use the latest date across all prior runs, not just
    # the most recently inserted row. Two runs are recorded; the later date must win.
    # Note: Spark's TimestampType strips tzinfo on collect, so naive datetimes are used.
    earlier = datetime(2024, 1, 1)
    later = datetime(2024, 6, 15)

    metadata_df = spark.createDataFrame(
        [
            Row(last_processed_date=earlier, run_timestamp=earlier, records_processed=10),
            Row(last_processed_date=later, run_timestamp=later, records_processed=5),
        ],
        schema=get_metadata_schema(),
    )

    with patch.object(spark, "table", return_value=metadata_df):
        result = _make_client(spark=spark)._get_last_processed_date(spark, "ttd_metadata", override=None)

    assert result == later
