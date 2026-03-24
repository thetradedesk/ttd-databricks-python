"""Unit tests for TtdDatabricksClient.batch_process() early-exit path.

Covers the case where the input table is empty: no API calls should be made,
and metadata (if configured) should record zero records processed.

spark.table() is patched to return a local empty DataFrame — no Delta tables needed.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient

_CONTEXT = AdvertiserContext(advertiser_id="adv123")
_REQUIRED_SCHEMA = StructType(
    [
        StructField("id_type", StringType(), False),
        StructField("id_value", StringType(), False),
        StructField("segment_name", StringType(), False),
    ]
)


def _make_client(spark: SparkSession) -> TtdDatabricksClient:
    return TtdDatabricksClient(
        data_api_client=MagicMock(spec=DataClient),
        api_token="test-token",
        spark=spark,
    )


def test_empty_input_writes_zero_to_metadata_and_returns(spark: SparkSession) -> None:
    empty_df = spark.createDataFrame([], _REQUIRED_SCHEMA)
    client = _make_client(spark)

    with patch.object(spark, "table", return_value=empty_df):
        with patch.object(client, "_write_metadata") as mock_write:
            client.batch_process(
                context=_CONTEXT,
                input_table="ttd_advertiser_input",
                output_table="ttd_advertiser_output",
                metadata_table="ttd_metadata",
            )

    mock_write.assert_called_once_with(spark, "ttd_metadata", 0)


def test_empty_input_without_metadata_table_returns_without_error(spark: SparkSession) -> None:
    empty_df = spark.createDataFrame([], _REQUIRED_SCHEMA)
    client = _make_client(spark)

    with patch.object(spark, "table", return_value=empty_df):
        # Should complete silently — no metadata_table means no write attempt
        client.batch_process(
            context=_CONTEXT,
            input_table="ttd_advertiser_input",
            output_table="ttd_advertiser_output",
        )
