"""Main SDK class for submitting Spark DataFrames to TTD API endpoints."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional

# ttd-data is the external SDK for the TTD Data API.
# Install via: pip install ttd-data
# DataClient is the main HTTP client. The TTD-Auth token is passed per API call.
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import TTDContext
from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, Row, SparkSession


class TtdDatabricksClient:
    """
    Main SDK class for submitting Spark DataFrames to TTD API endpoints.

    Designed to run inside Databricks notebooks where PySpark is available
    via the runtime (not as an installed dependency).

    Supports two usage patterns:

    1. Dependency Injection (recommended for testing):
       client = TtdDatabricksClient(data_api_client=DataClient(), api_token="...")

    2. Factory method (convenience for notebooks):
       client = TtdDatabricksClient.from_params(api_token="...")
    """

    def __init__(
        self,
        data_api_client: DataClient,
        api_token: str,
        spark: Optional[SparkSession] = None,
    ) -> None:
        """
        Initialize TTD Databricks client via dependency injection.

        Auto-detect spark session unless explicitly provided.

        Dependency Injection pattern:
        - data_api_client: Required. Injected DataClient instance (from ttd-data package).
        - api_token: Required. TTD-Auth token passed to each API call for authentication.

        Note: DataClient is from the external ttd-data package.
        For factory pattern (creating clients from tokens), use `from_params()` class method.
        """
        self._data_api_client = data_api_client
        self._api_token = api_token
        self._spark = spark

    @classmethod
    def from_params(
        cls,
        api_token: str,
        spark: Optional[SparkSession] = None,
        uid2_key: Optional[str] = None,
        server_url: Optional[str] = None,
    ) -> TtdDatabricksClient:
        """
        Factory method to create TtdDatabricksClient from authentication tokens.

        Creates DataClient internally with default configuration.

        - api_token: Required. TTD API token for authentication (TTD-Auth header).
        - spark: Optional. SparkSession. Auto-detected from Databricks context if not provided.
        - uid2_key: Optional. Reserved for future UID2 client support. Currently unused.
        - server_url: Optional. Override the default TTD Data API server URL.

        Returns: TtdDatabricksClient instance with internally created DataClient.
        """
        # DataClient uses default server URL (https://usw-data.adsrvr.org) unless overridden.
        # Authentication (api_token) is passed per API call.
        data_api_client = DataClient(server_url=server_url)
        return cls(
            data_api_client=data_api_client,
            api_token=api_token,
            spark=spark,
        )

    # ------------------------------------------------------------------
    # Ad hoc mode
    # ------------------------------------------------------------------

    def push_data(
        self,
        df: DataFrame,
        context: TTDContext,
        batch_size: int = 10,
    ) -> DataFrame:
        """
        Process DataFrame and return results with status columns.

        Validates that mandatory TTD columns are present for the context's endpoint.

        Returns: Original columns + success, error_code, error_message, processed_timestamp

        - df: Input Spark DataFrame. Must contain all non-nullable columns for context.endpoint.
          Nullable columns may be omitted — they will be filled with null automatically.
          Extra columns are preserved in the output but ignored during API submission.
        - context: Typed context object (AdvertiserContext, ThirdPartyContext, etc.)
          Contains endpoint-specific config (data_provider_id, advertiser_id, etc.)
        - batch_size: Number of rows per API request. Default 10.
        """
        from ttd_databricks_python.ttd_databricks.schemas import get_output_schema, validate_ttd_schema

        df = self._fill_nullable_columns(df, context.endpoint)
        validate_ttd_schema(df, context.endpoint)

        spark = self._get_spark()
        all_rows = df.collect()
        result_rows: list[dict[str, Any]] = []

        for batch_index, i in enumerate(range(0, len(all_rows), batch_size)):
            batch = all_rows[i : i + batch_size]
            timestamp = datetime.now(timezone.utc)
            api_results = self._call_api(context, batch, batch_index)

            for row, result in zip(batch, api_results, strict=True):
                merged = row.asDict()
                merged["success"] = result["success"]
                merged["error_code"] = result["error_code"]
                merged["error_message"] = result["error_message"]
                merged["processed_timestamp"] = timestamp
                result_rows.append(merged)

        return spark.createDataFrame(result_rows, schema=get_output_schema(df.schema))

    # ------------------------------------------------------------------
    # Batch processing mode
    # ------------------------------------------------------------------

    def batch_process(
        self,
        context: TTDContext,
        input_table: str,
        output_table: str,
        metadata_table: Optional[str] = None,
        process_new_records_only: bool = False,
        last_processed_date_override: Optional[datetime] = None,
        batch_size: int = 10,
        parallelism: int = 8,
    ) -> None:
        """
        Read from input table, batch process, write to output table.

        Updates metadata table if provided.

        - context: Typed context object (AdvertiserContext, ThirdPartyContext, etc.)
          Contains endpoint-specific config (data_provider_id, advertiser_id, etc.)
        - input_table: Delta table name to read from. Must contain all mandatory columns for
          context.endpoint. Nullable columns may be omitted — they will be filled with null automatically.
        - output_table: Delta table name to write results to.
        - metadata_table: Optional. Table to track last_processed_date and run stats.
        - process_new_records_only: If True, filters input to rows where
          updated_at > last_processed_date (read from metadata_table).
        - last_processed_date_override: Optional. Override the last_processed_date
          from metadata_table (useful for reprocessing).
        - batch_size: Number of rows per API request. Default 10.
        - parallelism: Number of parallel partitions for API calls. Default 8.
        """
        import pyspark.sql.functions as F

        from ttd_databricks_python.ttd_databricks.batching import process_partitions
        from ttd_databricks_python.ttd_databricks.exceptions import TTDConfigurationError
        from ttd_databricks_python.ttd_databricks.schemas import get_output_schema, validate_ttd_schema

        if process_new_records_only and metadata_table is None:
            raise TTDConfigurationError("metadata_table is required when process_new_records_only=True")

        spark = self._get_spark()

        if metadata_table is not None and not spark.catalog.tableExists(metadata_table):
            raise TTDConfigurationError(
                f"Metadata table '{metadata_table}' does not exist. Call setup_metadata_table() before batch_process()."
            )

        df = spark.table(input_table)

        if process_new_records_only:
            last_date = self._get_last_processed_date(spark, metadata_table, last_processed_date_override)
            if last_date is not None:
                df = df.filter(F.col("updated_at") > last_date)

        df = self._fill_nullable_columns(df, context.endpoint)
        validate_ttd_schema(df, context.endpoint)

        records_count = df.count()
        if records_count == 0:
            if metadata_table:
                self._write_metadata(spark, metadata_table, 0)
            return

        output_schema = get_output_schema(df.schema)
        output_df = process_partitions(df, batch_size, output_schema, self._api_token, context, parallelism)

        output_df.write.format("delta").mode("append").saveAsTable(output_table)

        if metadata_table:
            self._write_metadata(spark, metadata_table, records_count)

    # ------------------------------------------------------------------
    # Table setup utilities
    # ------------------------------------------------------------------

    def setup_input_table(
        self,
        endpoint: TTDEndpoint,
        table_name: Optional[str] = None,
        schema: None = None,  # TODO: support custom schemas
        location: Optional[str] = None,
    ) -> str:
        """
        Create input Delta table with TTD-compatible schema + updated_at column.

        Uses the default mandatory columns for the endpoint. The table is created
        only if it does not already exist (idempotent).

        - endpoint: Required. Endpoint enum to determine schema (TTDEndpoint.ADVERTISER, etc.).
        - table_name: Optional. If None, defaults to "ttd_{endpoint}_input"
          (e.g., "ttd_advertiser_input" for ADVERTISER).
        - schema: Not yet supported. Reserved for future custom schema support.
        - location: Optional. External storage path. If None, creates a managed table.

        Returns: The table name used (either provided or the generated default).
        """
        if schema is not None:
            raise NotImplementedError("Custom schema is not yet supported in setup_input_table")

        from delta.tables import DeltaTable
        from pyspark.sql.types import StructField, StructType, TimestampType

        from ttd_databricks_python.ttd_databricks.schemas import get_ttd_input_schema

        spark = self._get_spark()
        table_name = table_name or f"ttd_{endpoint.value}_input"

        # Mandatory columns + Optional columns + updated_at for incremental processing
        base_schema = get_ttd_input_schema(endpoint)
        full_schema = StructType(base_schema.fields + [StructField("updated_at", TimestampType(), True)])

        builder = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(full_schema)
        if location:
            builder = builder.location(location)
        builder.execute()

        return table_name

    def setup_output_table(
        self,
        endpoint: TTDEndpoint,
        input_schema: None = None,  # TODO: support custom input schemas
        table_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """
        Create output Delta table: TTD input columns + updated_at + status columns.

        Status columns added: success, error_code, error_message, processed_timestamp.
        The table is created only if it does not already exist (idempotent).

        - endpoint: Required. Endpoint enum for schema determination.
        - input_schema: Not yet supported. Reserved for future custom schema support.
        - table_name: Optional. If None, defaults to "ttd_{endpoint}_output"
          (e.g., "ttd_advertiser_output" for ADVERTISER).
        - location: Optional. External storage path. If None, creates a managed table.

        Returns: The table name used (either provided or the generated default).
        """
        if input_schema is not None:
            raise NotImplementedError("Custom input_schema is not yet supported in setup_output_table")

        from delta.tables import DeltaTable
        from pyspark.sql.types import StructField, StructType, TimestampType

        from ttd_databricks_python.ttd_databricks.schemas import get_output_schema, get_ttd_input_schema

        spark = self._get_spark()
        table_name = table_name or f"ttd_{endpoint.value}_output"

        # Mirror the input table schema (TTD columns + updated_at), then append status columns
        base_schema = get_ttd_input_schema(endpoint)
        base_with_updated_at = StructType(base_schema.fields + [StructField("updated_at", TimestampType(), True)])
        full_schema = get_output_schema(base_with_updated_at)

        builder = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(full_schema)
        if location:
            builder = builder.location(location)
        builder.execute()

        return table_name

    def setup_metadata_table(
        self,
        table_name: Optional[str] = None,
        location: Optional[str] = None,
    ) -> str:
        """
        Create metadata Delta table for tracking batch processing runs.

        Schema: last_processed_date (timestamp), run_timestamp (timestamp),
        records_processed (long).

        The table is created only if it does not already exist (idempotent).

        - table_name: Optional. If None, defaults to "ttd_metadata".
        - location: Optional. External storage path. If None, creates a managed table.

        Returns: The table name used (either provided or the generated default).
        """
        from delta.tables import DeltaTable

        from ttd_databricks_python.ttd_databricks.schemas import get_metadata_schema

        spark = self._get_spark()
        table_name = table_name or "ttd_metadata"

        builder = DeltaTable.createIfNotExists(spark).tableName(table_name).addColumns(get_metadata_schema())
        if location:
            builder = builder.location(location)
        builder.execute()

        return table_name

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _get_spark(self) -> SparkSession:
        """Return the SparkSession, auto-detecting if not provided.

        Raises:
            TTDConfigurationError: If no active SparkSession is found.
        """
        if self._spark is not None:
            return self._spark
        try:
            from pyspark.sql import SparkSession

            from ttd_databricks_python.ttd_databricks.exceptions import TTDConfigurationError

            spark = SparkSession.getActiveSession()
            if spark is None:
                raise TTDConfigurationError(
                    "No active SparkSession found. Pass spark= to TtdDatabricksClient "
                    "or run inside a Databricks cluster."
                )
            self._spark = spark
            return spark
        except ImportError as exc:
            raise TTDConfigurationError(
                "pyspark is not installed. TtdDatabricksClient must run in a "
                "Databricks environment or have pyspark available."
            ) from exc

    def _call_api(self, context: TTDContext, rows: list[Row], batch_index: int) -> list[dict[str, Any]]:
        """Call the endpoint API for a batch of Spark Rows.

        Delegates item-building and the API call to the endpoint-specific handler module,
        then applies shared failed_lines parsing to produce per-row result dicts with keys:
        success (bool), error_code (Optional[str]), error_message (Optional[str]).

        - rows: Spark Rows from df.collect(); must contain the mandatory columns for context.endpoint.
        - batch_index: Zero-based batch number used in TTDApiError if the call fails.

        Raises:
            TTDApiError: On unrecoverable HTTP errors (403, 413, 5xx, etc.).
        """
        import importlib

        from ttd_data.errors import APIError, NoResponseError

        from ttd_databricks_python.ttd_databricks.exceptions import TTDApiError
        from ttd_databricks_python.ttd_databricks.utils import parse_failed_lines

        handler = importlib.import_module(context.endpoint.handler_module)
        items = handler.build_items([row.asDict() for row in rows])

        failed_lines: list[Any] = []
        try:
            failed_lines = handler.call_api(self._data_api_client, context, items, self._api_token)
        except (APIError, NoResponseError) as exc:
            # SDK-level errors: HTTP errors (403, 413, 5xx) or no response received
            raw = getattr(exc, "raw_response", None)
            raise TTDApiError(
                status_code=getattr(raw, "status_code", None),
                response_text=str(exc),
                batch_index=batch_index,
            ) from exc
        except Exception as exc:
            # Unexpected non-SDK errors (e.g. programming errors, unexpected library exceptions)
            raise TTDApiError(
                status_code=None,
                response_text=str(exc),
                batch_index=batch_index,
            ) from exc

        return parse_failed_lines(failed_lines, len(rows))

    @staticmethod
    def _fill_nullable_columns(df: DataFrame, endpoint: TTDEndpoint) -> DataFrame:
        """Add any missing nullable schema columns to the DataFrame as typed null columns.

        This allows users to omit optional columns entirely — the library fills them in
        with the correct Spark type so downstream processing and type inference don't fail.
        Columns already present in the DataFrame are left untouched.
        """
        from pyspark.sql import functions as F

        from ttd_databricks_python.ttd_databricks.schemas import get_ttd_input_schema

        existing = set(df.schema.fieldNames())
        for field in get_ttd_input_schema(endpoint).fields:
            if field.nullable and field.name not in existing:
                df = df.withColumn(field.name, F.lit(None).cast(field.dataType))
        return df

    def _get_last_processed_date(
        self,
        spark: SparkSession,
        metadata_table: Optional[str],
        override: Optional[datetime],
    ) -> Optional[datetime]:
        """Return the last processed date for incremental filtering.

        Returns override if provided, otherwise reads the max last_processed_date
        from metadata_table. Returns None if metadata_table is None or the table is empty.
        """
        if override is not None:
            return override
        if metadata_table is None:
            return None

        import pyspark.sql.functions as F

        last_processed_date: Optional[datetime] = (
            spark.table(metadata_table).agg(F.max("last_processed_date")).collect()[0][0]
        )
        return last_processed_date

    def _write_metadata(self, spark: SparkSession, metadata_table: str, records_processed: int) -> None:
        """Append a run record to the metadata table."""
        from pyspark.sql import Row

        from ttd_databricks_python.ttd_databricks.schemas import get_metadata_schema

        now = datetime.now(timezone.utc)
        spark.createDataFrame(
            [Row(last_processed_date=now, run_timestamp=now, records_processed=records_processed)],
            schema=get_metadata_schema(),
        ).write.format("delta").mode("append").saveAsTable(metadata_table)
