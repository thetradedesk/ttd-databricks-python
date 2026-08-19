"""Generic Spark batching pipeline for TTD API calls.

Based on https://www.databricks.com/blog/scalable-spark-structured-streaming-rest-api-destinations
See the section "Design and Operational Considerations" for information on
"Exactly Once vs At Least Once Guarantees" and "Estimating Cluster Core Count for a Target Throughput".

Uses DataFrame.mapInPandas so the same code path runs on Databricks Serverless and
dedicated clusters. Inner imports are lazy so modules load per worker, not via driver
serialization.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from typing import TYPE_CHECKING, Any, Optional, cast

from pyspark.errors import PySparkException
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import TTDContext

if TYPE_CHECKING:
    import pandas as pd
    from ttd_data import ClientConfig

# Per-worker-process DataClient singleton. Each executor runs the mapInPandas function in a
# dedicated Python worker process, so there are no race conditions. Reusing the
# client allows HTTP connection reuse across batches via the connection pool.
_worker_client: Optional[DataClient] = None

# Fallback when sparkContext.defaultParallelism is unavailable (serverless / Spark
# Connect). Callers driving large tables should pass an explicit parallelism.
_DEFAULT_PARALLELISM = 16


def process_partitions(
    df: DataFrame,
    batch_size: int,
    output_schema: StructType,
    api_token: str,
    context: TTDContext,
    parallelism: Optional[int] = None,
    data_load_trace_id: Optional[str] = None,
    client_config: Optional[ClientConfig] = None,
) -> DataFrame:
    """Process all rows through the API using a single mapInPandas pass.

    Repartitions raw input rows to target parallelism, then processes each partition
    locally: chunks rows into batch_size groups, calls the API for each chunk,
    and yields result rows directly. Only one batch is held in memory at a time
    per partition — memory is constant regardless of table size.

    parallelism defaults to 2x sparkContext.defaultParallelism on dedicated clusters,
    suitable for I/O-bound API workloads where tasks spend most of their time waiting
    on server responses. Falls back to _DEFAULT_PARALLELISM on serverless / Spark
    Connect where sparkContext is unavailable.

    client_config is a snapshot of the driver DataClient's settings (server_url,
    retry_config, timeout_ms, uid2_config), used to rebuild an equivalent DataClient
    per worker.

    Does not raise. An auth or permission failure aborts its own partition — later rows there
    get error_code="ABORTED", never submitted and safe to re-run. Other partitions carry on.
    """
    if parallelism is None:
        try:
            parallelism = 2 * df.sparkSession.sparkContext.defaultParallelism
        except (PySparkException, NotImplementedError):
            parallelism = _DEFAULT_PARALLELISM

    all_input_cols = [c for c in df.columns if not c.startswith("_")]
    output_field_names = [f.name for f in output_schema.fields]
    handler_module = context.endpoint.handler_module

    def partition_to_results(pandas_df_iter: Iterable[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import importlib
        from datetime import datetime, timezone

        import pandas as pd
        from ttd_data import DataClient

        from ttd_databricks_python.ttd_databricks.constants import ABORTED_ERROR_CODE, DEFAULT_RETRY_CONFIG
        from ttd_databricks_python.ttd_databricks.utils import (
            attach_resolutions,
            classify_failure,
            empty_resolution_value,
            parse_failed_lines,
        )

        global _worker_client
        if _worker_client is None:
            # Workers rebuild the client from the picklable client_config snapshot;
            # DataClient itself can't be cloudpickled.
            if client_config is None:
                _worker_client = DataClient(timeout_ms=10_000, retry_config=DEFAULT_RETRY_CONFIG)
            else:
                _worker_client = DataClient.from_config(client_config)
        client = _worker_client
        handler = importlib.import_module(handler_module)

        # Why this partition stopped. Once set, later batches are never sent to The Trade Desk.
        abort_reason: Optional[str] = None

        def build_result_df(
            batch_rows: list[dict[str, Any]],
            timestamp: datetime,
            row_results: list[dict[str, Any]],
        ) -> pd.DataFrame:
            merged = [
                {**row_dict, **row_result, "processed_timestamp": timestamp}
                for row_dict, row_result in zip(batch_rows, row_results, strict=True)
            ]
            return pd.DataFrame(merged, columns=output_field_names)

        def failed_batch(batch_rows: list[dict[str, Any]], error_code: str, error_message: str) -> pd.DataFrame:
            """Mark every row in the batch with the same failure."""
            row_results = [
                {
                    "success": False,
                    "error_code": error_code,
                    "error_message": error_message,
                    **empty_resolution_value(),
                }
                for _ in batch_rows
            ]
            return build_result_df(batch_rows, datetime.now(timezone.utc), row_results)

        def call_batch(batch_rows: list[dict[str, Any]]) -> pd.DataFrame:
            timestamp = datetime.now(timezone.utc)

            def abort(error_code: str, error_message: str) -> pd.DataFrame:
                """Record this batch's own outcome, then stop sending the rest of the partition."""
                nonlocal abort_reason
                abort_reason = error_message
                return failed_batch(batch_rows, error_code, error_message)

            try:
                items = handler.build_items(batch_rows)
                raw_pii_ids_per_row = handler.collect_raw_pii_ids_per_row(batch_rows)
                failed_lines, identity_resolutions = handler.call_api(
                    client, context, items, api_token, data_load_trace_id
                )
                row_results = parse_failed_lines(failed_lines, len(batch_rows))
                attach_resolutions(row_results, raw_pii_ids_per_row, identity_resolutions)
            except Exception as exc:
                transient, error_code, error_message = classify_failure(exc)
                if transient:
                    return failed_batch(batch_rows, error_code, error_message)
                return abort(error_code, error_message)

            return build_result_df(batch_rows, timestamp, row_results)

        def process_batch(batch_rows: list[dict[str, Any]]) -> pd.DataFrame:
            """Call the API, unless an earlier batch already aborted this partition."""
            if abort_reason is not None:
                return failed_batch(
                    batch_rows, ABORTED_ERROR_CODE, f"Aborted batch due to unrecoverable error: {abort_reason}"
                )
            return call_batch(batch_rows)

        batch: list[dict[str, Any]] = []
        for pandas_df in pandas_df_iter:
            # pd.DataFrame.to_dict(orient="records") converts SQL NULLs to float NaN
            # for nullable columns, which downstream Pydantic handlers reject. Normalise
            # every null variant (NaN, NaT, pd.NA, None) back to plain None.
            normalised_df = pandas_df.astype(object).where(pandas_df.notna(), None)
            for row_dict in cast(list[dict[str, Any]], normalised_df.to_dict(orient="records")):
                batch.append(row_dict)
                if len(batch) == batch_size:
                    yield process_batch(batch)
                    batch = []
        if batch:
            yield process_batch(batch)

    return df.select(*all_input_cols).repartition(parallelism).mapInPandas(partition_to_results, schema=output_schema)
