"""Generic Spark batching pipeline for TTD API calls.

Based on https://www.databricks.com/blog/scalable-spark-structured-streaming-rest-api-destinations
See the section "Design and Operational Considerations" for information on
"Exactly Once vs At Least Once Guarantees" and "Estimating Cluster Core Count for a Target Throughput".

Inner imports are used inside partition_to_results so that modules are imported lazily
in each worker process rather than serialized from the driver.
"""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any, Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import TTDContext

# Per-worker-process DataClient singleton. Each executor runs mapPartitions tasks in a
# dedicated Python worker process, so there are no race conditions. Reusing the
# client allows HTTP connection reuse across batches via the connection pool.
_worker_client: Optional[DataClient] = None


def process_partitions(
    df: DataFrame,
    batch_size: int,
    output_schema: StructType,
    api_token: str,
    context: TTDContext,
    parallelism: Optional[int] = None,
    data_load_trace_id: Optional[str] = None,
) -> DataFrame:
    """Process all rows through the API using a single mapPartitions pass.

    Repartitions raw input rows to target parallelism, then processes each partition
    locally: chunks rows into batch_size groups, calls the API for each chunk,
    and yields result rows directly. Only one batch is held in memory at a time
    per partition — memory is constant regardless of table size.

    parallelism defaults to 2x the cluster's defaultParallelism, which is suitable
    for I/O-bound API workloads where tasks spend a notable about of time waiting on
    responses from the server.
    """
    if parallelism is None:
        parallelism = 2 * df.sparkSession.sparkContext.defaultParallelism

    all_input_cols = [c for c in df.columns if not c.startswith("_")]
    output_field_names = [f.name for f in output_schema.fields]
    handler_module = context.endpoint.handler_module

    def partition_to_results(rows: Any) -> Iterator[tuple[Any, ...]]:
        import http
        import importlib
        from datetime import datetime, timezone

        import httpx
        from ttd_data import DataClient
        from ttd_data.errors import DataError, NoResponseError

        from ttd_databricks_python.ttd_databricks.utils import parse_failed_lines

        global _worker_client
        if _worker_client is None:
            _worker_client = DataClient(timeout_ms=10_000)
        client = _worker_client
        handler = importlib.import_module(handler_module)

        def call_batch(batch_rows: list[dict[str, Any]]) -> Iterator[tuple[Any, ...]]:
            timestamp = datetime.now(timezone.utc)
            items = handler.build_items(batch_rows)

            def fail_batch(
                error_code: str | None,
                error_message: str,
            ) -> Iterator[tuple[Any, ...]]:
                for row_dict in batch_rows:
                    result = {
                        **row_dict,
                        "success": False,
                        "error_code": error_code,
                        "error_message": error_message,
                        "processed_timestamp": timestamp,
                    }
                    yield tuple(result[f] for f in output_field_names)

            failed_lines: list[Any] = []
            try:
                failed_lines = handler.call_api(client, context, items, api_token, data_load_trace_id)
            except (
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
                NoResponseError,
            ) as exc:
                # Transient: timeout, stale pooled connection, or no response.
                # Mark batch as failed and continue.
                yield from fail_batch(None, str(exc))
                return
            except DataError as exc:
                try:
                    error_code = http.HTTPStatus(exc.status_code).phrase
                except ValueError:
                    error_code = str(exc.status_code)
                if exc.status_code >= 500 or exc.status_code == 429:
                    # Transient error (server error or rate limit) — fail this batch and continue.
                    yield from fail_batch(error_code, exc.body)
                    return
                # 4xx errors (auth, bad request) — fail the job.
                raise RuntimeError(f"TTD API unrecoverable error: {exc}") from exc
            except Exception as exc:
                # Unexpected error — fail the job.
                raise RuntimeError(f"Unexpected error during API call: {exc}") from exc

            row_results = parse_failed_lines(failed_lines, len(batch_rows))
            for row_dict, row_result in zip(batch_rows, row_results, strict=True):
                result = {**row_dict, **row_result, "processed_timestamp": timestamp}
                yield tuple(result[f] for f in output_field_names)

        batch: list[dict[str, Any]] = []
        for row in rows:
            batch.append(row.asDict())
            if len(batch) == batch_size:
                yield from call_batch(batch)
                batch = []
        if batch:
            yield from call_batch(batch)

    return (
        df.select(*all_input_cols).repartition(parallelism).rdd.mapPartitions(partition_to_results).toDF(output_schema)
    )
