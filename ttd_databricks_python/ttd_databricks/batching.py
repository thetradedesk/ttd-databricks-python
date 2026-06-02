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

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import TTDContext

if TYPE_CHECKING:
    import pandas as pd
    from ttd_data.uid2 import UID2Config

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
    uid2_config: Optional[UID2Config] = None,
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
    """
    if parallelism is None:
        try:
            parallelism = 2 * df.sparkSession.sparkContext.defaultParallelism
        except Exception:
            parallelism = _DEFAULT_PARALLELISM

    all_input_cols = [c for c in df.columns if not c.startswith("_")]
    output_field_names = [f.name for f in output_schema.fields]
    handler_module = context.endpoint.handler_module

    def partition_to_results(pandas_df_iter: Iterable[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        import http
        import importlib
        from datetime import datetime, timezone

        import httpx
        import pandas as pd
        from ttd_data import DataClient
        from ttd_data.errors import DataError, NoResponseError

        from ttd_databricks_python.ttd_databricks.utils import (
            attach_resolutions,
            empty_resolution_value,
            parse_failed_lines,
        )

        global _worker_client
        if _worker_client is None:
            # uid2_config (a plain @dataclass) is closure-captured and cloudpickled to workers;
            # DataClient itself can't be — it holds open httpx connections.
            _worker_client = DataClient(timeout_ms=10_000, uid2_config=uid2_config)
        client = _worker_client
        handler = importlib.import_module(handler_module)

        def call_batch(batch_rows: list[dict[str, Any]]) -> pd.DataFrame:
            timestamp = datetime.now(timezone.utc)
            items = handler.build_items(batch_rows)
            raw_pii_ids_per_row = handler.collect_raw_pii_ids_per_row(batch_rows)

            def fail_batch(error_code: str | None, error_message: str) -> pd.DataFrame:
                results = [
                    {
                        **row_dict,
                        "success": False,
                        "error_code": error_code,
                        "error_message": error_message,
                        "processed_timestamp": timestamp,
                        **empty_resolution_value(),
                    }
                    for row_dict in batch_rows
                ]
                return pd.DataFrame(results, columns=output_field_names)

            failed_lines: list[Any] = []
            identity_resolutions: dict[str, Any] = {}
            try:
                failed_lines, identity_resolutions = handler.call_api(
                    client, context, items, api_token, data_load_trace_id
                )
            except (
                httpx.TimeoutException,
                httpx.RemoteProtocolError,
                NoResponseError,
            ) as exc:
                # Transient: timeout, stale pooled connection, or no response.
                # Mark batch as failed and continue.
                return fail_batch(None, str(exc))
            except DataError as exc:
                error_code = http.HTTPStatus(exc.status_code).phrase
                if exc.status_code >= 500:
                    # Transient server error, mark batch as failed and continue.
                    return fail_batch(error_code, exc.body)
                # 4xx errors (auth, bad request) — fail the job.
                raise RuntimeError(f"TTD API unrecoverable error: {exc}") from exc
            except Exception as exc:
                raise RuntimeError(f"Unexpected error during API call: {exc}") from exc

            row_results = parse_failed_lines(failed_lines, len(batch_rows))
            attach_resolutions(row_results, raw_pii_ids_per_row, identity_resolutions)
            merged = [
                {**row_dict, **row_result, "processed_timestamp": timestamp}
                for row_dict, row_result in zip(batch_rows, row_results, strict=True)
            ]
            return pd.DataFrame(merged, columns=output_field_names)

        batch: list[dict[str, Any]] = []
        for pandas_df in pandas_df_iter:
            # pd.DataFrame.to_dict(orient="records") converts SQL NULLs to float NaN
            # for nullable columns, which downstream Pydantic handlers reject. Normalise
            # every null variant (NaN, NaT, pd.NA, None) back to plain None.
            normalised_df = pandas_df.astype(object).where(pandas_df.notna(), None)
            for row_dict in cast(list[dict[str, Any]], normalised_df.to_dict(orient="records")):
                batch.append(row_dict)
                if len(batch) == batch_size:
                    yield call_batch(batch)
                    batch = []
        if batch:
            yield call_batch(batch)

    return df.select(*all_input_cols).repartition(parallelism).mapInPandas(partition_to_results, schema=output_schema)
