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
    parallelism: int,
) -> DataFrame:
    """Process all rows through the API using a single mapPartitions pass.

    Repartitions raw input rows to target parallelism, then processes each partition
    locally: chunks rows into batch_size groups, calls the API for each chunk,
    and yields result rows directly. Only one batch is held in memory at a time
    per partition — memory is constant regardless of table size.
    """
    all_input_cols = [c for c in df.columns if not c.startswith("_")]
    output_field_names = [f.name for f in output_schema.fields]
    handler_module = context.endpoint.handler_module

    def partition_to_results(rows: Any) -> Iterator[tuple[Any, ...]]:
        import importlib
        from datetime import datetime, timezone

        from ttd_data import DataClient
        from ttd_data.errors import APIError, NoResponseError
        from ttd_data.types import UNSET

        from ttd_databricks_python.ttd_databricks.utils import extract_item_number

        global _worker_client
        if _worker_client is None:
            _worker_client = DataClient()
        client = _worker_client
        handler = importlib.import_module(handler_module)

        def call_batch(batch_rows: list[dict[str, Any]]) -> Iterator[tuple[Any, ...]]:
            timestamp = datetime.now(timezone.utc)
            items = handler.build_items(batch_rows)

            failed_lines: list[Any] = []
            try:
                failed_lines = handler.call_api(client, context, items, api_token)
            except (APIError, NoResponseError) as exc:
                raise RuntimeError(f"TTD API unrecoverable error: {exc}") from exc

            failed_item_mapping: dict[int, dict[str, Optional[str]]] = {}
            for line in failed_lines:
                message = line.message if line.message is not UNSET else None
                error_code = line.error_code.value if (line.error_code and line.error_code is not UNSET) else None
                item_number = extract_item_number(message)
                if item_number is not None:
                    failed_item_mapping[item_number] = {"error_code": error_code, "error_message": message}

            for i, row_dict in enumerate(batch_rows, start=1):
                if i in failed_item_mapping:
                    success = False
                    err = failed_item_mapping[i]
                    error_code_val = err["error_code"]
                    error_message_val = err["error_message"]
                else:
                    success = True
                    error_code_val = None
                    error_message_val = None
                result = {
                    **row_dict,
                    "success": success,
                    "error_code": error_code_val,
                    "error_message": error_message_val,
                    "processed_timestamp": timestamp,
                }
                yield tuple(result[f] for f in output_field_names)

        batch: list[dict[str, Any]] = []
        for row in rows:
            row_dict = row.asDict()
            batch.append({c: row_dict[c] for c in all_input_cols})
            if len(batch) == batch_size:
                yield from call_batch(batch)
                batch = []
        if batch:
            yield from call_batch(batch)

    return df.repartition(parallelism).rdd.mapPartitions(partition_to_results).toDF(output_schema)
