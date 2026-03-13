"""Generic Spark batching pipeline for TTD API calls.

Based on https://www.databricks.com/blog/scalable-spark-structured-streaming-rest-api-destinations
See the section "Design and Operational Considerations" for information on
"Exactly Once vs At Least Once Guarantees" and "Estimating Cluster Core Count for a Target Throughput".

See https://community.databricks.com/t5/data-engineering/using-pyspark-databricks-udfs-with-outside-function-imports/m-p/138556
for an explanation of why the UDF uses inner imports rather than top-level ones.
"""

from __future__ import annotations

import math
from typing import Any, Optional

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from ttd_data import DataClient

from ttd_databricks_python.ttd_databricks.contexts import TTDContext
from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint

# Per-worker-process DataClient singleton. Each executor runs UDF tasks in a
# dedicated Python worker process, so there are no race conditions. Reusing the
# client allows HTTP connection reuse across batches via the connection pool.
_worker_client: Optional[DataClient] = None


def pre_batch_df(df: DataFrame, batch_size: int, records_count: int) -> DataFrame:
    """Assign batch IDs and group rows into one row per API call.

    Returns a DataFrame with one row per batch:
      _batch_id:   batch identifier
      _items_json: JSON array of all row fields — used to build the API payload
                   and to reconstruct per-row output
    """
    batch_count = math.ceil(records_count / batch_size)
    all_input_cols = [c for c in df.columns if not c.startswith("_")]

    df = df.withColumn("_batch_id", F.monotonically_increasing_id() % batch_count)

    return df.groupBy("_batch_id").agg(
        F.to_json(F.collect_list(F.struct(*[F.col(c) for c in all_input_cols]))).alias("_items_json"),
    )


def apply_udf_and_explode(batched_df: DataFrame, udf_fn: Any, output_schema: StructType, parallelism: int) -> DataFrame:
    """Apply endpoint UDF to batched_df and explode results back to per-row.

    Repartitions to target parallelism before applying the UDF so API calls
    run concurrently across workers.

    Returns a flat DataFrame with all output_schema fields as top-level columns.
    processed_timestamp is cast from the JSON string representation to TimestampType.
    """
    from pyspark.sql.types import ArrayType

    api_results_df = batched_df.repartition(parallelism).withColumn("_results_json", udf_fn(F.col("_items_json")))

    exploded_df = api_results_df.withColumn(
        "_result", F.explode(F.from_json(F.col("_results_json"), ArrayType(output_schema)))
    )

    return exploded_df.select(
        *[F.col(f"_result.{field.name}").alias(field.name) for field in output_schema.fields]
    ).withColumn("processed_timestamp", F.to_timestamp(F.col("processed_timestamp")))


def _build_generic_udf(api_token: str, context: TTDContext, handler_module: str) -> Any:
    """Build a batch UDF that delegates item-building and API calls to the endpoint handler.

    api_token, context, and handler_module are captured in the closure and serialized to
    worker processes via cloudpickle. handler_module is kept as a plain string so the
    handler module is imported lazily inside the UDF at execution time rather than
    serializing a live module object.
    """

    @F.udf("string")  # type: ignore[untyped-decorator]
    def call_ttd_api_batch(items_json: Optional[str]) -> Optional[str]:
        import importlib
        import json
        from datetime import datetime, timezone

        from ttd_data import DataClient
        from ttd_data.errors import APIError, NoResponseError
        from ttd_data.types import UNSET

        from ttd_databricks_python.ttd_databricks.utils import extract_item_number

        if items_json is None:
            return None
        items_data = json.loads(items_json)
        timestamp = datetime.now(timezone.utc).isoformat()

        # Reuse a per-worker-process DataClient singleton to allow HTTP connection reuse.
        global _worker_client
        if _worker_client is None:
            _worker_client = DataClient()
        client = _worker_client

        handler = importlib.import_module(handler_module)
        items = handler.build_items(items_data)

        failed_lines = []
        try:
            failed_lines = handler.call_api(client, context, items, api_token)
        except (APIError, NoResponseError) as exc:
            raise RuntimeError(f"TTD API unrecoverable error: {exc}") from exc

        # Map 1-based item number → error info (API identifies failures by "item #N" in message)
        failed_item_mapping: dict[int, dict[str, Optional[str]]] = {}
        for line in failed_lines:
            message = line.message if line.message is not UNSET else None
            error_code = line.error_code.value if (line.error_code and line.error_code is not UNSET) else None
            item_number = extract_item_number(message)
            if item_number is not None:
                failed_item_mapping[item_number] = {"error_code": error_code, "error_message": message}

        results: list[dict[str, Any]] = []
        for i, row in enumerate(items_data, start=1):
            r = dict(row)
            if i in failed_item_mapping:
                r["success"] = False
                r["error_code"] = failed_item_mapping[i]["error_code"]
                r["error_message"] = failed_item_mapping[i]["error_message"]
            else:
                r["success"] = True
                r["error_code"] = None
                r["error_message"] = None
            r["processed_timestamp"] = timestamp
            results.append(r)
        return json.dumps(results)

    return call_ttd_api_batch


def get_batch_udf(endpoint: TTDEndpoint, api_token: str, context: TTDContext) -> Any:
    """Return the endpoint-specific batch UDF, ready to apply to a batched DataFrame."""
    return _build_generic_udf(api_token, context, endpoint.handler_module)
