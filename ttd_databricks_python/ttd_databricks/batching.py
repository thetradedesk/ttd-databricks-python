"""Generic Spark batching pipeline for TTD API calls.

Based on https://www.databricks.com/blog/scalable-spark-structured-streaming-rest-api-destinations
See the section "Design and Operational Considerations" for information on
"Exactly Once vs At Least Once Guarantees" and "Estimating Cluster Core Count for a Target Throughput".

See https://community.databricks.com/t5/data-engineering/using-pyspark-databricks-udfs-with-outside-function-imports/m-p/138556
for an explanation of why UDF factories use inner functions/closures.
"""
import math
from typing import Callable

import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint

# Per-worker-process DataClient singleton. Each executor runs UDF tasks in a
# dedicated Python worker process, so there are no race conditions. Reusing the
# client allows HTTP connection reuse across batches via the connection pool.
_worker_client = None


def pre_batch_df(df: DataFrame, batch_size: int, records_count: int) -> DataFrame:
    """Assign batch IDs and group rows into one row per API call.

    Returns a DataFrame with one row per batch:
      _batch_id:   batch identifier
      _items_json: JSON array of all row fields — used to build the API payload
                   and to reconstruct per-row output
    """
    batch_count = math.ceil(records_count / batch_size)
    all_input_cols = [c for c in df.columns if not c.startswith("_")]

    # monotonically_increasing_id() assigns unique IDs locally per partition with no shuffle,
    # unlike row_number() which requires a global sort across all partitions.
    df = df.withColumn("_batch_id", F.monotonically_increasing_id() % batch_count)

    return df.groupBy("_batch_id").agg(
        F.to_json(F.collect_list(F.struct(*[F.col(c) for c in all_input_cols]))).alias("_items_json"),
    )


def apply_udf_and_explode(batched_df: DataFrame, udf_fn, output_schema, parallelism: int) -> DataFrame:
    """Apply endpoint UDF to batched_df and explode results back to per-row.

    Repartitions to target parallelism before applying the UDF so API calls
    run concurrently across workers.

    Returns a flat DataFrame with all output_schema fields as top-level columns.
    processed_timestamp is cast from the JSON string representation to TimestampType.
    """
    from pyspark.sql.types import ArrayType

    api_results_df = (
        batched_df
        .repartition(parallelism)
        .withColumn("_results_json", udf_fn(F.col("_items_json")))
    )

    exploded_df = api_results_df.withColumn(
        "_result", F.explode(F.from_json(F.col("_results_json"), ArrayType(output_schema)))
    )

    return exploded_df.select(
        *[F.col(f"_result.{field.name}").alias(field.name) for field in output_schema.fields]
    ).withColumn("processed_timestamp", F.to_timestamp(F.col("processed_timestamp")))


# ---------------------------------------------------------------------------
# Endpoint-specific UDF factories
# Each factory accepts (api_token, context_dict) and returns a Spark UDF.
# context_dict contains only serializable primitives so Spark can ship it to workers.
# ---------------------------------------------------------------------------

def _build_advertiser_udf(api_token: str, context_dict: dict):
    """Build the batch UDF for the ADVERTISER endpoint."""
    from ttd_databricks_python.ttd_databricks.schemas import (
        ADVERTISER_DATA_OPTIONAL_FIELDS,
        ADVERTISER_DATA_ITEM_OPTIONAL_FIELDS,
    )

    # Capture as closure variables so values are serialized with the UDF.
    adv_data_optional = ADVERTISER_DATA_OPTIONAL_FIELDS
    adv_item_optional = ADVERTISER_DATA_ITEM_OPTIONAL_FIELDS

    @F.udf("string")
    def call_ttd_api_batch(items_json):
        import json
        from datetime import datetime, timezone
        from ttd_data import DataClient
        from ttd_data.models import AdvertiserDataItem, AdvertiserData
        from ttd_data.errors import AdvertiserDataServerResponseError, APIError, NoResponseError
        from ttd_data.types import UNSET
        from ttd_databricks_python.ttd_databricks.utils import extract_item_number

        items_data = json.loads(items_json)
        timestamp = datetime.now(timezone.utc).isoformat()

        # Reuse a per-worker-process DataClient singleton to allow HTTP connection reuse.
        global _worker_client
        if _worker_client is None:
            _worker_client = DataClient()
        client = _worker_client

        items = []
        for d in items_data:
            adv_data_kwargs = {"name": d["segment_name"]}
            for field in adv_data_optional:
                if d.get(field) is not None:
                    adv_data_kwargs[field] = d[field]

            adv_item_kwargs = {
                d["id_type"]: d["id_value"],
                "data": [AdvertiserData(**adv_data_kwargs)],
            }
            for field in adv_item_optional:
                if d.get(field) is not None:
                    adv_item_kwargs[field] = d[field]

            items.append(AdvertiserDataItem(**adv_item_kwargs))

        failed_lines = []
        try:
            response = client.advertiser.ingest_advertiser_data(
                advertiser_id=context_dict["advertiser_id"],
                ttd_auth=api_token,
                data_provider_id=context_dict["data_provider_id"],
                items=items,
                server_url=context_dict["base_url_override"],
            )
            server_response = response.advertiser_data_server_response
            if server_response is not None:
                response_failed_lines = server_response.failed_lines
                if response_failed_lines is not UNSET and response_failed_lines is not None:
                    failed_lines = response_failed_lines
        except AdvertiserDataServerResponseError as exc:
            response_failed_lines = exc.data.failed_lines
            if response_failed_lines is not UNSET and response_failed_lines is not None:
                failed_lines = response_failed_lines
        except (APIError, NoResponseError) as exc:
            raise RuntimeError(f"TTD API unrecoverable error: {exc}") from exc

        # Map 1-based item number → error info (API identifies failures by "item #N" in message)
        failed_item_mapping = {}
        for line in failed_lines:
            message = line.message if line.message is not UNSET else None
            error_code = line.error_code.value if (line.error_code and line.error_code is not UNSET) else None
            item_number = extract_item_number(message)
            if item_number is not None:
                failed_item_mapping[item_number] = {"error_code": error_code, "error_message": message}

        results = []
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


# ---------------------------------------------------------------------------
# UDF registry — add new endpoints here as they are implemented.
# ---------------------------------------------------------------------------

_ENDPOINT_UDF_FACTORIES: dict[TTDEndpoint, Callable] = {
    TTDEndpoint.ADVERTISER: _build_advertiser_udf,
}


def get_batch_udf(endpoint: TTDEndpoint, api_token: str, context_dict: dict):
    """Return the endpoint-specific batch UDF, ready to apply to a batched DataFrame.

    Raises NotImplementedError if no UDF factory is registered for the endpoint.
    """
    if endpoint not in _ENDPOINT_UDF_FACTORIES:
        raise NotImplementedError(f"No batch UDF registered for endpoint: {endpoint}")
    return _ENDPOINT_UDF_FACTORIES[endpoint](api_token, context_dict)
