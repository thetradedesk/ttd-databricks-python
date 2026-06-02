"""mapInPandas wiring test for process_partitions.

Proves that Spark invokes our partition function in a worker, ships rows through
Arrow, and reassembles the output DataFrame with the declared schema. Scope is
strictly the .mapInPandas(...) plumbing — error-handling branches, handler-specific
behaviour, and SDK error mapping are out of scope.

A local HTTP server is used as the only cross-process-safe way to stand in for
the TTD API: mocks in the driver process do not propagate to Spark Python workers.
The server returns 500 to every request so the run terminates deterministically
without needing real credentials or wire-format responses.
"""

from __future__ import annotations

import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType

from ttd_databricks_python.ttd_databricks.batching import process_partitions
from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext
from ttd_databricks_python.ttd_databricks.schemas import get_output_schema

pytestmark = pytest.mark.spark


class _FailingHandler(BaseHTTPRequestHandler):
    """Responds 500 to every request. Tracks request count to prove the server was hit."""

    request_count = 0

    @classmethod
    def reset_count(cls) -> None:
        cls.request_count = 0

    def do_POST(self) -> None:  # noqa: N802 — required by stdlib BaseHTTPRequestHandler
        type(self).request_count += 1
        body = b'{"Message":"forced server error for test"}'
        self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 — base class param name
        pass


@pytest.fixture(scope="module")
def failing_server() -> Iterator[str]:
    """Run a localhost HTTP server that 500s on every request. Yields base URL."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _FailingHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        host, port = server.server_address[0], server.server_address[1]
        yield f"http://{host}:{port}"
    finally:
        server.shutdown()
        server.server_close()


def _advertiser_input_schema() -> StructType:
    """Minimal advertiser input schema — mandatory columns only."""
    return StructType(
        [
            StructField("id_type", StringType(), False),
            StructField("id_value", StringType(), False),
            StructField("segment_name", StringType(), False),
            StructField("cookie_mapping_partner_id", StringType(), True),
            StructField("timestamp_utc", TimestampType(), True),
        ]
    )


def test_mapinpandas_wires_up_and_round_trips(spark: SparkSession, failing_server: str) -> None:
    """mapInPandas invokes our partition function in a worker, preserves row count,
    preserves input values through Arrow, and returns the declared schema."""
    input_ids = [f"id-{i}" for i in range(7)]
    rows = [("TDID", id_value, "seg-a", None, None) for id_value in input_ids]
    input_df = spark.createDataFrame(rows, _advertiser_input_schema())
    output_schema = get_output_schema(input_df.schema)
    context = AdvertiserContext(advertiser_id="adv-test", base_url_override=failing_server)

    _FailingHandler.reset_count()
    result_df = process_partitions(
        df=input_df,
        batch_size=3,
        output_schema=output_schema,
        api_token="not-a-real-token",
        context=context,
        parallelism=2,
    )
    result_rows = result_df.collect()

    # 1. Our partition function actually executed in a Spark worker.
    assert _FailingHandler.request_count >= 1, "mapInPandas did not invoke our partition function"
    # 2. Row count round-trips through the Arrow pipeline.
    assert len(result_rows) == 7
    # 3. Declared output schema columns are present (loose check — avoid nullable/metadata flakes).
    assert result_df.schema.fieldNames() == output_schema.fieldNames()
    # 4. Input column values survive Arrow → pandas → dict → pandas → Arrow round-trip.
    assert {row["id_value"] for row in result_rows} == set(input_ids)
