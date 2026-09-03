"""mapInPandas wiring and abort-path tests for process_partitions.

Proves that Spark invokes our partition function in a worker, ships rows through
Arrow, and reassembles the output DataFrame with the declared schema, and that a
401/403 aborts the partition instead of failing the job.

A local HTTP server is used as the only cross-process-safe way to stand in for
the TTD API: mocks in the driver process do not propagate to Spark Python workers.
It returns a configured status to every request so runs terminate deterministically
without needing real credentials or wire-format responses.
"""

from __future__ import annotations

import threading
from collections import Counter
from collections.abc import Iterator
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, TimestampType
from ttd_data import ClientConfig

from ttd_databricks_python.ttd_databricks.batching import process_partitions
from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext
from ttd_databricks_python.ttd_databricks.schemas import get_output_schema

pytestmark = pytest.mark.spark

# Pinned so request counts stay exact: retry_config=None leaves the SDK's retry wrapper
# off, so each batch makes exactly one call even when the stub returns a retryable 5xx.
# Shared by both tests — workers cache one DataClient per process, so a differing config
# in a second test would be silently ignored.
_NO_RETRY_CLIENT_CONFIG = ClientConfig(
    server_url=None,
    retry_config=None,
    timeout_ms=10_000,
    ttd_auth="not-a-real-token",
    uid2_config=None,
    graphql_server_url=None,
)


class _StubHandler(BaseHTTPRequestHandler):
    """Responds with the configured status to every request. Tracks request count to prove the server was hit."""

    status_code = 500
    request_count = 0
    auth_headers: list[str | None] = []
    # ThreadingHTTPServer handles each request on its own thread; `+= 1` is a
    # non-atomic read-modify-write, so guard it rather than relying on the spark
    # fixture staying single-threaded.
    counter_lock = threading.Lock()

    @classmethod
    def configure(cls, status_code: int) -> None:
        with cls.counter_lock:
            cls.status_code = status_code
            cls.request_count = 0
            cls.auth_headers = []

    def do_POST(self) -> None:  # noqa: N802 — required by stdlib BaseHTTPRequestHandler
        with type(self).counter_lock:
            type(self).request_count += 1
            type(self).auth_headers.append(self.headers.get("TTD-Auth"))
        body = b'{"Message":"forced error for test"}'
        self.send_response(type(self).status_code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 — base class param name
        pass


@pytest.fixture(scope="module")
def stub_server() -> Iterator[str]:
    """Run a localhost HTTP server standing in for the TTD API. Yields base URL."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StubHandler)
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


def test_mapinpandas_wires_up_and_round_trips(spark: SparkSession, stub_server: str) -> None:
    """mapInPandas invokes our partition function in a worker, preserves row count,
    preserves input values through Arrow, and returns the declared schema."""
    input_ids = [f"id-{i}" for i in range(7)]
    rows = [("TDID", id_value, "seg-a", None, None) for id_value in input_ids]
    input_df = spark.createDataFrame(rows, _advertiser_input_schema())
    output_schema = get_output_schema(input_df.schema)
    context = AdvertiserContext(advertiser_id="adv-test", base_url_override=stub_server)

    _StubHandler.configure(500)
    result_df = process_partitions(
        df=input_df,
        batch_size=3,
        output_schema=output_schema,
        context=context,
        parallelism=2,
        client_config=_NO_RETRY_CLIENT_CONFIG,
    )
    result_rows = result_df.collect()

    # 1. Our partition function executed in a Spark worker as expected: 7 rows / batch_size=3 → 3 batches.
    assert _StubHandler.request_count == 3
    # 2. Row count round-trips through the Arrow pipeline.
    assert len(result_rows) == 7
    # 3. Declared output schema columns are present (loose check — avoid nullable/metadata flakes).
    assert result_df.schema.fieldNames() == output_schema.fieldNames()
    # 4. Input column values survive Arrow → pandas → dict → pandas → Arrow round-trip.
    assert {row["id_value"] for row in result_rows} == set(input_ids)
    # 5. The worker's rebuilt DataClient authenticates: ttd_auth travels in the client_config
    #    snapshot, not as a separate per-call argument.
    assert set(_StubHandler.auth_headers) == {"not-a-real-token"}


@pytest.mark.parametrize(
    "status_code",
    [
        401,  # missing / expired TTD auth token
        403,  # token valid but not entitled to this advertiser or data provider
    ],
)
def test_401_and_403_stop_partition_without_failing_job(
    spark: SparkSession, stub_server: str, status_code: int
) -> None:
    """401/403 stop the partition without raising. The batch that was sent keeps
    the server's own status; every row after it is ABORTED, meaning it was never submitted."""
    rows = [("TDID", f"id-{i}", "seg-a", None, None) for i in range(7)]
    input_df = spark.createDataFrame(rows, _advertiser_input_schema())
    output_schema = get_output_schema(input_df.schema)
    context = AdvertiserContext(advertiser_id="adv-test", base_url_override=stub_server)

    _StubHandler.configure(status_code)
    result_df = process_partitions(
        df=input_df,
        batch_size=3,
        output_schema=output_schema,
        context=context,
        parallelism=1,
        client_config=_NO_RETRY_CLIENT_CONFIG,
    )
    result_rows = result_df.collect()

    # Single partition: only the first batch is sent; the remaining two never leave the worker.
    assert _StubHandler.request_count == 1
    # No rows discarded — every input row is accounted for.
    assert len(result_rows) == 7
    assert all(row["success"] is False for row in result_rows)

    by_code = Counter(row["error_code"] for row in result_rows)
    # The 3 rows that were sent carry the server's own status, not ABORTED.
    assert by_code[HTTPStatus(status_code).phrase] == 3
    # The 4 rows after them were never submitted, so they are safe to re-run.
    assert by_code["ABORTED"] == 4


def test_other_4xx_fails_only_its_own_batch(spark: SparkSession, stub_server: str) -> None:
    """A 4xx other than 401/403 fails only its own batch; the partition keeps calling the API."""
    status_code = 400  # malformed request body
    rows = [("TDID", f"id-{i}", "seg-a", None, None) for i in range(7)]
    input_df = spark.createDataFrame(rows, _advertiser_input_schema())
    output_schema = get_output_schema(input_df.schema)
    context = AdvertiserContext(advertiser_id="adv-test", base_url_override=stub_server)

    _StubHandler.configure(status_code)
    result_df = process_partitions(
        df=input_df,
        batch_size=3,
        output_schema=output_schema,
        context=context,
        parallelism=1,
        client_config=_NO_RETRY_CLIENT_CONFIG,
    )
    result_rows = result_df.collect()

    # All 3 batches are attempted since the failure doesn't abort the partition.
    assert _StubHandler.request_count == 3
    assert len(result_rows) == 7
    assert all(row["success"] is False for row in result_rows)

    by_code = Counter(row["error_code"] for row in result_rows)
    assert by_code[HTTPStatus(status_code).phrase] == 7
