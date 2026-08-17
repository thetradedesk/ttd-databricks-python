"""Package-level constants for the TTD Databricks SDK."""

from ttd_data.utils import BackoffStrategy, RetryConfig

# DataOrigin ID automatically appended to every API call to identify
# data submitted via this SDK.
TTD_DATABRICKS_SDK_ORIGIN_ID = "ttd_databricks_sdk"

# Error Code for rows never sent to The Trade Desk, so re-running them is safe. Failures
# that may already have been ingested are named after their exception instead.
ABORTED_ERROR_CODE = "ABORTED"

DEFAULT_RETRY_CONFIG = RetryConfig(
    "backoff",
    BackoffStrategy(
        initial_interval=500,
        max_interval=8_000,
        exponent=2.0,
        max_elapsed_time=30_000,
        jitter_ms=250,
    ),
    retry_connection_errors=True,
)
