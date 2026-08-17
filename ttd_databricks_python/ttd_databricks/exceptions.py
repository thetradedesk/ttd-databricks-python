"""Custom exceptions for the TTD Databricks SDK."""

from __future__ import annotations


class TTDError(Exception):
    """Base exception for all TTD Databricks SDK errors."""

    pass


class TTDApiError(TTDError):
    """Raised when a batch hits a failure no later batch could survive, so the run must stop."""

    def __init__(self, response_text: str, batch_index: int, error_code: str) -> None:
        self.response_text = response_text
        self.batch_index = batch_index
        self.error_code = error_code
        super().__init__(f"TTD API error ({error_code}) for batch {batch_index}: {response_text}")


class TTDConfigurationError(TTDError):
    """Raised when the SDK is misconfigured (e.g., missing SparkSession, invalid endpoint)."""

    pass


class TTDSchemaValidationError(TTDError):
    """Raised when DataFrame schema validation fails."""

    def __init__(self, missing_columns: list[str], schema_type: str, endpoint_name: str) -> None:
        self.missing_columns = missing_columns
        self.schema_type = schema_type
        self.endpoint_name = endpoint_name
        super().__init__(
            f"Schema validation failed for {schema_type} schema on endpoint '{endpoint_name}'. "
            f"Missing columns: {missing_columns}"
        )
