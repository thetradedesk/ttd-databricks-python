"""Custom exceptions for the TTD Databricks SDK."""

from typing import List, Optional


class TTDError(Exception):
    """Base exception for all TTD Databricks SDK errors."""
    pass


class TTDApiError(TTDError):
    """Raised when the TTD API returns a non-2xx status for an entire batch request."""

    def __init__(self, status_code: Optional[int], response_text: str, batch_index: int):
        self.status_code = status_code
        self.response_text = response_text
        self.batch_index = batch_index
        status_str = str(status_code) if status_code is not None else "no response"
        super().__init__(
            f"TTD API error (HTTP {status_str}) for batch {batch_index}: {response_text}"
        )


class TTDConfigurationError(TTDError):
    """Raised when the SDK is misconfigured (e.g., missing SparkSession, invalid endpoint)."""
    pass


class TTDSchemaValidationError(TTDError):
    """Raised when DataFrame schema validation fails."""

    def __init__(self, missing_columns: List[str], schema_type: str, endpoint_name: str):
        self.missing_columns = missing_columns
        self.schema_type = schema_type
        self.endpoint_name = endpoint_name
        super().__init__(
            f"Schema validation failed for {schema_type} schema on endpoint '{endpoint_name}'. "
            f"Missing columns: {missing_columns}"
        )
