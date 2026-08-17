"""Unit tests for ttd_databricks.exceptions."""

from ttd_databricks_python.ttd_databricks.exceptions import (
    TTDApiError,
    TTDConfigurationError,
    TTDError,
    TTDSchemaValidationError,
)


def test_all_sdk_exceptions_inherit_ttd_error():
    # Callers can catch all SDK errors with a single `except TTDError`
    assert issubclass(TTDApiError, TTDError)
    assert issubclass(TTDConfigurationError, TTDError)
    assert issubclass(TTDSchemaValidationError, TTDError)
