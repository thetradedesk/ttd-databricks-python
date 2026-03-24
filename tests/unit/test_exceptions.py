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


def test_api_error_none_status_code_shows_no_response_in_message():
    # status_code=None means no HTTP response was received
    err = TTDApiError(status_code=None, response_text="timeout", batch_index=0)
    assert "no response" in str(err)
