"""Unit tests for TtdDatabricksClient._call_api().

_call_api :
  1. Delegates item-building and the API call to the endpoint handler module.
  2. Maps failed_lines (by item number) to per-row result dicts.
     Rows with a parseable item number get their specific error.
     Rows without one fall back to the unattributable error (if any).
  3. Raises TTDApiError on APIError / NoResponseError from the handler.

The handler module import is patched so no real API calls are made.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ttd_data import DataClient
from ttd_data.errors import NoResponseError

from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext
from ttd_databricks_python.ttd_databricks.exceptions import TTDApiError
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient


def _make_client() -> TtdDatabricksClient:
    return TtdDatabricksClient(data_api_client=MagicMock(spec=DataClient), api_token="test-token")


def _make_rows(*dicts: dict[str, Any]) -> list[MagicMock]:
    rows = []
    for d in dicts:
        row = MagicMock()
        row.asDict.return_value = d
        rows.append(row)
    return rows


def _make_failed_line(item_number: int, error_code: str = "INVALID", message: str | None = None) -> MagicMock:
    line = MagicMock()
    line.message = f"Validation failed for item #{item_number}" if message is None else message
    line.error_code.value = error_code
    return line


_CONTEXT = AdvertiserContext(advertiser_id="adv123")
_ROW = {"id_type": "TDID", "id_value": "abc", "segment_name": "seg"}


# --------------------------------------------------------------------------- #
# All-success path                                                              #
# --------------------------------------------------------------------------- #


def test_all_rows_succeed_when_no_failed_lines():
    client = _make_client()
    rows = _make_rows(_ROW, _ROW, _ROW)
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock()] * 3
    mock_handler.call_api.return_value = []

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert all(r["success"] is True for r in results)
    assert all(r["error_code"] is None and r["error_message"] is None for r in results)


# --------------------------------------------------------------------------- #
# Partial failure path                                                          #
# --------------------------------------------------------------------------- #


def test_failed_row_is_marked_with_success_false_and_error_details():
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.call_api.return_value = [_make_failed_line(1, error_code="INVALID_ID", message="Bad id for item #1")]

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert results[0]["success"] is False
    assert results[0]["error_code"] == "INVALID_ID"
    assert results[0]["error_message"] == "Bad id for item #1"
    assert results[1]["success"] is True  # unaffected row


def test_only_unattributable_error_applies_fallback_to_all_rows():
    # If the API returns an error line with no "item #N", we can't attribute it to a specific
    # row — the error is used as a fallback applied to every row in the batch.
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    failed = MagicMock()
    failed.message = "General error, no item number"
    failed.error_code.value = "UNKNOWN"
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.call_api.return_value = [failed]

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert all(r["success"] is False for r in results)
    assert all(r["error_code"] == "UNKNOWN" for r in results)
    assert all(r["error_message"] == "General error, no item number" for r in results)


def test_attributable_row_gets_specific_error_others_get_unattributable_fallback():
    # Row with a parseable item number gets its own error; rows without one fall back
    # to the unattributable error. Both rows still fail, but with different details.
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    unattributable = MagicMock()
    unattributable.message = "General error, no item number"
    unattributable.error_code.value = "UNKNOWN"
    mock_handler.call_api.return_value = [
        _make_failed_line(1, error_code="INVALID_ID", message="Bad id for item #1"),
        unattributable,
    ]

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert results[0]["success"] is False
    assert results[0]["error_code"] == "INVALID_ID"   # specific error preserved
    assert results[1]["success"] is False
    assert results[1]["error_code"] == "UNKNOWN"       # unattributable as fallback


def test_failed_line_with_null_message_and_code_fails_all_rows():
    # A failed line with no item number, no message, and no error_code still triggers
    # the fail-all path by setting the unattributable error to all rows.
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    null_failed = MagicMock()
    null_failed.message = None
    null_failed.error_code.value = None
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.call_api.return_value = [null_failed]

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert all(r["success"] is False for r in results)
    assert all(r["error_code"] is None for r in results)
    assert all(r["error_message"] is None for r in results)


# --------------------------------------------------------------------------- #
# Error propagation                                                             #
# --------------------------------------------------------------------------- #


def test_no_response_error_from_handler_returns_failed_results():
    client = _make_client()
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock()]

    class _FakeNoResponseError(NoResponseError):
        def __init__(self):
            super().__init__("No response")

        def __str__(self):
            return "No response"

    mock_handler.call_api.side_effect = _FakeNoResponseError()

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, _make_rows(_ROW), batch_index=0)
        assert len(results) == 1
        assert results[0]["success"] is False
        assert results[0]["error_message"] == "No response"


def test_unexpected_exception_from_handler_raises_ttd_api_error_with_message():
    client = _make_client()
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock()]
    mock_handler.call_api.side_effect = ValueError("unexpected error")

    with patch("importlib.import_module", return_value=mock_handler):
        with pytest.raises(TTDApiError) as exc_info:
            client._call_api(_CONTEXT, _make_rows(_ROW), batch_index=5)

    assert exc_info.value.batch_index == 5
    assert "unexpected error" in exc_info.value.response_text
