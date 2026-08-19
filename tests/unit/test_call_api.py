"""Unit tests for TtdDatabricksClient._call_api().

_call_api :
  1. Delegates item-building and the API call to the endpoint handler module.
  2. Maps failed_lines (by item number) to per-row result dicts.
     Rows with an item_number get their specific error.
     Rows without one fall back to the unattributable error (if any).
  3. Marks all rows in the batch as failed for any error other than auth/permission.
  4. Raises TTDApiError on an auth/permission failure, carrying the error_code the
     failing batch's rows should get.

The handler module import is patched so no real API calls are made.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import httpx

from ttd_data import DataClient
from ttd_data.errors import DataError, NoResponseError, ResponseValidationError

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


def _make_mock_handler() -> MagicMock:
    mock_handler = MagicMock()
    mock_handler.collect_raw_pii_ids_per_row.side_effect = lambda rows_data: [[] for _ in rows_data]
    return mock_handler


def _make_failed_line(item_number: int, error_code: str = "INVALID", message: str | None = None) -> MagicMock:
    line = MagicMock()
    line.item_number = str(item_number)
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
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock()] * 3
    mock_handler.call_api.return_value = ([], {})

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
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.call_api.return_value = (
        [_make_failed_line(1, error_code="INVALID_ID", message="Bad id for item #1")],
        {},
    )

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert results[0]["success"] is False
    assert results[0]["error_code"] == "INVALID_ID"
    assert results[0]["error_message"] == "Bad id for item #1"
    assert results[1]["success"] is True  # unaffected row


def test_only_unattributable_error_applies_fallback_to_all_rows():
    # If the API returns an error line with no item_number, we can't attribute it to a specific
    # row — the error is used as a fallback applied to every row in the batch.
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    failed = MagicMock()
    failed.item_number = None
    failed.message = "General error, no item number"
    failed.error_code.value = "UNKNOWN"
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.call_api.return_value = ([failed], {})

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert all(r["success"] is False for r in results)
    assert all(r["error_code"] == "UNKNOWN" for r in results)
    assert all(r["error_message"] == "General error, no item number" for r in results)


def test_attributable_row_gets_specific_error_others_get_unattributable_fallback():
    # Row with an item_number gets its own error; rows without one fall back
    # to the unattributable error. Both rows still fail, but with different details.
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    unattributable = MagicMock()
    unattributable.item_number = None
    unattributable.message = "General error, no item number"
    unattributable.error_code.value = "UNKNOWN"
    mock_handler.call_api.return_value = (
        [
            _make_failed_line(1, error_code="INVALID_ID", message="Bad id for item #1"),
            unattributable,
        ],
        {},
    )

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=0)

    assert results[0]["success"] is False
    assert results[0]["error_code"] == "INVALID_ID"  # specific error preserved
    assert results[1]["success"] is False
    assert results[1]["error_code"] == "UNKNOWN"  # unattributable as fallback


def test_failed_line_with_null_message_and_code_fails_all_rows():
    # A failed line with no item_number, no message, and no error_code still triggers
    # the fail-all path by setting the unattributable error to all rows.
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    null_failed = MagicMock()
    null_failed.item_number = None
    null_failed.message = None
    null_failed.error_code.value = None
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.call_api.return_value = ([null_failed], {})

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
    mock_handler = _make_mock_handler()
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
        assert results[0]["error_message"] == "_FakeNoResponseError: No response"
        # No HTTP status to report, so the exception name is the code — never NULL, which
        # would be indistinguishable from a succeeded row.
        assert results[0]["error_code"] == "_FakeNoResponseError"


def test_400_error_fails_only_its_own_batch():
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = 400
    raw.text = "This endpoint is not configured to accept data from tracking tag foo"
    raw.headers = httpx.Headers({})
    mock_handler.call_api.side_effect = DataError("batch error", raw)

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, rows, batch_index=2)

    assert len(results) == 2
    assert all(r["success"] is False for r in results)
    assert all(r["error_code"] == "Bad Request" for r in results)
    assert all("not configured" in r["error_message"] for r in results)


@pytest.mark.parametrize(("status_code", "expected_error_code"), [(401, "Unauthorized"), (403, "Forbidden")])
def test_401_and_403_raise_ttd_api_error(status_code: int, expected_error_code: str):
    client = _make_client()
    rows = _make_rows(_ROW, _ROW)
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = status_code
    raw.text = "not authorized"
    raw.headers = httpx.Headers({})
    mock_handler.call_api.side_effect = DataError("auth error", raw)

    with patch("importlib.import_module", return_value=mock_handler):
        with pytest.raises(TTDApiError) as exc_info:
            client._call_api(_CONTEXT, rows, batch_index=2)

    assert exc_info.value.error_code == expected_error_code
    assert exc_info.value.batch_index == 2
    assert "not authorized" in exc_info.value.response_text


def test_response_validation_failure_fails_only_its_own_batch():
    # Schema drift: the server returns 200 but the body doesn't match the SDK's model.
    client = _make_client()
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock()]

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = 200
    raw.text = '{"FailedLines": "not-a-list"}'
    raw.headers = httpx.Headers({})
    mock_handler.call_api.side_effect = ResponseValidationError(
        "Response validation failed", raw, ValueError("type mismatch"), body=raw.text
    )

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, _make_rows(_ROW), batch_index=3)

    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["error_code"] == "ResponseValidationError"


def test_unexpected_exception_is_reported_named_after_the_failure():
    client = _make_client()
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock()]
    mock_handler.call_api.side_effect = ValueError("bug in the SDK")

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, _make_rows(_ROW), batch_index=5)

    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["error_code"] == "ValueError"
    assert results[0]["error_message"] == "ValueError: bug in the SDK"


def test_build_items_failure_fails_only_its_own_batch():
    # A malformed row makes build_items raise. Nothing was sent, and the failure is specific
    # to this batch's own rows, so it must fail just this batch rather than raising and
    # aborting every later batch too.
    client = _make_client()
    mock_handler = _make_mock_handler()
    mock_handler.build_items.side_effect = ValueError("id_type 'Banana' is not supported")

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, _make_rows(_ROW), batch_index=1)

    mock_handler.call_api.assert_not_called()
    assert len(results) == 1
    assert results[0]["success"] is False
    assert results[0]["error_code"] == "ValueError"
    assert "not supported" in results[0]["error_message"]


def test_non_standard_status_code_does_not_raise_out_of_the_handler():
    # Load balancers and proxies (AWS ALB 460/463/464, nginx 499) return codes HTTPStatus()
    # rejects. The phrase lookup must not blow up and escape as an unhandled ValueError.
    client = _make_client()
    mock_handler = _make_mock_handler()
    mock_handler.build_items.return_value = [MagicMock()]

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = 520
    raw.text = "web server returned an unknown error"
    raw.headers = httpx.Headers({})
    mock_handler.call_api.side_effect = DataError("unknown error", raw, body=raw.text)

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(_CONTEXT, _make_rows(_ROW), batch_index=0)

    # Not a 401/403, so this batch fails and later batches still run.
    assert results[0]["success"] is False
    assert results[0]["error_code"] == "520"
