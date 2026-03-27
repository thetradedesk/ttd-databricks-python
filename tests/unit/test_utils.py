"""Unit tests for ttd_databricks.utils."""

from ttd_databricks_python.ttd_databricks.utils import extract_item_number


def test_extracts_item_number_from_api_error_message():
    assert extract_item_number("Invalid DAID, item #2") == 2


def test_extracts_multi_digit_item_number():
    assert extract_item_number("Failed to process item #10") == 10


def test_extraction_is_case_insensitive():
    assert extract_item_number("ITEM #3 error") == 3


def test_returns_none_when_no_item_number_in_message():
    assert extract_item_number("some generic error message") is None


def test_returns_none_for_none_input():
    assert extract_item_number(None) is None


def test_returns_none_for_empty_string():
    assert extract_item_number("") is None
