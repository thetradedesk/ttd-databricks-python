"""Unit tests for UID2 identity-resolution surfacing.

Covers:
  - id_types.is_raw_pii_id_type is case-insensitive
  - handlers' collect_raw_pii_ids_per_row returns list[list[str]] for every endpoint
  - offline_conversion handler maps raw PII types to UserIdType placeholder codes
  - utils.attach_resolutions merges per-row resolutions into uid2_resolutions
  - get_output_schema appends a uniform uid2_resolutions array<struct> column
  - _call_api populates resolutions end-to-end (success, 5xx failure paths)
  - from_params wires uid2_config into the driver and worker DataClients
"""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from ttd_data import DataClient
from ttd_data.uid2 import UID2Resolution

import ttd_databricks_python.ttd_databricks.handlers.advertiser as adv_handler
import ttd_databricks_python.ttd_databricks.handlers.offline_conversion as oc_handler
from ttd_databricks_python.ttd_databricks.contexts import AdvertiserContext, OfflineConversionContext
from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint
from ttd_databricks_python.ttd_databricks.id_types import is_raw_pii_id_type
from ttd_databricks_python.ttd_databricks.schemas import (
    UID2_RESOLUTIONS_COLUMN,
    get_output_schema,
)
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient
from ttd_databricks_python.ttd_databricks.utils import attach_resolutions

# --------------------------------------------------------------------------- #
# id_types normalization                                                        #
# --------------------------------------------------------------------------- #


def test_is_raw_pii_id_type_is_case_insensitive() -> None:
    assert is_raw_pii_id_type("Email") is True
    assert is_raw_pii_id_type("email") is True
    assert is_raw_pii_id_type("TDID") is False


# --------------------------------------------------------------------------- #
# Handler collect_raw_pii_ids_per_row                                           #
# --------------------------------------------------------------------------- #


class TestCollectRawPiiIdsPerRow:
    def test_advertiser_returns_singleton_list_for_raw_pii_types_else_empty(self) -> None:
        rows = [
            {"id_type": "TDID", "id_value": "tdid-abc", "segment_name": "s"},
            {"id_type": "Email", "id_value": "user@example.com", "segment_name": "s"},
            {"id_type": "HashedPhone", "id_value": "hashedphone-1", "segment_name": "s"},
        ]
        assert adv_handler.collect_raw_pii_ids_per_row(rows) == [[], ["user@example.com"], ["hashedphone-1"]]

    def test_offline_conversion_collects_raw_ids_from_user_ids_in_order(self) -> None:
        # `user_ids` is array<struct<type, id>> per the schema — entries arrive as
        # pyspark Row in prod; using dicts here matches the same named-access contract.
        rows = [
            {
                "tracking_tag_id": "t1",
                "timestamp_utc": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "user_ids": [
                    {"type": "TDID", "id": "tdid-1"},
                    {"type": "Email", "id": "a@example.com"},
                    {"type": "HashedPhone", "id": "hashedphone-1"},
                ],
            },
            {
                "tracking_tag_id": "t2",
                "timestamp_utc": datetime(2024, 1, 1, tzinfo=timezone.utc),
                "user_ids": [{"type": "UID2", "id": "uid2-x"}],
            },
        ]
        # Order mirrors submission order; only raw PII entries are kept.
        assert oc_handler.collect_raw_pii_ids_per_row(rows) == [["a@example.com", "hashedphone-1"], []]


# --------------------------------------------------------------------------- #
# offline_conversion: UserIdType placeholder codes                              #
# --------------------------------------------------------------------------- #


class TestOfflineConversionUserIdTypeCodes:
    _TS = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _MINIMAL_OFFLINE_CONVERSION_REQUEST = {"tracking_tag_id": "t1", "timestamp_utc": _TS}

    @pytest.mark.parametrize(
        ("type_name", "expected_code"),
        [
            # Resolved-to-UID2 placeholders the SDK rewrites pre-wire.
            ("Email", "-3"),
            ("Phone", "-4"),
            ("HashedEmail", "-1"),
            ("HashedPhone", "-2"),
            # Canonical codes preserved.
            ("TDID", "0"),
            ("UID2", "2"),
            ("UID2Token", "3"),
            ("RampID", "6"),
        ],
    )
    def test_user_id_type_codes(self, type_name: str, expected_code: str) -> None:
        row = {**self._MINIMAL_OFFLINE_CONVERSION_REQUEST, "user_ids": [{"type": type_name, "id": "x"}]}
        item = oc_handler.build_items([row])[0]
        assert item.user_id_array[0][0] == expected_code

    def test_unknown_user_id_type_raises(self) -> None:
        row = {**self._MINIMAL_OFFLINE_CONVERSION_REQUEST, "user_ids": [{"type": "NotAnIdType", "id": "x"}]}
        with pytest.raises(ValueError, match="Unknown user_ids type"):
            oc_handler.build_items([row])


# --------------------------------------------------------------------------- #
# advertiser handler routes raw PII id_type to the correct SDK field           #
# --------------------------------------------------------------------------- #


def test_raw_pii_id_type_routes_to_correct_field() -> None:
    row = {"id_type": "Email", "id_value": "user@example.com", "segment_name": "seg"}
    item = adv_handler.build_items([row])[0]
    # Email maps to the `email` field; uid2 stays unset.
    assert item.email == "user@example.com"


# --------------------------------------------------------------------------- #
# attach_resolutions                                                            #
# --------------------------------------------------------------------------- #


def _mapped_resolution(current: str, refresh: datetime | None = None) -> UID2Resolution:
    return UID2Resolution(current_raw_uid=current, previous_raw_uid=None, refresh_from=refresh)


def _unmapped_resolution(reason: str) -> UID2Resolution:
    return UID2Resolution(unmapped_reason=reason)


class TestAttachResolutionsSingleId:
    _RESULTS_INPUT = [
        {"success": True, "error_code": None, "error_message": None},
        {"success": True, "error_code": None, "error_message": None},
        {"success": True, "error_code": None, "error_message": None},
    ]

    def test_mapped_row_gets_singleton_array(self) -> None:
        results = [r.copy() for r in self._RESULTS_INPUT]
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        attach_resolutions(
            results,
            raw_pii_ids_per_row=[["a@example.com"], [], ["missing@example.com"]],
            identity_resolutions={
                "a@example.com": _mapped_resolution("uid2-aaa", refresh=ts),
                "missing@example.com": _unmapped_resolution("optout"),
            },
        )
        # Mapped row: array with one entry.
        assert results[0][UID2_RESOLUTIONS_COLUMN] == [
            {
                "submitted_id": "a@example.com",
                "current_uid2": "uid2-aaa",
                "previous_uid2": None,
                "refresh_from": ts,
                "unmapped_reason": None,
            }
        ]
        # Row 1 used a non-PII id_type — array is empty.
        assert results[1][UID2_RESOLUTIONS_COLUMN] == []
        # Row 2 was unmapped — array with one entry carrying the reason.
        assert len(results[2][UID2_RESOLUTIONS_COLUMN]) == 1
        assert results[2][UID2_RESOLUTIONS_COLUMN][0]["unmapped_reason"] == "optout"
        assert results[2][UID2_RESOLUTIONS_COLUMN][0]["current_uid2"] is None
        assert results[2][UID2_RESOLUTIONS_COLUMN][0]["submitted_id"] == "missing@example.com"

    def test_raw_id_with_no_resolution_entry_yields_empty_array(self) -> None:
        # If the SDK didn't return a resolution for the submitted raw id (e.g. uid2_config
        # not configured), the array is empty even though the row used a raw PII id_type.
        results = [{"success": True, "error_code": None, "error_message": None}]
        attach_resolutions(
            results,
            raw_pii_ids_per_row=[["a@example.com"]],
            identity_resolutions={},
        )
        assert results[0][UID2_RESOLUTIONS_COLUMN] == []


class TestAttachResolutionsOfflineConversion:
    def test_offline_conversion_emits_array_aligned_with_user_ids(self) -> None:
        results = [
            {"success": True, "error_code": None, "error_message": None},
            {"success": True, "error_code": None, "error_message": None},
        ]
        attach_resolutions(
            results,
            raw_pii_ids_per_row=[["a@example.com", "hashedphone-1"], []],
            identity_resolutions={
                "a@example.com": _mapped_resolution("uid2-aaa"),
                "hashedphone-1": _unmapped_resolution("invalid"),
            },
        )
        first = results[0][UID2_RESOLUTIONS_COLUMN]
        assert len(first) == 2
        assert first[0]["current_uid2"] == "uid2-aaa"
        assert first[0]["submitted_id"] == "a@example.com"
        assert first[1]["unmapped_reason"] == "invalid"
        assert first[1]["submitted_id"] == "hashedphone-1"
        # Row with no raw ids gets an empty list.
        assert results[1][UID2_RESOLUTIONS_COLUMN] == []


# --------------------------------------------------------------------------- #
# get_output_schema emits the uniform uid2_resolutions array<struct> column     #
# --------------------------------------------------------------------------- #


@pytest.mark.spark
class TestOutputSchemaUniformArrayShape:
    def test_emits_uid2_resolutions_array_struct(self) -> None:
        from pyspark.sql.types import ArrayType, StructType

        from ttd_databricks_python.ttd_databricks.schemas import get_ttd_input_schema

        out = get_output_schema(get_ttd_input_schema(TTDEndpoint.ADVERTISER))
        field = next(f for f in out.fields if f.name == UID2_RESOLUTIONS_COLUMN)
        assert isinstance(field.dataType, ArrayType)
        assert isinstance(field.dataType.elementType, StructType)
        inner_names = {f.name for f in field.dataType.elementType.fields}
        assert inner_names == {"submitted_id", "current_uid2", "previous_uid2", "refresh_from", "unmapped_reason"}


# --------------------------------------------------------------------------- #
# _validate_output_table_schema pre-flight check                                #
# --------------------------------------------------------------------------- #


class TestValidateOutputTableSchema:
    def _expected_schema(self):
        from pyspark.sql.types import (
            ArrayType,
            BooleanType,
            StringType,
            StructField,
            StructType,
            TimestampType,
        )

        return StructType(
            [
                StructField("id_value", StringType(), False),
                StructField("success", BooleanType(), True),
                StructField("processed_timestamp", TimestampType(), True),
                StructField(
                    UID2_RESOLUTIONS_COLUMN,
                    ArrayType(StructType([StructField("submitted_id", StringType(), True)]), True),
                    True,
                ),
            ]
        )

    def test_passes_when_table_does_not_exist(self) -> None:
        spark = MagicMock()
        spark.catalog.tableExists.return_value = False
        # No exception, no schema read.
        TtdDatabricksClient._validate_output_table_schema(spark, "missing_table", self._expected_schema())
        spark.table.assert_not_called()

    def test_passes_when_table_has_all_expected_columns(self) -> None:
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.table.return_value.schema = self._expected_schema()
        TtdDatabricksClient._validate_output_table_schema(spark, "ok_table", self._expected_schema())

    def test_raises_with_alter_table_hint_when_column_missing(self) -> None:
        from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

        from ttd_databricks_python.ttd_databricks.exceptions import TTDConfigurationError

        # Existing table is missing `uid2_resolutions`.
        existing = StructType(
            [
                StructField("id_value", StringType(), False),
                StructField("success", BooleanType(), True),
                StructField("processed_timestamp", TimestampType(), True),
            ]
        )
        spark = MagicMock()
        spark.catalog.tableExists.return_value = True
        spark.table.return_value.schema = existing

        with pytest.raises(TTDConfigurationError) as exc_info:
            TtdDatabricksClient._validate_output_table_schema(spark, "stale_table", self._expected_schema())

        message = str(exc_info.value)
        assert UID2_RESOLUTIONS_COLUMN in message
        assert "ALTER TABLE stale_table ADD COLUMNS" in message
        assert "array<struct<submitted_id:string>>" in message


# --------------------------------------------------------------------------- #
# _call_api integration: resolution column is populated end-to-end              #
# --------------------------------------------------------------------------- #


def _make_client() -> TtdDatabricksClient:
    return TtdDatabricksClient(data_api_client=MagicMock(spec=DataClient))


def _make_rows(*dicts: dict) -> list[MagicMock]:
    rows = []
    for d in dicts:
        row = MagicMock()
        row.asDict.return_value = d
        rows.append(row)
    return rows


def test_call_api_attaches_singleton_array_for_single_id_endpoint() -> None:
    client = _make_client()
    rows = _make_rows(
        {"id_type": "Email", "id_value": "user@example.com", "segment_name": "seg"},
        {"id_type": "TDID", "id_value": "tdid-abc", "segment_name": "seg"},
    )
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.collect_raw_pii_ids_per_row.return_value = [["user@example.com"], []]
    mock_handler.call_api.return_value = (
        [],
        {"user@example.com": _mapped_resolution("uid2-resolved")},
    )

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(AdvertiserContext(advertiser_id="adv"), rows, batch_index=0)

    # Single-id endpoint emits the same array<struct> shape — singleton for mapped rows.
    assert len(results[0][UID2_RESOLUTIONS_COLUMN]) == 1
    assert results[0][UID2_RESOLUTIONS_COLUMN][0]["current_uid2"] == "uid2-resolved"
    assert results[0][UID2_RESOLUTIONS_COLUMN][0]["submitted_id"] == "user@example.com"
    # Row with non-PII id_type → empty array.
    assert results[1][UID2_RESOLUTIONS_COLUMN] == []


def test_call_api_attaches_uid2_resolutions_array_for_offline_conversion() -> None:
    client = _make_client()
    rows = _make_rows({"tracking_tag_id": "t", "timestamp_utc": datetime.now(timezone.utc)})
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock()]
    mock_handler.collect_raw_pii_ids_per_row.return_value = [["a@example.com"]]
    mock_handler.call_api.return_value = (
        [],
        {"a@example.com": _mapped_resolution("uid2-y")},
    )

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(OfflineConversionContext(data_provider_id="dp"), rows, batch_index=0)

    assert len(results[0][UID2_RESOLUTIONS_COLUMN]) == 1
    assert results[0][UID2_RESOLUTIONS_COLUMN][0]["current_uid2"] == "uid2-y"


def test_call_api_5xx_failure_leaves_resolution_null_for_all_rows() -> None:
    import httpx
    from ttd_data.errors import DataError

    client = _make_client()
    rows = _make_rows(
        {"id_type": "Email", "id_value": "u@example.com", "segment_name": "s"},
        {"id_type": "Email", "id_value": "v@example.com", "segment_name": "s"},
    )
    mock_handler = MagicMock()
    mock_handler.build_items.return_value = [MagicMock(), MagicMock()]
    mock_handler.collect_raw_pii_ids_per_row.return_value = [["u@example.com"], ["v@example.com"]]

    raw = MagicMock(spec=httpx.Response)
    raw.status_code = 503
    raw.text = "Service Unavailable"
    raw.headers = httpx.Headers({})
    mock_handler.call_api.side_effect = DataError("boom", raw)

    with patch("importlib.import_module", return_value=mock_handler):
        results = client._call_api(AdvertiserContext(advertiser_id="adv"), rows, batch_index=0)

    assert all(r["success"] is False for r in results)
    assert all(r[UID2_RESOLUTIONS_COLUMN] == [] for r in results)


# --------------------------------------------------------------------------- #
# from_params / batch_process config wiring                                     #
# --------------------------------------------------------------------------- #


def _sample_retry_config():
    from ttd_data.utils import BackoffStrategy, RetryConfig

    return RetryConfig(
        strategy="backoff",
        backoff=BackoffStrategy(initial_interval=1000, max_interval=10000, exponent=1.5, max_elapsed_time=30000),
        retry_connection_errors=True,
    )


def test_batch_process_config_is_derived_from_data_api_client() -> None:
    # batch_process rebuilds the per-worker DataClient from data_api_client.config.
    from ttd_data.uid2 import IdentityScope, UID2Config

    uid2_cfg = UID2Config(
        base_url="https://uid2.example.com",
        api_key="key",
        client_secret="secret",
        identity_scope=IdentityScope.UID2,
    )
    retry_cfg = _sample_retry_config()

    client = TtdDatabricksClient.from_params(api_token="tok", uid2_config=uid2_cfg, retry_config=retry_cfg)

    assert client._data_api_client.config.uid2_config is uid2_cfg
    assert client._data_api_client.config.retry_config is retry_cfg
    assert client._data_api_client.config.ttd_auth == "tok"
