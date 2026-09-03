"""Unit tests for handler build_items() functions.

These are pure Python data-transformation tests — no Spark, no real API calls.
"""

from datetime import datetime, timezone

import numpy as np
import pytest
from ttd_data.models import AdvertiserDataItem, OfflineConversionDataItem, PartnerDsrDataItem, ThirdPartyDataItem
from ttd_data.types import UNSET

import ttd_databricks_python.ttd_databricks.handlers.advertiser as adv_handler
import ttd_databricks_python.ttd_databricks.handlers.deletion_optout_advertiser as del_adv_handler
import ttd_databricks_python.ttd_databricks.handlers.deletion_optout_merchant as del_merch_handler
import ttd_databricks_python.ttd_databricks.handlers.deletion_optout_thirdparty as del_tp_handler
import ttd_databricks_python.ttd_databricks.handlers.offline_conversion as oc_handler
import ttd_databricks_python.ttd_databricks.handlers.third_party as tp_handler
from ttd_databricks_python.ttd_databricks.id_types import normalize_id_type

# UNSET is not a singleton — the SDK creates fresh Unset() instances per field.
# Use isinstance check rather than identity (is).
_UnsetType = type(UNSET)


# An array<struct> column reaches build_items as a list via the adhoc path
# (collect + asDict) and as a numpy array via the batch path (mapInPandas).
# build_items must handle both, so array-column tests run against each shape.
def _build_array_column(array_type: type, items: list[dict]) -> list | np.ndarray:
    return items if array_type is list else np.array(items, dtype=object)


# --------------------------------------------------------------------------- #
# Advertiser handler                                                            #
# --------------------------------------------------------------------------- #


class TestAdvertiserBuildItems:
    _MINIMAL = {"id_type": "TDID", "id_value": "test-tdid-value", "segment_name": "test-segment-name"}

    def test_builds_advertiser_data_item_with_correct_fields(self):
        # Handler maps id_type → AdvertiserDataItem field dynamically: {d["id_type"]: d["id_value"]}
        item = adv_handler.build_items([self._MINIMAL])[0]
        assert isinstance(item, AdvertiserDataItem)
        assert item.tdid == "test-tdid-value"
        assert item.data[0].name == "test-segment-name"

    def test_none_optional_fields_are_not_sent_to_api(self):
        # None values must remain UNSET — sending None would be an invalid API payload
        row = {**self._MINIMAL, "cookie_mapping_partner_id": None, "ttl_in_minutes": None}
        item = adv_handler.build_items([row])[0]
        assert isinstance(item.cookie_mapping_partner_id, _UnsetType)
        assert isinstance(item.data[0].ttl_in_minutes, _UnsetType)

    def test_optional_fields_are_passed_through_when_provided(self):
        row = {**self._MINIMAL, "ttl_in_minutes": 1440, "cookie_mapping_partner_id": "test-partner-id"}
        item = adv_handler.build_items([row])[0]
        assert item.data[0].ttl_in_minutes == 1440
        assert item.cookie_mapping_partner_id == "test-partner-id"

    def test_non_tdid_id_types_map_correctly(self):
        for id_type in ["DAID", "UID2", "RampID"]:
            row = {**self._MINIMAL, "id_type": id_type, "id_value": f"test-{id_type}-value"}
            assert getattr(adv_handler.build_items([row])[0], normalize_id_type(id_type)) == f"test-{id_type}-value"


# --------------------------------------------------------------------------- #
# Third Party handler                                                           #
# --------------------------------------------------------------------------- #


class TestThirdPartyBuildItems:
    _MINIMAL = {"id_type": "TDID", "id_value": "test-tdid-value", "segment_name": "test-segment-name"}

    def test_builds_third_party_data_item_with_correct_fields(self):
        item = tp_handler.build_items([self._MINIMAL])[0]
        assert isinstance(item, ThirdPartyDataItem)
        assert item.tdid == "test-tdid-value"
        assert item.data[0].name == "test-segment-name"

    def test_none_optional_fields_are_not_sent_to_api(self):
        row = {**self._MINIMAL, "cookie_mapping_partner_id": None, "ttl_in_minutes": None}
        item = tp_handler.build_items([row])[0]
        assert isinstance(item.cookie_mapping_partner_id, _UnsetType)
        assert isinstance(item.data[0].ttl_in_minutes, _UnsetType)

    def test_optional_fields_are_passed_through_when_provided(self):
        row = {**self._MINIMAL, "ttl_in_minutes": 720}
        assert tp_handler.build_items([row])[0].data[0].ttl_in_minutes == 720


# --------------------------------------------------------------------------- #
# Deletion/OptOut handlers                                                      #
# All three share the same PartnerDsrDataItem pattern.                         #
# --------------------------------------------------------------------------- #


def test_deletion_optout_advertiser_returns_partner_dsr_item_with_correct_id():
    item = del_adv_handler.build_items([{"id_type": "TDID", "id_value": "test-advertiser-tdid"}])[0]
    assert isinstance(item, PartnerDsrDataItem)
    assert item.tdid == "test-advertiser-tdid"


def test_deletion_optout_thirdparty_returns_partner_dsr_item_with_correct_id():
    item = del_tp_handler.build_items([{"id_type": "UID2", "id_value": "test-thirdparty-uid2"}])[0]
    assert isinstance(item, PartnerDsrDataItem)
    assert item.uid2 == "test-thirdparty-uid2"


def test_deletion_optout_merchant_returns_partner_dsr_item_with_correct_id():
    item = del_merch_handler.build_items([{"id_type": "TDID", "id_value": "test-merchant-tdid"}])[0]
    assert isinstance(item, PartnerDsrDataItem)
    assert item.tdid == "test-merchant-tdid"


# --------------------------------------------------------------------------- #
# Offline Conversion handler                                                    #
# --------------------------------------------------------------------------- #


class TestOfflineConversionBuildItems:
    _TS = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _MINIMAL = {"tracking_tag_id": "test-tracking-tag-id", "timestamp_utc": _TS}

    def test_builds_offline_conversion_data_item_with_correct_fields(self):
        item = oc_handler.build_items([self._MINIMAL])[0]
        assert isinstance(item, OfflineConversionDataItem)
        assert item.tracking_tag_id == "test-tracking-tag-id"
        assert isinstance(item.timestamp_utc, datetime)
        assert isinstance(item.user_id_array, _UnsetType)

    @pytest.mark.parametrize("array_type", [list, np.ndarray])
    def test_user_ids_converted_to_user_id_array_with_type_codes(self, array_type):
        row = {
            **self._MINIMAL,
            "user_ids": _build_array_column(
                array_type, [{"type": "TDID", "id": "test-tdid-value"}, {"type": "DAID", "id": "test-daid-value"}]
            ),
        }
        item = oc_handler.build_items([row])[0]
        assert item.user_id_array == [["0", "test-tdid-value"], ["1", "test-daid-value"]]

    def test_all_user_id_types_map_to_correct_codes(self):
        type_map = {
            "TDID": "0",
            "DAID": "1",
            "UID2": "2",
            "UID2Token": "3",
            "EUID": "4",
            "EUIDToken": "5",
            "RampID": "6",
        }
        for id_type, expected_code in type_map.items():
            row = {**self._MINIMAL, "user_ids": [{"type": id_type, "id": f"test-{id_type}-value"}]}
            assert oc_handler.build_items([row])[0].user_id_array[0][0] == expected_code

    def test_none_optional_fields_are_not_sent_to_api(self):
        row = {**self._MINIMAL, "order_id": None, "value": None}
        item = oc_handler.build_items([row])[0]
        assert isinstance(item.order_id, _UnsetType)
        assert isinstance(item.value, _UnsetType)

    def test_optional_fields_are_passed_through_when_provided(self):
        row = {**self._MINIMAL, "order_id": "test-order-id", "value": "99.99", "country": "US"}
        item = oc_handler.build_items([row])[0]
        assert item.order_id == "test-order-id"
        assert item.value == "99.99"
        assert item.country == "US"

    @pytest.mark.parametrize("array_type", [list, np.ndarray])
    def test_multi_element_line_items(self, array_type):
        row = {
            **self._MINIMAL,
            "line_items": _build_array_column(
                array_type,
                [
                    {"item_code": "sku1", "name": "first", "qty": "1", "price": "9.99", "cat": "books"},
                    {"item_code": "sku2", "name": "second", "qty": "2", "price": "5.00", "cat": "toys"},
                ],
            ),
        }
        item = oc_handler.build_items([row])[0]
        assert len(item.line_items) == 2
        assert item.line_items[0].item_code == "sku1"

    @pytest.mark.parametrize("array_type", [list, np.ndarray])
    def test_multi_element_privacy_settings(self, array_type):
        row = {
            **self._MINIMAL,
            "privacy_settings": _build_array_column(
                array_type,
                [
                    {"privacy_type": "GDPR", "is_applicable": "true", "consent_string": "abc"},
                    {"privacy_type": "CCPA", "is_applicable": "false", "consent_string": "xyz"},
                ],
            ),
        }
        item = oc_handler.build_items([row])[0]
        assert len(item.privacy_settings) == 2
        assert item.privacy_settings[0].privacy_type == "GDPR"

    @pytest.mark.parametrize("array_type", [list, np.ndarray])
    def test_collect_raw_pii_ids_keeps_only_pii_types(self, array_type):
        rows = [
            {
                **self._MINIMAL,
                "user_ids": _build_array_column(
                    array_type,
                    [{"type": "Email", "id": "a@example.com"}, {"type": "TDID", "id": "device-1"}],
                ),
            }
        ]
        assert oc_handler.collect_raw_pii_ids_per_row(rows) == [["a@example.com"]]

    def test_collect_raw_pii_ids_handles_missing_user_ids(self):
        assert oc_handler.collect_raw_pii_ids_per_row([self._MINIMAL]) == [[]]
