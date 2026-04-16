"""Unit tests for handler build_items() functions.

These are pure Python data-transformation tests — no Spark, no real API calls.
"""

from datetime import datetime, timezone

import ttd_databricks_python.ttd_databricks.handlers.advertiser as adv_handler
from ttd_databricks_python.ttd_databricks.id_types import normalize_id_type
import ttd_databricks_python.ttd_databricks.handlers.deletion_optout_advertiser as del_adv_handler
import ttd_databricks_python.ttd_databricks.handlers.deletion_optout_merchant as del_merch_handler
import ttd_databricks_python.ttd_databricks.handlers.deletion_optout_thirdparty as del_tp_handler
import ttd_databricks_python.ttd_databricks.handlers.offline_conversion as oc_handler
import ttd_databricks_python.ttd_databricks.handlers.third_party as tp_handler
from ttd_data.models import AdvertiserDataItem, OfflineConversionDataItem, PartnerDsrDataItem, ThirdPartyDataItem
from ttd_data.types import UNSET

# UNSET is not a singleton — the SDK creates fresh Unset() instances per field.
# Use isinstance check rather than identity (is).
_UnsetType = type(UNSET)


# --------------------------------------------------------------------------- #
# Advertiser handler                                                            #
# --------------------------------------------------------------------------- #


class TestAdvertiserBuildItems:
    _MINIMAL = {"id_type": "TDID", "id_value": "test-tdid-value", "segment_name": "test-segment-name"}

    def test_builds_advertiser_data_item_with_correct_fields(self):
        # Handler maps id_type → AdvertiserDataItem field dynamically: {d["id_type"]: d["id_value"]}
        item = adv_handler.build_items([self._MINIMAL])[0]
        assert isinstance(item, AdvertiserDataItem)
        assert getattr(item, "tdid") == "test-tdid-value"
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
        assert getattr(item, "tdid") == "test-tdid-value"
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
    assert getattr(item, "tdid") == "test-advertiser-tdid"


def test_deletion_optout_thirdparty_returns_partner_dsr_item_with_correct_id():
    item = del_tp_handler.build_items([{"id_type": "UID2", "id_value": "test-thirdparty-uid2"}])[0]
    assert isinstance(item, PartnerDsrDataItem)
    assert getattr(item, "uid2") == "test-thirdparty-uid2"


def test_deletion_optout_merchant_returns_partner_dsr_item_with_correct_id():
    item = del_merch_handler.build_items([{"id_type": "TDID", "id_value": "test-merchant-tdid"}])[0]
    assert isinstance(item, PartnerDsrDataItem)
    assert getattr(item, "tdid") == "test-merchant-tdid"


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

    def test_user_ids_converted_to_user_id_array_with_type_codes(self):
        row = {
            **self._MINIMAL,
            "user_ids": [{"type": "TDID", "id": "test-tdid-value"}, {"type": "DAID", "id": "test-daid-value"}],
        }
        item = oc_handler.build_items([row])[0]
        assert item.user_id_array == [["0", "test-tdid-value"], ["1", "test-daid-value"]]

    def test_all_user_id_types_map_to_correct_codes(self):
        type_map = {
            "TDID": "0", "DAID": "1", "UID2": "2", "UID2Token": "3",
            "EUID": "4", "EUIDToken": "5", "RampID": "6",
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