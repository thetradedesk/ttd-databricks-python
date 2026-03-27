"""Unit tests for ttd_databricks.contexts."""

import pickle

from ttd_data.models import DataOrigin, DataOriginType, PartnerDsrRequestType

from ttd_databricks_python.ttd_databricks.contexts import (
    AdvertiserContext,
    DeletionOptOutAdvertiserContext,
    DeletionOptOutMerchantContext,
    DeletionOptOutThirdPartyContext,
    OfflineConversionContext,
    ThirdPartyContext,
)
from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint


_REQUEST_TYPE = PartnerDsrRequestType.OPT_OUT
_DATA_ORIGINS = [DataOrigin(id="test-origin", type=DataOriginType.DATA_PROVIDER)]


# --------------------------------------------------------------------------- #
# Meaningful defaults                                                           #
# --------------------------------------------------------------------------- #


def test_third_party_is_user_id_already_hashed_defaults_false():
    # False is the safe default — callers rely on this to avoid accidentally
    # treating unhashed IDs as pre-hashed
    assert ThirdPartyContext(data_provider_id="prov123").is_user_id_already_hashed is False


# --------------------------------------------------------------------------- #
# Pickle serialization                                                          #
# Contexts are serialized into Spark UDF closures via cloudpickle.             #
# Fields and endpoint must survive the round-trip.                             #
# --------------------------------------------------------------------------- #


def test_advertiser_context_verify_context_pickling():
    ctx = AdvertiserContext(advertiser_id="adv123", data_provider_id="prov456", data_origins=_DATA_ORIGINS)
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.advertiser_id == "adv123"
    assert restored.data_provider_id == "prov456"
    assert restored.endpoint == TTDEndpoint.ADVERTISER
    assert restored.data_origins[0].id == "test-origin"
    assert restored.data_origins[0].type == DataOriginType.DATA_PROVIDER


def test_third_party_context_verify_context_pickling():
    ctx = ThirdPartyContext(data_provider_id="prov123", is_user_id_already_hashed=True, data_origins=_DATA_ORIGINS)
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.data_provider_id == "prov123"
    assert restored.is_user_id_already_hashed is True
    assert restored.data_origins[0].id == "test-origin"
    assert restored.data_origins[0].type == DataOriginType.DATA_PROVIDER


def test_offline_conversion_context_verify_context_pickling():
    ctx = OfflineConversionContext(data_provider_id="prov123", data_origins=_DATA_ORIGINS)
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.data_provider_id == "prov123"
    assert restored.data_origins[0].id == "test-origin"
    assert restored.data_origins[0].type == DataOriginType.DATA_PROVIDER


def test_deletion_optout_advertiser_context_verify_context_pickling():
    ctx = DeletionOptOutAdvertiserContext(advertiser_id="adv123", request_type=_REQUEST_TYPE)
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.advertiser_id == "adv123"
    assert restored.request_type == _REQUEST_TYPE


def test_deletion_optout_thirdparty_context_verify_context_pickling():
    ctx = DeletionOptOutThirdPartyContext(
        data_provider_id="prov123", request_type=_REQUEST_TYPE, brand_id="brand99"
    )
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.data_provider_id == "prov123"
    assert restored.request_type == _REQUEST_TYPE
    assert restored.brand_id == "brand99"


def test_deletion_optout_merchant_context_verify_context_pickling():
    ctx = DeletionOptOutMerchantContext(merchant_id=99, request_type=_REQUEST_TYPE)
    restored = pickle.loads(pickle.dumps(ctx))
    assert restored.merchant_id == 99
    assert restored.request_type == _REQUEST_TYPE
