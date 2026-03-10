"""Typed context classes for TTD API endpoints.

Note: context objects are passed directly into Spark UDF closures and serialized
via cloudpickle to worker processes. All fields must be picklable (strings, bools,
ints, None, or picklable Enums). Do not add fields that hold open connections,
file handles, or locks.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

from ttd_data.models import PartnerDsrRequestType

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint


@dataclass(kw_only=True)
class TTDContext(ABC):
    """Base class for all TTD endpoint context objects.

    base_url_override: Overrides the default base URL for API requests.
    If None, each endpoint uses its own default.
    """

    base_url_override: Optional[str] = None

    @property
    @abstractmethod
    def endpoint(self) -> TTDEndpoint:
        """The TTD API endpoint this context targets."""


@dataclass
class AdvertiserContext(TTDContext):
    """Typed context for /data/advertiser endpoint.

    advertiser_id: AdvertiserId for API requests.
    data_provider_id: Optional DataProviderId for API requests. Default None (omitted from request).
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    advertiser_id: str
    data_provider_id: Optional[str] = None
    endpoint: TTDEndpoint = field(default=TTDEndpoint.ADVERTISER, init=False)


@dataclass
class ThirdPartyContext(TTDContext):
    """Typed context for /data/thirdparty endpoint.

    data_provider_id: DataProviderId for API requests.
    is_user_id_already_hashed: If True, user IDs are treated as pre-hashed. Default False.
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    data_provider_id: str
    is_user_id_already_hashed: bool = False
    endpoint: TTDEndpoint = field(default=TTDEndpoint.THIRD_PARTY, init=False)


@dataclass
class DeletionOptOutAdvertiserContext(TTDContext):
    """Typed context for /data/deletion-optout/advertiser endpoint.

    advertiser_id: AdvertiserId for API requests.
    request_type: Either "OptOut" or "Deletion".
    data_provider_id: Optional DataProviderId for API requests. Default None (omitted from request).
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    advertiser_id: str
    request_type: PartnerDsrRequestType
    data_provider_id: Optional[str] = None
    endpoint: TTDEndpoint = field(default=TTDEndpoint.DELETION_OPTOUT_ADVERTISER, init=False)


@dataclass
class DeletionOptOutThirdPartyContext(TTDContext):
    """Typed context for /data/deletion-optout/thirdparty endpoint.

    data_provider_id: DataProviderId for API requests.
    request_type: Either "OptOut" or "Deletion".
    brand_id: Optional BrandId for API requests. Default None.
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    data_provider_id: str
    request_type: PartnerDsrRequestType
    brand_id: Optional[str] = None
    endpoint: TTDEndpoint = field(default=TTDEndpoint.DELETION_OPTOUT_THIRDPARTY, init=False)


@dataclass
class DeletionOptOutMerchantContext(TTDContext):
    """Typed context for /data/deletion-optout/merchant endpoint.

    merchant_id: MerchantId for API requests.
    request_type: Either "OptOut" or "Deletion".
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    merchant_id: int
    request_type: PartnerDsrRequestType
    endpoint: TTDEndpoint = field(default=TTDEndpoint.DELETION_OPTOUT_MERCHANT, init=False)
