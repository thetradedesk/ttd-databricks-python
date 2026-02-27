"""Typed context classes for TTD API endpoints."""

from dataclasses import dataclass, field
from typing import Optional

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint


@dataclass
class AdvertiserContext:
    """Typed context for /data/advertiser endpoint.

    data_provider_id: DataProviderId for API requests.
    advertiser_id: AdvertiserId for API requests.
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    data_provider_id: str
    advertiser_id: str
    base_url_override: Optional[str] = None
    endpoint: TTDEndpoint = field(default=TTDEndpoint.ADVERTISER, init=False)


@dataclass
class ThirdPartyContext:
    """Typed context for /data/thirdparty endpoint.

    data_provider_id: DataProviderId for API requests.
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    data_provider_id: str
    base_url_override: Optional[str] = None
    endpoint: TTDEndpoint = field(default=TTDEndpoint.THIRDPARTY, init=False)


# Future endpoints can add their own context classes
