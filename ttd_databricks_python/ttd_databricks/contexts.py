"""Typed context classes for TTD API endpoints.

Note: context objects are passed directly into Spark UDF closures and serialized
via cloudpickle to worker processes. All fields must be picklable (strings, bools,
ints, None). Do not add fields that hold open connections, file handles, or locks.
"""

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
    is_user_id_already_hashed: If True, user IDs are treated as pre-hashed. Default False.
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    data_provider_id: str
    is_user_id_already_hashed: bool = False
    base_url_override: Optional[str] = None
    endpoint: TTDEndpoint = field(default=TTDEndpoint.THIRD_PARTY, init=False)
