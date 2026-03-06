"""Typed context classes for TTD API endpoints.

Note: context objects are passed directly into Spark UDF closures and serialized
via cloudpickle to worker processes. All fields must be picklable (strings, bools,
ints, None). Do not add fields that hold open connections, file handles, or locks.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

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

    data_provider_id: DataProviderId for API requests.
    advertiser_id: AdvertiserId for API requests.
    base_url_override: Overrides default base URL. If None, uses endpoint-specific default.
    """

    data_provider_id: str
    advertiser_id: str
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
