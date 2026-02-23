"""Typed context classes for TTD API endpoints."""

from typing import Optional

from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint


class AdvertiserContext:
    """Typed context for /data/advertiser endpoint."""

    def __init__(
        self,
        data_provider_id: str,
        advertiser_id: str,
        base_url_override: Optional[str] = None,
    ):
        """
        Context for advertiser endpoint operations.

        - data_provider_id: DataProviderId for API requests
        - advertiser_id: AdvertiserId for API requests
        - base_url_override: Optional. Overrides default base URL for this endpoint.
          If None, uses endpoint-specific default.
        """
        self.data_provider_id = data_provider_id
        self.advertiser_id = advertiser_id
        self.endpoint = TTDEndpoint.ADVERTISER
        self.base_url_override = base_url_override

    def __repr__(self) -> str:
        return (
            f"AdvertiserContext("
            f"data_provider_id={self.data_provider_id!r}, "
            f"advertiser_id={self.advertiser_id!r}, "
            f"endpoint={self.endpoint!r}, "
            f"base_url_override={self.base_url_override!r})"
        )


class ThirdPartyContext:
    """Typed context for /data/thirdparty endpoint."""

    def __init__(
        self,
        data_provider_id: str,
        base_url_override: Optional[str] = None,
        # Future: may have additional thirdparty-specific fields
    ):
        """
        Context for thirdparty endpoint operations.

        - data_provider_id: DataProviderId for API requests
        - base_url_override: Optional. Overrides default base URL for this endpoint.
          If None, uses endpoint-specific default.
        """
        self.data_provider_id = data_provider_id
        self.endpoint = TTDEndpoint.THIRDPARTY
        self.base_url_override = base_url_override

    def __repr__(self) -> str:
        return (
            f"ThirdPartyContext("
            f"data_provider_id={self.data_provider_id!r}, "
            f"endpoint={self.endpoint!r}, "
            f"base_url_override={self.base_url_override!r})"
        )


# Future endpoints can add their own context classes
