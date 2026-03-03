"""TTD API endpoint enumeration."""

from enum import Enum


class TTDEndpoint(str, Enum):
    """Enumeration of supported TTD API endpoints."""

    ADVERTISER = "advertiser"
    THIRD_PARTY = "thirdparty"

    @property
    def path(self) -> str:
        """URL path segment for this endpoint."""
        return {
            TTDEndpoint.ADVERTISER: "/data/advertiser",
            TTDEndpoint.THIRD_PARTY: "/data/thirdparty",
        }[self]

    @property
    def display_name(self) -> str:
        """Human-readable name for logging."""
        return {
            TTDEndpoint.ADVERTISER: "Advertiser",
            TTDEndpoint.THIRD_PARTY: "Third Party",
        }[self]

    @property
    def handler_module(self) -> str:
        """Fully-qualified module path for the endpoint-specific handler."""
        return {
            TTDEndpoint.ADVERTISER:  "ttd_databricks_python.ttd_databricks.handlers.advertiser",
            TTDEndpoint.THIRD_PARTY: "ttd_databricks_python.ttd_databricks.handlers.third_party",
        }[self]

    @property
    def schema_module(self) -> str:
        """Fully-qualified module path for the endpoint-specific schema definitions."""
        return {
            TTDEndpoint.ADVERTISER:  "ttd_databricks_python.ttd_databricks.schemas.advertiser",
            TTDEndpoint.THIRD_PARTY: "ttd_databricks_python.ttd_databricks.schemas.third_party",
        }[self]
