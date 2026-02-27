"""TTD API endpoint enumeration."""

from enum import Enum


class TTDEndpoint(str, Enum):
    """Enumeration of supported TTD API endpoints."""

    ADVERTISER = "advertiser"
    THIRDPARTY = "thirdparty"

    @property
    def path(self) -> str:
        """URL path segment for this endpoint."""
        return {
            TTDEndpoint.ADVERTISER: "/data/advertiser",
            TTDEndpoint.THIRDPARTY: "/data/thirdparty",
        }[self]

    @property
    def display_name(self) -> str:
        """Human-readable name for logging."""
        return {
            TTDEndpoint.ADVERTISER: "Advertiser",
            TTDEndpoint.THIRDPARTY: "Third Party",
        }[self]
