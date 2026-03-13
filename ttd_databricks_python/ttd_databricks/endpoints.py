"""TTD API endpoint enumeration."""

from __future__ import annotations

from enum import Enum


class TTDEndpoint(str, Enum):
    """Enumeration of supported TTD API endpoints."""

    ADVERTISER = "advertiser"
    THIRD_PARTY = "thirdparty"
    DELETION_OPTOUT_ADVERTISER = "deletion_optout_advertiser"
    DELETION_OPTOUT_THIRDPARTY = "deletion_optout_thirdparty"
    DELETION_OPTOUT_MERCHANT = "deletion_optout_merchant"

    @property
    def path(self) -> str:
        """URL path segment for this endpoint."""
        return {
            TTDEndpoint.ADVERTISER: "/data/advertiser",
            TTDEndpoint.THIRD_PARTY: "/data/thirdparty",
            TTDEndpoint.DELETION_OPTOUT_ADVERTISER: "/data/deletion-optout/advertiser",
            TTDEndpoint.DELETION_OPTOUT_THIRDPARTY: "/data/deletion-optout/thirdparty",
            TTDEndpoint.DELETION_OPTOUT_MERCHANT: "/data/deletion-optout/merchant",
        }[self]

    @property
    def display_name(self) -> str:
        """Human-readable name for logging."""
        return {
            TTDEndpoint.ADVERTISER: "Advertiser",
            TTDEndpoint.THIRD_PARTY: "Third Party",
            TTDEndpoint.DELETION_OPTOUT_ADVERTISER: "Deletion/Opt-Out Advertiser",
            TTDEndpoint.DELETION_OPTOUT_THIRDPARTY: "Deletion/Opt-Out Third Party",
            TTDEndpoint.DELETION_OPTOUT_MERCHANT: "Deletion/Opt-Out Merchant",
        }[self]

    @property
    def handler_module(self) -> str:
        """Fully-qualified module path for the endpoint-specific handler."""
        return {
            TTDEndpoint.ADVERTISER: "ttd_databricks_python.ttd_databricks.handlers.advertiser",
            TTDEndpoint.THIRD_PARTY: "ttd_databricks_python.ttd_databricks.handlers.third_party",
            TTDEndpoint.DELETION_OPTOUT_ADVERTISER: (
                "ttd_databricks_python.ttd_databricks.handlers.deletion_optout_advertiser"
            ),
            TTDEndpoint.DELETION_OPTOUT_THIRDPARTY: (
                "ttd_databricks_python.ttd_databricks.handlers.deletion_optout_thirdparty"
            ),
            TTDEndpoint.DELETION_OPTOUT_MERCHANT: (
                "ttd_databricks_python.ttd_databricks.handlers.deletion_optout_merchant"
            ),
        }[self]

    @property
    def schema_module(self) -> str:
        """Fully-qualified module path for the endpoint-specific schema definitions."""
        return {
            TTDEndpoint.ADVERTISER: "ttd_databricks_python.ttd_databricks.schemas.advertiser",
            TTDEndpoint.THIRD_PARTY: "ttd_databricks_python.ttd_databricks.schemas.third_party",
            TTDEndpoint.DELETION_OPTOUT_ADVERTISER: (
                "ttd_databricks_python.ttd_databricks.schemas.deletion_optout_advertiser"
            ),
            TTDEndpoint.DELETION_OPTOUT_THIRDPARTY: (
                "ttd_databricks_python.ttd_databricks.schemas.deletion_optout_thirdparty"
            ),
            TTDEndpoint.DELETION_OPTOUT_MERCHANT: (
                "ttd_databricks_python.ttd_databricks.schemas.deletion_optout_merchant"
            ),
        }[self]
