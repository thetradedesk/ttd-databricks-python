"""TTD Databricks SDK - public API surface."""

from ttd_databricks_python.ttd_databricks.contexts import (
    TTDContext,
    AdvertiserContext,
    ThirdPartyContext,
    DeletionOptOutAdvertiserContext,
    DeletionOptOutThirdPartyContext,
    DeletionOptOutMerchantContext,
)
from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint
from ttd_databricks_python.ttd_databricks.exceptions import (
    TTDError,
    TTDApiError,
    TTDConfigurationError,
    TTDSchemaValidationError,
)
from ttd_databricks_python.ttd_databricks.schemas import (
    SchemaType,
    get_ttd_input_schema,
    get_output_schema,
    get_metadata_schema,
    validate_ttd_schema,
)
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient

__all__ = [
    # Client
    "TtdDatabricksClient",
    # Contexts
    "TTDContext",
    "AdvertiserContext",
    "ThirdPartyContext",
    "DeletionOptOutAdvertiserContext",
    "DeletionOptOutThirdPartyContext",
    "DeletionOptOutMerchantContext",
    # Endpoints
    "TTDEndpoint",
    # Exceptions
    "TTDError",
    "TTDApiError",
    "TTDConfigurationError",
    "TTDSchemaValidationError",
    # Schemas
    "SchemaType",
    "get_ttd_input_schema",
    "get_output_schema",
    "get_metadata_schema",
    "validate_ttd_schema",
]
