"""TTD Databricks SDK - public API surface."""

from ttd_data.models import DataOrigin, DataOriginType

from ttd_databricks_python.ttd_databricks.constants import TTD_DATABRICKS_SDK_ORIGIN_ID
from ttd_databricks_python.ttd_databricks.contexts import (
    AdvertiserContext,
    DeletionOptOutAdvertiserContext,
    DeletionOptOutMerchantContext,
    DeletionOptOutThirdPartyContext,
    OfflineConversionContext,
    ThirdPartyContext,
    TTDContext,
)
from ttd_databricks_python.ttd_databricks.endpoints import TTDEndpoint
from ttd_databricks_python.ttd_databricks.exceptions import (
    TTDApiError,
    TTDConfigurationError,
    TTDError,
    TTDSchemaValidationError,
)
from ttd_databricks_python.ttd_databricks.schemas import (
    SchemaType,
    get_metadata_schema,
    get_output_schema,
    get_ttd_input_schema,
    validate_ttd_schema,
)
from ttd_databricks_python.ttd_databricks.ttd_client import TtdDatabricksClient

__all__ = [
    # Client
    "TtdDatabricksClient",
    # DataOrigins
    "DataOrigin",
    "DataOriginType",
    "TTD_DATABRICKS_SDK_ORIGIN_ID",
    # Contexts
    "TTDContext",
    "AdvertiserContext",
    "ThirdPartyContext",
    "DeletionOptOutAdvertiserContext",
    "DeletionOptOutThirdPartyContext",
    "DeletionOptOutMerchantContext",
    "OfflineConversionContext",
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
