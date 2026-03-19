# ttd-databricks

Python SDK for submitting Spark DataFrames to The Trade Desk Data API endpoints from Databricks. Supports both ad hoc and incremental batch processing with built-in schema validation and per-row error tracking.

## Table of Contents

- [SDK Installation](#sdk-installation)
- [Initial Setup](#initial-setup)
- [SDK Example Usage](#sdk-example-usage)
- [Authentication](#authentication)
- [Available Resources and Operations](#available-resources-and-operations)
- [Error Handling](#error-handling)
- [Server Selection](#server-selection)
- [Custom HTTP Client](#custom-http-client)

---

## SDK Installation

```bash
pip install ttd-databricks
```

Requires Python 3.10 or higher. Intended to run inside a Databricks environment where PySpark is available via the runtime.

---

## Initial Setup

### 1. Create a Client

The client is the entry point for all SDK operations. Create it once and reuse it across calls.

```python
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

# SparkSession is auto-detected from the Databricks runtime if not provided.
client = TtdDatabricksClient.from_params(api_token="<ttd-auth-token>")
```

See [Authentication](#authentication) for alternative client creation options.

---

### 2. Create a Context

A context specifies which TTD endpoint to target and carries the identifiers (advertiser ID, data provider ID, etc.) required by that endpoint. You create one context per endpoint you intend to use.

```python
from ttd_databricks_python.ttd_databricks import AdvertiserContext

# Each endpoint has its own context class. See Available Resources and Operations
# for the full list.
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    data_provider_id="<data-provider-id>",  # optional
)
```

---

### 3. Set Up Delta Tables

If you plan to use batch processing, create the required Delta tables upfront. These calls are idempotent — safe to run on every notebook execution.

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint

# Input table: schema matches the required columns for the chosen endpoint.
input_table = client.setup_input_table(endpoint=TTDEndpoint.ADVERTISER)

# Output table: mirrors the input schema plus status columns
# (success, error_code, error_message, processed_timestamp).
output_table = client.setup_output_table(endpoint=TTDEndpoint.ADVERTISER)

# Metadata table: tracks the last successful run timestamp for incremental processing.
metadata_table = client.setup_metadata_table()
```

You can also supply custom table names and storage locations:

```python
input_table = client.setup_input_table(
    endpoint=TTDEndpoint.ADVERTISER,
    table_name="my_catalog.my_schema.advertiser_input",
    location="abfss://container@storage.dfs.core.windows.net/advertiser_input",
)
```

---

## SDK Example Usage

The SDK supports two processing modes.

### Ad Hoc Mode (`push_data`)

Use this to process a DataFrame directly and receive results inline.

```python
from ttd_databricks_python.ttd_databricks import (
    TtdDatabricksClient,
    AdvertiserContext,
    TTDEndpoint,
)

# Create the client using your TTD auth token.
# SparkSession is auto-detected from the Databricks runtime if not provided.
client = TtdDatabricksClient.from_params(api_token="<ttd-auth-token>")

# Create a context for the target endpoint.
# The context identifies which advertiser/provider to push data to
# and is passed to every API call.
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    data_provider_id="<data-provider-id>",  # optional
)

# Push the DataFrame to the TTD API in batches.
# Returns the input DataFrame enriched with status columns.
result_df = client.push_data(
    df=input_df,
    context=context,
    batch_size=10,  # number of rows per API request
)
# result_df contains all input columns plus:
# success, error_code, error_message, processed_timestamp
```

### Batch Processing Mode (`batch_process`)

Use this for incremental, distributed processing backed by Delta tables. Supports checkpoint-based filtering to process only new records.

```python
# Create managed Delta tables for input, output, and processing metadata.
# These are idempotent — safe to call on every run.
input_table = client.setup_input_table(endpoint=TTDEndpoint.ADVERTISER)
output_table = client.setup_output_table(endpoint=TTDEndpoint.ADVERTISER)
metadata_table = client.setup_metadata_table()  # tracks last processed timestamp

# Run the batch pipeline. With process_new_records_only=True, only rows
# added since the last successful run (tracked via metadata_table) are sent.
client.batch_process(
    context=context,
    input_table=input_table,
    output_table=output_table,
    metadata_table=metadata_table,
    process_new_records_only=True,  # incremental; set False to reprocess all rows
    batch_size=10,                  # rows per API request
    parallelism=8,                  # number of concurrent Spark tasks
)
```

---

## Authentication

All API calls require a TTD auth token passed at client creation time.

### Factory Method (recommended for notebooks)

```python
client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",  # your TTD platform API token
    spark=spark,                   # optional; auto-detected from Databricks context
    server_url=None,               # optional; see Server Selection
)
```

### Dependency Injection (recommended for testing)

Provide your own `DataClient` instance to control the underlying HTTP transport directly.

```python
from ttd_data import DataClient
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

# Instantiate DataClient separately to configure HTTP settings (e.g. server URL).
data_client = DataClient()

client = TtdDatabricksClient(
    data_api_client=data_client,
    api_token="<ttd-auth-token>",
    spark=spark,  # optional
)
```

---

## Available Resources and Operations

Each endpoint is represented by a context dataclass that configures the API call.

### Advertiser — `/data/advertiser`

```python
from ttd_databricks_python.ttd_databricks import AdvertiserContext

# Targets the /data/advertiser endpoint.
# advertiser_id is required; data_provider_id scopes data to a specific provider.
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    data_provider_id="<data-provider-id>",  # optional
)
```

Required input columns: `id_type`, `id_value`, `segment_name`

---

### Third Party — `/data/thirdparty`

```python
from ttd_databricks_python.ttd_databricks import ThirdPartyContext

# Targets the /data/thirdparty endpoint.
# Set is_user_id_already_hashed=True if id_value is pre-hashed (e.g. SHA-256).
context = ThirdPartyContext(
    data_provider_id="<data-provider-id>",
    is_user_id_already_hashed=False,  # optional; default False
)
```

Required input columns: `id_type`, `id_value`, `segment_name`

---

### Offline Conversion — `/providerapi/offlineconversion`

```python
from ttd_databricks_python.ttd_databricks import OfflineConversionContext

# Targets the /providerapi/offlineconversion endpoint.
context = OfflineConversionContext(
    data_provider_id="<data-provider-id>",
)
```

Required input columns: `tracking_tag_id`, `timestamp_utc`

---

### Deletion / Opt-Out

Three endpoints are supported for DSR (Data Subject Request) workflows:

```python
from ttd_databricks_python.ttd_databricks import (
    DeletionOptOutAdvertiserContext,
    DeletionOptOutThirdPartyContext,
    DeletionOptOutMerchantContext,
    PartnerDsrRequestType,
)

# Use DELETION to remove user data, or OPT_OUT to suppress future targeting.

# Scoped to a specific advertiser
context = DeletionOptOutAdvertiserContext(
    advertiser_id="<advertiser-id>",
    request_type=PartnerDsrRequestType.DELETION,
)

# Scoped to a third-party data provider
context = DeletionOptOutThirdPartyContext(
    data_provider_id="<data-provider-id>",
    request_type=PartnerDsrRequestType.DELETION,
)

# Scoped to a merchant
context = DeletionOptOutMerchantContext(
    merchant_id=12345,
    request_type=PartnerDsrRequestType.DELETION,
)
```

Required input columns for all deletion/opt-out endpoints: `id_type`, `id_value`

---

### Inspecting Schemas

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint
from ttd_databricks_python.ttd_databricks.schemas import get_ttd_input_schema

schema = get_ttd_input_schema(TTDEndpoint.ADVERTISER)
schema.printTreeString()
```

---

## Error Handling

All SDK exceptions inherit from `TTDError`.

```python
from ttd_databricks_python.ttd_databricks.exceptions import (
    TTDError,
    TTDApiError,
    TTDConfigurationError,
    TTDSchemaValidationError,
)

try:
    result_df = client.push_data(df=input_df, context=context)
except TTDSchemaValidationError as e:
    print(f"Missing columns: {e.missing_columns}")
except TTDApiError as e:
    print(f"API error on batch {e.batch_index}: {e.status_code} — {e.response_text}")
except TTDConfigurationError as e:
    print(f"Configuration error: {e}")
```

| Exception | Cause |
|---|---|
| `TTDSchemaValidationError` | DataFrame is missing required columns for the endpoint |
| `TTDApiError` | HTTP error or no response from the TTD API |
| `TTDConfigurationError` | SparkSession not found or PySpark not installed |

For `push_data`, row-level errors are also captured inline in the result DataFrame via the `success`, `error_code`, and `error_message` columns — so processing is not interrupted by individual row failures.

---

## Server Selection

The default server is `https://usw-data.adsrvr.org`. You can override it globally or per-request.

### Global Override

```python
client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",
    server_url="https://custom-server.example.com",
)
```

### Per-Request Override

```python
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    base_url_override="https://custom-server.example.com",
)
```

---

## Custom HTTP Client

The underlying HTTP client is provided by the `ttd-data` SDK via `DataClient`. You can inject a custom instance to configure the server URL or connection behaviour.

```python
from ttd_data import DataClient
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

data_client = DataClient(server_url="https://custom-server.example.com")

client = TtdDatabricksClient(
    data_api_client=data_client,
    api_token="<ttd-auth-token>",
)
```

In batch processing mode, a `DataClient` singleton is maintained per Spark worker process to enable HTTP connection reuse across batches, reducing overhead during distributed execution.
