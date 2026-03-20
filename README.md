# ttd-databricks

Python SDK for integrating Databricks with The Trade Desk Data API. Supports First Party Data, Third Party Data, Offline Conversion Data, and Deletion/Opt-Out workflows.

- **Ad hoc mode** — push a DataFrame directly and receive per-row results inline
- **Batch mode** — run incremental pipelines backed by Delta tables with processing checkpoints
- Built-in schema validation and per-row error tracking

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

A context specifies which TTD endpoint to target and carries the identifiers (advertiser ID, data provider ID, etc.) required by that endpoint. A single context can be created per endpoint and reused across multiple calls.

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

If you plan to use batch processing, use the following helpers to set up the input, output, and metadata Delta tables. These can be created once and reused for all future executions.

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint

# Input table: schema matches the required columns for the chosen endpoint.
# Created as a managed table in the default metastore location if no location is provided.
# Default table name: ttd_{endpoint}_input (e.g. "ttd_advertiser_input").
input_table = client.setup_input_table(endpoint=TTDEndpoint.ADVERTISER)

# Output table: mirrors the input schema plus status columns
# (success, error_code, error_message, processed_timestamp).
# Default table name: ttd_{endpoint}_output (e.g. "ttd_advertiser_output").
output_table = client.setup_output_table(endpoint=TTDEndpoint.ADVERTISER)

# Metadata table: tracks run history (last_processed_date, run_timestamp, records_processed).
# Default table name: "ttd_metadata".
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

# Push the DataFrame to the TTD Data API in batches.
# Returns the input DataFrame enriched with status columns.
result_df = client.push_data(
    df=input_df,
    context=context,
    batch_size=1600,  # number of rows per API request
)
# result_df contains all input columns plus:
# success, error_code, error_message, processed_timestamp
```

### Batch Processing Mode (`batch_process`)

Use this for incremental, distributed processing backed by Delta tables. Supports incremental filtering to process only records added since the last run.

```python
# Tables set up during Initial Setup (see above).
# input_table, output_table, metadata_table already created.

# Run the batch pipeline. With process_new_records_only=True, only rows
# added since the last successful run (tracked via metadata_table) are sent.
client.batch_process(
    context=context,
    input_table=input_table,
    output_table=output_table,
    metadata_table=metadata_table,
    process_new_records_only=True,  # incremental; set False to reprocess all rows
    batch_size=1600,                  # rows per API request
    parallelism=16,                  # number of concurrent Spark tasks
)
```

To reprocess from a specific date (e.g. for a backfill), use `last_processed_date_override` to override the last processed date stored in the metadata table:

```python
from datetime import datetime

client.batch_process(
    context=context,
    input_table=input_table,
    output_table=output_table,
    metadata_table=metadata_table,
    process_new_records_only=True,
    last_processed_date_override=datetime(2025, 1, 1),  # reprocess from this date
)
```

---

## Authentication

All API calls require a TTD auth token passed at client creation time.

### Factory Method (recommended for notebooks)

```python
# spark is the SparkSession available in the Databricks notebook runtime.
client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",  # your TTD platform API token
    spark=spark,                   # optional; auto-detected from Databricks context
    # server_url="https://..."      # optional; see Server Selection
)
```

### Dependency Injection (recommended for testing)

Provide your own [`DataClient`](https://github.com/thetradedesk/ttd-data-python/blob/main/src/ttd_data/sdk.py) instance to control the underlying HTTP transport directly.
Use this when you need to configure options not exposed by `from_params()`, or to inject a mock in tests.

```python
from ttd_data import DataClient
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

# Configure DataClient with custom HTTP settings.
data_client = DataClient(
    server_url="https://custom-server.example.com",  # override default server URL
    timeout_ms=10000,                                 # request timeout in milliseconds
)

client = TtdDatabricksClient(
    data_api_client=data_client,
    api_token="<ttd-auth-token>",
    spark=spark,  # optional; spark variable available from the Databricks notebook runtime
)
```

---

## Available Resources and Operations

Each endpoint is represented by a context dataclass that configures the API call.

### First-Party Data — `/data/advertiser`

```python
from ttd_databricks_python.ttd_databricks import AdvertiserContext

# Targets the /data/advertiser endpoint.
# advertiser_id is required; data_provider_id scopes data to a specific provider.
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    data_provider_id="<data-provider-id>",  # optional
)
```

See [schema](#inspecting-schemas) for required and optional columns — `TTDEndpoint.ADVERTISER`

---

### Third-Party Data — `/data/thirdparty`

```python
from ttd_databricks_python.ttd_databricks import ThirdPartyContext

# Targets the /data/thirdparty endpoint.
# Set is_user_id_already_hashed=True if id_value is pre-hashed (e.g. SHA-256).
context = ThirdPartyContext(
    data_provider_id="<data-provider-id>",
    is_user_id_already_hashed=False,  # optional; default False
)
```

See [schema](#inspecting-schemas) for required and optional columns — `TTDEndpoint.THIRD_PARTY`

---

### Offline Conversion — `/providerapi/offlineconversion`

```python
from ttd_databricks_python.ttd_databricks import OfflineConversionContext

# Targets the /providerapi/offlineconversion endpoint.
context = OfflineConversionContext(
    data_provider_id="<data-provider-id>",
)
```

See [schema](#inspecting-schemas) for required and optional columns — `TTDEndpoint.OFFLINE_CONVERSION`

---

### Deletion / Opt-Out — Advertiser — `/data/deletion-optout/advertiser`

Deletion/Opt-Out endpoint scoped to a specific advertiser.

```python
from ttd_databricks_python.ttd_databricks import DeletionOptOutAdvertiserContext, PartnerDsrRequestType

# request_type controls the action:
#   PartnerDsrRequestType.DELETION  — remove user data
#   PartnerDsrRequestType.OPT_OUT   — suppress future targeting
context = DeletionOptOutAdvertiserContext(
    advertiser_id="<advertiser-id>",
    request_type=PartnerDsrRequestType.OPT_OUT,  # or OPT_OUT
    data_provider_id="<data-provider-id>",        # optional
)
```

See [schema](#inspecting-schemas) for required and optional columns — `TTDEndpoint.DELETION_OPTOUT_ADVERTISER`

---

### Deletion / Opt-Out — Third Party — `/data/deletion-optout/thirdparty`

Deletion/Opt-Out endpoint scoped to a third-party data provider.

```python
from ttd_databricks_python.ttd_databricks import DeletionOptOutThirdPartyContext, PartnerDsrRequestType

context = DeletionOptOutThirdPartyContext(
    data_provider_id="<data-provider-id>",
    request_type=PartnerDsrRequestType.OPT_OUT,  # or OPT_OUT
    brand_id="<brand-id>",                        # optional
)
```

See [schema](#inspecting-schemas) for required and optional columns — `TTDEndpoint.DELETION_OPTOUT_THIRDPARTY`

---

### Deletion / Opt-Out — Merchant — `/data/deletion-optout/merchant`

Deletion/Opt-Out endpoint scoped to a merchant.

```python
from ttd_databricks_python.ttd_databricks import DeletionOptOutMerchantContext, PartnerDsrRequestType

context = DeletionOptOutMerchantContext(
    merchant_id="<merchant-id>",
    request_type=PartnerDsrRequestType.OPT_OUT,  # or OPT_OUT
)
```

See [schema](#inspecting-schemas) for required and optional columns — `TTDEndpoint.DELETION_OPTOUT_MERCHANT`

---

### Inspecting Schemas

Retrieve the full input schema for an endpoint:

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint
from ttd_databricks_python.ttd_databricks.schemas import get_ttd_input_schema

schema = get_ttd_input_schema(TTDEndpoint.ADVERTISER)
schema.printTreeString()
```

Get just the required column names (useful for DataFrame preparation):

```python
from ttd_databricks_python.ttd_databricks.schemas import get_required_column_names

required_cols = get_required_column_names(TTDEndpoint.ADVERTISER)
# e.g. ["id_type", "id_value", "segment_name"]
```

Pre-validate a DataFrame before calling `push_data` to catch schema issues early:

```python
from ttd_databricks_python.ttd_databricks.schemas import validate_ttd_schema

# Raises TTDSchemaValidationError if any required columns are missing.
validate_ttd_schema(df=input_df, endpoint=TTDEndpoint.ADVERTISER)
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
| `TTDApiError` | HTTP error or no response from the TTD Data API |
| `TTDConfigurationError` | SparkSession not found or PySpark not installed |

For `push_data`, row-level errors are also captured inline in the result DataFrame via the `success`, `error_code`, and `error_message` columns — so processing is not interrupted by individual row failures.

---

## Server Selection

Each endpoint has its own default server URL, sourced from the `ttd-data` SDK:

| Endpoint | Path | Default Server |
|---|---|---|
| First-Party Data | `/data/advertiser` | `https://usw-data.adsrvr.org` |
| Third-Party Data | `/data/thirdparty` | `https://bulk-data.adsrvr.org` |
| Offline Conversion | `/providerapi/offlineconversion` | `https://offlineattrib.adsrvr.org` |
| Deletion / Opt-Out — Advertiser | `/data/deletion-optout/advertiser` | `https://usw-data.adsrvr.org` |
| Deletion / Opt-Out — Third Party | `/data/deletion-optout/thirdparty` | `https://usw-data.adsrvr.org` |
| Deletion / Opt-Out — Merchant | `/data/deletion-optout/merchant` | `https://usw-data.adsrvr.org` |

These can be overridden globally at the client level, or per-request via the context.

### Global Override

Applies to all endpoints on the client:

```python
client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",
    server_url="https://custom-server.example.com",
)
```

### Per-Request Override

Applies only to calls made with that context, leaving the client default unchanged for other endpoints:

```python
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    base_url_override="https://custom-server.example.com",
)
```

---

## Custom HTTP Client

The underlying HTTP client is provided by the `ttd-data` SDK via [`DataClient`](https://github.com/thetradedesk/ttd-data-python/blob/main/src/ttd_data/sdk.py). You can inject a custom instance to configure the server URL or connection behaviour.

```python
from ttd_data import DataClient
from ttd_data.utils.retries import BackoffStrategy, RetryConfig
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

data_client = DataClient(
    server_url="https://custom-server.example.com",  # override default server URL
    timeout_ms=10000,                                 # request timeout in milliseconds
    retry_config=RetryConfig("backoff", BackoffStrategy(1000, 60000, 1.5, 3600000), True),  # custom retry config
)

client = TtdDatabricksClient(
    data_api_client=data_client,
    api_token="<ttd-auth-token>",
)
```

In batch processing mode, a `DataClient` singleton is maintained per Spark worker process to enable HTTP connection reuse across batches, reducing overhead during distributed execution.
