# ttd-databricks

> **Alpha:** This SDK is in early development. APIs may change without notice between releases.

Python SDK for integrating Databricks with The Trade Desk Data API. Supports First Party Data, Third Party Data, Offline Conversion Data, and Deletion/Opt-Out workflows.

**Key features:**

- **Ad hoc mode** — push a DataFrame directly and receive per-row results inline
- **Batch mode** — run incremental pipelines backed by Delta tables with processing checkpoints
- **Schema validation and error tracking** — required columns are checked before submission, and every row comes back with its own success or error status

## Table of Contents

- [Example Notebooks](#example-notebooks)
- [SDK Installation](#sdk-installation)
- [Quickstart](#quickstart)
  - [1. Create a Client](#1-create-a-client)
    - [Authentication](#authentication)
  - [2. Create a Context](#2-create-a-context)
  - [3. Inspect the Schema and Prepare Your Input DataFrame](#3-inspect-the-schema-and-prepare-your-input-dataframe)
  - [4. Send the Data](#4-send-the-data)
    - [4a. Ad Hoc — `push_data`](#4a-ad-hoc--push_data)
    - [4b. Batch Processing — `batch_process`](#4b-batch-processing--batch_process)
- [Supported Data API Endpoints](#supported-data-api-endpoints) — by use case:
  - [First-Party Data (1P)](#first-party-data--dataadvertiser) — `/data/advertiser`
  - [Third-Party Data (3P)](#third-party-data--datathirdparty) — `/data/thirdparty`
  - [Offline Conversion (CAPI)](#offline-conversion--providerapiofflineconversion) — `/providerapi/offlineconversion`
  - [Deletion / Opt-Out — Advertiser](#deletion--opt-out--advertiser--datadeletion-optoutadvertiser) — `/data/deletion-optout/advertiser`
  - [Deletion / Opt-Out — Third Party](#deletion--opt-out--third-party--datadeletion-optoutthirdparty) — `/data/deletion-optout/thirdparty`
  - [Deletion / Opt-Out — Merchant](#deletion--opt-out--merchant--datadeletion-optoutmerchant) — `/data/deletion-optout/merchant`
- [Error Handling](#error-handling)
- [Optional Configuration](#optional-configuration)
  - [UID2 Support](#uid2-support)
  - [Server Selection](#server-selection)
  - [Custom HTTP Client](#custom-http-client)

## Example Notebooks

The following table maps each use case supported by the SDK to the Trade Desk endpoint its data is sent to, and to a quickstart example notebook.

The example notebooks are for users who want to dive straight in and try the SDK — each one is runnable and covers the whole flow, from credentials through to reading per-row results. The sections after this break the same integration down step by step.

| Use case | Destination Endpoint to Which Data Is Sent | Example Notebook |
|---|---|---|
| First-party data (1P) | `POST /data/advertiser` | [First Party Data (1PD) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/First%20Party%20Data%20%281PD%29%20Example%20Notebook.ipynb) |
| Third-party data (3P) | `POST /data/thirdparty` | [Third Party Data (3PD) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Third%20Party%20Data%20%283PD%29%20Example%20Notebook.ipynb) |
| Offline conversion (CAPI) | `POST /providerapi/offlineconversion` | [Offline Conversion Data (CAPI) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Offline%20Conversion%20Data%20%28CAPI%29%20Example%20Notebook.ipynb) |
| Deletion and opt-out | `POST /data/deletion-optout/*` | [Deletion and Opt-Out (DSR) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Deletion%20and%20Opt-Out%20%28DSR%29%20Example%20Notebook.ipynb) |

---

## SDK Installation

```bash
pip install ttd-databricks
```

Requires Python 3.10 or higher. Intended to run inside a Databricks environment where PySpark is available via the runtime.

---

## Quickstart

The following steps break down the process of integrating with the ttd-databricks SDK, using first-party data as the worked example. Every other use case follows the same steps with a different context and different input columns — see [Supported Data API Endpoints](#supported-data-api-endpoints) for the use cases supported and the example notebook for each.

### 1. Create a Client

The client is the entry point for all SDK operations. Create it once and reuse it across calls. There are two ways to create it — pick one.

**i) Factory — `from_params` builds the `DataClient` for you:**

```python
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",
    # spark=spark,                   # optional; auto-detected from the Databricks runtime
    # server_url="https://...",      # optional; see Server Selection
    # retry_config=RetryConfig(...), # optional; 429/5xx are retried by default, None disables
    # timeout_ms=10000,              # optional; per-request timeout in milliseconds
)
```

**ii) Dependency injection — you build the `DataClient` and pass it in:**

```python
from ttd_data import DataClient
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

client = TtdDatabricksClient(
    data_api_client=DataClient(),
    api_token="<ttd-auth-token>",
)
```

The rest of this README uses (i). To configure the underlying HTTP transport, or to inject a mock in tests, use (ii) — see [Custom HTTP Client](#custom-http-client).

#### Authentication

All underlying API calls made within the SDK authenticate with a TTD API token, passed as `api_token` at client creation as shown above and sent as the `TTD-Auth` header. The SDK does not support `TtdSignature` based authentication.

See [OpenTTD](https://open.thetradedesk.com/advertiser/docsApp/Foundations/resources/doc/PlatformAuthentication) for instructions on how to create your API token.

---

### 2. Create a Context

A context specifies which TTD endpoint to target and carries the identifiers (advertiser ID, data provider ID, etc.) required by that endpoint. A single context can be created per endpoint and reused across multiple calls.

```python
from ttd_databricks_python.ttd_databricks import AdvertiserContext

# Each endpoint has its own context class. See Supported Data API Endpoints
# for the full list.
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    data_provider_id="<data-provider-id>",  # optional
)
```

---

### 3. Inspect the Schema and Prepare Your Input DataFrame

Each endpoint has its own input schema. Retrieve it, and the subset of columns that are mandatory, straight from the SDK:

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint, get_ttd_input_schema
from ttd_databricks_python.ttd_databricks.schemas import get_required_column_names

input_schema = get_ttd_input_schema(TTDEndpoint.ADVERTISER)

for field in input_schema.fields:
    print(f"{field.name}: {field.dataType.simpleString()} (nullable={field.nullable})")

required_cols = get_required_column_names(TTDEndpoint.ADVERTISER)
# e.g. ["id_type", "id_value", "segment_name"]
```

Nullable columns may be omitted from your DataFrame — they are filled with null automatically.

Now build the DataFrame. Always pass `schema=`. Without it Spark infers the types, and nested columns (such as offline conversion's `user_ids`) come out as `MapType` instead of the `array<struct>` the API requires.

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint, get_ttd_input_schema

input_schema = get_ttd_input_schema(TTDEndpoint.ADVERTISER)

rows = [
    {"id_type": "TDID",   "id_value": "123e4567-e89b-12d3-a456-426652340000",
     "segment_name": "my_first_segment", "ttl_in_minutes": 43200},
    {"id_type": "DAID",   "id_value": "a9342d1f-69f1-4bf8-bc2b-1f20eb451f21",
     "segment_name": "my_first_segment", "ttl_in_minutes": 43200},
    {"id_type": "UID2",   "id_value": "48MjlfIUZpOKNAm9nod7/jCLAXUYsnE1tpVHQSDS0uo=",
     "segment_name": "my_first_segment", "ttl_in_minutes": 43200},
]

# spark is the SparkSession available in the Databricks notebook runtime.
input_df = spark.createDataFrame(rows, schema=input_schema)
```

Optionally, pre-validate the DataFrame to catch missing columns before you send anything:

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint
from ttd_databricks_python.ttd_databricks.schemas import validate_ttd_schema

# Raises TTDSchemaValidationError if any required columns are missing.
validate_ttd_schema(df=input_df, endpoint=TTDEndpoint.ADVERTISER)
```

> **Tip:** Start with a handful of rows. Confirming the end-to-end flow on a small sample is much easier to troubleshoot than a full load.

---

### 4. Send the Data

There are two ways to send your data. Pick one — you do not need both.

| | Ad hoc (`push_data`) | Batch processing (`batch_process`) |
|---|---|---|
| **State management** | None. Every call sends every row you give it. | Provided. A metadata table records progress, so each run sends only rows added since the last one. |
| **Input** | A DataFrame you build in the notebook | A Delta input table |
| **Output** | Returned inline as a DataFrame | Written to a Delta output table |
| **Best for** | One-off loads and first tests | Recurring pipelines |

Neither raises on API or row-level failures — every outcome is reported inline on the row.

#### 4a. Ad Hoc — `push_data`

`push_data` submits the DataFrame in batches and returns your input columns enriched with per-row status.

```python
result_df = client.push_data(
    df=input_df,
    context=context,
    batch_size=1600,          # number of rows per API request
    # data_load_trace_id="",  # optional; sent as DataLoadTraceId, for debugging
)
```

`push_data` adds these columns to your input:

| Column | Meaning |
|---|---|
| `success` | `True` if the row was accepted |
| `error_code` | Failure category, `null` on success |
| `error_message` | Human-readable reason, `null` on success |
| `processed_timestamp` | When the row was submitted |
| `uid2_resolutions` | Raw identifier to UID2 mapping, empty unless `uid2_config` was set |

```python
from pyspark.sql.functions import col

total     = result_df.count()
succeeded = result_df.filter(col("success")).count()

print(f"Total: {total} | Succeeded: {succeeded} | Failed: {total - succeeded}")

failed_df = result_df.filter(~col("success"))
if failed_df.count():
    failed_df.select("error_code", "error_message").show(truncate=False)
```

#### 4b. Batch Processing — `batch_process`

Use this for incremental, distributed processing backed by Delta tables. Only records added since the last run are sent.

**One time steps:** Create the input, output, and metadata Delta tables. These are created once and reused by every future run — the metadata table is what tracks how far the last run got, so do not drop or recreate it between runs. The `setup_*` helpers return the existing table if it is already there, so they are safe to re-run.

```python
from ttd_databricks_python.ttd_databricks import TTDEndpoint

# Input table: schema matches the required columns for the chosen endpoint.
# Created as a managed table in the default metastore location if no location is provided.
# Default table name: ttd_{endpoint}_input (e.g. "ttd_advertiser_input").
input_table = client.setup_input_table(endpoint=TTDEndpoint.ADVERTISER)

# Output table: mirrors the input schema plus status columns
# (success, error_code, error_message, processed_timestamp, uid2_resolutions).
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

**Every run:**

**1. Append new rows to the input table.** `setup_input_table` creates it empty — the SDK does
not populate it. In production this is your upstream pipeline's job; in a notebook it is a
DataFrame appended to the table.

Set `updated_at` on every row you append. That column is what `process_new_records_only=True`
filters on, so rows without it are never picked up incrementally.

```python
from pyspark.sql import functions as F
from ttd_databricks_python.ttd_databricks import TTDEndpoint, get_ttd_input_schema

input_schema = get_ttd_input_schema(TTDEndpoint.ADVERTISER)

rows = [
    {"id_type": "TDID", "id_value": "123e4567-e89b-12d3-a456-426652340000",
     "segment_name": "my_first_segment", "ttl_in_minutes": 43200},
]

(
    spark.createDataFrame(rows, schema=input_schema)
         .withColumn("updated_at", F.current_timestamp())
         .write.format("delta").mode("append").saveAsTable(input_table)
)
```

**2. Call `batch_process`.** Re-running it picks up only rows appended since the last run.

```python
# Run the batch pipeline. With process_new_records_only=True, only rows
# added since the last successful run (tracked via metadata_table) are sent.
client.batch_process(
    context=context,
    input_table=input_table,
    output_table=output_table,
    metadata_table=metadata_table,
    process_new_records_only=True,  # incremental; set False to reprocess all rows
    batch_size=1600,                # rows per API request
    parallelism=8,                  # parallel partitions for API calls; default 8
    # data_load_trace_id="",        # optional; sent as DataLoadTraceId, for debugging
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

## Supported Data API Endpoints

Each Data API endpoint is represented by a context dataclass. You never pass an endpoint yourself: choosing a context selects the endpoint, its request shape, and its default server. The endpoints are listed here for reference, to connect each SDK context to the API documentation that describes it.

Each section below gives the context, its mandatory columns, a sample input DataFrame, and the example notebook for that use case. The three deletion/opt-out endpoints share one input shape, so the sample appears once, under Advertiser. To list an endpoint's columns programmatically or validate a DataFrame before sending, see [Quickstart step 3](#3-inspect-the-schema-and-prepare-your-input-dataframe).

| Data API Endpoint | Context | OpenTTD API Documentation |
|---|---|---|
| `POST /data/advertiser` | `AdvertiserContext` | [OpenTTD Documentation](https://open.thetradedesk.com/advertiser/docsApp/GuidesAdvertiser/data/doc/post-data-advertiser-firstparty)<br>[OpenTTD Documentation (External Provider)](https://open.thetradedesk.com/provider/docsApp/GuidesProvider/audience/doc/post-data-advertiser-external) |
| `POST /data/thirdparty` | `ThirdPartyContext` | [OpenTTD Documentation](https://open.thetradedesk.com/provider/docsApp/GuidesProvider/audience/doc/post-data-thirdparty) |
| `POST /providerapi/offlineconversion` | `OfflineConversionContext` | [OpenTTD Documentation](https://open.thetradedesk.com/advertiser/docsApp/GuidesAdvertiser/data/doc/post-providerapi-offlineconversion) |
| `POST /data/deletion-optout/advertiser` | `DeletionOptOutAdvertiserContext` | [OpenTTD Documentation](https://open.thetradedesk.com/advertiser/docsApp/GuidesAdvertiser/data/doc/post-data-deletion-optout-advertiser)<br>[OpenTTD Documentation (External Provider)](https://open.thetradedesk.com/provider/docsApp/GuidesProvider/audience/doc/post-data-deletion-optout-advertiser-external) |
| `POST /data/deletion-optout/thirdparty` | `DeletionOptOutThirdPartyContext` | [OpenTTD Documentation ](https://open.thetradedesk.com/provider/docsApp/GuidesProvider/audience/doc/post-data-deletion-optout-thirdparty) |
| `POST /data/deletion-optout/merchant` | `DeletionOptOutMerchantContext` | [OpenTTD Documentation](https://open.thetradedesk.com/provider/docsApp/GuidesProvider/retail/doc/post-data-deletion-optout-merchant) |

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

**Mandatory columns:** `id_type`, `id_value`, `segment_name`. The Quickstart uses this endpoint — see [step 3](#3-inspect-the-schema-and-prepare-your-input-dataframe) for a sample input DataFrame.

**Example notebook:** [First Party Data (1PD) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/First%20Party%20Data%20%281PD%29%20Example%20Notebook.ipynb).

**Schema:** [advertiser.py](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/ttd_databricks_python/ttd_databricks/schemas/advertiser.py).

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

**Mandatory columns:** `id_type`, `id_value`, `segment_name`. `segment_name` is your third-party segment identifier.

```python
input_schema = get_ttd_input_schema(TTDEndpoint.THIRD_PARTY)

rows = [
    {"id_type": "TDID",    "id_value": "123e4567-e89b-12d3-a456-426652340000",
     "segment_name": "1210", "ttl_in_minutes": 43200},
    {"id_type": "ID5",     "id_value": "ID5-c62drGF0EC6wsCZVFDbTbZwi33eB0uZTIC8FxJpzsQ",
     "segment_name": "1800", "ttl_in_minutes": 43200},
    {"id_type": "FirstId", "id_value": "8934d279bba4c7d652a02f624dc334e3",
     "segment_name": "1810", "ttl_in_minutes": 43200},
]

input_df = spark.createDataFrame(rows, schema=input_schema)
```

**Example notebook:** [Third Party Data (3PD) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Third%20Party%20Data%20%283PD%29%20Example%20Notebook.ipynb).

**Schema:** [third_party.py](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/ttd_databricks_python/ttd_databricks/schemas/third_party.py).

---

### Offline Conversion — `/providerapi/offlineconversion`

```python
from ttd_databricks_python.ttd_databricks import OfflineConversionContext

# Targets the /providerapi/offlineconversion endpoint.
context = OfflineConversionContext(
    data_provider_id="<data-provider-id>",
)
```

**Mandatory columns:** `tracking_tag_id`, `timestamp_utc`. This endpoint takes a different shape from the audience endpoints — one row per conversion **event**, with identities nested in `user_ids` rather than flat `id_type`/`id_value` columns. `user_ids` is required unless `impression_id` is provided.

```python
from datetime import datetime, timezone

input_schema = get_ttd_input_schema(TTDEndpoint.OFFLINE_CONVERSION)

rows = [
    {"tracking_tag_id": "<tracking-tag-id>",
     "timestamp_utc": datetime(2026, 1, 15, 10, 11, 30, tzinfo=timezone.utc),
     "user_ids": [{"type": "TDID", "id": "123e4567-e89b-12d3-a456-426652340000"}]},
    {"tracking_tag_id": "<tracking-tag-id>",
     "timestamp_utc": datetime(2026, 1, 15, 10, 11, 30, tzinfo=timezone.utc),
     "user_ids": [{"type": "DAID", "id": "a9342d1f-69f1-4bf8-bc2b-1f20eb451f21"}],
     "order_id": "order-10045", "value": "59.98", "value_currency": "USD",
     "event_name": "purchase"},
]

# schema= is required here: without it `user_ids` is inferred as MapType.
input_df = spark.createDataFrame(rows, schema=input_schema)
```

**Example notebook:** [Offline Conversion Data (CAPI) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Offline%20Conversion%20Data%20%28CAPI%29%20Example%20Notebook.ipynb).

**Schema:** [offline_conversion.py](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/ttd_databricks_python/ttd_databricks/schemas/offline_conversion.py).

---

### Deletion / Opt-Out — Advertiser — `/data/deletion-optout/advertiser`

Deletion/Opt-Out endpoint scoped to a specific advertiser.

```python
from ttd_data.models import PartnerDsrRequestType

from ttd_databricks_python.ttd_databricks import DeletionOptOutAdvertiserContext

# request_type controls the action:
#   PartnerDsrRequestType.DELETION  — remove user data
#   PartnerDsrRequestType.OPT_OUT   — suppress future targeting
context = DeletionOptOutAdvertiserContext(
    advertiser_id="<advertiser-id>",
    request_type=PartnerDsrRequestType.OPT_OUT,  # or DELETION
    data_provider_id="<data-provider-id>",        # optional
)
```

**Mandatory columns:** `id_type`, `id_value` — the only two columns this schema has. All three deletion/opt-out endpoints take the same input shape.

```python
input_schema = get_ttd_input_schema(TTDEndpoint.DELETION_OPTOUT_ADVERTISER)

rows = [
    {"id_type": "TDID",   "id_value": "123e4567-e89b-12d3-a456-426652340000"},
    {"id_type": "DAID",   "id_value": "a9342d1f-69f1-4bf8-bc2b-1f20eb451f21"},
    {"id_type": "UID2",   "id_value": "48MjlfIUZpOKNAm9nod7/jCLAXUYsnE1tpVHQSDS0uo="},
]

input_df = spark.createDataFrame(rows, schema=input_schema)
```

**Example notebook:** [Deletion and Opt-Out (DSR) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Deletion%20and%20Opt-Out%20%28DSR%29%20Example%20Notebook.ipynb).

**Schema:** [deletion_optout_advertiser.py](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/ttd_databricks_python/ttd_databricks/schemas/deletion_optout_advertiser.py).

---

### Deletion / Opt-Out — Third Party — `/data/deletion-optout/thirdparty`

Deletion/Opt-Out endpoint scoped to a third-party data provider.

```python
from ttd_data.models import PartnerDsrRequestType

from ttd_databricks_python.ttd_databricks import DeletionOptOutThirdPartyContext

context = DeletionOptOutThirdPartyContext(
    data_provider_id="<data-provider-id>",
    request_type=PartnerDsrRequestType.OPT_OUT,  # or DELETION
    brand_id="<brand-id>",                        # optional
)
```

**Mandatory columns:** `id_type`, `id_value` — same input shape as the advertiser endpoint above.

**Example notebook:** [Deletion and Opt-Out (DSR) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Deletion%20and%20Opt-Out%20%28DSR%29%20Example%20Notebook.ipynb).

**Schema:** [deletion_optout_thirdparty.py](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/ttd_databricks_python/ttd_databricks/schemas/deletion_optout_thirdparty.py).

---

### Deletion / Opt-Out — Merchant — `/data/deletion-optout/merchant`

Deletion/Opt-Out endpoint scoped to a merchant.

```python
from ttd_data.models import PartnerDsrRequestType

from ttd_databricks_python.ttd_databricks import DeletionOptOutMerchantContext

context = DeletionOptOutMerchantContext(
    merchant_id=123456,  # int, not a string
    request_type=PartnerDsrRequestType.OPT_OUT,  # or DELETION
)
```

**Mandatory columns:** `id_type`, `id_value` — same input shape as the advertiser endpoint above.

**Example notebook:** [Deletion and Opt-Out (DSR) Example Notebook.ipynb](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/example_notebook/Deletion%20and%20Opt-Out%20%28DSR%29%20Example%20Notebook.ipynb).

**Schema:** [deletion_optout_merchant.py](https://github.com/thetradedesk/ttd-databricks-python/blob/add-easy-start-examples-per-usecase-to-sdk-docs/ttd_databricks_python/ttd_databricks/schemas/deletion_optout_merchant.py).

---

## Error Handling

All SDK exceptions inherit from `TTDError`.

`TTDSchemaValidationError` and `TTDConfigurationError` are raised up front, before any data is sent. Schema validation and the Spark and Delta table checks all run ahead of the first API call. These propagate to you rather than being reported inline, so no DataFrame is returned and no rows reach The Trade Desk.

Neither `push_data` nor `batch_process` raises an exception on API call failures. Both always return, including the rows that already succeeded, and every outcome is captured inline in the returned DataFrame via the `success`, `error_code`, and `error_message` columns.

An unrecoverable error is an auth or permission failure (`401`/`403`), which would recur on every following call. When one occurs, the batch that hit it is recorded with that error's own code, and the remaining rows are recorded with `error_code="ABORTED"`. Those rows are never sent to The Trade Desk, so they are safe to re-run. Transient failures such as `429` and `5xx` are retried; if they still fail, they fail only their own batch and later batches carry on.

```python
from ttd_databricks_python.ttd_databricks.exceptions import (
    TTDError,
    TTDConfigurationError,
    TTDSchemaValidationError,
)

try:
    result_df = client.push_data(df=input_df, context=context)
except TTDSchemaValidationError as e:
    print(f"Missing columns: {e.missing_columns}")
except TTDConfigurationError as e:
    print(f"Configuration error: {e}")
```

| Exception | Cause |
|---|---|
| `TTDSchemaValidationError` | DataFrame is missing required columns for the endpoint |
| `TTDConfigurationError` | SparkSession not found, PySpark not installed, a required Delta table is missing, or an existing output table is missing expected columns |
| `TTDApiError` | A batch hit a failure no later batch could survive, such as an auth or permission error. Raised internally and caught by `push_data` and `batch_process`, which report it inline instead of propagating it |

---

## Optional Configuration

None of the following is required to send data. Each endpoint already targets its own default server, retries are on by default, and UID2 resolution is only needed if you send raw email addresses or phone numbers.

The following are optional configurations clients can leverage to customize their integration.

### UID2 Support

UID2s you have already resolved can be sent as-is, with `id_type` set to `UID2`. No configuration is needed for that.

The SDK can also resolve raw email addresses and phone numbers (including pre-hashed variants) to UID2s for you. Attach a `uid2_config` when you create the client:

```python
from ttd_data.uid2 import IdentityScope, UID2Config
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

uid2_config = UID2Config(
    base_url="<your-uid2-operator-url>",
    api_key="<your-uid2-api-key>",
    client_secret="<your-uid2-client-secret>",
    identity_scope=IdentityScope.UID2,  # use IdentityScope.EUID for European identities
)

client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",
    uid2_config=uid2_config,
)
```

Both client styles accept it. With dependency injection, pass it to the `DataClient` instead:

```python
from ttd_data import DataClient
from ttd_databricks_python.ttd_databricks import TtdDatabricksClient

client = TtdDatabricksClient(
    data_api_client=DataClient(uid2_config=uid2_config),
    api_token="<ttd-auth-token>",
)
```

Then set `id_type` to `Email`, `Phone`, `HashedEmail`, or `HashedPhone` with the corresponding value in `id_value`. From there the rows go through `push_data` and `batch_process` like any other identifier type, and nothing else about your pipeline changes:

```python
rows = [
    {"id_type": "Email",       "id_value": "user@example.com",
     "segment_name": "my_first_segment", "ttl_in_minutes": 43200},
    {"id_type": "HashedEmail", "id_value": "tMmiiTI7IaAcPpQPFQ65uMVCWH8av9jw4cwf/F5HVRQ=",
     "segment_name": "my_first_segment", "ttl_in_minutes": 43200},
]

result_df = client.push_data(df=spark.createDataFrame(rows, schema=input_schema), context=context)

result_df.select("id_type", "success", "error_message", "uid2_resolutions").show(truncate=False)
```

Each identifier is resolved by your UID2 operator before the request leaves Databricks, so The Trade Desk only ever receives resolved UID2s, never raw emails or phone numbers. Resolution happens per row in both `push_data` and `batch_process`, and the raw-identifier-to-UID2 mapping comes back in the `uid2_resolutions` column.

---

### Server Selection

The following table shows the mapping between the context in `ttd-databricks`, the destination endpoint the data is sent to, and the default server used. The default servers are already set in the SDK, so you do not need to choose one. You do, however, have the flexibility to override the server that data is sent to. For the servers available to you, see [servers available for advertisers](https://open.thetradedesk.com/advertiser/docsApp/GuidesAdvertiser/data/doc/DataApiCallsAdvertiser#first-pd) and [servers available for data providers](https://open.thetradedesk.com/provider/docsApp/GuidesProvider/audience/doc/DataApiCallsProvider#third-pd) on OpenTTD.

| Context | Destination Endpoint | Default Server |
|---|---|---|
| `AdvertiserContext` | `POST /data/advertiser` | `https://usw-data.adsrvr.org` |
| `ThirdPartyContext` | `POST /data/thirdparty` | `https://bulk-data.adsrvr.org` |
| `OfflineConversionContext` | `POST /providerapi/offlineconversion` | `https://offlineattrib.adsrvr.org` |
| `DeletionOptOutAdvertiserContext` | `POST /data/deletion-optout/advertiser` | `https://usw-data.adsrvr.org` |
| `DeletionOptOutThirdPartyContext` | `POST /data/deletion-optout/thirdparty` | `https://usw-data.adsrvr.org` |
| `DeletionOptOutMerchantContext` | `POST /data/deletion-optout/merchant` | `https://usw-data.adsrvr.org` |

Override globally at the client level, or per request via the context:

#### Global Override

Applies to all endpoints on the client:

```python
client = TtdDatabricksClient.from_params(
    api_token="<ttd-auth-token>",
    server_url="https://custom-server.example.com",
)
```

#### Per-Request Override

Applies only to calls made with that context, leaving the client default unchanged for other endpoints:

```python
context = AdvertiserContext(
    advertiser_id="<advertiser-id>",
    base_url_override="https://custom-server.example.com",
)
```

---

### Custom HTTP Client

The underlying HTTP client is provided by the `ttd-data` SDK via [`DataClient`](https://github.com/thetradedesk/ttd-data-python/blob/main/src/ttd_data/sdk.py). You can inject a custom instance to configure the server URL or connection behaviour, or to inject a mock in tests.

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
