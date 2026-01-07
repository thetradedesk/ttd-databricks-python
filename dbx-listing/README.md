This is a CLI tool for creating and updating Databricks marketplace listings.

```
usage: dbx-listing [-h] [--dry-run] directory

Manage a Databricks marketplace listing

positional arguments:
  directory   The path to the directory containing the marketplace listing.

options:
  -h, --help  show this help message and exit
  --dry-run   Dry run (default: False)
```

## Authentication

The tool uses [Databricks unified authentication](https://docs.databricks.com/aws/en/dev-tools/auth/unified-auth). Refer to the Databricks documentation for more information.

## Managing a marketplace listing

### Define the listing

Create the following files in a directory:
- `listing.json`
- `description.md`
- `listing_id.json` (optional)

You can find an example listing in `examples/listings/example_listing`.

#### listing.json

This file contains the listing details, as well as information about the marketplace listing embedded notebooks.

Schema:

- `listing` (object) See the request sample in [Create a listing](https://docs.databricks.com/api/workspace/providerlistings/create) in the Databricks API documentation.  
- `notebooks` (Array of object)
  - `display_name` (optional string) The display name for the notebook.
  - `path` (string) The path to the notebook in the Databricks workspace.
  
#### description.md

This file contains the listing description in Markdown format. The description lives outside of `listing.json` for ease of editing.

#### listing_id.json

This file contains the listing ID. If the file doesn't exist, a new listing will be created. When a new listing is created, the tool will write the listing ID to this file.

The listing ID is in its own file to avoid reformatting `listing.json`.

Schema:

- `listing_id`: (string) The listing ID.

### Creating or updating the listing

Run the tool, pointing at the directory:

```
uv run dbx-listing /path/to/your/listing
```

## Troubleshooting

### Listing is updated though nothing has changed

One possible cause is that the *exported notebook HTML* has changed, even though the source notebooks haven't changed. Check the logs to see if the notebook contents have changed.

### Permissions

The client (user or service principal) requires:
- The [Marketplace admin role](https://docs.databricks.com/aws/en/marketplace/get-started-provider#assign-the-marketplace-admin-role)
- *Read* permission on the listing notebooks
- *Owner* of the listing share
