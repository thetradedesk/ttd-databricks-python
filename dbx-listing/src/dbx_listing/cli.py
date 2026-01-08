import argparse
import logging
from databricks.sdk import WorkspaceClient
from dbx_listing.commands import create_or_update_listing


def main():
    logging.basicConfig(level=logging.INFO)

    args = parse_args()

    w = WorkspaceClient()

    create_or_update_listing(w, args.directory, args.dry_run)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Manage a Databricks marketplace listing")

    parser.add_argument(
        "directory",
        help="The path to the directory containing the marketplace listing.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Dry run (default: False)"
    )

    return parser.parse_args()
