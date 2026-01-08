import base64
import logging
from pathlib import Path
from typing import List
from dbx_listing.actions import Action, CreateListing, NoOp, UpdateListingAndNotebooks, UpdateListingOnly
from dbx_listing.utils import copy_detail, copy_listing, copy_summary
import jsondiff
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import marketplace, workspace
from dbx_listing.listing_dir import ListingDir, HTMLExportedNotebook
import requests


logger = logging.getLogger(__name__)


def create_or_update_listing(w: WorkspaceClient, directory_path: str, dry_run: bool) -> None:
    logger.info("dry_run=%s", dry_run)

    listing_dir = ListingDir.from_path(Path(directory_path), lambda path: export_notebook_content(w, path))

    action = determine_action(w, listing_dir=listing_dir)

    match action:
        case NoOp():
            logger.info("Listing is up-to-date, nothing to do")
        case CreateListing() | UpdateListingOnly() | UpdateListingAndNotebooks():
            action.execute(w, listing_dir=listing_dir, dry_run=dry_run)


def determine_action(w: WorkspaceClient, listing_dir: ListingDir) -> Action:
    if listing_dir.listing.id is None:
        return CreateListing()

    listing = listing_dir.listing

    assert listing.id is not None
    logger.info("Getting current listing, listing_id=%s", listing.id)

    current_listing = w.provider_listings.get(id=listing.id).listing
    assert current_listing is not None

    logger.info("Comparing desired listing to current listing...")

    are_listings_eq = eq_listing(current_listing, listing)

    assert current_listing.detail is not None
    assert current_listing.detail.embedded_notebook_file_infos is not None
    are_notebook_contents_eq = eq_notebooks(current_listing.detail.embedded_notebook_file_infos, listing_dir.notebooks)

    logger.info("Listing fields are equal: %s", are_listings_eq)
    logger.info("Notebooks contents are equal: %s", are_notebook_contents_eq)

    if are_listings_eq and are_notebook_contents_eq:
        return NoOp()

    if are_notebook_contents_eq:
        return UpdateListingOnly(current_listing)

    return UpdateListingAndNotebooks()


def get_notebook_contents(file_infos: List[marketplace.FileInfo]) -> List[bytes]:
    result: List[bytes] = []
    for info in file_infos:
        download_link = info.download_link
        assert download_link is not None
        response = requests.get(download_link)
        response.raise_for_status()
        result.append(response.content)
    return result


def eq_listing(current_listing: marketplace.Listing, desired_listing: marketplace.Listing) -> bool:
    current_listing_normalised = clear_listing_fields(current_listing).as_dict()
    desired_listing_normalised = clear_listing_fields(desired_listing).as_dict()
    if current_listing_normalised != desired_listing_normalised:
        logger.info(
            "One or more listing fields differ: %s",
            jsondiff.diff(current_listing_normalised, desired_listing_normalised),
        )
        return False
    return True


def eq_notebooks(current_notebooks: List[marketplace.FileInfo], desired_notebooks: List[HTMLExportedNotebook]) -> bool:
    # Note that we're comparing the notebooks after exporting them as HTML.
    # The exported HTML can change even if the notebook hasn't changed.
    return [fi.display_name for fi in current_notebooks] == [
        nb.display_name for nb in desired_notebooks
    ] and get_notebook_contents(current_notebooks) == [nb.contents for nb in desired_notebooks]


def clear_listing_fields(listing: marketplace.Listing) -> marketplace.Listing:
    copy = copy_listing(listing)
    copy.summary = clear_summary_fields(listing.summary)
    assert listing.detail is not None
    copy.detail = clear_detail_fields(listing.detail)
    return copy


def clear_summary_fields(summary: marketplace.ListingSummary) -> marketplace.ListingSummary:
    copy = copy_summary(summary)
    copy.created_at = None
    copy.created_by = None
    copy.created_by_id = None
    copy.published_at = None
    copy.published_by = None
    copy.updated_at = None
    copy.updated_by = None
    copy.updated_by_id = None
    return copy


def clear_detail_fields(detail: marketplace.ListingDetail) -> marketplace.ListingDetail:
    copy = copy_detail(detail)
    # Notebooks are compared separately.
    copy.embedded_notebook_file_infos = None
    copy.privacy_policy_link = None
    copy.terms_of_service = None
    return copy


def export_notebook_content(w: WorkspaceClient, notebook_path: str) -> bytes:
    export_response = w.workspace.export(path=notebook_path, format=workspace.ExportFormat.HTML)
    assert export_response.content is not None
    return base64.b64decode(export_response.content)
