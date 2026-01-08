import json
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import marketplace
from dbx_listing.listing_dir import ListingDir
from dbx_listing.listing_id import ListingId
from dbx_listing.utils import copy_listing, create_embedded_notebooks


logger = logging.getLogger(__name__)


class NoOp:
    pass


class CreateListing:
    def execute(self, w: WorkspaceClient, listing_dir: ListingDir, dry_run: bool) -> None:
        if dry_run:
            logger.info("Skipping creating listing (dry-run)")
            return

        listing = listing_dir.listing

        logger.info('Creating listing "%s"...', listing.summary.name)

        # https://docs.databricks.com/api/workspace/providerlistings/create
        # We create the listing in DRAFT status then create the notebook(s) using the returned listing ID.
        # The listing ID is generated server-side and is required for creating the notebooks.
        # Finally, we update the listing with the notebooks and set its status to the desired value.
        listing_id = CreateListing._create_draft_listing(w, listing)
        logger.info("Created listing, listing_id=%s", listing_id)

        embedded_notebook_file_infos = create_embedded_notebooks(w, listing_id, listing_dir.notebooks)
        assert listing.detail is not None
        listing.detail.embedded_notebook_file_infos = embedded_notebook_file_infos

        w.provider_listings.update(id=listing_id, listing=listing)

        listing_id_path = listing_dir.listing_id_path
        logger.info("Writing listing ID %s to %s", listing_id, listing_id_path)

        listing_id_json = json.dumps(ListingId(listing_id=listing_id).as_dict(), sort_keys=True, indent=4)
        listing_id_path.write_text(listing_id_json, encoding="utf-8")

    @staticmethod
    def _create_draft_listing(w: WorkspaceClient, listing: marketplace.Listing) -> str:
        listing = copy_listing(listing)
        listing.summary.status = marketplace.ListingStatus.DRAFT

        create_listing_response = w.provider_listings.create(listing)

        listing_id = create_listing_response.listing_id
        assert listing_id is not None
        return listing_id


class UpdateListingOnly:
    def __init__(self, current_listing: marketplace.Listing):
        self.current_listing = current_listing

    def execute(self, w: WorkspaceClient, listing_dir: ListingDir, dry_run: bool) -> None:
        if dry_run:
            logger.info("Skipping updating listing (dry-run)")
            return

        listing = listing_dir.listing
        assert listing.id is not None
        assert listing.detail is not None
        assert self.current_listing.detail is not None
        listing.detail.embedded_notebook_file_infos = self.current_listing.detail.embedded_notebook_file_infos

        logger.info("Updating listing...")
        w.provider_listings.update(id=listing.id, listing=listing)
        logger.info("Updated listing")


class UpdateListingAndNotebooks:
    def execute(self, w: WorkspaceClient, listing_dir: ListingDir, dry_run: bool) -> None:
        if dry_run:
            logger.info("Skipping updating listing and notebooks (dry-run)")
            return

        listing = listing_dir.listing
        assert listing.id is not None
        assert listing.detail is not None
        listing.detail.embedded_notebook_file_infos = create_embedded_notebooks(
            w, listing_id=listing.id, notebooks=listing_dir.notebooks
        )

        logger.info("Updating listing...")
        w.provider_listings.update(id=listing.id, listing=listing)
        logger.info("Updated listing")


Action = NoOp | CreateListing | UpdateListingOnly | UpdateListingAndNotebooks
