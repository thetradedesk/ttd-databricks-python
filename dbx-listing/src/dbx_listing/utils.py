import logging
from typing import List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import marketplace
import requests
from dbx_listing.listing_dir import HTMLExportedNotebook


logger = logging.getLogger(__name__)


def create_embedded_notebook(
    w: WorkspaceClient, listing_id: str, notebook_content: bytes, display_name: Optional[str]
) -> marketplace.FileInfo:
    file_parent = marketplace.FileParent(parent_id=listing_id, file_parent_type=marketplace.FileParentType.LISTING)

    logger.info("Creating notebook, display name=%s...", display_name)

    create_file_response = w.provider_files.create(
        file_parent=file_parent,
        marketplace_file_type=marketplace.MarketplaceFileType.EMBEDDED_NOTEBOOK,
        mime_type="text/html",
        display_name=display_name,
    )

    logger.info("Created notebook")

    assert create_file_response.upload_url is not None
    assert create_file_response.file_info is not None

    logger.info("Writing notebook to S3...")

    put_response = requests.put(
        create_file_response.upload_url,
        data=notebook_content,
        headers={"x-amz-server-side-encryption": "AES256"},
        timeout=30,
    )
    put_response.raise_for_status()

    logger.info("Successfully wrote notebook to S3")

    return create_file_response.file_info


def create_embedded_notebooks(
    w: WorkspaceClient, listing_id: str, notebooks: List[HTMLExportedNotebook]
) -> List[marketplace.FileInfo]:
    result: List[marketplace.FileInfo] = []
    for notebook in notebooks:
        file_info = create_embedded_notebook(
            w,
            listing_id=listing_id,
            notebook_content=notebook.contents,
            display_name=notebook.display_name,
        )
        result.append(file_info)
    return result


def copy_listing(listing: marketplace.Listing) -> marketplace.Listing:
    return marketplace.Listing.from_dict(listing.as_dict())


def copy_summary(summary: marketplace.ListingSummary) -> marketplace.ListingSummary:
    return marketplace.ListingSummary.from_dict(summary.as_dict())


def copy_detail(detail: marketplace.ListingDetail) -> marketplace.ListingDetail:
    return marketplace.ListingDetail.from_dict(detail.as_dict())
