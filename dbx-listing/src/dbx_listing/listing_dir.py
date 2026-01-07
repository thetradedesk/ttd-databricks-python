from dataclasses import dataclass, field
import json
from pathlib import Path
from typing import Callable, List, NamedTuple, Optional
from databricks.sdk.service import marketplace
from dbx_listing.listing_id import ListingId


class HTMLExportedNotebook(NamedTuple):
    display_name: Optional[str]
    contents: bytes


@dataclass(frozen=True)
class ListingDir:
    parent_directory: Path
    listing: marketplace.Listing
    notebooks: List[HTMLExportedNotebook] = field(default_factory=list)

    @staticmethod
    def from_path(path: Path, read_notebook_content: Callable[[str], bytes]) -> "ListingDir":
        path = path.resolve()

        listing_path = path / "listing.json"
        listing_json = json.loads(listing_path.read_text(encoding="utf-8"))
        listing = marketplace.Listing.from_dict(listing_json["listing"])

        listing.id = ListingDir._read_listing_id(path)
        assert listing.detail is not None
        listing.detail.description = ListingDir._read_description(path)

        notebook_contents = [
            HTMLExportedNotebook(notebook['display_name'], read_notebook_content(notebook['path']))
            for notebook in listing_json["notebooks"]
        ]

        return ListingDir(
            parent_directory=path,
            listing=listing,
            notebooks=notebook_contents,
        )

    @property
    def listing_id_path(self) -> Path:
        return ListingDir._listing_id_path(self.parent_directory)

    @staticmethod
    def _read_description(parent_directory: Path) -> str:
        path = parent_directory / "description.md"
        return path.read_text()

    @staticmethod
    def _read_listing_id(parent_directory: Path) -> Optional[str]:
        path = ListingDir._listing_id_path(parent_directory)
        if path.is_file():
            listing_id_json = json.loads(path.read_text(encoding="utf-8"))
            return ListingId.from_dict(listing_id_json).listing_id
        return None

    @staticmethod
    def _listing_id_path(parent_directory: Path):
        return parent_directory / "listing_id.json"
