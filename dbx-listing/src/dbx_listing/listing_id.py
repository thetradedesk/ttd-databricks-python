from dataclasses import asdict, dataclass
from typing import Any, Dict

import dacite


@dataclass(frozen=True)
class ListingId:
    listing_id: str

    @staticmethod
    def from_dict(data: Dict[str, Any]) -> "ListingId":
        return dacite.from_dict(data_class=ListingId, data=data)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)
