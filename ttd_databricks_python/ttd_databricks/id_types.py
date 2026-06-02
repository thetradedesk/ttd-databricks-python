"""User-facing id_type names and normalization to ttd-data Pydantic field names."""

from __future__ import annotations

# TTD public documentation specifies id_type values in PascalCase/ALLCAPS.
# Keys are uppercased so that normalization is case-insensitive — both
# "RampID" and "rampid" resolve to the same Python field name.
_NORMALIZATION: dict[str, str] = {
    "TDID": "tdid",
    "DAID": "daid",
    "UID2": "uid2",
    "UID2TOKEN": "uid2_token",
    "EUID": "euid",
    "EUIDTOKEN": "euid_token",
    "RAMPID": "ramp_id",
    "ID5": "id5",
    "NETID": "net_id",
    "FIRSTID": "first_id",
    "MERKURYID": "merkury_id",
    "IQVIAPPID": "iqvia_ppid",
    "EMAIL": "email",
    "PHONE": "phone",
    "HASHEDEMAIL": "hashed_email",
    "HASHEDPHONE": "hashed_phone",
}

# id_type values that are raw PII identifiers resolved client-side to a UID2/EUID
# by the upstream SDK when a UID2Config is configured on DataClient. Rows using
# these identifiers populate a per-row resolution in the output table.
RAW_PII_ID_TYPES: frozenset[str] = frozenset({"EMAIL", "PHONE", "HASHEDEMAIL", "HASHEDPHONE"})


def is_raw_pii_id_type(id_type: str) -> bool:
    """Return True if id_type is a raw PII identifier (Email/Phone/Hashed*)."""
    return id_type.upper() in RAW_PII_ID_TYPES


VALID_ID_TYPES: frozenset[str] = frozenset(_NORMALIZATION)


def normalize_id_type(id_type: str) -> str:
    """Map a user-facing id_type to the Python field name expected by ttd-data models.

    Matching is case-insensitive. Raises ValueError for unrecognized values.
    """
    try:
        return _NORMALIZATION[id_type.upper()]
    except KeyError:
        valid = ", ".join(sorted(_NORMALIZATION))
        raise ValueError(f"Unknown id_type {id_type!r}. Must be one of: {valid}.") from None
