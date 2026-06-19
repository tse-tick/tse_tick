# tse_tick/constants.py
"""Enumerations for the public API.

``DataType`` and ``Language`` give IDE autocomplete and a single source of truth
for the magic strings used throughout the package. Both subclass ``str``, so a
member is accepted anywhere the equivalent string is today — comparisons,
dict/set membership, ``pathlib`` path building and f-strings all see the value::

    >>> from tse_tick import DataType
    >>> DataType.INDIVIDUAL_STOCK == "individual_stock"
    True
    >>> f"{DataType.INDIVIDUAL_STOCK}"
    'individual_stock'
"""

from enum import Enum
from typing import List


class DataType(str, Enum):
    """NEEDS data type. The value is the canonical string used across the API."""

    INDIVIDUAL_STOCK = "individual_stock"
    STOCK_SUMMARY = "stock_summary"
    INDICES = "indices"
    INDICES_SUMMARY = "indices_summary"

    def __str__(self) -> str:
        # Return the bare value (not "DataType.INDIVIDUAL_STOCK") so f-strings and
        # str() match the legacy magic-string behaviour on every Python version.
        return self.value

    @classmethod
    def values(cls) -> List[str]:
        """All data-type values as plain strings, in declaration order."""
        return [member.value for member in cls]


class Language(str, Enum):
    """Output column-name language."""

    EN = "en"
    JP = "jp"

    def __str__(self) -> str:
        return self.value

    @classmethod
    def values(cls) -> List[str]:
        """All language values as plain strings, in declaration order."""
        return [member.value for member in cls]


# --------------------------------------------------------------------------- #
# Canonical data-type groupings — the SINGLE source of truth for the "which types
# are X" checks. Every module references these instead of repeating literal
# tuples/sets, so the classification can't drift between files (the cause of
# several past inconsistencies). All are plain strings so membership works with
# the bare data_type strings the public API accepts.
# --------------------------------------------------------------------------- #
DATA_TYPES = tuple(DataType.values())          # all four, declaration order
VALID_DATA_TYPES = frozenset(DATA_TYPES)       # validation / membership

# Daily-aggregate "summary" types: one row per (date, code), NO intraday Execution
# Time (start/end-time filters raise), monthly packaging, date-only Parquet partition.
SUMMARY_TYPES = frozenset({DataType.STOCK_SUMMARY.value, DataType.INDICES_SUMMARY.value})
# Tick types: carry an intraday Execution Time (time-filterable; event windows apply).
TICK_TYPES = frozenset({DataType.INDIVIDUAL_STOCK.value, DataType.INDICES.value})
# Index (vs stock) types: a numeric Index Code identifier, and the 2016 legacy
# "…010" record code in discovery.
INDEX_TYPES = frozenset({DataType.INDICES.value, DataType.INDICES_SUMMARY.value})

# The two classes must partition the four types (guards against future drift).
assert SUMMARY_TYPES | TICK_TYPES == VALID_DATA_TYPES and not (SUMMARY_TYPES & TICK_TYPES)


def validate_data_type(data_type: str) -> None:
    """Raise ``ValueError`` if ``data_type`` is not one of the four NEEDS types."""
    if data_type not in VALID_DATA_TYPES:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(VALID_DATA_TYPES)}"
        )
