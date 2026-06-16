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
