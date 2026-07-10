"""
tse_tick — Tokyo Stock Exchange tick data processing for NEEDS data.

Authors and contributions:
    Kazumi Li         — Schema definitions, package architecture, maintainer
    Masataka Hayashi  — Initial pandas-based prototype
    Peter Romero      — Original concept and initial project design

Developed at Keio University, Nakatsuma Seminar.
"""

# tse_tick/__init__.py
import polars as pl

__version__ = "0.13.0"
__author__ = "Kazumi Li, Masataka Hayashi, Peter Romero"
__email__ = "kaiwenli@keio.jp"
__license__ = "MIT"
__copyright__ = "Copyright 2025-2026"

# Inclusive (min_year, max_year) of NEEDS calendar years the package targets. The
# dataset spans 2016-2025 (the parser itself is not year-limited). Single source of
# truth for get_supported_years() and the get_info() banner so they can't drift
# (they previously disagreed: a dynamic (2016, current_year) vs a hardcoded banner).
_SUPPORTED_YEARS = (2016, 2025)


def _configure_windows_console() -> None:
    """Make ``print(df)`` safe on a legacy Windows console.

    Two distinct glyph problems break a bare ``print(df)`` on the default Windows
    console codepage (cp1252):

    * Polars renders DataFrames with Unicode **box-drawing** borders — fixed by
      switching Polars to ASCII table borders.
    * The frame's **content** still carries non-cp1252 glyphs even with ASCII
      borders: the ``datetime[μs]`` dtype header (U+03BC), column names like
      ``"Executions ≤3 units"`` (U+2264), and exchange values with ``—``
      (U+2014). These are fixed by reconfiguring ``stdout``/``stderr`` to UTF-8.

    Both are no-ops off Windows; opt out of both with ``TSE_TICK_ASCII_TABLES=0``.
    ``tse_tick.display(df)`` remains an explicit, cross-platform UTF-8 alternative
    regardless of this setting.
    """
    import os
    import sys

    if sys.platform != "win32":
        return
    if os.environ.get("TSE_TICK_ASCII_TABLES", "1") == "0":
        return
    try:
        pl.Config.set_ascii_tables(True)
    except Exception:  # a cosmetic tweak must never break import
        pass
    # ASCII borders don't help the cell/column glyphs above, so make the standard
    # streams UTF-8 too. Heavily guarded: skip streams that can't be reconfigured
    # (pytest capture, redirected pipes) or are already UTF-8, and never let this
    # break import.
    for _stream_name in ("stdout", "stderr"):
        _stream = getattr(sys, _stream_name, None)
        _reconfigure = getattr(_stream, "reconfigure", None)
        if _reconfigure is None:
            continue
        try:
            _encoding = (getattr(_stream, "encoding", "") or "").lower()
            if _encoding not in ("utf-8", "utf8"):
                _reconfigure(encoding="utf-8", errors="backslashreplace")
        except Exception:
            pass


_configure_windows_console()


def display(df, *, file=None) -> None:
    """Print a Polars DataFrame as UTF-8 regardless of the console encoding.

    A cross-platform alternative to ``print(df)`` that never raises
    ``UnicodeEncodeError`` on a legacy Windows console: it writes the rendered
    table to the stream's binary buffer as UTF-8, bypassing the console codec.

    Args:
        df: A Polars DataFrame (anything with a ``__str__`` works).
        file: Target text stream; defaults to ``sys.stdout``.
    """
    import sys

    stream = sys.stdout if file is None else file
    text = str(df) + "\n"
    buffer = getattr(stream, "buffer", None)
    if buffer is not None:
        buffer.write(text.encode("utf-8", errors="replace"))
        buffer.flush()
    else:
        stream.write(text)


from .enhanced import (
    create_df,
    export_to_csv,
    discover_zips,
    parse_period,
    read_ticks,
    NoDataWarning,
    TruncationWarning,
    LargeResultWarning,
    OneShotMemoryError,
)

from .schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_23,
    get_schema_indices_summary,
    get_japanese_column_mapping,
)

from .ingest import (
    ingest_single_zip,
    ingest_directory,
    ingest_year,
    ingest_year_from_root,
    ingest_period,
    ingest_event_windows_period,
    extract_to_store,
)

from .io.parquet import (
    write_partitioned_parquet,
    read_parquet_partition,
    write_event_window_parquet,
    read_partitioned_parquet,
)

from .event_window import extract_event_window, extract_batch_event_windows

from .features import (
    compute_spread,
    compute_depth,
    compute_flow_imbalance,
    compute_volatility,
    compute_all_features,
)

from .constants import DataType, Language
from .translate import translate, mapping

try:
    from .query import (
        query_ticks,
        query_sql,
        get_available_dates,
        get_available_tickers,
    )
    _DUCKDB_AVAILABLE = True
except ImportError:
    _DUCKDB_AVAILABLE = False

    def _duckdb_unavailable(*args, **kwargs):
        raise ImportError(
            "DuckDB is required for query functions. Install it with: pip install duckdb>=0.9.0"
        )

    query_ticks = _duckdb_unavailable
    query_sql = _duckdb_unavailable
    get_available_dates = _duckdb_unavailable
    get_available_tickers = _duckdb_unavailable

__all__ = [
    "create_df",
    "read_ticks",
    "export_to_csv",
    "display",
    "discover_zips",
    "parse_period",
    "NoDataWarning",
    "TruncationWarning",
    "LargeResultWarning",
    "OneShotMemoryError",
    "get_schema_individual_stock_95",
    "get_schema_summary_83",
    "get_schema_indices_23",
    "get_schema_indices_summary",
    "get_japanese_column_mapping",
    "ingest_single_zip",
    "ingest_directory",
    "ingest_year",
    "ingest_year_from_root",
    "ingest_period",
    "ingest_event_windows_period",
    "extract_to_store",
    "write_partitioned_parquet",
    "read_parquet_partition",
    "write_event_window_parquet",
    "read_partitioned_parquet",
    "query_ticks",
    "query_sql",
    "get_available_dates",
    "get_available_tickers",
    "extract_event_window",
    "extract_batch_event_windows",
    "compute_spread",
    "compute_depth",
    "compute_flow_imbalance",
    "compute_volatility",
    "compute_all_features",
    "translate",
    "mapping",
    "DataType",
    "Language",
    "get_version",
    "get_info",
    "get_supported_data_types",
    "get_supported_years",
    "__version__",
    "__author__",
]


def __dir__():
    """Curate ``dir(tse_tick)`` to the documented public API.

    Without this, ``dir(tse_tick)`` also lists incidental submodules — most
    confusingly ``ingest`` (the module) right next to ``ingest_period`` etc., so a
    novice tries ``tse_tick.ingest(...)`` and hits "'module' object is not
    callable" (run10 F2). Submodules remain importable (``tse_tick.ingest.…``);
    they're just not advertised here.
    """
    return sorted(__all__)


def get_version():
    """Return the installed tse_tick version string (same as ``tse_tick.__version__``)."""
    return __version__


def get_supported_data_types():
    """Return the four NEEDS data-type names tse_tick supports.

    ``["individual_stock", "stock_summary", "indices", "indices_summary"]`` —
    the valid ``data_type`` values for :func:`read_ticks`, :func:`query_ticks`,
    the ``ingest_*`` functions, etc. Derived from the :class:`DataType` enum so it
    can never drift from it.
    """
    # Derive from the DataType enum so this list can never drift from it.
    return DataType.values()


def get_supported_years():
    """Return the inclusive ``(min_year, max_year)`` of NEEDS years the package targets.

    The NEEDS dataset spans 2016-2025; this returns that range (the parser itself
    is not year-limited). It matches the "Year Range" shown by :func:`get_info`.
    """
    return _SUPPORTED_YEARS


def get_info(path=None):
    """Return a human-readable summary of the package as a string.

    To display it, print the return value: ``print(tse_tick.get_info())``. The
    function itself does **not** print, so wrapping it in ``print`` shows the
    banner once, not twice (it previously printed *and* returned).

    ``get_info`` describes the **package**, not a dataset, so it takes no path. The
    optional ``path`` argument exists only to give a guiding error (instead of a
    raw ``TypeError``) if you pass one — to inspect data use :func:`read_ticks`
    (raw ZIPs) or, on a built store, :func:`get_available_dates` /
    :func:`get_available_tickers`.
    """
    if path is not None:
        raise ValueError(
            "get_info() describes the tse_tick package and takes no dataset path; "
            "to inspect data use read_ticks(...), or on a built store "
            "get_available_dates() / get_available_tickers()."
        )
    info = f"""
    tse_tick v{__version__}
    ========================

    Author: {__author__}
    License: {__license__}

    Supported Data Types (output fields):
    - individual_stock (TICST120) - 95 fields
    - stock_summary (TICSS110) - 82 fields (83 raw)
    - indices (TICIT110) - 10 fields (23 raw, 15 in 2016)
    - indices_summary (TICIS110) - 17 fields

    Year Range: {_SUPPORTED_YEARS[0]}-{_SUPPORTED_YEARS[1]}

    Languages: English (en), Japanese (jp)

    Quick Start (two access paths):
    # One-shot - read raw ZIPs straight to a filtered DataFrame (no store).
    # For individual_stock + a ticker it opens only the ticker's parts (part-pruned):
    >>> import tse_tick
    >>> df = tse_tick.read_ticks("DATA_ROOT", ticker_filter={{"7203"}},
    ...                          date="20240201", start_time="09:00:00",
    ...                          end_time="11:30:00")
    >>> tse_tick.display(df)   # UTF-8 print (Windows-safe alternative to print(df))

    # Two-stage (recommended for repeated reads) - build a reusable store once,
    # then query it (sub-second). extract_to_store does both in one call:
    >>> df = tse_tick.extract_to_store("DATA_ROOT", store, "202402", ["7203", "9984"])
    >>> from tse_tick import DataType
    >>> tse_tick.query_ticks(store, data_type=DataType.INDIVIDUAL_STOCK,
    ...                      ticker=7203, date="20240201",
    ...                      start_time="09:00:00", end_time="11:30:00")

    CLI Usage:
    >>> tse-tick ingest --data-type individual_stock --period 2024 \\
                --input-root /path/to/data --output-root /path/to/store
    >>> tse-tick export --data-type individual_stock --tickers 7203 \\
                --period 20240201-20240205 --input-root /path/to/data --output toyota.csv

    For more information, visit:
    https://github.com/tse-tick/tse_tick
    """
    return info
