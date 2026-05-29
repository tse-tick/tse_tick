"""
tse_tick — Tokyo Stock Exchange tick data processing for NEEDS data.

Authors and contributions:
    Kazumi Li         — Schema definitions, package architecture, maintainer
    Peter Romero      — Original concept and initial project design
    Masataka Hayashi  — Initial pandas-based prototype

Developed at Keio University, Nakatsuma Seminar.
"""

# tse_tick/__init__.py
import polars as pl

__version__ = "0.2.3"
__author__ = "Kazumi Li"
__email__ = "kaiwenli@keio.jp"
__license__ = "MIT"
__copyright__ = "Copyright 2025-2026"

from .enhanced import create_df, export_to_csv, discover_zips, parse_period

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
    "export_to_csv",
    "discover_zips",
    "parse_period",
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
    "__version__",
    "__author__",
]


def get_version():
    return __version__


def get_supported_data_types():
    return ["individual_stock", "stock_summary", "indices", "indices_summary"]


def get_supported_years():
    from datetime import datetime as _dt
    return (2016, _dt.now().year)


def get_info():
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

    Year Range: 2016-2023

    Languages: English (en), Japanese (jp)

    Quick Start:
    >>> import tse_tick
    >>> df = tse_tick.create_df("data.zip", language='en')
    >>> tse_tick.export_to_csv("data.zip", "output.csv")

    CLI Usage:
    >>> tse-tick ingest --data-type individual_stock --period 2024 \\
                --input-root /path/to/data --output-root /path/to/store

    For more information, visit:
    https://github.com/jevwithwind/tse_tick
    """
    print(info)
