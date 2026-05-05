# tests/test_query.py
"""Tests for tse_tick.query — DuckDB-based Parquet query interface."""

import pytest
from tse_tick.query import (
    query_ticks,
    query_sql,
    get_available_dates,
    get_available_tickers,
)


def test_query_ticks_returns_dataframe():
    """
    query_ticks should return a pandas DataFrame with TICST120 columns when
    no filters are applied to a populated Parquet store.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_filters_by_date():
    """
    Passing date='20230104' should return only rows where Data Date is
    2023-01-04, exercising partition pruning on the date= directory.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_filters_by_ticker():
    """
    Passing ticker=7203 should return only rows for Toyota (code 7203),
    exercising partition pruning on the ticker= directory.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_filters_by_time_range():
    """
    start_time='14:00:00' and end_time='15:00:00' should return only rows
    where Execution Time is within [14:00:00, 15:00:00].
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_column_pruning():
    """
    Passing columns=['Execution Time', 'Execution Price'] should return a
    DataFrame with exactly those two columns, not all 95.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_indices_data_type():
    """
    query_ticks with data_type='indices' should return TICIT110 columns
    (10 fields) and ticker is interpreted as Index Code.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_empty_result():
    """
    Querying for a ticker/date combination that has no data should return
    an empty DataFrame, not raise an exception.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_ticks_unknown_data_type_raises():
    """
    query_ticks should raise ValueError for an unrecognised data_type string.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_sql_executes_aggregation():
    """
    query_sql should execute a GROUP BY aggregation and return correct row
    counts when run against a real TICST120 Parquet store.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_sql_ticks_alias_is_registered():
    """
    The table alias 'ticks' must be resolvable in the SQL string passed to
    query_sql without any additional setup by the caller.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_query_sql_missing_parquet_raises():
    """
    query_sql should raise FileNotFoundError when no Parquet files exist for
    the specified data_type under data_dir.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_get_available_dates_returns_sorted_list():
    """
    get_available_dates should return a sorted list of 'YYYYMMDD' strings
    matching the date= partition directories present in the store.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_get_available_dates_missing_store_raises():
    """
    get_available_dates should raise FileNotFoundError when the data_type
    subdirectory does not exist under data_dir.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_get_available_tickers_returns_sorted_integers():
    """
    get_available_tickers should return a sorted list of int ticker codes
    from the ticker= partition directories in the store.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_get_available_tickers_filters_by_date():
    """
    Passing date='20230104' should return only tickers that have a partition
    directory under date=20230104, not tickers from other dates.
    """
    pytest.skip("Waiting for NEEDS data access")
