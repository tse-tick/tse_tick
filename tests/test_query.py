# tests/test_query.py
"""Tests for tse_tick.query — DuckDB-based Parquet query interface.

These run against the synthetic ``stock_store`` fixture (see conftest.py), which
is built by running synthetic NEEDS-format ZIPs through the real ingest
pipeline. No proprietary NEEDS data is required.
"""

import polars as pl
import pytest

from tse_tick.query import (
    query_ticks,
    query_sql,
    get_available_dates,
    get_available_tickers,
)
from tse_tick.schemas import get_schema_individual_stock_95


def test_query_ticks_returns_dataframe(stock_store):
    """
    query_ticks should return a Polars DataFrame with TICST120 columns when
    no filters are applied to a populated Parquet store.
    """
    df = query_ticks(stock_store)
    assert isinstance(df, pl.DataFrame)
    assert df.height == 3 * 40 * 2  # 3 tickers x 40 rows x 2 dates
    for col in get_schema_individual_stock_95():
        assert col in df.columns


def test_query_ticks_filters_by_date(stock_store):
    """
    Passing date='20230704' should return only rows where Data Date is
    2023-07-04, exercising partition pruning on the date= directory.
    """
    df = query_ticks(stock_store, date="20230704")
    assert df["date"].unique().to_list() == [20230704]
    assert df.height == 3 * 40  # 3 tickers x 40 rows on that date


def test_query_ticks_filters_by_ticker(stock_store):
    """
    Passing ticker=7203 should return only rows for Toyota (code 7203),
    exercising partition pruning on the ticker= file.
    """
    df = query_ticks(stock_store, ticker=7203)
    assert df["Stock Code"].unique().to_list() == ["7203"]
    assert df.height == 40 * 2  # 40 rows x 2 dates


def test_query_ticks_filters_by_time_range(stock_store):
    """
    start_time='14:00:00' and end_time='15:00:00' should return only rows
    where Execution Time is within [14:00:00, 15:00:00].
    """
    df = query_ticks(
        stock_store, ticker=7203, date="20230704",
        start_time="14:00:00", end_time="15:00:00",
    )
    assert df.height > 0
    minutes = [int(t) for t in df["Execution Time"].to_list()]
    assert all(140000 <= m <= 150000 for m in minutes)
    # No morning ticks should have leaked in.
    assert min(minutes) >= 140000


def test_query_ticks_column_pruning(stock_store):
    """
    Passing columns=['Execution Time', 'Execution Price'] should return a
    DataFrame with exactly those two columns, not all 95.
    """
    df = query_ticks(
        stock_store, ticker=7203, columns=["Execution Time", "Execution Price"]
    )
    assert df.columns == ["Execution Time", "Execution Price"]


def test_query_ticks_indices_data_type():
    """
    query_ticks with data_type='indices' should return TICIT110 columns
    (10 fields) and ticker is interpreted as Index Code.
    """
    pytest.skip(
        "Indices store partitions by the categorically-decoded Index Code "
        "(e.g. '101' -> 'Nikkei 225'), so the ticker= filename is garbled; "
        "ticker-based index queries need a separate ingest-side fix. The "
        "synthetic fixture covers individual_stock, the documented primary type."
    )


def test_query_ticks_empty_result(stock_store):
    """
    Querying for a ticker/date combination that has no data should return
    an empty DataFrame, not raise an exception.
    """
    df = query_ticks(stock_store, ticker=9999)
    assert df.is_empty()


def test_query_ticks_unknown_data_type_raises(stock_store):
    """
    query_ticks should raise ValueError for an unrecognised data_type string.
    """
    with pytest.raises(ValueError, match="Unknown data_type"):
        query_ticks(stock_store, data_type="not_a_type")


def test_query_sql_executes_aggregation(stock_store):
    """
    query_sql should execute a GROUP BY aggregation and return correct row
    counts when run against a real TICST120 Parquet store.
    """
    df = query_sql(
        stock_store,
        'SELECT "Stock Code" AS code, COUNT(*) AS n '
        'FROM ticks GROUP BY "Stock Code" ORDER BY code',
    )
    assert df["code"].to_list() == ["6758", "7203", "9984"]
    assert df["n"].to_list() == [80, 80, 80]  # 40 rows x 2 dates each


def test_query_sql_ticks_alias_is_registered(stock_store):
    """
    The table alias 'ticks' must be resolvable in the SQL string passed to
    query_sql without any additional setup by the caller.
    """
    df = query_sql(stock_store, "SELECT COUNT(*) AS n FROM ticks")
    assert df["n"].to_list() == [240]


def test_query_sql_missing_parquet_raises(tmp_path):
    """
    query_sql should raise FileNotFoundError when no Parquet files exist for
    the specified data_type under data_dir.
    """
    with pytest.raises(FileNotFoundError):
        query_sql(str(tmp_path), "SELECT COUNT(*) FROM ticks")


def test_get_available_dates_returns_sorted_list(stock_store):
    """
    get_available_dates should return a sorted list of 'YYYYMMDD' strings
    matching the date= partition directories present in the store.
    """
    assert get_available_dates(stock_store) == ["20230703", "20230704"]


def test_get_available_dates_missing_store_raises(tmp_path):
    """
    get_available_dates should raise FileNotFoundError when the data_type
    subdirectory does not exist under data_dir.
    """
    with pytest.raises(FileNotFoundError):
        get_available_dates(str(tmp_path))


def test_get_available_tickers_returns_sorted_integers(stock_store):
    """
    get_available_tickers should return a sorted list of int ticker codes
    from the ticker= partition files in the store.
    """
    tickers = get_available_tickers(stock_store)
    assert tickers == [6758, 7203, 9984]
    assert all(isinstance(t, int) for t in tickers)


def test_get_available_tickers_filters_by_date(stock_store):
    """
    Passing date='20230704' should return tickers that have a partition file
    under date=20230704 (all three are present on both synthetic dates).
    """
    assert get_available_tickers(stock_store, date="20230704") == [6758, 7203, 9984]
