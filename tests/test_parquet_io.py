# tests/test_parquet_io.py
"""Tests for tse_tick.io.parquet — Parquet read/write utilities."""

import pytest
from tse_tick.io.parquet import write_partitioned_parquet, read_parquet_partition


def test_write_partitioned_parquet_creates_directory():
    """
    write_partitioned_parquet should create output_dir/data_type/ if it does
    not already exist, and return the path as a string.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_individual_stock_layout():
    """
    For data_type='individual_stock', partition directories should follow
    the pattern: output_dir/individual_stock/date=YYYYMMDD/ticker=NNNN.parquet
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_stock_summary_layout():
    """
    For data_type='stock_summary', partition directories should follow
    the pattern: output_dir/stock_summary/date=YYYYMMDD/ticker=NNNN.parquet
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_indices_layout():
    """
    For data_type='indices', second-level partition key should be
    index_code= (e.g. index_code=101 for Nikkei 225).
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_indices_summary_layout():
    """
    For data_type='indices_summary', partition layout should use
    date= then index_code= (same as indices data type).
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_custom_partition_cols():
    """
    Passing partition_cols=['Data Date'] should override the default two-
    level partitioning and produce a flat date= layout.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_unknown_data_type_raises():
    """
    write_partitioned_parquet should raise ValueError when data_type is not
    one of the four recognised values.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_write_partitioned_parquet_missing_partition_col_raises():
    """
    If a custom partition_col is specified but absent from df, a ValueError
    should be raised before any files are written.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_read_parquet_partition_roundtrip():
    """
    A DataFrame written by write_partitioned_parquet should be exactly
    recoverable by read_parquet_partition with no filters applied
    (same shape and column values, allowing for row order differences).
    """
    pytest.skip("Waiting for NEEDS data access")


def test_read_parquet_partition_date_filter():
    """
    Passing date='20230104' should return only rows from that partition
    directory, not rows from other dates in the same store.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_read_parquet_partition_ticker_filter():
    """
    Passing ticker=7203 should return only rows from the ticker=7203
    partition, not rows for other tickers.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_read_parquet_partition_column_pruning():
    """
    Passing columns=['Execution Price', 'Volume'] should return a DataFrame
    with exactly those two columns — no extra columns loaded from disk.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_read_parquet_partition_missing_store_raises():
    """
    read_parquet_partition should raise FileNotFoundError when no Parquet
    files exist for the given data_type under data_dir.
    """
    pytest.skip("Waiting for NEEDS data access")


def test_read_parquet_partition_data_date_is_datetime():
    """
    The 'Data Date' column in the returned DataFrame should have dtype
    datetime64[ns], matching the output of create_df().
    """
    pytest.skip("Waiting for NEEDS data access")
