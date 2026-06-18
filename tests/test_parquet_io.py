# tests/test_parquet_io.py
"""Tests for tse_tick.io.parquet — Parquet read/write utilities.

Write-layout tests build tiny DataFrames inline (they exercise the writer
itself, which is the function under test). Read tests use the synthetic
``stock_store`` fixture, which is produced by running synthetic NEEDS-format
ZIPs through the real ingest pipeline (see conftest.py).
"""

import polars as pl
import pytest

from tse_tick.io.parquet import write_partitioned_parquet, read_parquet_partition


def _mk(date: str = "20230704", code: str = "7203", code_col: str = "Stock Code",
        n: int = 4) -> pl.DataFrame:
    """Minimal partitionable frame with the required partition columns."""
    return pl.DataFrame({
        "Data Date": [date] * n,
        code_col: [code] * n,
        "Execution Price": [float(1000 + i) for i in range(n)],
        "Volume": [100] * n,
    })


def test_write_partitioned_parquet_creates_directory(tmp_path):
    """
    write_partitioned_parquet should create output_dir/data_type/ if it does
    not already exist, and return the path as a string.
    """
    result = write_partitioned_parquet(_mk(), str(tmp_path), "individual_stock")
    assert isinstance(result, str)
    assert (tmp_path / "individual_stock").exists()


def test_write_partitioned_parquet_individual_stock_layout(tmp_path):
    """
    For data_type='individual_stock', partition directories should follow
    the pattern: output_dir/individual_stock/date=YYYYMMDD/ticker=NNNN.parquet
    """
    write_partitioned_parquet(_mk(date="20230704", code="7203"), str(tmp_path), "individual_stock")
    assert (tmp_path / "individual_stock" / "date=20230704" / "ticker=7203.parquet").exists()


def test_write_partitioned_parquet_stock_summary_layout(tmp_path):
    """
    For data_type='stock_summary' (a daily-aggregate type) the store partitions
    by date only — one file per date with the code kept as a column — to avoid
    the tens-of-thousands-of-tiny-files fan-out a per-(date, ticker) layout caused.
    """
    write_partitioned_parquet(_mk(date="20230704", code="6758"), str(tmp_path), "stock_summary")
    date_dir = tmp_path / "stock_summary" / "date=20230704"
    assert (date_dir / "20230704.parquet").exists()
    assert not list(date_dir.glob("ticker=*.parquet"))          # no per-ticker fan-out
    assert "Stock Code" in pl.read_parquet(date_dir / "20230704.parquet").columns


def test_write_partitioned_parquet_indices_layout(tmp_path):
    """
    For data_type='indices', the second-level partition value is the Index
    Code. The store encodes the second level in the filename, so the shipped
    layout is date=YYYYMMDD/ticker=NNNN.parquet (the partition *value* is the
    index code; the filename key is "ticker=", not "index_code=").
    """
    write_partitioned_parquet(
        _mk(date="20230704", code="101", code_col="Index Code"), str(tmp_path), "indices"
    )
    assert (tmp_path / "indices" / "date=20230704" / "ticker=101.parquet").exists()


def test_write_partitioned_parquet_indices_summary_layout(tmp_path):
    """
    For data_type='indices_summary' (a daily-aggregate type) the store likewise
    partitions by date only (one file per date, Index Code kept as a column).
    """
    write_partitioned_parquet(
        _mk(date="20230704", code="113", code_col="Index Code"), str(tmp_path), "indices_summary"
    )
    date_dir = tmp_path / "indices_summary" / "date=20230704"
    assert (date_dir / "20230704.parquet").exists()
    assert not list(date_dir.glob("ticker=*.parquet"))
    assert "Index Code" in pl.read_parquet(date_dir / "20230704.parquet").columns


def test_write_partitioned_parquet_custom_partition_cols(tmp_path):
    """
    Passing partition_cols=['Data Date'] should override the default two-level
    partitioning and produce a flat date= layout with a per-date file.
    """
    write_partitioned_parquet(
        _mk(date="20230704"), str(tmp_path), "individual_stock", partition_cols=["Data Date"]
    )
    assert (tmp_path / "individual_stock" / "date=20230704" / "20230704.parquet").exists()


def test_write_partitioned_parquet_unknown_data_type_raises(tmp_path):
    """
    write_partitioned_parquet should raise ValueError when data_type is not
    one of the four recognised values.
    """
    with pytest.raises(ValueError, match="Unknown data_type"):
        write_partitioned_parquet(_mk(), str(tmp_path), "bad_type")


def test_write_partitioned_parquet_missing_partition_col_raises(tmp_path):
    """
    If a partition column is absent from df, a ValueError should be raised
    before any files are written.
    """
    df = pl.DataFrame({"foo": [1, 2]})
    with pytest.raises(ValueError, match="Partition column"):
        write_partitioned_parquet(df, str(tmp_path), "individual_stock")


def test_read_parquet_partition_roundtrip(tmp_path):
    """
    A DataFrame written by write_partitioned_parquet should be recoverable by
    read_parquet_partition with no filters (same row count, original columns
    present), allowing for an added Hive partition column and row order.
    """
    df = _mk(date="20230704", code="7203", n=6)
    write_partitioned_parquet(df, str(tmp_path), "individual_stock")
    result = read_parquet_partition(str(tmp_path), "individual_stock")
    assert result.height == df.height
    for col in df.columns:
        assert col in result.columns


def test_read_parquet_partition_date_filter(stock_store):
    """
    Passing date='20230704' should return only rows from that partition
    directory, not rows from other dates in the same store.
    """
    result = read_parquet_partition(stock_store, "individual_stock", date="20230704")
    assert result.height == 3 * 40
    assert result["date"].unique().to_list() == [20230704]


def test_read_parquet_partition_ticker_filter(stock_store):
    """
    Passing ticker=7203 should return only rows for that ticker, not rows for
    other tickers.
    """
    result = read_parquet_partition(stock_store, "individual_stock", ticker=7203)
    assert result["Stock Code"].unique().to_list() == ["7203"]
    assert result.height == 40 * 2


def test_read_parquet_partition_column_pruning(stock_store):
    """
    Passing columns=['Execution Price', 'Volume'] should return a DataFrame
    with exactly those two columns.
    """
    result = read_parquet_partition(
        stock_store, "individual_stock", columns=["Execution Price", "Volume"]
    )
    assert set(result.columns) == {"Execution Price", "Volume"}


def test_read_parquet_partition_missing_store_raises(tmp_path):
    """
    read_parquet_partition should raise FileNotFoundError when no Parquet
    files exist for the given data_type under data_dir.
    """
    with pytest.raises(FileNotFoundError):
        read_parquet_partition(str(tmp_path), "individual_stock")


def test_read_parquet_partition_data_date_is_datetime(stock_store):
    """
    The 'Data Date' column in the returned DataFrame should be a temporal
    dtype, matching the output of create_df().
    """
    result = read_parquet_partition(stock_store, "individual_stock", ticker=7203)
    assert result["Data Date"].dtype.is_temporal()
