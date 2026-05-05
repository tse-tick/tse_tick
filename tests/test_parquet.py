# tests/test_parquet.py
import datetime
from pathlib import Path

import polars as pl
import pytest

from tse_tick.io.parquet import (
    write_event_window_parquet,
    read_partitioned_parquet,
    write_partitioned_parquet,
    read_parquet_partition,
)


def _mock_event_window_df(n_rows: int = 10, date_str: str = "20170315") -> pl.DataFrame:
    dt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    rows = []
    for i in range(n_rows):
        rows.append({
            "Data Date": dt,
            "Execution Time": f"{9 + i // 60:02d}{i % 60:02d}00",
            "Stock Code": "7203",
            "Execution Price": float(1000 + i),
            "Volume": 100,
            "Sell Quote 1 Best": float(1001 + i),
            "Buy Quote 1 Best": float(999 + i),
            "event_ticker": "7203",
            "event_type": "earnings",
            "session_type": "after_hours",
            "reaction_anchor": "2017-03-16 09:00:00",
        })
    return pl.DataFrame(rows)


def _mock_general_df(date_str: str = "20230104", ticker: int = 7203) -> pl.DataFrame:
    dt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"
    rows = []
    for i in range(5):
        rows.append({
            "Data Date": dt,
            "Stock Code": str(ticker),
            "Execution Time": f"{9 + i:02d}{30:02d}00",
            "Execution Price": float(2000 + i),
            "Volume": 200,
        })
    return pl.DataFrame(rows)


def test_write_event_window_parquet_creates_layout(tmp_path):
    df = _mock_event_window_df(date_str="20170315")
    write_event_window_parquet(df, str(tmp_path))
    expected = tmp_path / "year=2017" / "month=03" / "20170315.parquet"
    assert expected.exists()


def test_write_event_window_parquet_read_concat_rewrite(tmp_path):
    df1 = _mock_event_window_df(n_rows=5, date_str="20170315")
    df2 = _mock_event_window_df(n_rows=3, date_str="20170315")
    write_event_window_parquet(df1, str(tmp_path))
    write_event_window_parquet(df2, str(tmp_path))
    result = pl.read_parquet(tmp_path / "year=2017" / "month=03" / "20170315.parquet")
    assert len(result) == 8


def test_write_event_window_parquet_multiple_dates(tmp_path):
    df = pl.DataFrame({
        "Data Date": ["2017-03-15", "2017-03-15", "2017-03-16"],
        "Execution Time": ["093000", "093100", "090000"],
        "Stock Code": ["7203", "7203", "7203"],
        "Execution Price": [1000.0, 1001.0, 1002.0],
        "Volume": [100, 100, 100],
        "event_ticker": ["7203", "7203", "7203"],
        "event_type": ["earnings"] * 3,
        "session_type": ["after_hours"] * 3,
        "reaction_anchor": [None] * 3,
    })
    write_event_window_parquet(df, str(tmp_path))

    f1 = tmp_path / "year=2017" / "month=03" / "20170315.parquet"
    f2 = tmp_path / "year=2017" / "month=03" / "20170316.parquet"
    assert f1.exists()
    assert f2.exists()
    assert len(pl.read_parquet(f1)) == 2
    assert len(pl.read_parquet(f2)) == 1


def test_read_partitioned_parquet_roundtrip(tmp_path):
    df = _mock_event_window_df(n_rows=6, date_str="20170315")
    write_event_window_parquet(df, str(tmp_path))
    result = read_partitioned_parquet(str(tmp_path))
    assert len(result) == 6
    assert "Stock Code" in result.columns


def test_read_partitioned_parquet_year_filter(tmp_path):
    df17 = _mock_event_window_df(n_rows=4, date_str="20170315")
    df18 = _mock_event_window_df(n_rows=3, date_str="20180110")
    write_event_window_parquet(df17, str(tmp_path))
    write_event_window_parquet(df18, str(tmp_path))
    result17 = read_partitioned_parquet(str(tmp_path), year=2017)
    result18 = read_partitioned_parquet(str(tmp_path), year=2018)
    assert len(result17) == 4
    assert len(result18) == 3


def test_read_partitioned_parquet_missing_raises(tmp_path):
    nonexistent = tmp_path / "does_not_exist"
    with pytest.raises(FileNotFoundError):
        read_partitioned_parquet(str(nonexistent))


def test_write_partitioned_parquet_creates_directory(tmp_path):
    df = _mock_general_df()
    result = write_partitioned_parquet(df, str(tmp_path), "individual_stock")
    assert Path(result).exists()
    assert "individual_stock" in result


def test_write_partitioned_parquet_individual_stock_layout(tmp_path):
    df = _mock_general_df(date_str="20230104", ticker=7203)
    write_partitioned_parquet(df, str(tmp_path), "individual_stock")
    expected = tmp_path / "individual_stock" / "date=20230104" / "ticker=7203.parquet"
    assert expected.exists()


def test_write_partitioned_parquet_unknown_data_type_raises(tmp_path):
    df = _mock_general_df()
    with pytest.raises(ValueError, match="Unknown data_type"):
        write_partitioned_parquet(df, str(tmp_path), "bad_type")


def test_write_partitioned_parquet_missing_partition_col_raises(tmp_path):
    df = pl.DataFrame({"foo": [1]})
    with pytest.raises(ValueError):
        write_partitioned_parquet(df, str(tmp_path), "individual_stock")


def test_read_parquet_partition_missing_store_raises(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_parquet_partition(str(tmp_path), "individual_stock")


def test_read_parquet_partition_roundtrip(tmp_path):
    df = _mock_general_df(date_str="20230104", ticker=7203)
    write_partitioned_parquet(df, str(tmp_path), "individual_stock")
    result = read_parquet_partition(str(tmp_path), "individual_stock")
    assert len(result) == len(df)
