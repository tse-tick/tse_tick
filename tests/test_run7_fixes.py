# tests/test_run7_fixes.py
"""Regression tests for the run7 real-data bug report.

F1 — the `rows` cap must notify via a capturable `TruncationWarning` (was a
logging/stderr message on a different channel than `NoDataWarning`). F2 — the
store-only discovery helpers must point a user who passed a raw NEEDS folder to
the right tool. F3 (Major) — `stock_summary`/`indices_summary` stores partition
by date only (one file per date, code kept as a column) instead of one tiny file
per (date × ticker). F4 — summary `*Time` columns normalize to fixed-width
`HHMMSS` across eras. Plus a 2016 era-discovery regression guard.
"""
import warnings

import polars as pl
import pytest

from tse_tick import create_df, ingest_single_zip, read_ticks
from tse_tick.enhanced import TruncationWarning, discover_zips
from tse_tick.query import query_ticks, get_available_tickers, get_available_dates
from tests.synthetic_data import (
    indices_csv,
    stock_summary_csv,
    write_zip,
)


def _zip(path, member, payload):
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_zip(path, member, payload)


# --------------------------------------------------------------------------- #
# F1 — the rows cap warns via a capturable TruncationWarning
# --------------------------------------------------------------------------- #
def test_f1_rows_cap_emits_truncation_warning(tmp_path):
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=8))
    with pytest.warns(TruncationWarning):
        df = read_ticks(str(zp), data_type="indices", date="20230508", rows=3)
    assert df.height == 3


def test_f1_truncation_warning_is_capturable_userwarning(tmp_path):
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=8))
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        read_ticks(str(zp), data_type="indices", date="20230508", rows=3)
    assert any(isinstance(w.message, TruncationWarning) for w in rec)
    assert issubclass(TruncationWarning, UserWarning)


def test_f1_no_truncation_warning_under_cap(tmp_path):
    zp = _zip(tmp_path / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=8))
    with warnings.catch_warnings():
        warnings.simplefilter("error", TruncationWarning)
        df = read_ticks(str(zp), data_type="indices", date="20230508", rows=100)
    assert df.height == 8


# --------------------------------------------------------------------------- #
# F2 — store helpers on a raw NEEDS folder point to the right tool
# --------------------------------------------------------------------------- #
def test_f2_get_available_tickers_error_points_to_ingest(tmp_path):
    raw = tmp_path / "TICSS110"
    raw.mkdir()
    with pytest.raises(FileNotFoundError, match="ingest"):
        get_available_tickers(str(raw), data_type="stock_summary")


def test_f2_get_available_dates_error_points_to_ingest(tmp_path):
    raw = tmp_path / "TICSS110"
    raw.mkdir()
    with pytest.raises(FileNotFoundError, match="ingest"):
        get_available_dates(str(raw), data_type="stock_summary")


# --------------------------------------------------------------------------- #
# F3 — summary stores partition by date only (one file/date, code as a column)
# --------------------------------------------------------------------------- #
def test_f3_stock_summary_store_is_one_file_per_date(tmp_path):
    zp = _zip(tmp_path / "HTICSS110.202305.zip", "HTICSS110.202305.csv",
              stock_summary_csv("20230508", ["7203", "6758", "9984"]))
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2023)
    date_dir = store / "stock_summary" / "date=20230508"
    files = list(date_dir.glob("*.parquet"))
    assert len(files) == 1                              # not one file per ticker
    assert not list(date_dir.glob("ticker=*.parquet"))  # no per-ticker fan-out


def test_f3_query_ticks_summary_filters_by_code_column(tmp_path):
    zp = _zip(tmp_path / "HTICSS110.202305.zip", "HTICSS110.202305.csv",
              stock_summary_csv("20230508", ["7203", "6758"]))
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2023)

    one = query_ticks(str(store), "stock_summary", ticker=7203, date="20230508")
    assert one.height == 1
    assert one["Stock Code"].to_list() == ["7203"]

    both = query_ticks(str(store), "stock_summary", date="20230508")
    assert set(both["Stock Code"].to_list()) == {"7203", "6758"}


def test_f3_query_ticks_summary_unknown_ticker_is_typed_empty(tmp_path):
    zp = _zip(tmp_path / "HTICSS110.202305.zip", "HTICSS110.202305.csv",
              stock_summary_csv("20230508", ["7203"]))
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2023)
    df = query_ticks(str(store), "stock_summary", ticker=9999, date="20230508")
    assert df.height == 0
    assert "Stock Code" in df.columns          # typed-empty, not a crash


def test_f3_get_available_tickers_summary_reads_code_column(tmp_path):
    zp = _zip(tmp_path / "HTICSS110.202305.zip", "HTICSS110.202305.csv",
              stock_summary_csv("20230508", ["7203", "6758", "9984"]))
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="stock_summary", year=2023)
    assert get_available_tickers(str(store), "stock_summary") == ["6758", "7203", "9984"]
    assert get_available_dates(str(store), "stock_summary") == ["20230508"]


def test_f3_summary_types_partition_by_date_only():
    """Both summary types coarsen to a date-only partition; tick types keep the
    per-ticker partition (where each ticker-day is many rows, so no fan-out)."""
    from tse_tick.io.parquet import _DEFAULT_PARTITION_COLS
    assert _DEFAULT_PARTITION_COLS["stock_summary"] == ["Data Date"]
    assert _DEFAULT_PARTITION_COLS["indices_summary"] == ["Data Date"]
    assert _DEFAULT_PARTITION_COLS["individual_stock"][-1] == "Stock Code"
    assert _DEFAULT_PARTITION_COLS["indices"][-1] == "Index Code"


# --------------------------------------------------------------------------- #
# F4 — summary *Time columns normalize to a fixed-width 6-char HHMMSS
# --------------------------------------------------------------------------- #
def test_f4_summary_time_2023_width_normalized(tmp_path):
    zp = _zip(tmp_path / "HTICSS110.202305.zip", "HTICSS110.202305.csv",
              stock_summary_csv("20230508", ["7203"], time_value="090005000000"))
    df = create_df(str(zp), auto_detect=False, data_type="stock_summary", year=2023)
    assert df["AM Opening Time"][0] == "090005"   # 12-char HHMMSSffffff -> HHMMSS
    assert df["AM Close Time"][0] == "090005"


def test_f4_summary_time_2016_width_normalized(tmp_path):
    zp = _zip(tmp_path / "HTICSS110.201609.zip", "HTICSS110.201609.csv",
              stock_summary_csv("20160901", ["7203"], time_value="0900"))
    df = create_df(str(zp), auto_detect=False, data_type="stock_summary", year=2016)
    assert df["AM Opening Time"][0] == "090000"   # 4-char HHMM -> HHMMSS


# --------------------------------------------------------------------------- #
# Era regression (report §5): 2016 legacy …010 index data stays discoverable
# --------------------------------------------------------------------------- #
def test_2016_indices_summary_010_discoverable(tmp_path):
    zp = _zip(tmp_path / "個別株式2016" / "TICIS010" / "201609" / "HTICIS010.201609.zip",
              "HTICIS010.201609.csv", b"x")
    found = discover_zips(str(tmp_path), "indices_summary", [2016])
    assert len(found) == 1
