# tests/test_read_ticks.py
"""Tests for read_ticks — the one-shot raw-ZIP reader (no Parquet store).

Uses the synthetic NEEDS-format generators (no proprietary data): a single ZIP,
a flat directory of ZIPs, a structured ``{year}/{yearmonth}/`` root, and an
index-tick ZIP.
"""

import polars as pl
import pytest

from tse_tick import read_ticks, TruncationWarning
from tests.synthetic_data import individual_stock_csv, indices_csv, write_zip

_BASE = {"7203": 2100, "6758": 13000}


@pytest.fixture(scope="module")
def single_stock_zip(tmp_path_factory):
    d = tmp_path_factory.mktemp("rt_single")
    zp = d / "HTICST120.20230704.1.zip"
    write_zip(zp, "HTICST120.20230704.1.csv",
              individual_stock_csv("20230704", ["7203", "6758"], rows_per_ticker=40,
                                   base_prices=_BASE))
    return zp


@pytest.fixture(scope="module")
def flat_dir(tmp_path_factory):
    d = tmp_path_factory.mktemp("rt_flat")
    for date in ("20230703", "20230704"):
        zp = d / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203", "6758"], rows_per_ticker=20,
                                       base_prices=_BASE))
    return d


@pytest.fixture(scope="module")
def structured_root(tmp_path_factory):
    root = tmp_path_factory.mktemp("rt_root")
    month_dir = root / "2023" / "202307"
    month_dir.mkdir(parents=True)
    for date in ("20230703", "20230704"):
        zp = month_dir / f"HTICST120.{date}.1.zip"
        write_zip(zp, f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203", "6758"], rows_per_ticker=20,
                                       base_prices=_BASE))
    return root


@pytest.fixture(scope="module")
def indices_zip(tmp_path_factory):
    d = tmp_path_factory.mktemp("rt_idx")
    zp = d / "HTICIT110.20230704.1.zip"
    write_zip(zp, "HTICIT110.20230704.1.csv",
              indices_csv("20230704", ["101", "113"], rows_per_code=16))
    return zp


def _stock_codes(df):
    return set(df["Stock Code"].cast(pl.String).str.strip_chars().str.slice(0, 4).to_list())


def test_single_zip_all_tickers(single_stock_zip):
    df = read_ticks(str(single_stock_zip), data_type="individual_stock")
    assert isinstance(df, pl.DataFrame)
    assert df.shape[1] == 95
    assert _stock_codes(df) == {"7203", "6758"}


def test_ticker_filter(single_stock_zip):
    df = read_ticks(str(single_stock_zip), ticker_filter={"7203"})
    assert _stock_codes(df) == {"7203"}
    assert df.height == 40


def test_no_match_returns_empty(single_stock_zip):
    df = read_ticks(str(single_stock_zip), ticker_filter={"9999"})
    assert isinstance(df, pl.DataFrame)
    assert df.height == 0


def test_time_window(single_stock_zip):
    df = read_ticks(str(single_stock_zip), ticker_filter={"7203"},
                    start_time="09:00:00", end_time="11:30:00")
    et = df["Execution Time"].cast(pl.String).to_list()
    assert et and all("090000" <= t <= "113000" for t in et)
    assert df.height < 40  # afternoon ticks excluded


def test_structured_root_date_and_time(structured_root):
    df = read_ticks(str(structured_root), ticker_filter={"7203"}, date="20230704",
                    start_time="12:30:00", end_time="15:00:00")
    assert df.height > 0
    assert set(df["Data Date"].dt.strftime("%Y%m%d").to_list()) == {"20230704"}
    assert all("123000" <= t <= "150000"
               for t in df["Execution Time"].cast(pl.String).to_list())


def test_structured_root_requires_date(structured_root):
    with pytest.raises(ValueError):
        read_ticks(str(structured_root), ticker_filter={"7203"})


def test_flat_dir_date_filter(flat_dir):
    df = read_ticks(str(flat_dir), ticker_filter={"6758"}, date="20230703")
    assert set(df["Data Date"].dt.strftime("%Y%m%d").to_list()) == {"20230703"}
    assert _stock_codes(df) == {"6758"}


def test_flat_dir_all_dates(flat_dir):
    df = read_ticks(str(flat_dir), ticker_filter={"7203"})
    assert set(df["Data Date"].dt.strftime("%Y%m%d").to_list()) == {"20230703", "20230704"}


def test_columns_projection(single_stock_zip):
    cols = ["Data Date", "Execution Time", "Execution Price"]
    df = read_ticks(str(single_stock_zip), ticker_filter={"7203"}, columns=cols)
    assert df.columns == cols


def test_columns_projection_missing_raises(single_stock_zip):
    with pytest.raises(ValueError):
        read_ticks(str(single_stock_zip), ticker_filter={"7203"}, columns=["No Such Column"])


def test_rows_cap(single_stock_zip):
    df = read_ticks(str(single_stock_zip), rows=10)
    assert df.height == 10


def test_indices_code_filter_and_time(indices_zip):
    df = read_ticks(str(indices_zip), data_type="indices", ticker_filter={"101"})
    assert set(df["Index Code"].cast(pl.String).to_list()) == {"101"}
    df2 = read_ticks(str(indices_zip), data_type="indices", ticker_filter={"101"},
                     start_time="09:00:00", end_time="10:00:00")
    assert 0 < df2.height <= df.height


def test_summary_time_filter_raises():
    # *_summary types are daily aggregates: start/end times are rejected before
    # any file access, so a nonexistent source still yields the ValueError.
    with pytest.raises(ValueError):
        read_ticks("nonexistent.zip", data_type="indices_summary", start_time="09:00:00")


def test_unknown_data_type_raises(single_stock_zip):
    with pytest.raises(ValueError):
        read_ticks(str(single_stock_zip), data_type="not_a_type")


def test_zip_parts_sorted_chronologically(tmp_path):
    # parts .1, .2, .10 must resolve in numeric (time) order, not lexical 1,10,2
    from tse_tick.enhanced import _resolve_source_zips
    for n in (10, 1, 2):
        zp = tmp_path / f"HTICST120.20240104.{n}.zip"
        write_zip(zp, f"HTICST120.20240104.{n}.csv",
                  individual_stock_csv("20240104", ["7203"], rows_per_ticker=2, base_prices={"7203": 2100}))
    resolved = _resolve_source_zips(str(tmp_path), "individual_stock", "20240104")
    nums = [int(p.name.split(".")[2]) for p in resolved]
    assert nums == [1, 2, 10]


def test_read_ticks_warns_on_row_cap(tmp_path):
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(zp, "HTICST120.20240104.1.csv",
              individual_stock_csv("20240104", ["7203"], rows_per_ticker=10, base_prices={"7203": 2100}))
    # The cap now warns via a capturable TruncationWarning (not logging), the same
    # warnings channel as NoDataWarning.
    with pytest.warns(TruncationWarning, match="row cap"):
        df = read_ticks(str(zp), ticker_filter={"7203"}, rows=3)
    assert df.height == 3
