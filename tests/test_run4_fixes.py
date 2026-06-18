# tests/test_run4_fixes.py
"""Regression tests for the run4 real-data bug report.

Covers F1 (query_ticks ORDER BY on summary types), F2/F3 (ticker_filter for
non-individual_stock in ingest and under language="jp"), F4/F6 (jp + 4-digit
time filtering), F5 (2016 index empty-frame crash + …010 discovery), F7 (monthly
sub-month over-return), F8 (parse_period single day/month), and F10/F11
(get_info return + namespace hygiene).
"""
from datetime import datetime, time
from pathlib import Path

import polars as pl

import tse_tick
from tse_tick import ingest_year_from_root, read_ticks
from tse_tick.core import _tick_datetime_expr
from tse_tick.enhanced import discover_zips, parse_period, _empty_typed_frame, detect_data_type_and_year
from tse_tick.query import query_ticks, get_available_tickers
from tests.synthetic_data import indices_csv, write_zip


def _indices_zip(path: Path, member: str, payload: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    return write_zip(path, member, payload)


# --------------------------------------------------------------------------- #
# F1 — query_ticks must not hard-code ORDER BY "Execution Time" (summary types)
# --------------------------------------------------------------------------- #
def test_f1_query_ticks_summary_no_execution_time(tmp_path):
    store = tmp_path / "store"
    pdir = store / "stock_summary" / "date=20230510"
    pdir.mkdir(parents=True)
    pl.DataFrame(
        {
            "Record Type": ["Stock Summary", "Stock Summary"],
            "Data Date": pl.Series([datetime(2023, 5, 10), datetime(2023, 5, 10)]),
            "Exchange Code": ["Tokyo", "Nagoya"],
            "Security Type": ["First", "First"],
            "Stock Code": ["7203", "7203"],
            "AM Close Price": [2100.0, 2101.0],
        }
    ).write_parquet(pdir / "ticker=7203.parquet")

    # Before the fix this raised duckdb BinderException (no "Execution Time").
    df = query_ticks(str(store), "stock_summary", ticker=7203, date="20230510")
    assert df.height == 2
    assert "Execution Time" not in df.columns


# --------------------------------------------------------------------------- #
# F2 — ingest ticker_filter must prune non-individual_stock types
# --------------------------------------------------------------------------- #
def test_f2_ingest_ticker_filter_prunes_indices(tmp_path):
    zp = tmp_path / "個別株式2023" / "TICIT110" / "202305" / "HTICIT110.20230508.1.zip"
    _indices_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101", "113"], rows_per_code=8))
    store = tmp_path / "store"
    ingest_year_from_root(str(tmp_path), str(store), 2023, "indices", ticker_filter={"101"})
    assert get_available_tickers(str(store), "indices") == [101]


# --------------------------------------------------------------------------- #
# F3 — read_ticks(ticker_filter) must work under language="jp"
# --------------------------------------------------------------------------- #
def test_f3_read_ticks_ticker_filter_honored_under_jp(tmp_path):
    zp = tmp_path / "HTICIT110.20230508.1.zip"
    _indices_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101", "113"], rows_per_code=8))
    en = read_ticks(str(zp), data_type="indices", ticker_filter={"101"}, date="20230508", language="en")
    jp = read_ticks(str(zp), data_type="indices", ticker_filter={"101"}, date="20230508", language="jp")
    assert en.height == 8           # only Nikkei 225 (code 101), not both codes
    assert jp.height == en.height   # jp must prune identically (was the whole month)


# --------------------------------------------------------------------------- #
# F4 — read_ticks(start/end time) must work under language="jp"
# --------------------------------------------------------------------------- #
def test_f4_read_ticks_time_filter_under_jp(tmp_path):
    zp = tmp_path / "HTICIT110.20230508.1.zip"
    _indices_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101"], rows_per_code=16))
    common = dict(data_type="indices", ticker_filter={"101"}, date="20230508",
                  start_time="09:00:00", end_time="11:30:00")
    en = read_ticks(str(zp), language="en", **common)
    jp = read_ticks(str(zp), language="jp", **common)   # was: ValueError
    assert 0 < en.height < 16
    assert jp.height == en.height


# --------------------------------------------------------------------------- #
# F5 — 2016 index empty-frame must not crash; …010 files must be discoverable
# --------------------------------------------------------------------------- #
def test_f5_empty_typed_frame_2016_indices_no_crash():
    df = _empty_typed_frame("indices", "en", 2016)   # was: ColumnNotFoundError "Update Time"
    assert df.height == 0
    assert df.width > 0


def test_f5_discovers_2016_era_010_index_files(tmp_path):
    zp = tmp_path / "個別株式2016" / "TICIT010" / "201601" / "HTICIT010.201601.zip"
    _indices_zip(zp, "HTICIT010.201601.csv", indices_csv("20160104", ["101"], rows_per_code=4))
    found = discover_zips(str(tmp_path), "indices", [2016])
    assert len(found) == 1


# --------------------------------------------------------------------------- #
# F6 — _tick_datetime_expr must parse 4-digit HHMM (2016 index Execution Time)
# --------------------------------------------------------------------------- #
def test_f6_tick_datetime_expr_handles_4digit_hhmm():
    df = pl.DataFrame({"Data Date": [datetime(2016, 9, 1)], "Execution Time": ["0900"]})
    got = df.select(_tick_datetime_expr().dt.time().alias("t"))["t"][0]
    assert got == time(9, 0, 0)   # was: null (slice produced "09:00:")


# --------------------------------------------------------------------------- #
# F7 — monthly types: a single-day request must not return the whole month
# --------------------------------------------------------------------------- #
def test_f7_monthly_single_day_request_prunes_to_day(tmp_path):
    payload = indices_csv("20230508", ["101"], rows_per_code=8) + indices_csv("20230509", ["101"], rows_per_code=8)
    zp = tmp_path / "個別株式2023" / "TICIT110" / "202305" / "HTICIT110.202305.zip"
    _indices_zip(zp, "HTICIT110.202305.csv", payload)
    df = read_ticks(str(tmp_path), data_type="indices", ticker_filter={"101"}, date="20230508")
    assert df.height == 8   # only 2023-05-08, not both days in the monthly ZIP


# --------------------------------------------------------------------------- #
# F8 — parse_period must accept a bare single day / single month
# --------------------------------------------------------------------------- #
def test_f8_parse_period_single_month_and_day():
    month = parse_period("202305")
    assert month["granularity"] == "month"
    assert month["months_by_year"] == {2023: [5]}
    day = parse_period("20230508")
    assert day["granularity"] == "date"
    assert day["dates"] == ["20230508"]


# --------------------------------------------------------------------------- #
# F10 / F11 — get_info returns a string; os/sys don't leak into the namespace
# --------------------------------------------------------------------------- #
def test_f10_get_info_returns_string():
    out = tse_tick.get_info()
    assert isinstance(out, str)
    assert "tse_tick" in out
    assert tse_tick.get_info.__doc__ is not None


def test_f11_stdlib_modules_not_exposed():
    assert "os" not in dir(tse_tick)
    assert "sys" not in dir(tse_tick)


# --------------------------------------------------------------------------- #
# F9 / F12 — Index Code is the raw code for both index types; unknown codes
# (e.g. 108, absent from the name table) show as the code, not "Unknown (108)"
# --------------------------------------------------------------------------- #
def test_f9_f12_indices_index_code_is_raw(tmp_path):
    zp = tmp_path / "HTICIT110.20230508.1.zip"
    _indices_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101", "108"], rows_per_code=4))
    df = read_ticks(str(zp), data_type="indices", date="20230508")
    codes = set(df["Index Code"].cast(pl.String).to_list())
    assert codes == {"101", "108"}          # raw codes (F9), not decoded names
    assert "Nikkei 225" not in codes        # F9: indices no longer decodes to a name
    assert "Unknown (108)" not in codes     # F12: an unknown code is just its code


def test_f9_indices_filter_still_accepts_display_name(tmp_path):
    zp = tmp_path / "HTICIT110.20230508.1.zip"
    _indices_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101", "113"], rows_per_code=4))
    by_code = read_ticks(str(zp), data_type="indices", ticker_filter={"101"}, date="20230508")
    by_name = read_ticks(str(zp), data_type="indices", ticker_filter={"Nikkei 225"}, date="20230508")
    assert by_code.height == 4 and by_name.height == by_code.height


# Bonus: HTICIS* files auto-detect as indices_summary, not stock_summary.
def test_bonus_autodetect_indices_summary_not_stock_summary(tmp_path):
    d = tmp_path / "2023"
    d.mkdir()
    write_zip(d / "HTICIS110.202305.zip", "HTICIS110.202305.csv", b"x")
    assert detect_data_type_and_year(str(d))[0] == "indices_summary"
