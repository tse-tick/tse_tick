# tests/test_run6_fixes.py
"""Regression tests for the run6 real-data bug report.

F1 — `stock_summary` measure columns must be numeric (were String, so `.mean()`
silently returned None). F2 — a time filter on `individual_stock` must retain
in-window quote-only book rows (blank `Execution Time`, real `Update Time`),
not just trade-coincident snapshots, for both `read_ticks` and `query_ticks`.
"""
import polars as pl

from tse_tick import create_df, ingest_single_zip, read_ticks
from tse_tick.query import query_ticks
from tests.synthetic_data import (
    stock_summary_csv,
    individual_stock_with_quote_rows_csv,
    write_zip,
)


# --------------------------------------------------------------------------- #
# F1 — stock_summary numeric columns must come back numeric
# --------------------------------------------------------------------------- #
def test_f1_stock_summary_measures_are_numeric(tmp_path):
    zp = tmp_path / "HTICSS110.202305.zip"
    write_zip(zp, "HTICSS110.202305.csv", stock_summary_csv("20230508", ["7203"]))
    df = read_ticks(str(zp), data_type="stock_summary", ticker_filter={"7203"}, date="20230508")

    assert df["Daily VWAP"].dtype == pl.Float64
    assert df["AM VWAP"].dtype == pl.Float64
    assert df["PM Total Volume"].dtype == pl.Float64
    # The headline symptom: an aggregation returns a number, not None.
    assert df["Daily VWAP"].mean() is not None
    assert df["Daily VWAP"].mean() > 0
    # Code and time columns stay non-numeric.
    assert df["Stock Code"].dtype == pl.String
    assert df["AM Opening Time"].dtype == pl.String


def test_f1_stock_summary_no_stray_string_measures(tmp_path):
    zp = tmp_path / "HTICSS110.202305.zip"
    write_zip(zp, "HTICSS110.202305.csv", stock_summary_csv("20230508", ["7203"]))
    df = create_df(str(zp), auto_detect=False, data_type="stock_summary", year=2023)
    string_cols = {c for c, d in zip(df.columns, df.dtypes) if d == pl.String}
    # Only id/code/time columns may remain String; every measure is numeric.
    for c in string_cols:
        assert ("Time" in c) or c in {
            "Record Type", "Exchange Code", "Security Type", "Stock Code",
        }, f"unexpected String measure column: {c!r}"


# --------------------------------------------------------------------------- #
# F2 — time filter retains in-window quote-only rows via Update Time
# --------------------------------------------------------------------------- #
def _quote_zip(tmp_path):
    payload = individual_stock_with_quote_rows_csv(
        "20240104", "7203",
        trade_times=["093000", "100000"],            # 2 trades, in-window
        quote_times=["093500", "101500", "160000"],  # 2 in-window quotes + 1 after 15:00
    )
    zp = tmp_path / "HTICST120.20240104.1.zip"
    write_zip(zp, "HTICST120.20240104.1.csv", payload)
    return zp


def test_f2_read_ticks_keeps_inwindow_quote_rows(tmp_path):
    zp = _quote_zip(tmp_path)
    full = read_ticks(str(zp), ticker_filter={"7203"}, date="20240104")
    assert full.height == 5  # 2 trades + 3 quote-only rows

    windowed = read_ticks(str(zp), ticker_filter={"7203"}, date="20240104",
                          start_time="09:00:00", end_time="15:00:00")
    # 2 trades + 2 in-window quotes kept; the 16:00 quote dropped. Before the fix
    # only the 2 trade rows survived (blank Execution Time excluded the quotes).
    assert windowed.height == 4
    # The blank Execution Time is preserved in the output (filter-only fallback).
    assert "" in windowed["Execution Time"].to_list()


def test_f2_read_ticks_drops_out_of_window_quotes(tmp_path):
    zp = _quote_zip(tmp_path)
    # A morning-only window keeps the 09:30 trade + 09:35 quote, drops 10:00/10:15/16:00.
    df = read_ticks(str(zp), ticker_filter={"7203"}, date="20240104",
                    start_time="09:00:00", end_time="09:40:00")
    assert df.height == 2


def test_f2_query_ticks_keeps_inwindow_quote_rows(tmp_path):
    zp = _quote_zip(tmp_path)
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2024)

    full = query_ticks(str(store), "individual_stock", ticker=7203, date="20240104")
    assert full.height == 5

    windowed = query_ticks(str(store), "individual_stock", ticker=7203, date="20240104",
                           start_time="09:00:00", end_time="15:00:00")
    assert windowed.height == 4  # not just the 2 trade rows
