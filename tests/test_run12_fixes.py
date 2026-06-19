# tests/test_run12_fixes.py
"""Regression tests for the run11 bug report.

F1 (Major) — a flat folder of a *monthly*-packaged type (indices / *_summary)
with a single-*day* date returned a false-empty result: the flat-folder discovery
matched the date token as a filename substring, so a day never matched a
"…YYYYMM.zip" monthly file. The flat path now resolves dates the same way as a
structured root (via discover_zips), mapping a day onto its monthly file.
F2 — get_supported_years() is documented and consistent with the get_info banner.
F3 — get_info() returns the banner without printing it (so print() shows it once).
"""
import polars as pl

import tse_tick
from tse_tick import read_ticks
from tests.synthetic_data import (
    indices_csv,
    stock_summary_csv,
    individual_stock_csv,
    write_zip,
)


def _dates(df):
    return {str(d)[:10] for d in df["Data Date"].to_list()}


# --------------------------------------------------------------------------- #
# F1 (MAJOR) — flat folder of a monthly type + single-day date finds the day
# --------------------------------------------------------------------------- #
def test_f1_flat_month_folder_single_day_indices(tmp_path):
    # A generically-named flat folder holding one MONTHLY zip (two days inside).
    d = tmp_path / "mydata"
    d.mkdir()
    payload = (indices_csv("20230508", ["101"], rows_per_code=6)
               + indices_csv("20230509", ["101"], rows_per_code=6))
    write_zip(d / "HTICIT110.202305.zip", "HTICIT110.202305.csv", payload)

    df = read_ticks(str(d), data_type="indices", ticker_filter={"101"}, date="20230508")
    assert df.height == 6                 # was (0, 10) + a misleading NoDataWarning
    assert _dates(df) == {"2023-05-08"}   # pruned to the requested day, not the month


def test_f1_flat_month_folder_single_day_stock_summary(tmp_path):
    d = tmp_path / "mydata"
    d.mkdir()
    payload = stock_summary_csv("20230508", ["7203"]) + stock_summary_csv("20230509", ["7203"])
    write_zip(d / "HTICSS110.202305.zip", "HTICSS110.202305.csv", payload)

    df = read_ticks(str(d), data_type="stock_summary", ticker_filter={"7203"}, date="20230508")
    assert df.height == 1                 # was (0, 82)
    assert _dates(df) == {"2023-05-08"}


def test_f1_flat_month_folder_month_date_still_works(tmp_path):
    d = tmp_path / "mydata"
    d.mkdir()
    payload = (indices_csv("20230508", ["101"], rows_per_code=6)
               + indices_csv("20230509", ["101"], rows_per_code=6))
    write_zip(d / "HTICIT110.202305.zip", "HTICIT110.202305.csv", payload)

    df = read_ticks(str(d), data_type="indices", ticker_filter={"101"}, date="202305")
    assert df.height == 12                # both days (month granularity), unchanged


def test_f1_flat_daily_folder_single_day_unaffected(tmp_path):
    # Daily-packaged individual_stock still filters to the requested day and does
    # not over-read other days in the folder.
    d = tmp_path / "mydata"
    d.mkdir()
    for date in ("20230703", "20230704"):
        write_zip(d / f"HTICST120.{date}.1.zip", f"HTICST120.{date}.1.csv",
                  individual_stock_csv(date, ["7203"], rows_per_ticker=6, base_prices={"7203": 2100}))
    df = read_ticks(str(d), ticker_filter={"7203"}, date="20230703")
    assert _dates(df) == {"2023-07-03"}


# --------------------------------------------------------------------------- #
# F2 — get_supported_years() documented + consistent with the get_info banner
# --------------------------------------------------------------------------- #
def test_f2_supported_years_consistent_and_documented():
    yrs = tse_tick.get_supported_years()
    assert yrs == (2016, 2025)                                   # not (2016, current_year)
    assert (tse_tick.get_supported_years.__doc__ or "").strip()
    assert f"{yrs[0]}-{yrs[1]}" in tse_tick.get_info()          # banner matches


def test_f2_get_version_has_docstring():
    assert (tse_tick.get_version.__doc__ or "").strip()


# --------------------------------------------------------------------------- #
# F3 — get_info() returns the banner without printing it
# --------------------------------------------------------------------------- #
def test_f3_get_info_does_not_print(capsys):
    out = tse_tick.get_info()
    assert capsys.readouterr().out == ""           # no internal print
    assert isinstance(out, str) and "tse_tick" in out


def test_f3_print_get_info_shows_banner_once(capsys):
    print(tse_tick.get_info())
    assert capsys.readouterr().out.count("tse_tick v") == 1   # once, not twice
