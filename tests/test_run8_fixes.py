# tests/test_run8_fixes.py
"""Regression tests for the run8 real-data bug report.

F1 (Major) — extract_event_window crashed on individual_stock because it parsed
Execution Time from every row, but quote-only rows have a blank Execution Time;
it must fall back to Update Time (as query_ticks does). F2/F3 — event-window
data_type param + docstrings; before/after require event_time. F4 — parse_period
error lists the single YYYYMM/YYYYMMDD forms. F5 — public helpers have docstrings.
F6 — the no-data warning hints at a data_type/folder mismatch, not just holidays.
"""
import warnings

import polars as pl
import pytest

import tse_tick
from tse_tick import (
    ingest_single_zip,
    extract_event_window,
    extract_batch_event_windows,
    read_ticks,
)
from tse_tick.enhanced import parse_period
from tests.synthetic_data import (
    individual_stock_with_quote_rows_csv,
    indices_csv,
    write_zip,
)


def _stock_store_with_quotes(tmp_path):
    """An individual_stock store whose 09:25–09:35 window holds a trade row plus
    quote-only rows (blank Execution Time, real Update Time)."""
    payload = individual_stock_with_quote_rows_csv(
        "20230508", "7203",
        trade_times=["093000"],                       # the event instant (a trade)
        quote_times=["092800", "093200", "100000"],   # 2 in-window quotes + 1 out
    )
    zp = tmp_path / "raw" / "HTICST120.20230508.1.zip"
    zp.parent.mkdir(parents=True, exist_ok=True)
    write_zip(zp, "HTICST120.20230508.1.csv", payload)
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="individual_stock", year=2023)
    return str(store)


# --------------------------------------------------------------------------- #
# F1 — event window must not crash on blank-Execution-Time quote rows
# --------------------------------------------------------------------------- #
def test_f1_event_window_handles_blank_execution_time(tmp_path):
    store = _stock_store_with_quotes(tmp_path)
    df = extract_event_window(store, ticker=7203, event_date="20230508",
                              event_time="09:30:00", before="5min", after="5min")
    # 1 trade (09:30) + 2 in-window quotes (09:28, 09:32); the 10:00 quote is out.
    assert df.height == 3
    assert "seconds_from_event" in df.columns
    # every row gets a value (quote rows via the Update Time fallback), no crash
    assert df["seconds_from_event"].null_count() == 0
    assert 0.0 in df["seconds_from_event"].to_list()        # the 09:30 trade == event
    assert set(df["seconds_from_event"].to_list()) == {-120.0, 0.0, 120.0}


def test_f1_batch_event_window_not_none(tmp_path):
    store = _stock_store_with_quotes(tmp_path)
    events = pl.DataFrame(
        {"ticker": ["7203"], "event_date": ["20230508"], "event_time": ["09:30:00"]}
    )
    res = extract_batch_event_windows(store, events, before="5min", after="5min")
    key = "7203_20230508_09:30:00"
    assert res[key] is not None          # was None (silent failure) before the fix
    assert res[key].height == 3


# --------------------------------------------------------------------------- #
# F2 — before/after only apply with event_time (omit = full day, documented)
# --------------------------------------------------------------------------- #
def test_f2_no_event_time_returns_full_day(tmp_path):
    store = _stock_store_with_quotes(tmp_path)
    full = extract_event_window(store, 7203, "20230508")
    # Without event_time there is no anchor, so before/after don't restrict.
    attempt = extract_event_window(store, 7203, "20230508", before="1min", after="1min")
    assert attempt.height == full.height
    assert "event_time" in extract_event_window.__doc__


# --------------------------------------------------------------------------- #
# F3 — data_type param: tick types only; indices reachable; docstring present
# --------------------------------------------------------------------------- #
def test_f3_event_window_rejects_summary_types(tmp_path):
    with pytest.raises(ValueError, match="tick types|Execution Time"):
        extract_event_window(str(tmp_path), 7203, "20230508",
                             event_time="09:30:00", data_type="stock_summary")


def test_f3_event_window_supports_indices(tmp_path):
    zp = tmp_path / "raw" / "HTICIT110.20230508.1.zip"
    zp.parent.mkdir(parents=True, exist_ok=True)
    write_zip(zp, "HTICIT110.20230508.1.csv", indices_csv("20230508", ["101"], rows_per_code=16))
    store = tmp_path / "store"
    ingest_single_zip(str(zp), str(store), data_type="indices", year=2023)
    df = extract_event_window(str(store), ticker=101, event_date="20230508",
                              event_time="10:00:00", before="60min", after="60min",
                              data_type="indices")
    assert df.height > 0
    assert "seconds_from_event" in df.columns
    assert df["seconds_from_event"].null_count() == 0


def test_f3_event_window_has_docstring():
    doc = extract_event_window.__doc__ or ""
    assert doc.strip() and "data_type" in doc


# --------------------------------------------------------------------------- #
# F4 — parse_period error lists the single YYYYMM / YYYYMMDD forms
# --------------------------------------------------------------------------- #
def test_f4_parse_period_error_lists_single_forms():
    with pytest.raises(ValueError) as exc:
        parse_period("2023-05-08")               # natural date with dashes
    msg = str(exc.value)
    assert "single" in msg.lower()
    assert "YYYYMM" in msg and "YYYYMMDD" in msg


# --------------------------------------------------------------------------- #
# F5 — public helpers carry docstrings
# --------------------------------------------------------------------------- #
def test_f5_public_helpers_have_docstrings():
    for fn in (
        tse_tick.ingest_year,
        tse_tick.ingest_year_from_root,
        tse_tick.get_supported_data_types,
        tse_tick.extract_event_window,
        tse_tick.extract_batch_event_windows,
    ):
        assert (fn.__doc__ or "").strip(), f"{fn.__name__} is missing a docstring"


# --------------------------------------------------------------------------- #
# F6 — no-ZIPs warning hints at a data_type/folder mismatch, not only holidays
# --------------------------------------------------------------------------- #
def test_f6_no_zips_warning_mentions_data_type_mismatch(tmp_path):
    # An indices folder read as individual_stock finds no HTICST120 files.
    d = tmp_path / "個別株式2023" / "TICIT110" / "202305"
    d.mkdir(parents=True)
    write_zip(d / "HTICIT110.20230508.1.zip", "HTICIT110.20230508.1.csv",
              indices_csv("20230508", ["101"], rows_per_code=4))
    with pytest.warns(tse_tick.NoDataWarning, match="data_type"):
        df = read_ticks(str(tmp_path), data_type="individual_stock", date="20230508")
    assert df.height == 0
