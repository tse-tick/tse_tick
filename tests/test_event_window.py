# tests/test_event_window.py
import datetime

import pandas as pd
import polars as pl
import pytest

from tse_tick.event_window import (
    extract_event_window,
    extract_batch_event_windows,
    _filter_ticks_for_events,
)


def _make_ticst120_pl(
    date_str: str = "20170315",
    tickers: list[int] | None = None,
    times_hhmm: list[tuple[int, int]] | None = None,
) -> pl.DataFrame:
    if tickers is None:
        tickers = [7203]
    if times_hhmm is None:
        times_hhmm = [(h, m) for h in range(9, 15) for m in range(0, 60, 5)]

    dt = pd.Timestamp(f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}")
    rows = []
    for ticker in tickers:
        for (hh, mm) in times_hhmm:
            rows.append({
                "Data Date": dt,
                "Stock Code": str(ticker),
                "Execution Time": datetime.time(hh, mm, 0),
                "Execution Price": float(1000 + hh),
                "Volume": 100,
                "Sell Quote 1 Best": float(1001 + hh),
                "Buy Quote 1 Best": float(999 + hh),
                "Sell Quote Vol 1": float(500),
                "Buy Quote Vol 1": float(600),
            })
    return pl.DataFrame(rows)


def _make_events_pl(
    ticker: int,
    anchor_str: str,
    event_type: str = "earnings",
    session_type: str = "after_hours",
) -> pl.DataFrame:
    anchor = pd.Timestamp(anchor_str).tz_localize("Asia/Tokyo")
    return pl.DataFrame([{
        "ticker": ticker,
        "event_type": event_type,
        "session_type": session_type,
        "reaction_anchor_dt": anchor,
    }])


def test_filter_ticks_basic_intraday():
    df = _make_ticst120_pl(date_str="20170315", tickers=[7203])
    events = _make_events_pl(7203, "2017-03-15 10:30:00", session_type="intraday")
    result = _filter_ticks_for_events(df, events, window_minutes=120)
    assert not result.is_empty()


def test_filter_ticks_after_hours_next_day_anchor():
    df = _make_ticst120_pl(date_str="20170315", tickers=[7203])
    events = _make_events_pl(7203, "2017-03-16 09:00:00", session_type="after_hours")
    result = _filter_ticks_for_events(df, events, window_minutes=120)
    assert result.is_empty()


def test_filter_ticks_anchor_same_day_window_end():
    df = _make_ticst120_pl(date_str="20170315", tickers=[7203])
    events = _make_events_pl(7203, "2017-03-15 14:00:00", session_type="intraday")
    result = _filter_ticks_for_events(df, events, window_minutes=120)
    assert not result.is_empty()
    exec_times = result["Execution Time"].to_list()
    for t in exec_times:
        if isinstance(t, datetime.time):
            mins = t.hour * 60 + t.minute
        else:
            mins = 0
        assert mins >= 720, f"No ticks before 12:00 should be in +/-120 min window, got {mins}"


def test_filter_ticks_adds_tag_columns():
    df = _make_ticst120_pl(date_str="20170315", tickers=[7203])
    events = _make_events_pl(7203, "2017-03-15 10:00:00", event_type="earnings", session_type="intraday")
    result = _filter_ticks_for_events(df, events, window_minutes=60)
    for col in ("event_ticker", "event_type", "session_type", "reaction_anchor"):
        assert col in result.columns
    non_null = result["event_ticker"].cast(pl.String).to_list()
    assert len(non_null) > 0
    assert non_null[0] == "7203"


def test_filter_ticks_empty_df_returns_empty():
    df = _make_ticst120_pl().clear()
    events = _make_events_pl(7203, "2017-03-15 10:00:00")
    result = _filter_ticks_for_events(df, events, window_minutes=120)
    assert result.is_empty()


def test_filter_ticks_empty_events_returns_empty():
    df = _make_ticst120_pl()
    events = pl.DataFrame(schema={"ticker": pl.String, "reaction_anchor_dt": pl.Datetime, "event_type": pl.String, "session_type": pl.String})
    result = _filter_ticks_for_events(df, events, window_minutes=120)
    assert result.is_empty()


def test_filter_ticks_multiple_tickers():
    df = _make_ticst120_pl(tickers=[7203, 6758])
    ev1 = _make_events_pl(7203, "2017-03-15 10:00:00")
    ev2 = _make_events_pl(6758, "2017-03-15 10:00:00")
    events = pl.concat([ev1, ev2], how="vertical")
    result = _filter_ticks_for_events(df, events, window_minutes=60)
    found_tickers = [str(s).strip()[:4] for s in result["Stock Code"].to_list()]
    assert "7203" in found_tickers
    assert "6758" in found_tickers


def test_filter_ticks_wrong_ticker_excluded():
    df = _make_ticst120_pl(tickers=[7203, 6758])
    events = _make_events_pl(7203, "2017-03-15 10:00:00")
    result = _filter_ticks_for_events(df, events, window_minutes=60)
    tickers_in_result = [str(s).strip()[:4] for s in result["Stock Code"].to_list()] if not result.is_empty() else []
    assert "6758" not in tickers_in_result


def test_extract_event_window_returns_dataframe():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_seconds_from_event_column():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_respects_before_after():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_no_event_time_full_day():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_empty_result_for_nonexistent_ticker():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_column_pruning():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_invalid_date_raises():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_event_window_invalid_before_raises():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_batch_event_windows_returns_dict():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_batch_event_windows_key_format():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_batch_event_windows_fullday_key():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_batch_event_windows_missing_ticker_col_raises():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_batch_event_windows_parallel_same_result():
    pytest.skip("Waiting for NEEDS data access")


def test_extract_batch_event_windows_failed_event_has_none_value():
    pytest.skip("Waiting for NEEDS data access")
