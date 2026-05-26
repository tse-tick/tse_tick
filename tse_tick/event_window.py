# tse_tick/event_window.py
import datetime
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import polars as pl


_OFFSET_MAP = {
    "1min": datetime.timedelta(minutes=1),
    "5min": datetime.timedelta(minutes=5),
    "10min": datetime.timedelta(minutes=10),
    "30min": datetime.timedelta(minutes=30),
    "60min": datetime.timedelta(hours=1),
    "120min": datetime.timedelta(hours=2),
    "180min": datetime.timedelta(hours=3),
    "240min": datetime.timedelta(hours=4),
    "1h": datetime.timedelta(hours=1),
    "2h": datetime.timedelta(hours=2),
    "3h": datetime.timedelta(hours=3),
    "4h": datetime.timedelta(hours=4),
}


def _to_offset(offset_str: str) -> datetime.timedelta:
    if offset_str in _OFFSET_MAP:
        return _OFFSET_MAP[offset_str]
    if offset_str.endswith("min"):
        try:
            return datetime.timedelta(minutes=int(offset_str[:-3]))
        except ValueError:
            pass
    if offset_str.endswith("h"):
        try:
            return datetime.timedelta(hours=int(offset_str[:-1]))
        except ValueError:
            pass
    raise ValueError(f"Invalid offset string: {offset_str}")


def _filter_ticks_for_events(
    raw_df: pl.DataFrame,
    events: pl.DataFrame,
    window_minutes: int = 120,
) -> pl.DataFrame:
    if raw_df.is_empty() or events.is_empty():
        return raw_df.clear()

    date_part = raw_df["Data Date"].cast(pl.Date).cast(pl.String)
    time_raw = raw_df["Execution Time"].cast(pl.String)
    has_colon = time_raw.str.contains(":")
    time_str = (
        pl.when(has_colon)
        .then(time_raw)
        .otherwise(
            time_raw.str.slice(0, 2) + ":"
            + time_raw.str.slice(2, 2) + ":"
            + time_raw.str.slice(4, 2)
        )
    )
    tick_dt = (date_part + " " + time_str).str.to_datetime(
        "%Y-%m-%d %H:%M:%S", strict=False
    )

    raw_df = raw_df.with_columns(
        tick_dt.dt.replace_time_zone("Asia/Tokyo").alias("_tick_dt"),
        raw_df["Stock Code"]
        .cast(pl.String)
        .str.strip_chars()
        .str.slice(0, 4)
        .alias("_stock_4"),
    )

    window_delta = datetime.timedelta(minutes=window_minutes)
    event_rows = events.to_dicts()

    result_parts = []

    for ev_row in event_rows:
        ticker_str = str(ev_row.get("ticker", "")).strip().split(".")[0].zfill(4)
        anchor = ev_row.get("reaction_anchor_dt")
        if anchor is None:
            continue

        if hasattr(anchor, "tz_localize") and anchor.tzinfo is None:
            try:
                anchor = anchor.tz_localize("Asia/Tokyo")
            except Exception:
                pass

        lower = anchor - window_delta
        upper = anchor + window_delta

        matching = raw_df.filter(
            (pl.col("_stock_4") == ticker_str)
            & (pl.col("_tick_dt") >= lower)
            & (pl.col("_tick_dt") <= upper)
        )
        if matching.is_empty():
            continue

        matching = matching.with_columns(
            pl.lit(ticker_str).alias("event_ticker"),
            pl.lit(str(ev_row.get("event_type", ""))).alias("event_type"),
            pl.lit(str(ev_row.get("session_type", ""))).alias("session_type"),
            pl.lit(anchor).alias("reaction_anchor"),
        )

        result_parts.append(matching)

    if not result_parts:
        result = raw_df.clear()
    else:
        result = pl.concat(result_parts, how="vertical")
    internal_cols = [c for c in ["_tick_dt", "_stock_4"] if c in result.columns]
    if internal_cols:
        result = result.drop(internal_cols)
    return result


def extract_event_window(
    data_dir: str,
    ticker: int,
    event_date: str,
    event_time: Optional[str] = None,
    before: str = "60min",
    after: str = "60min",
    columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    from tse_tick.query import query_ticks

    if len(event_date) != 8 or not event_date.isdigit():
        raise ValueError(f"event_date must be 'YYYYMMDD', got {event_date!r}")

    start_time: Optional[str] = None
    end_time: Optional[str] = None
    event_dt: Optional[datetime.datetime] = None

    if event_time is not None:
        before_offset = _to_offset(before)
        after_offset = _to_offset(after)

        try:
            event_dt = datetime.datetime.strptime(
                f"{event_date[:4]}-{event_date[4:6]}-{event_date[6:]} {event_time}",
                "%Y-%m-%d %H:%M:%S",
            )
        except Exception:
            raise ValueError(f"Invalid event_time format: {event_time!r}")

        start_dt = event_dt - before_offset
        end_dt = event_dt + after_offset
        start_time = start_dt.strftime("%H:%M:%S")
        end_time = end_dt.strftime("%H:%M:%S")

    df = query_ticks(
        data_dir,
        data_type="individual_stock",
        ticker=ticker,
        date=event_date,
        start_time=start_time,
        end_time=end_time,
        columns=columns,
    )

    if event_time is not None and event_dt is not None and not df.is_empty():
        exec_strs = df["Execution Time"].cast(pl.String)
        exec_dts = [
            datetime.datetime.strptime(
                f"{event_date[:4]}-{event_date[4:6]}-{event_date[6:]} {ts[:2]}:{ts[2:4]}:{ts[4:6]}",
                "%Y-%m-%d %H:%M:%S",
            )
            for ts in exec_strs.to_list()
        ]
        seconds = [(dt - event_dt).total_seconds() for dt in exec_dts]
        df = df.with_columns(pl.Series("seconds_from_event", seconds))

    return df


def extract_batch_event_windows(
    data_dir: str,
    events_df: pl.DataFrame,
    ticker_col: str = "ticker",
    date_col: str = "event_date",
    time_col: str = "event_time",
    before: str = "60min",
    after: str = "60min",
    columns: Optional[list[str]] = None,
    max_workers: int = 1,
    progress: bool = True,
) -> dict[str, Optional[pl.DataFrame]]:
    if ticker_col not in events_df.columns:
        raise ValueError(f"ticker_col {ticker_col!r} not in events_df")
    if date_col not in events_df.columns:
        raise ValueError(f"date_col {date_col!r} not in events_df")

    total = len(events_df)
    results: dict[str, Optional[pl.DataFrame]] = {}

    rows_data = events_df.to_dicts()

    def _process(idx, row):
        ticker = int(row[ticker_col])

        date_val = row[date_col]
        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y%m%d")
        else:
            date_str = str(date_val).replace("-", "")

        time_val = row.get(time_col) if time_col in events_df.columns else None
        is_missing = time_val is None or (isinstance(time_val, float) and (time_val != time_val)) or time_val == ""
        if is_missing:
            time_str = None
            key = f"{ticker}_{date_str}_fullday"
        else:
            time_str = str(time_val)
            key = f"{ticker}_{date_str}_{time_str}"

        try:
            df = extract_event_window(
                data_dir, ticker, date_str, time_str,
                before=before, after=after, columns=columns,
            )
            if progress:
                print(f"[{idx + 1}/{total}] ticker={ticker} date={date_str} -> {len(df)} rows")
            return key, df
        except Exception as exc:
            warnings.warn(f"Failed event {key}: {exc}")
            if progress:
                print(f"[{idx + 1}/{total}] ticker={ticker} date={date_str} -> ERROR: {exc}")
            return key, None

    if max_workers > 1:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(_process, i, row): i for i, row in enumerate(rows_data)
            }
            for future in as_completed(futures):
                key, df = future.result()
                results[key] = df
    else:
        for i, row in enumerate(rows_data):
            key, df = _process(i, row)
            results[key] = df

    return results
