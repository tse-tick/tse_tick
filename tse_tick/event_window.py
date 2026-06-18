# tse_tick/event_window.py
import datetime
import logging
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import polars as pl

from .core import _tick_datetime

logger = logging.getLogger(__name__)


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

    tick_dt = _tick_datetime(raw_df)

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
    data_type: str = "individual_stock",
) -> pl.DataFrame:
    """Extract the ticks around one event from a built Parquet store.

    Queries the ``ingest_*`` store via :func:`tse_tick.query_ticks` for a single
    ``ticker`` on ``event_date``. When ``event_time`` is given the result is
    restricted to ``[event_time - before, event_time + after]`` and a
    ``seconds_from_event`` column is added (signed seconds of each tick relative
    to the event). When ``event_time`` is **omitted** the whole trading day is
    returned and ``before`` / ``after`` are **not** applied — there is no anchor to
    centre a window on.

    Quote-only book updates have a blank ``Execution Time`` but a real
    ``Update Time``; ``seconds_from_event`` falls back to ``Update Time`` for those
    rows (matching how :func:`tse_tick.query_ticks` time-filters them), so the
    whole in-window order book is timed rather than crashing on the blank field.

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        ticker: Stock code (``individual_stock``) or index code (``indices``).
        event_date: Event day as ``"YYYYMMDD"``.
        event_time: Event time-of-day ``"HH:MM:SS"``; ``None`` returns the day.
        before: Window extent before the event (e.g. ``"30min"``, ``"1h"``).
        after: Window extent after the event.
        columns: Column projection; ``None`` selects all columns.
        data_type: A tick type — ``"individual_stock"`` or ``"indices"`` (the two
            ``*_summary`` types are daily aggregates with no intraday timestamp, so
            event windows don't apply and are rejected).

    Returns:
        A Polars DataFrame of the window (with ``seconds_from_event`` when
        ``event_time`` is given); empty if nothing matches.
    """
    from tse_tick.query import query_ticks

    _TICK_TYPES = {"individual_stock", "indices"}
    if data_type not in _TICK_TYPES:
        raise ValueError(
            f"extract_event_window supports only tick types with an Execution Time "
            f"({sorted(_TICK_TYPES)}); got {data_type!r}. The *_summary types are "
            f"daily aggregates with no intraday timestamp."
        )

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
            raise ValueError(f"Invalid event_time format (expected 'HH:MM:SS'): {event_time!r}")

        start_dt = event_dt - before_offset
        end_dt = event_dt + after_offset
        start_time = start_dt.strftime("%H:%M:%S")
        end_time = end_dt.strftime("%H:%M:%S")

    df = query_ticks(
        data_dir,
        data_type=data_type,
        ticker=ticker,
        date=event_date,
        start_time=start_time,
        end_time=end_time,
        columns=columns,
    )

    if (
        event_time is not None
        and event_dt is not None
        and not df.is_empty()
        and "Execution Time" in df.columns
    ):
        from .core import _tick_datetime_expr

        # Quote-only rows have a blank Execution Time but a real Update Time (the
        # rows query_ticks keeps via its Update Time fallback). Use the same
        # effective time so seconds_from_event is defined for every row instead of
        # blowing up on an empty "HHMMSS" -> "YYYY-MM-DD ::" (the run8 crash).
        exec_raw = pl.col("Execution Time").cast(pl.String).str.strip_chars()
        if "Update Time" in df.columns:
            eff_time = (
                pl.when(exec_raw.is_null() | (exec_raw == ""))
                .then(pl.col("Update Time"))
                .otherwise(pl.col("Execution Time"))
            )
        else:
            eff_time = pl.col("Execution Time")

        # Build the timestamp from the known event_date (so it still works when a
        # columns= projection drops Data Date) + the effective time, via the
        # shared tick-timestamp parser.
        date_str = f"{event_date[:4]}-{event_date[4:6]}-{event_date[6:]}"
        df = df.with_columns(
            eff_time.alias("_eff_time"),
            pl.lit(date_str).alias("_event_date"),
        )
        seconds = (
            (_tick_datetime_expr("_event_date", "_eff_time") - pl.lit(event_dt))
            .dt.total_seconds()
            .cast(pl.Float64)
            .alias("seconds_from_event")
        )
        df = df.with_columns(seconds).drop(["_eff_time", "_event_date"])

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
    data_type: str = "individual_stock",
) -> dict[str, Optional[pl.DataFrame]]:
    """Extract event windows for many events from a built Parquet store.

    Calls :func:`extract_event_window` once per row of ``events_df`` and returns a
    dict keyed by ``"{ticker}_{date}_{time}"`` (or ``"{ticker}_{date}_fullday"``
    when the time is missing). A row whose extraction raises is recorded as
    ``None`` (with a ``warnings.warn``) so one bad event never aborts the batch.

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        events_df: One row per event; must have ``ticker_col`` and ``date_col``
            (``time_col`` optional — a missing/blank time gives a full-day result).
        ticker_col / date_col / time_col: Column names in ``events_df``.
        before / after: Window extent on each side of the event (see
            :func:`extract_event_window`).
        columns: Column projection passed through to each query.
        max_workers: Thread workers for concurrent extraction (``1`` = serial).
        progress: Log a per-event progress line.
        data_type: A tick type — ``"individual_stock"`` or ``"indices"``.

    Returns:
        ``{event_key: DataFrame | None}`` — ``None`` for any event that failed.
    """
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
                before=before, after=after, columns=columns, data_type=data_type,
            )
            if progress:
                logger.info("[%d/%d] ticker=%s date=%s -> %d rows", idx + 1, total, ticker, date_str, len(df))
            return key, df
        except Exception as exc:
            warnings.warn(f"Failed event {key}: {exc}")
            if progress:
                logger.info("[%d/%d] ticker=%s date=%s -> ERROR: %s", idx + 1, total, ticker, date_str, exc)
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
