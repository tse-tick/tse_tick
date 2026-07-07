# tse_tick/ingest.py
"""Ingest raw NEEDS ZIPs into a Hive-partitioned Parquet store.

This is the ``tse_tick.ingest`` *submodule*, not a callable. The entry points are
the functions re-exported at the top level: :func:`ingest_period` (a structured
``{year}/{yearmonth}/`` root), :func:`ingest_year_from_root`, :func:`ingest_year`,
:func:`ingest_directory` (a flat folder of ZIPs), :func:`ingest_single_zip`, and
:func:`ingest_event_windows_period` (the event-window store) — e.g. call
``tse_tick.ingest_period(...)``, not ``tse_tick.ingest(...)``.
"""
import gc
import glob as _glob
import logging
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable, Optional, Union

import polars as pl

from tse_tick.enhanced import create_df, detect_data_type_and_year, discover_zips, parse_period, _zip_date_token, _filter_codes, _prune_parts_by_ticker, _normalize_ticker_filter, OneShotMemoryError
from tse_tick.io.parquet import write_partitioned_parquet, write_event_window_parquet
from tse_tick.event_window import _filter_ticks_for_events
from tse_tick.constants import validate_data_type

logger = logging.getLogger(__name__)

_MAX_WORKERS = 8


def ingest_single_zip(
    zip_path: str,
    output_dir: str,
    data_type: Optional[str] = None,
    year: Optional[int] = None,
    language: str = "en",
    ticker_filter: Optional[set] = None,
) -> dict:
    """Ingest one raw NEEDS ZIP into the Hive-partitioned Parquet store.

    Cleans the ZIP with :func:`tse_tick.create_df` and writes per-ticker Parquet
    files under ``output_dir/<data_type>/date=YYYYMMDD/ticker=NNNN.parquet``.

    Args:
        zip_path: Path to the NEEDS ``.zip``.
        output_dir: Store root to write under.
        data_type: NEEDS type; auto-detected from the path when ``None``.
        year: Era year; auto-detected from the path when ``None``.
        language: Output column-name language (``"en"`` / ``"jp"``).
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        A dict with keys ``"zip_path"``, ``"data_type"``, ``"year"``, ``"rows"``
        (rows written) and ``"output_path"`` (the store dir, or ``None`` when the
        ZIP yielded no rows).
    """
    path = Path(zip_path)
    if not path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    if data_type is None or year is None:
        data_type, year = detect_data_type_and_year(str(path))

    df = create_df(str(path), language=language, auto_detect=False, data_type=data_type, year=year, ticker_filter=ticker_filter)
    rows = len(df)
    if rows == 0:
        return {
            "zip_path": str(path.resolve()),
            "data_type": data_type,
            "year": year,
            "rows": 0,
            "output_path": None,
        }
    out_path = write_partitioned_parquet(df, output_dir, data_type)

    return {
        "zip_path": str(path.resolve()),
        "data_type": data_type,
        "year": year,
        "rows": rows,
        "output_path": out_path,
    }


def ingest_directory(
    input_dir: str,
    output_dir: str,
    data_type: Optional[str] = None,
    language: str = "en",
    max_workers: int = 1,
    progress: bool = True,
    ticker_filter: Optional[set] = None,
) -> list[dict]:
    """Ingest every ``.zip`` in a single flat directory into the Parquet store.

    Cleans each ZIP with :func:`tse_tick.create_df` and writes per-ticker Parquet
    files via :func:`ingest_single_zip`. Globs ``*.zip`` **directly** under
    ``input_dir`` (non-recursive); for a structured ``{year}/{yearmonth}/`` NEEDS
    root use :func:`ingest_period` / :func:`ingest_year_from_root` instead.

    Args:
        input_dir: Directory containing the NEEDS ``.zip`` files.
        output_dir: Store root to write under.
        data_type: NEEDS type; auto-detected per ZIP from the path when ``None``.
        language: Output column-name language (``"en"`` / ``"jp"``).
        max_workers: Parallel worker processes (capped at 8); ``1`` is serial.
        progress: Log a per-ZIP progress line.
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per ZIP (see :func:`ingest_single_zip`); a failed ZIP
        contributes ``{"zip_path": ..., "error": ...}`` instead.
    """
    if max_workers > _MAX_WORKERS:
        logger.warning("max_workers=%d exceeds cap of %d, limiting to %d", max_workers, _MAX_WORKERS, _MAX_WORKERS)
        max_workers = _MAX_WORKERS

    in_path = Path(input_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    zip_files = sorted(in_path.glob("*.zip"))
    total = len(zip_files)
    results: list[dict] = []

    def _process(zf: Path) -> dict:
        try:
            return ingest_single_zip(str(zf), output_dir, data_type=data_type, language=language, ticker_filter=ticker_filter)
        except OneShotMemoryError:
            raise  # a one-shot OOM aborts loudly; never record it as a skipped zip
        except Exception as exc:
            return {"zip_path": str(zf), "error": str(exc)}

    if max_workers > 1:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(_process, zf): zf for zf in zip_files}
            done = 0
            for future in as_completed(futures):
                done += 1
                meta = future.result()
                results.append(meta)
                if progress:
                    fname = Path(meta.get("zip_path", "")).name
                    rows = meta.get("rows", "error")
                    logger.info("[%d/%d] %s -> %s rows", done, total, fname, rows)
    else:
        for i, zf in enumerate(zip_files, 1):
            meta = _process(zf)
            results.append(meta)
            if progress:
                fname = Path(meta.get("zip_path", "")).name
                rows = meta.get("rows", "error")
                logger.info("[%d/%d] %s -> %s rows", i, total, fname, rows)

    return results


def ingest_year(
    input_dir: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    max_workers: int = 1,
    ticker_filter: Optional[set] = None,
) -> list[dict]:
    """Ingest every ``.zip`` for one ``year`` from a **flat** directory.

    Globs ``*.zip`` directly under ``input_dir`` (non-recursive) and keeps those
    whose filename contains ``str(year)``, ingesting each via
    :func:`ingest_single_zip`. For a structured ``{year}/{yearmonth}/`` NEEDS root
    use :func:`ingest_year_from_root` (or :func:`ingest_period`) instead.

    Args:
        input_dir: Directory holding the NEEDS ``.zip`` files.
        output_dir: Store root to write under.
        year: Era year (also selects era-specific parsing).
        data_type: One of the four NEEDS types.
        language: Output column-name language (``"en"`` / ``"jp"``).
        max_workers: Reserved for parallel ingestion.
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per ZIP (see :func:`ingest_single_zip`); a failed ZIP
        contributes ``{"zip_path": ..., "error": ...}`` instead.
    """
    validate_data_type(data_type)

    in_path = Path(input_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    year_zips = sorted(f for f in in_path.glob("*.zip") if str(year) in f.name)
    results: list[dict] = []

    for zf in year_zips:
        try:
            meta = ingest_single_zip(str(zf), output_dir, data_type=data_type, year=year, language=language, ticker_filter=ticker_filter)
        except OneShotMemoryError:
            raise
        except Exception as exc:
            meta = {"zip_path": str(zf), "error": str(exc)}
        results.append(meta)

    return results


def _ingest_date_group(date_str, zip_paths, output_dir, data_type, year, language, ticker_filter):
    """Read every ZIP part of one date, concat, and write each ticker file once.

    This is the multi-part-per-day unit: NEEDS splits a trading day across parts
    by ticker range (plus a closing tail), so all parts must be read and
    concatenated before writing — otherwise later parts get skipped (resume) or
    overwrite earlier ones.
    """
    parts: list = []
    for zp in zip_paths:
        try:
            df = create_df(
                str(zp), language=language, auto_detect=False,
                data_type=data_type, year=year, ticker_filter=ticker_filter,
            )
        except (zipfile.BadZipFile, EOFError) as exc:
            logger.error("Corrupt zip %s: %s", Path(zp).name, exc)
            continue
        except OneShotMemoryError:
            # A one-shot OOM must abort the date group, not silently write a partial
            # day that resume then marks complete (alpha-review finding 1).
            raise
        except Exception as exc:
            logger.error("Error reading %s: %s", Path(zp).name, exc)
            continue
        # create_df's ticker_filter only drives the individual_stock raw-byte fast
        # path; for the other types prune here so ingest honors ticker_filter too.
        if ticker_filter and data_type != "individual_stock":
            df = _filter_codes(df, data_type, {str(t).strip() for t in ticker_filter})
        if not df.is_empty():
            parts.append(df)
        del df
    if not parts:
        gc.collect()
        return {"date": date_str, "parts": len(zip_paths), "rows": 0, "output_path": None}
    combined = pl.concat(parts, how="vertical")
    del parts
    gc.collect()
    rows = len(combined)
    out_path = write_partitioned_parquet(combined, output_dir, data_type)
    del combined
    gc.collect()
    return {"date": date_str, "parts": len(zip_paths), "rows": rows, "output_path": out_path}


def _ingest_grouped(zip_paths, output_dir, data_type, year, language, resume, ticker_filter):
    """Group ZIP parts by date and ingest each date as a unit (all parts → write once).

    Resume is keyed per-date (a date is written atomically), so later parts of a
    date are never skipped or overwritten — fixing the multi-part data loss.
    """
    # Part-pruning: for a ticker-filtered individual_stock ingest, open only the
    # contiguous run of parts that holds the ticker(s) per day, not every part.
    # Degrades to all parts when the ascending-code layout can't be confirmed, so
    # the store contents are identical — only the ingest I/O shrinks.
    if ticker_filter and data_type == "individual_stock":
        zip_paths = _prune_parts_by_ticker(list(zip_paths), ticker_filter)

    output_root_path = Path(output_dir) / data_type
    groups: dict = {}
    for zp in zip_paths:
        tok = _zip_date_token(Path(zp).name)
        if tok is None:
            continue
        groups.setdefault(tok, []).append(zp)

    results: list[dict] = []
    for date_str, parts in groups.items():
        if resume and output_root_path.exists():
            date_dir = output_root_path / f"date={date_str}"
            if date_dir.exists() and any(date_dir.glob("ticker=*.parquet")):
                continue
        meta = _ingest_date_group(date_str, parts, output_dir, data_type, year, language, ticker_filter)
        results.append(meta)
        logger.info("  %s (%d parts) -> %s rows", date_str, meta["parts"], meta["rows"])
    return results


def ingest_year_from_root(
    input_root: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    resume: bool = True,
    ticker_filter: Optional[set] = None,
) -> list[dict]:
    """Ingest a whole ``year`` from a **structured** NEEDS root into the store.

    Discovers the year's ZIPs with :func:`tse_tick.discover_zips` (handling the
    ``{year}/{yearmonth}/`` tree, the legacy ``…010`` index code, and nested
    delivery trees) and ingests each day as a unit, writing partitioned Parquet.

    Args:
        input_root: Root of (or a folder above) the NEEDS delivery tree.
        output_dir: Store root to write under.
        year: Year to ingest.
        data_type: One of the four NEEDS types.
        language: Output column-name language (``"en"`` / ``"jp"``).
        resume: Skip dates already present in the store (default ``True``).
        ticker_filter: Optional ``set`` of stock/index codes to keep.

    Returns:
        One result dict per ingested date (``{"date", "parts", "rows",
        "output_path"}``).
    """
    validate_data_type(data_type)

    zip_paths = discover_zips(input_root, data_type, [year])
    return _ingest_grouped(zip_paths, output_dir, data_type, year, language, resume, ticker_filter)


def ingest_period(
    input_root: str,
    output_dir: str,
    period: str,
    data_type: str,
    language: str = "en",
    resume: bool = True,
    max_workers: int = 1,
    ticker_filter: Optional[set] = None,
) -> list[dict]:
    """Ingest a whole period from a structured NEEDS root into the Parquet store.

    Resolves ``period`` with :func:`tse_tick.parse_period`, discovers the ZIPs
    with :func:`tse_tick.discover_zips`, and ingests each via
    :func:`ingest_single_zip`.

    Args:
        input_root: Root of the ``{year}/{yearmonth}/`` NEEDS hierarchy.
        output_dir: Store root to write under.
        period: ``"YYYY"``, a single ``"YYYYMM"`` or ``"YYYYMMDD"``, or a
            ``"YYYYMM-YYYYMM"`` / ``"YYYYMMDD-YYYYMMDD"`` range (the same forms
            :func:`tse_tick.parse_period` accepts).
        data_type: NEEDS type to ingest.
        language: Output column-name language (``"en"`` / ``"jp"``).
        resume: Skip dates whose Parquet output already exists (default ``True``).
        max_workers: Reserved for parallel ingestion.
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per processed ZIP (see :func:`ingest_single_zip`); a
        failed ZIP contributes ``{"zip_path": ..., "error": ...}`` instead.
    """
    validate_data_type(data_type)

    parsed = parse_period(period)
    granularity = parsed["granularity"]
    years = parsed["years"]

    if granularity == "year":
        results: list[dict] = []
        for year in years:
            results.extend(
                ingest_year_from_root(
                    input_root, output_dir, year, data_type, language, resume, ticker_filter=ticker_filter
                )
            )
        return results

    if granularity == "month":
        results = []
        months_by_year: dict = parsed["months_by_year"]
        for year, months in months_by_year.items():
            zip_paths = discover_zips(input_root, data_type, [year], months=list(months))
            results.extend(
                _process_zips(zip_paths, output_dir, data_type, year, language, resume, ticker_filter=ticker_filter)
            )
        return results

    if granularity == "date":
        results = []
        dates: list = parsed["dates"]
        date_years = sorted(set(int(d[:4]) for d in dates))
        for year in date_years:
            year_dates = [d for d in dates if d.startswith(str(year))]
            year_months = sorted(set(int(d[4:6]) for d in year_dates))
            zip_paths = discover_zips(input_root, data_type, [year], months=year_months, dates=year_dates)
            results.extend(
                _process_zips(zip_paths, output_dir, data_type, year, language, resume, ticker_filter=ticker_filter)
            )
        return results

    raise ValueError(f"Unknown granularity: {granularity}")


def extract_to_store(
    input_root: str,
    output_dir: str,
    period: str,
    ticker: Union[str, Iterable[str]],
    *,
    data_type: str = "individual_stock",
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    language: str = "en",
    resume: bool = True,
) -> "pl.DataFrame":
    """Two-stage extraction in ONE call: ingest one or more tickers for a period
    into a reusable Parquet store, then return the queried DataFrame.

    The one-liner for "give me this ticker (or these tickers) for this period."
    Prefer it over :func:`tse_tick.read_ticks` whenever the data will be read more
    than once, or when a whole month of active tickers would exceed ``read_ticks``'s
    10M-row cap: the expensive raw scan is paid **once** into the store (resume-safe
    and, for ``individual_stock``, part-pruned to the tickers' parts), and every
    later :func:`tse_tick.query_ticks` against ``output_dir`` is sub-second.

    Args:
        input_root: Structured NEEDS root (``{year}/{yearmonth}/`` or a folder
            above it — the same inputs the ``ingest_*`` functions accept).
        output_dir: Parquet store to build/reuse.
        period: ``"YYYY"``, ``"YYYYMM"``, ``"YYYYMMDD"``, or a ``"start-end"`` range.
        ticker: One code **or an iterable of codes**, e.g. ``"7203"`` (Toyota),
            ``["7203", "9984"]``, or ``"101"`` (Nikkei 225). ``int`` codes work too.
        data_type: One of the four NEEDS types (default ``"individual_stock"``).
        start_time: Optional intraday lower bound ``"HH:MM:SS"`` (tick types only;
            the two ``*_summary`` types are daily aggregates and raise if given).
        end_time: Optional intraday upper bound ``"HH:MM:SS"``.
        language: Output column-name language (``"en"`` / ``"jp"``).
        resume: Skip dates already in the store (default ``True``).

    Returns:
        The queried Polars DataFrame for the requested ticker(s) — columns match
        :func:`tse_tick.query_ticks` (the read columns plus a ``date`` column). With
        several tickers the per-ticker frames are concatenated (in sorted code
        order). A multi-day ``period`` returns every stored day for those tickers;
        build into a fresh ``output_dir`` to get exactly ``period``.

    Requires the optional ``[query]`` extra (DuckDB). Example::

        >>> df = tse_tick.extract_to_store("G:/NEEDS", "toyota_sb_store",
        ...                                "202201", ["7203", "9984"])
    """
    from tse_tick.query import query_ticks

    tickers = _normalize_ticker_filter(ticker)
    if not tickers:
        raise ValueError("extract_to_store: at least one ticker is required")

    # Stage 1 — ingest every ticker in one part-pruned pass into the reusable store.
    ingest_period(
        input_root, output_dir, period, data_type,
        language=language, resume=resume, ticker_filter=tickers,
    )
    # Stage 2 — query each ticker. query_ticks takes a single day or all stored dates
    # (None); a fresh store holds exactly (tickers, period), so scope to the day only
    # when period is one. Concatenate the per-ticker frames in sorted-code order.
    query_date = period if (period.isdigit() and len(period) == 8) else None
    frames = []
    for code in sorted(tickers):
        kw = dict(data_type=data_type, ticker=code, date=query_date)
        if start_time is not None:
            kw["start_time"] = start_time
        if end_time is not None:
            kw["end_time"] = end_time
        frames.append(query_ticks(output_dir, **kw))
    # Drop empty per-ticker frames before concatenating: query_ticks omits the
    # `date` partition column from a 0-row result (there is no partition dir to
    # derive it from), so an absent ticker's frame has one fewer column and would
    # break a vertical concat. An absent ticker contributes no rows regardless.
    non_empty = [f for f in frames if f.height > 0]
    if not non_empty:
        return frames[0]  # every requested ticker absent for the period -> typed-empty
    return non_empty[0] if len(non_empty) == 1 else pl.concat(non_empty, how="vertical")


def _process_zips(
    zip_paths: list,
    output_dir: str,
    data_type: str,
    year: int,
    language: str = "en",
    resume: bool = True,
    ticker_filter: Optional[set] = None,
) -> list[dict]:
    return _ingest_grouped(zip_paths, output_dir, data_type, year, language, resume, ticker_filter)


def ingest_event_windows_period(
    input_root: str,
    output_dir: str,
    period: str,
    filter_csv: str,
    window_minutes: int = 120,
    resume: bool = True,
    max_workers: int = 1,
) -> None:
    """Build the event-window Parquet store for a period from an events CSV.

    For each event in ``filter_csv`` whose ``zip_date`` falls in ``period``, reads
    that day's ``individual_stock`` ZIPs and keeps the ticks within
    ``±window_minutes`` of the event's ``reaction_anchor_dt`` (per ticker), writing
    them to the event-window store via :func:`write_event_window_parquet`
    (``year=YYYY/month=MM/<date>.parquet``). Corrupt ZIPs are logged and skipped.

    Args:
        input_root: Root of the ``{year}/{yearmonth}/`` NEEDS hierarchy.
        output_dir: Root of the event-window store to write.
        period: ``"YYYY"``, single ``"YYYYMM"`` / ``"YYYYMMDD"``, or a
            ``"YYYYMM-YYYYMM"`` / ``"YYYYMMDD-YYYYMMDD"`` range.
        filter_csv: CSV of events with ``zip_date``, ``event_date``, ``ticker`` and
            ``reaction_anchor_dt`` columns.
        window_minutes: Half-width of the window kept around each anchor.
        resume: Skip dates whose output file already exists (default ``True``).
        max_workers: Reserved for parallel ingestion.

    Returns:
        ``None`` — results are written to the store; progress goes to ``logging``.
    """
    parsed = parse_period(period)
    granularity = parsed["granularity"]
    years = parsed["years"]

    fl = pl.read_csv(
        filter_csv,
        schema_overrides={"zip_date": pl.String, "event_date": pl.String, "ticker": pl.String},
    )

    fl = fl.with_columns(
        pl.col("reaction_anchor_dt")
        .str.to_datetime(strict=False)
        .dt.replace_time_zone("Asia/Tokyo")
        .alias("reaction_anchor_dt")
    )

    period_dates_set: set[str] = set()
    if granularity == "year":
        for y in years:
            fl_year = fl.filter(pl.col("zip_date").str.slice(0, 4) == str(y))
            period_dates_set.update(fl_year["zip_date"].drop_nulls().unique().to_list())
    elif granularity == "month":
        months_by_year = parsed["months_by_year"]
        for y, months in months_by_year.items():
            month_prefixes = [f"{y}{m:02d}" for m in months]
            fl_month = fl.filter(pl.col("zip_date").str.slice(0, 6).is_in(month_prefixes))
            period_dates_set.update(fl_month["zip_date"].drop_nulls().unique().to_list())
    elif granularity == "date":
        dates = parsed["dates"]
        fl_date = fl.filter(pl.col("zip_date").is_in(dates))
        period_dates_set.update(fl_date["zip_date"].drop_nulls().unique().to_list())

    if not period_dates_set:
        logger.warning("No events in period %s found in %s", period, filter_csv)
        return

    unique_zip_dates = sorted(period_dates_set)
    total_dates = len(unique_zip_dates)

    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    _logs_dir = out_root / "_ingest_logs"
    _logs_dir.mkdir(parents=True, exist_ok=True)
    corrupt_log_path = _logs_dir / "corrupt_zips.txt"

    zip_counter = 0
    skipped_by_resume = 0

    with corrupt_log_path.open("a", encoding="utf-8") as corrupt_log:
        for date_str in unique_zip_dates:
            zip_year = date_str[:4]
            zip_month = date_str[4:6]
            yearmonth = date_str[:6]

            output_file = (
                out_root / f"year={zip_year}" / f"month={zip_month}" / f"{date_str}.parquet"
            )

            if resume and output_file.exists():
                skipped_by_resume += 1
                continue

            zip_pattern = str(
                Path(input_root) / zip_year / yearmonth / f"HTICST120.{date_str}.*.zip"
            )
            zip_files = sorted(_glob.glob(zip_pattern))

            if not zip_files:
                logger.warning("No ZIPs found for %s: %s", date_str, zip_pattern)
                continue

            events_for_date = fl.filter(pl.col("zip_date") == date_str)

            if events_for_date.is_empty():
                continue

            needed_tickers = set(
                events_for_date["ticker"]
                .str.strip_chars()
                .str.split(".")
                .list.first()
                .str.zfill(4)
                .to_list()
            )

            all_filtered_parts = []

            for zip_path in zip_files:
                zip_counter += 1
                zip_fname = Path(zip_path).name
                raw_df = None
                filtered = None

                try:
                    raw_df = create_df(
                        zip_path, language="en",
                        auto_detect=False, data_type="individual_stock",
                        year=int(zip_year),
                        ticker_filter=needed_tickers,
                    )

                    if raw_df.is_empty():
                        continue

                    filtered = _filter_ticks_for_events(
                        raw_df, events_for_date, window_minutes=window_minutes
                    )

                    if not filtered.is_empty():
                        all_filtered_parts.append(filtered)

                    if zip_counter % 50 == 0:
                        logger.info(
                            "[%d/%d+] Processing %s - %s ticks matched",
                            zip_counter, total_dates, zip_fname, f"{len(filtered):,}",
                        )

                except (zipfile.BadZipFile, EOFError) as exc:
                    corrupt_log.write(f"{zip_path}\n")
                    corrupt_log.flush()
                    logger.error("Corrupt zip %s: %s", zip_fname, exc)

                except OneShotMemoryError:
                    # A one-shot OOM must abort, not mislabel this ZIP as corrupt and
                    # drop its event ticks (alpha-review finding 2).
                    raise

                except Exception as exc:
                    corrupt_log.write(f"{zip_path}\n")
                    corrupt_log.flush()
                    logger.error("Error processing %s: %s", zip_fname, exc, exc_info=True)

                finally:
                    del raw_df, filtered
                    gc.collect()

            if all_filtered_parts:
                combined = pl.concat(all_filtered_parts, how="vertical")
                internal_cols = [c for c in ["_tick_dt", "_stock_4"] if c in combined.columns]
                if internal_cols:
                    combined = combined.drop(internal_cols)
                combined_rows = len(combined)
                write_event_window_parquet(combined, output_dir)
                del combined
                gc.collect()
                logger.info("  %s: %s event-window ticks written", date_str, f"{combined_rows:,}")
            else:
                logger.info("  %s: no matching ticks after filtering", date_str)

    if skipped_by_resume:
        logger.info("Resume: skipped %d already-processed dates", skipped_by_resume)
