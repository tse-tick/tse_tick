# tse_tick/ingest.py
import gc
import glob as _glob
import logging
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import polars as pl

from tse_tick.enhanced import create_df, detect_data_type_and_year, discover_zips, parse_period, _zip_date_token, _filter_codes
from tse_tick.io.parquet import write_partitioned_parquet, write_event_window_parquet
from tse_tick.event_window import _filter_ticks_for_events

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
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if data_type not in valid_types:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(valid_types)}"
        )

    in_path = Path(input_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    year_zips = sorted(f for f in in_path.glob("*.zip") if str(year) in f.name)
    results: list[dict] = []

    for zf in year_zips:
        try:
            meta = ingest_single_zip(str(zf), output_dir, data_type=data_type, year=year, language=language, ticker_filter=ticker_filter)
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
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if data_type not in valid_types:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(valid_types)}"
        )

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
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if data_type not in valid_types:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(valid_types)}"
        )

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
