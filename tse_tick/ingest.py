# tse_tick/ingest.py
import gc
import glob as _glob
import logging
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Optional

import polars as pl

from tse_tick.enhanced import create_df, detect_data_type_and_year, discover_zips, parse_period
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
) -> dict:
    path = Path(zip_path)
    if not path.exists():
        raise FileNotFoundError(f"ZIP not found: {zip_path}")

    if data_type is None or year is None:
        data_type, year = detect_data_type_and_year(str(path))

    df = create_df(str(path), language=language, auto_detect=False, data_type=data_type, year=year)
    rows = len(df)
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
) -> list[dict]:
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
            return ingest_single_zip(str(zf), output_dir, data_type=data_type, language=language)
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
                    print(f"[{done}/{total}] {fname} -> {rows} rows")
    else:
        for i, zf in enumerate(zip_files, 1):
            meta = _process(zf)
            results.append(meta)
            if progress:
                fname = Path(meta.get("zip_path", "")).name
                rows = meta.get("rows", "error")
                print(f"[{i}/{total}] {fname} -> {rows} rows")

    return results


def ingest_year(
    input_dir: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    max_workers: int = 1,
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
            meta = ingest_single_zip(str(zf), output_dir, data_type=data_type, year=year, language=language)
        except Exception as exc:
            meta = {"zip_path": str(zf), "error": str(exc)}
        results.append(meta)

    return results


def ingest_year_from_root(
    input_root: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    resume: bool = True,
) -> list[dict]:
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if data_type not in valid_types:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(valid_types)}"
        )

    zip_paths = discover_zips(input_root, data_type, [year])
    results: list[dict] = []

    output_root_path = Path(output_dir) / data_type

    for zip_path in zip_paths:
        zip_basename = zip_path.name
        date_str = None
        for part in zip_basename.split("."):
            if len(part) == 8 and part.isdigit() and part.startswith("20"):
                date_str = part
                break

        if date_str and resume and output_root_path.exists():
            expected_ticker_files = list(
                (output_root_path / f"date={date_str}").glob("ticker=*.parquet")
            ) if (output_root_path / f"date={date_str}").exists() else []
            if expected_ticker_files:
                continue

        try:
            meta = ingest_single_zip(
                str(zip_path), str(output_dir), data_type=data_type, year=year, language=language
            )
        except Exception as exc:
            meta = {"zip_path": str(zip_path), "error": str(exc)}
        results.append(meta)
        print(f"  {zip_basename} -> {meta.get('rows', 'error')} rows")

    return results


def ingest_period(
    input_root: str,
    output_dir: str,
    period: str,
    data_type: str,
    language: str = "en",
    resume: bool = True,
    max_workers: int = 1,
) -> list[dict]:
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
                    input_root, output_dir, year, data_type, language, resume
                )
            )
        return results

    if granularity == "month":
        results = []
        months_by_year: dict = parsed["months_by_year"]
        for year, months in months_by_year.items():
            zip_paths = discover_zips(input_root, data_type, [year], months=list(months))
            results.extend(
                _process_zips(zip_paths, output_dir, data_type, year, language, resume)
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
                _process_zips(zip_paths, output_dir, data_type, year, language, resume)
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
) -> list[dict]:
    results: list[dict] = []
    output_root_path = Path(output_dir) / data_type

    for zip_path in zip_paths:
        zip_basename = Path(zip_path).name
        date_str = None
        for part in zip_basename.split("."):
            if len(part) == 8 and part.isdigit() and part.startswith("20"):
                date_str = part
                break

        if date_str and resume and output_root_path.exists():
            date_dir = output_root_path / f"date={date_str}"
            expected_ticker_files = (
                list(date_dir.glob("ticker=*.parquet")) if date_dir.exists() else []
            )
            if expected_ticker_files:
                continue

        try:
            meta = ingest_single_zip(
                str(zip_path), str(output_dir), data_type=data_type, year=year, language=language
            )
        except Exception as exc:
            meta = {"zip_path": str(zip_path), "error": str(exc)}
        results.append(meta)
        print(f"  {zip_basename} -> {meta.get('rows', 'error')} rows")

    return results


def ingest_event_windows(
    year: int,
    input_dir: str,
    output_dir: str,
    filter_csv: str,
    window_minutes: int = 120,
) -> None:
    fl = pl.read_csv(
        filter_csv,
        schema_overrides={"zip_date": pl.String, "event_date": pl.String, "ticker": pl.String},
    )

    fl = fl.with_columns(
        pl.col("event_date").str.to_date("%Y-%m-%d", strict=False).dt.year().alias("_event_year")
    )
    fl = fl.filter(pl.col("_event_year") == year).drop(["_event_year"])

    if fl.is_empty():
        logger.warning("No events for year %d in %s", year, filter_csv)
        return

    fl = fl.with_columns(
        pl.col("reaction_anchor_dt")
        .str.to_datetime(strict=False)
        .dt.replace_time_zone("Asia/Tokyo")
        .alias("reaction_anchor_dt")
    )

    unique_zip_dates = sorted(fl["zip_date"].drop_nulls().unique().to_list())
    total_dates = len(unique_zip_dates)

    out_root = Path(output_dir)
    out_root.mkdir(parents=True, exist_ok=True)
    corrupt_log_path = out_root / "corrupt_zips.txt"
    corrupt_log = corrupt_log_path.open("a", encoding="utf-8")

    zip_counter = 0

    try:
        for date_str in unique_zip_dates:
            zip_year = date_str[:4]
            zip_month = date_str[4:6]
            yearmonth = date_str[:6]

            zip_pattern = str(
                Path(input_dir) / zip_year / yearmonth / f"HTICST120.{date_str}.*.zip"
            )
            zip_files = sorted(_glob.glob(zip_pattern))

            if not zip_files:
                logger.warning("No ZIPs found for %s: %s", date_str, zip_pattern)
                continue

            output_file = (
                out_root / f"year={zip_year}" / f"month={zip_month}" / f"{date_str}.parquet"
            )
            if output_file.exists():
                continue

            events_for_date = fl.filter(pl.col("zip_date") == date_str)

            for zip_path in zip_files:
                zip_counter += 1
                zip_fname = Path(zip_path).name
                raw_df = None
                filtered = None

                try:
                    needed_tickers = set(
                        events_for_date["ticker"]
                        .str.strip_chars()
                        .str.split(".")
                        .list.first()
                        .str.zfill(4)
                        .to_list()
                    )
                    raw_df = create_df(zip_path, language="en", ticker_filter=needed_tickers)

                    filtered = _filter_ticks_for_events(
                        raw_df, events_for_date, window_minutes=window_minutes
                    )

                    matched = len(filtered)

                    if matched > 0:
                        write_event_window_parquet(filtered, output_dir)

                    if zip_counter % 50 == 0:
                        print(
                            f"[{zip_counter}/{total_dates}] "
                            f"Processing {zip_fname} - {matched:,} ticks matched"
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

    finally:
        corrupt_log.close()
