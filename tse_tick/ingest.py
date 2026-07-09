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
import multiprocessing
import os
import sys
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional, Union

import polars as pl

from tse_tick.enhanced import create_df, detect_data_type_and_year, discover_zips, parse_period, _zip_date_token, _filter_codes, _prune_parts_by_ticker, _normalize_ticker_filter, OneShotMemoryError
from tse_tick.io.parquet import write_partitioned_parquet, write_event_window_parquet
from tse_tick.event_window import _filter_ticks_for_events
from tse_tick.constants import validate_data_type

logger = logging.getLogger(__name__)

# ProcessPoolExecutor defaults to the 'fork' start method on Linux. Forking a process that
# has already initialised Polars' (rayon) thread pool DEADLOCKS the worker — fork copies the
# lock state but not the threads holding those locks, so the child hangs the first time it
# touches Polars. Force 'spawn' (a fresh interpreter, as on Windows/macOS) for every ingest
# pool; it also lets each worker read POLARS_MAX_THREADS at its own Polars import.
_MP_SPAWN = multiprocessing.get_context("spawn")


_RAM_SAFETY_FRACTION = 0.7   # use at most this fraction of available RAM for worker frames
_FILTERED_WORKER_GB = 0.5    # ticker-filtered / summary / index: a small per-worker frame
_FULLFRAME_EXPANSION = 8.0   # compressed part bytes -> peak per-worker RAM (full-frame day)


def _cpu_cap() -> int:
    """This machine's logical core count — the CPU ceiling for parallel ingest."""
    return os.cpu_count() or 1


def _available_ram_gb() -> float:
    """Best-effort available physical RAM in GB; ``0.0`` if it can't be determined."""
    if sys.platform == "win32":
        try:
            import ctypes

            class _MS(ctypes.Structure):
                _fields_ = [
                    ("dwLength", ctypes.c_ulong), ("dwMemoryLoad", ctypes.c_ulong),
                    ("ullTotalPhys", ctypes.c_ulonglong), ("ullAvailPhys", ctypes.c_ulonglong),
                    ("ullTotalPageFile", ctypes.c_ulonglong), ("ullAvailPageFile", ctypes.c_ulonglong),
                    ("ullTotalVirtual", ctypes.c_ulonglong), ("ullAvailVirtual", ctypes.c_ulonglong),
                    ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
                ]

            m = _MS()
            m.dwLength = ctypes.sizeof(_MS)
            if ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(m)):
                return m.ullAvailPhys / 1e9
        except Exception:
            pass
        return 0.0
    try:  # Linux / macOS: available physical pages
        return (os.sysconf("SC_AVPHYS_PAGES") * os.sysconf("SC_PAGE_SIZE")) / 1e9
    except (ValueError, OSError, AttributeError):
        return 0.0


def _estimate_worker_gb(units, data_type, ticker_filter) -> float:
    """Rough peak RAM (GB) one worker needs for its largest unit of work.

    A worker holds one whole date's frame in memory. Ticker-filtered reads (only the
    matching rows are materialised) and the summary / index types (small daily frames)
    need little; a **full-frame** ``individual_stock`` day is the whole day — every part,
    decompressed and cleaned — estimated from the largest day's total compressed part
    bytes times an expansion factor. ``units`` is an iterable of ``(label, [part paths])``
    (one entry per date group, or per ZIP for the flat path).
    """
    if data_type not in (None, "individual_stock") or ticker_filter:
        return _FILTERED_WORKER_GB
    biggest = 0
    for _label, parts in units:
        total = 0
        for p in parts:
            try:
                total += Path(p).stat().st_size
            except OSError:
                continue  # skip one vanished/unreadable part, don't zero the whole day
        if total > biggest:
            biggest = total
    return max(_FILTERED_WORKER_GB, (biggest / 1e9) * _FULLFRAME_EXPANSION)


def _cap_workers(requested: int, per_worker_gb: Optional[float] = None) -> int:
    """Clamp a requested worker count by what this machine's cores AND RAM allow.

    **Cores:** never more workers than logical cores. **RAM:** each worker process holds a
    whole trading day's frame, so N workers must fit in available RAM — a naive
    core-count cap ignores this and can OOM a full-frame parallel ingest, where one busy
    ``individual_stock`` day is many GB (this is why the old flat cap of 8 was NOT simply
    raised to ``os.cpu_count()``). When ``per_worker_gb`` is given and available RAM can be
    read, workers are capped so ``N x per_worker_gb`` stays within
    ``_RAM_SAFETY_FRACTION`` of available RAM; when RAM can't be read, a heavy per-worker
    estimate only warns. Filtered / summary ingests estimate a small per-worker frame and
    parallelize freely.
    """
    cap = _cpu_cap()
    workers = max(1, min(requested, cap))
    if requested > cap:
        logger.warning(
            "max_workers=%d exceeds this machine's %d logical cores; using %d",
            requested, cap, cap,
        )
    if workers > 1 and per_worker_gb and per_worker_gb > 0:
        avail = _available_ram_gb()
        if avail > 0:
            ram_cap = max(1, int((avail * _RAM_SAFETY_FRACTION) / per_worker_gb))
            if ram_cap < workers:
                logger.warning(
                    "Limiting workers %d -> %d: ~%.1f GB/worker x %d would exceed %d%% of "
                    "%.1f GB available RAM. Each worker holds a whole trading day; lower "
                    "max_workers, ticker-filter, or run serially for a full-frame ingest.",
                    workers, ram_cap, per_worker_gb, workers,
                    int(_RAM_SAFETY_FRACTION * 100), avail,
                )
                workers = ram_cap
        elif per_worker_gb > _FILTERED_WORKER_GB:
            logger.warning(
                "Could not read available RAM to size workers; each of %d workers holds "
                "~%.1f GB (a whole trading day). Lower max_workers if RAM-constrained.",
                workers, per_worker_gb,
            )
    return workers


@contextmanager
def _bounded_polars_threads(workers: int, n_tasks: int):
    """Cap each spawned worker's Polars thread pool for the lifetime of a pool.

    W worker processes that each let Polars use every core would oversubscribe
    (W x cores threads) and erase the multi-core win. Sizing each worker's
    ``POLARS_MAX_THREADS`` to ``cores // concurrency`` keeps the total near the core
    count. Spawn-based workers (Windows/macOS — the primary target) inherit this env
    var and read it when they import Polars; on fork-based platforms Polars is already
    initialised in the parent, so this is a harmless no-op there.
    """
    concurrency = max(1, min(workers, n_tasks))
    per_worker = max(1, _cpu_cap() // concurrency)
    key = "POLARS_MAX_THREADS"
    prev = os.environ.get(key)
    os.environ[key] = str(per_worker)
    try:
        yield
    finally:
        if prev is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = prev


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


def _ingest_single_zip_safe(zip_path, output_dir, data_type, language, ticker_filter):
    """Module-level ingest-one-ZIP task for ``ingest_directory``'s process pool.

    A local closure cannot be pickled under the ``spawn`` start method (Windows/macOS),
    which silently broke ``ingest_directory(..., max_workers>1)``; a module-level function
    pickles fine. A one-shot OOM aborts loudly (never recorded as a skipped ZIP); any
    other error is recorded as ``{"zip_path", "error"}``.
    """
    try:
        return ingest_single_zip(
            str(zip_path), output_dir, data_type=data_type,
            language=language, ticker_filter=ticker_filter,
        )
    except OneShotMemoryError:
        raise
    except Exception as exc:
        return {"zip_path": str(zip_path), "error": str(exc)}


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
        max_workers: Parallel worker processes; ``1`` is serial. Capped by the machine's
            logical cores AND available RAM (each worker holds one ZIP's frame).
        progress: Log a per-ZIP progress line.
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per ZIP (see :func:`ingest_single_zip`); a failed ZIP
        contributes ``{"zip_path": ..., "error": ...}`` instead.
    """
    in_path = Path(input_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    zip_files = sorted(in_path.glob("*.zip"))
    total = len(zip_files)
    results: list[dict] = []

    # RAM-aware worker cap: a flat-directory worker holds one ZIP's frame. Estimate per
    # worker from the largest ZIP (full-frame individual_stock) and clamp to cores + RAM.
    max_workers = _cap_workers(
        max_workers,
        per_worker_gb=_estimate_worker_gb(
            [(zf.name, [zf]) for zf in zip_files], data_type, ticker_filter
        ),
    )

    if max_workers > 1 and total > 1:
        with _bounded_polars_threads(max_workers, total):
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=_MP_SPAWN) as executor:
                futures = {
                    executor.submit(_ingest_single_zip_safe, zf, output_dir,
                                    data_type, language, ticker_filter): zf
                    for zf in zip_files
                }
                done = 0
                for future in as_completed(futures):
                    done += 1
                    meta = future.result()  # a one-shot OOM propagates and aborts
                    results.append(meta)
                    if progress:
                        fname = Path(meta.get("zip_path", "")).name
                        rows = meta.get("rows", "error")
                        logger.info("[%d/%d] %s -> %s rows", done, total, fname, rows)
    else:
        for i, zf in enumerate(zip_files, 1):
            meta = _ingest_single_zip_safe(zf, output_dir, data_type, language, ticker_filter)
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
        max_workers: This flat-directory path is serial; pass a structured NEEDS root
            to :func:`ingest_period` / :func:`ingest_year_from_root` for parallel
            per-date ingestion. ``>1`` here logs a warning instead of silently no-op'ing.
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per ZIP (see :func:`ingest_single_zip`); a failed ZIP
        contributes ``{"zip_path": ..., "error": ...}`` instead.
    """
    validate_data_type(data_type)

    if max_workers > 1:
        logger.warning(
            "ingest_year runs the flat-directory path serially; max_workers=%d is "
            "ignored. Use ingest_period / ingest_year_from_root (a structured root) for "
            "parallel per-date ingestion.", max_workers,
        )

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
    # Prune the day's parts to the requested ticker(s) HERE, not on the parent, so
    # pruning stays interleaved with this date's write (issue #39: a partition lands
    # per day and a resumed run prunes only the dates it ingests) and, under
    # parallelism, each worker prunes its own date concurrently. `_prune_parts_by_ticker`
    # groups by day internally, so per-date pruning selects the identical parts.
    if ticker_filter and data_type == "individual_stock":
        zip_paths = _prune_parts_by_ticker(zip_paths, ticker_filter)
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
    # Keep the per-date gc.collect() (issue #43 proposed removing them as "pure waste"):
    # a full-frame individual_stock day holds every part of the day at once and peaks
    # within ~0.6 GB of the RAM ceiling on a 34 GB box (measured; HEAD OOM-crashes there
    # too), so prompt collection between the concat and the write gives real headroom at
    # a cost dwarfed by the multi-second-per-day I/O. `del` drops the reference first.
    del parts
    gc.collect()
    rows = len(combined)
    out_path = write_partitioned_parquet(combined, output_dir, data_type)
    del combined
    gc.collect()
    return {"date": date_str, "parts": len(zip_paths), "rows": rows, "output_path": out_path}


def _ingest_grouped(zip_paths, output_dir, data_type, year, language, resume, ticker_filter,
                    max_workers=1):
    """Group ZIP parts by date and ingest each date as a unit (all parts → write once).

    Resume is keyed per-date (a date is written atomically), so later parts of a
    date are never skipped or overwritten — fixing the multi-part data loss.

    Each date is a fully independent unit (read its parts → concat → clean → write one
    ``date=`` partition), so with ``max_workers > 1`` the per-date units run across a
    process pool (issue #43); ``max_workers=1`` is serial. Only the cheap resume-skip
    stays on the parent; each date's part-prune (issue #39) runs inside
    :func:`_ingest_date_group` so it remains interleaved with that date's write (and, in
    parallel, happens in the worker). Store bytes are identical to the serial path (each
    date writes its own dir); the results list is sorted by date so its order is
    deterministic regardless of worker completion order.
    """
    output_root_path = Path(output_dir) / data_type
    groups: dict = {}
    for zp in zip_paths:
        tok = _zip_date_token(Path(zp).name)
        if tok is None:
            continue
        groups.setdefault(tok, []).append(zp)

    # Build the per-date task list on the parent applying only the cheap resume-skip
    # (a stat on the date dir) so a resumed run neither re-dispatches nor re-prunes an
    # already-written date. The expensive per-date part-prune (issue #39) is done inside
    # _ingest_date_group so it stays interleaved with each day's write — a partition
    # lands per day (incremental progress) rather than the whole period being pruned
    # before the first write — and, under parallelism, each worker prunes its own date.
    tasks: list = []
    for date_str, parts in groups.items():
        if resume and output_root_path.exists():
            date_dir = output_root_path / f"date={date_str}"
            if date_dir.exists() and any(date_dir.glob("ticker=*.parquet")):
                continue
        tasks.append((date_str, parts))

    # RAM-aware: each worker holds one whole date's frame. Estimate the largest day's
    # per-worker peak (from its part sizes for a full-frame individual_stock ingest; small
    # for filtered / summary / index) and let _cap_workers clamp to what RAM allows.
    workers = _cap_workers(
        max_workers, per_worker_gb=_estimate_worker_gb(tasks, data_type, ticker_filter)
    )

    if workers <= 1 or len(tasks) <= 1:
        results: list[dict] = []
        for date_str, parts in tasks:
            meta = _ingest_date_group(date_str, parts, output_dir, data_type, year, language, ticker_filter)
            results.append(meta)
            logger.info("  %s (%d parts) -> %s rows", date_str, meta["parts"], meta["rows"])
        return results

    results = []
    with _bounded_polars_threads(workers, len(tasks)):
        with ProcessPoolExecutor(max_workers=workers, mp_context=_MP_SPAWN) as executor:
            futures = {
                executor.submit(
                    _ingest_date_group, date_str, parts, output_dir,
                    data_type, year, language, ticker_filter,
                ): date_str
                for date_str, parts in tasks
            }
            for future in as_completed(futures):
                # A one-shot OOM in any worker propagates here and aborts the whole
                # ingest rather than silently leaving a partial period behind.
                meta = future.result()
                results.append(meta)
                logger.info("  %s (%d parts) -> %s rows", meta["date"], meta["parts"], meta["rows"])
    results.sort(key=lambda m: m["date"])
    return results


def ingest_year_from_root(
    input_root: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    resume: bool = True,
    max_workers: int = 1,
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
        max_workers: Parallel worker processes for the independent per-date ingests;
            ``1`` is serial. Capped by the machine's logical cores AND available RAM
            (each worker holds a whole day's frame).
        ticker_filter: Optional ``set`` of stock/index codes to keep.

    Returns:
        One result dict per ingested date (``{"date", "parts", "rows",
        "output_path"}``).
    """
    validate_data_type(data_type)

    zip_paths = discover_zips(input_root, data_type, [year])
    return _ingest_grouped(
        zip_paths, output_dir, data_type, year, language, resume, ticker_filter, max_workers
    )


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
        max_workers: Parallel worker processes for the independent per-date ingests;
            ``1`` is serial. Capped by the machine's logical cores AND available RAM
            (each worker holds a whole day's frame). Wired through every granularity
            (year / month / date).
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
                    input_root, output_dir, year, data_type, language, resume,
                    max_workers=max_workers, ticker_filter=ticker_filter
                )
            )
        return results

    if granularity == "month":
        results = []
        months_by_year: dict = parsed["months_by_year"]
        for year, months in months_by_year.items():
            zip_paths = discover_zips(input_root, data_type, [year], months=list(months))
            results.extend(
                _process_zips(zip_paths, output_dir, data_type, year, language, resume,
                              max_workers=max_workers, ticker_filter=ticker_filter)
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
                _process_zips(zip_paths, output_dir, data_type, year, language, resume,
                              max_workers=max_workers, ticker_filter=ticker_filter)
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
        order). **All** matching rows are returned — this queries with ``limit=None``,
        so unlike a bare :func:`query_ticks` call it is not subject to the default
        10M-row cap (a whole month of a very active ticker exceeds it). A multi-day
        ``period`` returns every stored day for those tickers; build into a fresh
        ``output_dir`` to get exactly ``period``.

    Requires the optional ``[query]`` extra (DuckDB). Example::

        >>> df = tse_tick.extract_to_store("G:/NEEDS", "toyota_sb_store",
        ...                                "202201", ["7203", "9984"])
    """
    from tse_tick.query import _query_extract_batch

    tickers = _normalize_ticker_filter(ticker)
    if not tickers:
        raise ValueError("extract_to_store: at least one ticker is required")

    # Stage 1 — ingest every ticker in one part-pruned pass into the reusable store.
    ingest_period(
        input_root, output_dir, period, data_type,
        language=language, resume=resume, ticker_filter=tickers,
    )
    # Stage 2 — one DuckDB connection and one scan for ALL tickers (issue #44), replacing
    # the per-ticker query_ticks(limit=None) loop + concat (and, for the two summary types,
    # N full-store scans with one). Returns the same multiset of rows in the same
    # (code, Data Date, time) order; the summary types are byte-identical. A fresh store
    # holds exactly (tickers, period), so scope to the day only when period is a single
    # day. limit is None — extract returns ALL rows (a whole month of a very active ticker
    # exceeds query_ticks' default 10M exploratory cap).
    query_date = period if (period.isdigit() and len(period) == 8) else None
    return _query_extract_batch(
        output_dir, data_type, tickers,
        date=query_date, start_time=start_time, end_time=end_time,
    )


def _process_zips(
    zip_paths: list,
    output_dir: str,
    data_type: str,
    year: int,
    language: str = "en",
    resume: bool = True,
    max_workers: int = 1,
    ticker_filter: Optional[set] = None,
) -> list[dict]:
    return _ingest_grouped(
        zip_paths, output_dir, data_type, year, language, resume, ticker_filter, max_workers
    )


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
        max_workers: The event-window builder runs serially; ``>1`` logs a warning
            instead of silently no-op'ing (parallelising this path is out of scope of
            the per-date ingest parallelism in :func:`ingest_period`).

    Returns:
        ``None`` — results are written to the store; progress goes to ``logging``.
    """
    if max_workers > 1:
        logger.warning(
            "ingest_event_windows_period runs serially; max_workers=%d is ignored.",
            max_workers,
        )

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
