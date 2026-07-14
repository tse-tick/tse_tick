# tse_tick/ingest.py
"""Ingest raw NEEDS ZIPs into a Hive-partitioned Parquet store.

This is the ``tse_tick.ingest`` *submodule*, not a callable. The entry points are
the functions re-exported at the top level: :func:`ingest_period` (a structured
``{year}/{yearmonth}/`` root), :func:`ingest_year_from_root`, :func:`ingest_year`,
:func:`ingest_directory` (a flat folder of ZIPs), :func:`ingest_single_zip`, and
:func:`ingest_event_windows_period` (the event-window store) — e.g. call
``tse_tick.ingest_period(...)``, not ``tse_tick.ingest(...)``.
"""
import calendar
import gc
import glob as _glob
import json
import logging
import multiprocessing
import os
import sys
import warnings
import zipfile
from concurrent.futures import ProcessPoolExecutor, as_completed
from concurrent.futures.process import BrokenProcessPool
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable, Optional, Union, cast

import polars as pl

from tse_tick.enhanced import create_df, detect_data_type_and_year, discover_zips, parse_period, _zip_date_token, _zip_sort_key, _detect_data_type_from_path, _filter_codes, _prune_parts_by_ticker, _normalize_ticker_filter, _stock_family_roots, _code_matches_family, LargeResultWarning, NoDataWarning, OneShotMemoryError, PartialIngestWarning, IngestWorkerError
from tse_tick.io.parquet import (
    PartitionedParquetAppender,
    write_partitioned_parquet,
    write_event_window_parquet,
)
from tse_tick.event_window import _filter_ticks_for_events
from tse_tick.constants import validate_data_type, validate_time_filter_support

logger = logging.getLogger(__name__)

# ProcessPoolExecutor defaults to the 'fork' start method on Linux. Forking a process that
# has already initialised Polars' (rayon) thread pool DEADLOCKS the worker — fork copies the
# lock state but not the threads holding those locks, so the child hangs the first time it
# touches Polars. Force 'spawn' (a fresh interpreter, as on Windows/macOS) for every ingest
# pool; it also lets each worker read POLARS_MAX_THREADS at its own Polars import.
_MP_SPAWN = multiprocessing.get_context("spawn")


_RAM_SAFETY_FRACTION = 0.7   # use at most this fraction of available RAM for worker frames
_FILTERED_WORKER_GB = 0.5    # summary / index (small daily frames), and the small-day floor
# individual_stock, PER filtered code: an active name's day peaks ~1.1 GB (measured:
# 7203+9984 for 20240403 = 990,975 rows -> 2.21 GB peak), rounded up for headroom. This
# was a flat 0.5 for ANY filter, so the RAM cap never bound and the 16-worker Jupyter
# default overcommitted a 34 GB box -> a killed worker (BrokenProcessPool).
_TICKER_WORKER_GB = 1.5
# A STREAMING filtered day (morsel -> per-ticker row group) peaks at ~one morsel
# plus writer buffers, not at the day: measured 2.40 GB for the worst real day
# seen (7203+9984 on 20250409, 4,673,760 rows — 24.52 GB before). It does not grow
# with the day's rows or the number of codes kept, so this is a constant with
# headroom rather than an estimate of the data.
_STREAM_WORKER_GB = 3.0
_FULLFRAME_EXPANSION = 8.0   # compressed part bytes -> peak per-worker RAM (full-frame day)
# Above this many codes a filtered day would hold too many concurrent Parquet
# writers (a full-frame day is thousands), so it keeps the concat write path.
_MAX_STREAM_TICKERS = 64
_LARGE_EXTRACT_ROWS = 10_000_000  # extract_to_store warns before materializing more rows


def _cpu_cap() -> int:
    """This machine's logical core count — the CPU ceiling for parallel ingest."""
    return os.cpu_count() or 1


# Opt-in default worker count for the Python API (int or "auto"). The CLI and
# interactive sessions default to auto on their own; a plain script stays serial
# unless this is set (or max_workers is passed) — see _resolve_max_workers.
_WORKERS_ENV = "TSE_TICK_MAX_WORKERS"
_workers_hint_emitted = False


def _interactive_main() -> bool:
    """True in a REPL/Jupyter/`python -c` session — ``__main__`` has no
    ``__file__`` there, so a spawn worker's bootstrap has no user script to
    re-import and parallel ingest is safe without a ``__main__`` guard."""
    return getattr(sys.modules.get("__main__"), "__file__", None) is None


def _log_workers_hint_once() -> None:
    """One-time nudge when a multi-core machine defaults to a serial ingest."""
    global _workers_hint_emitted
    if _workers_hint_emitted:
        return
    _workers_hint_emitted = True
    logger.info(
        "Ingesting serially. This machine has %d logical cores — pass "
        "max_workers=\"auto\" (or set %s=auto) to run the independent per-date "
        "ingests in parallel; from a script, the call must sit under "
        "if __name__ == \"__main__\":", _cpu_cap(), _WORKERS_ENV,
    )


def _resolve_max_workers(max_workers, allow_default_auto: bool = True) -> int:
    """Resolve a ``max_workers`` value (int, ``"auto"``, or ``None``) to an int.

    ``"auto"`` requests this machine's logical core count (the RAM-aware
    :func:`_cap_workers` still clamps it later). ``None`` — the API default —
    resolves to the ``TSE_TICK_MAX_WORKERS`` env var when set, to auto in an
    interactive session (safe: spawn has nothing to re-import there), and to
    serial (1, plus a one-time hint on a multi-core box) in a script. A BLIND
    auto default for scripts would be unsafe: spawn re-imports the calling
    script in every worker, so an unguarded script's top-level side effects
    would re-run once per worker before the bootstrap guard stops it. Resolve
    ONLY in the parent process, never in worker-executed code.
    ``allow_default_auto=False`` keeps a serial-only path quiet: ``None`` /
    ``"auto"`` / env opt-ins resolve to 1 with no hint, and only an explicit
    int survives (its >1 warning stays meaningful).
    """
    if isinstance(max_workers, str):
        if max_workers.strip().lower() != "auto":
            raise ValueError(
                f"max_workers must be a positive int, 'auto', or None; got {max_workers!r}"
            )
        return _cpu_cap() if allow_default_auto else 1
    if max_workers is None:
        if not allow_default_auto:
            return 1
        env = os.environ.get(_WORKERS_ENV, "").strip()
        if env:
            if env.lower() == "auto":
                return _cpu_cap()
            try:
                return max(1, int(env))
            except ValueError:
                logger.warning(
                    "Ignoring invalid %s=%r (expected an int or 'auto')", _WORKERS_ENV, env
                )
        if _interactive_main():
            return _cpu_cap()
        if _cpu_cap() > 1:
            _log_workers_hint_once()
        return 1
    return max(1, int(max_workers))


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


def _filtered_worker_gb(full_gb: float, n_tickers: int) -> float:
    """Per-worker GB for a ticker-FILTERED ``individual_stock`` day.

    A filtered worker materialises only the matching rows, so its peak scales with
    how many codes are kept — but it can never exceed the whole day's frame, hence
    the ``full_gb`` clamp (which also keeps a small day's estimate small).

    A **streaming** day (the usual filtered case — see :data:`_MAX_STREAM_TICKERS`)
    no longer scales at all: it writes morsel-sized row groups, so its peak is the
    bounded :data:`_STREAM_WORKER_GB` whatever the day's size or the number of codes
    kept. That is the point of round-20 — the estimate stops being a guess about the
    data, which is what could never be made right (bytes do not predict filtered
    rows: an extreme day keeps ~100% of a pruned part, a normal one ~15%).

    A wider filter still takes the concat write path, where the worker does hold the
    day: there, scale by :data:`_TICKER_WORKER_GB` per code (~1.1 GB measured for one
    active code's day), clamped by the whole day's frame.
    """
    if n_tickers <= _MAX_STREAM_TICKERS:
        return min(full_gb, _STREAM_WORKER_GB)
    return min(full_gb, _TICKER_WORKER_GB * max(1, n_tickers))


def _estimate_worker_gb(units, data_type, ticker_filter) -> float:
    """Rough peak RAM (GB) one worker needs for its largest unit of work.

    A worker holds one whole date's frame in memory. The summary / index types have
    small daily frames; a **full-frame** ``individual_stock`` day is the whole day —
    every part, decompressed and cleaned — estimated from the largest day's total
    compressed part bytes times an expansion factor. A ticker-filtered
    ``individual_stock`` day sits between the two and scales with the number of codes
    kept (see :func:`_filtered_worker_gb`). ``units`` is an iterable of
    ``(label, [part paths])`` (one entry per date group, or per ZIP for the flat path).
    """
    if data_type not in (None, "individual_stock"):
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
    full_gb = max(_FILTERED_WORKER_GB, (biggest / 1e9) * _FULLFRAME_EXPANSION)
    if ticker_filter:
        return _filtered_worker_gb(full_gb, len(ticker_filter))
    return full_gb


def _worker_died_error(done: int, total: int, workers: int) -> IngestWorkerError:
    """Build the actionable error for a killed pool worker (see IngestWorkerError).

    ``BrokenProcessPool`` says only "terminated abruptly" — no cause, no remedy —
    so name the usual cause (memory: N workers x one trading day's frame each),
    say the finished dates are resume-safe, and point at ``max_workers``.
    """
    return IngestWorkerError(
        f"A parallel ingest worker was terminated abruptly after {done} of {total} "
        f"unit(s) completed. The usual cause is running out of memory: {workers} "
        f"worker(s) each hold one trading day's frame, and a day whose part-pruning "
        f"cannot be confirmed is read in full (several GB). The completed dates are "
        f"already written and resume-safe — re-run the same call with a lower "
        f"max_workers (e.g. max_workers=2, or max_workers=1 to go serial) and it "
        f"continues where it stopped."
    )


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


def _require_input_root(input_root: str) -> None:
    """Fail fast on a nonexistent input root.

    The structured-root discovery globs, and globbing a mistyped path just
    matches nothing — the ingest then reported "Done: 0 succeeded, 0 failed"
    as if it were success. The flat path has always raised; now both do.
    """
    if not Path(input_root).exists():
        raise FileNotFoundError(f"Input root not found: {input_root}")


def _warn_zero_discovery(input_root: str, data_type: str, scope: str) -> None:
    """Capturable warning when discovery finds no ZIPs at all for a request.

    Not an error: a period that is all holidays legitimately matches nothing.
    But silence here made a wrong data_type or a root one level too deep look
    like a successful no-op ingest.
    """
    warnings.warn(
        f"No {data_type!r} ZIPs found under {input_root!r} for {scope}. "
        f"Check that the root contains the NEEDS delivery tree for this data "
        f"type (any nesting works) and that the period has trading days.",
        NoDataWarning,
        stacklevel=3,
    )


def _reject_bootstrap_reimport() -> None:
    """Fail fast, with an actionable message, when an ingest entry point is
    re-executed by a spawn worker that is still bootstrapping.

    Parallel ingest starts workers with the ``spawn`` method (deliberate — ``fork``
    deadlocks Polars), and spawn re-imports the caller's script in every worker. If
    the user's ingest call sits at module top level (no ``__main__`` guard), each
    worker re-runs it during bootstrap: without this check that surfaces as the
    stdlib's cryptic ``freeze_support`` / "bootstrapping phase" ``RuntimeError``
    (audit finding B1) — and a serial top-level call would silently re-ingest in
    every worker. ``_inheriting`` is the same signal the stdlib's own guard checks.
    """
    if getattr(multiprocessing.current_process(), "_inheriting", False):
        raise RuntimeError(
            "A tse_tick ingest call was re-executed inside a spawn worker process "
            "while it was starting up. Your ingest_* / extract_to_store call runs "
            "at module top level; parallel ingest (max_workers > 1) starts worker "
            "processes with the 'spawn' method, which re-imports your script in "
            "every worker. Wrap the call in a main guard:\n\n"
            '    if __name__ == "__main__":\n'
            "        ...your ingest call...\n"
        )


def _valid_parquet_file(path: Path) -> bool:
    """True if ``path`` looks like a complete Parquet file (magic bytes at both ends).

    A write killed partway (crash, OOM, Ctrl-C, a reaped job) used to leave a
    truncated final file with no footer; an existence-only resume then skipped —
    and permanently trusted — that corrupt partition (audit finding B11, observed
    live). Writes are atomic now, but stores written by older versions (or hit by
    external truncation) still need the resume-side check. This probes only the
    8 magic bytes — cheap enough to run per file on every resume."""
    try:
        with open(path, "rb") as fh:
            if fh.read(4) != b"PAR1":
                return False
            fh.seek(-4, os.SEEK_END)
            return fh.read(4) == b"PAR1"
    except OSError:
        return False


# Per-date coverage marker: records WHICH coverage (full / which tickers) produced
# a date partition, so resume can tell "this date is done for THIS request" apart
# from "some files exist here". Named with a leading underscore so pyarrow dataset
# discovery ignores it and no `*.parquet` glob ever matches it.
_COVERAGE_MARKER = "_ingest_coverage.json"


def _read_coverage_marker(date_dir: Path) -> Optional[dict]:
    """The parsed coverage marker of a date partition, or ``None`` when absent
    or unreadable (a legacy pre-marker partition)."""
    try:
        with open(Path(date_dir) / _COVERAGE_MARKER, "r", encoding="utf-8") as fh:
            data = json.load(fh)
    except (OSError, ValueError):
        return None
    return data if isinstance(data, dict) else None


def _write_coverage_marker(date_dir, ticker_filter: Optional[set], complete: bool = True) -> None:
    """Record the coverage a date partition was written with (atomically).

    Coverage accumulates: a completed prior marker's coverage is merged in, since
    re-ingesting with a different ``ticker_filter`` only ADDS that filter's
    ``ticker=`` files — the previously written ones remain valid. A prior marker
    flagged incomplete (a day that lost parts, audit finding M1) is NOT merged:
    its files may be partial, so only the current write's coverage is trusted.
    ``complete=False`` records that THIS write lost parts, which keeps the date
    resume-eligible.
    """
    date_dir = Path(date_dir)
    if not date_dir.exists():
        return
    tickers = {str(t).strip() for t in ticker_filter} if ticker_filter else None
    full = tickers is None
    prior = _read_coverage_marker(date_dir)
    if prior is not None and prior.get("complete", True):
        if prior.get("full"):
            full = True
        elif not full:
            tickers |= {str(t) for t in (prior.get("tickers") or [])}
    payload = {
        "full": full,
        "tickers": [] if full else sorted(tickers),
        "complete": bool(complete),
    }
    tmp = date_dir / f".{_COVERAGE_MARKER}.{os.getpid()}.tmp"
    try:
        tmp.write_text(json.dumps(payload), encoding="utf-8")
        os.replace(tmp, date_dir / _COVERAGE_MARKER)
    except OSError as exc:  # a failed marker only costs a re-ingest, never data
        logger.warning("Could not write coverage marker in %s: %s", date_dir, exc)
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass


def _coverage_satisfied(date_dir: Path, ticker_filter: Optional[set]) -> bool:
    """True when a date partition's recorded coverage includes this request.

    Existence of files alone is NOT coverage: a store built for ticker A used to
    resume-skip a later request for ticker B, silently returning an empty/partial
    result (audit finding H2). With a marker: a complete full ingest satisfies
    everything; a complete filtered ingest satisfies subsets of its tickers; an
    incomplete day (lost parts) satisfies nothing. Without a marker (a legacy
    store): a full request keeps the old skip-on-existence semantics, and a
    filtered request is satisfied only when every requested code already has its
    ``ticker=`` file — an absent file is ambiguous (never traded vs. never
    ingested), so the date is re-ingested once and the marker written.
    """
    wanted = {str(t).strip() for t in ticker_filter} if ticker_filter else None
    marker = _read_coverage_marker(date_dir)
    if marker is None:
        if wanted is None:
            return True
        # Family matching: a filtered Stage 1 ingests the requested code's whole
        # share-class family, and on a day the parent didn't trade only a
        # suffixed class file may exist — that still covers the (rooted) request.
        codes_present = {
            p.stem[len("ticker="):] for p in Path(date_dir).glob("ticker=*.parquet")
        }
        return all(
            any(_code_matches_family(c, t) for c in codes_present) for t in wanted
        )
    if not marker.get("complete", True):
        return False
    if marker.get("full"):
        return True
    if wanted is None:
        return False
    return wanted <= {str(t) for t in (marker.get("tickers") or [])}


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
    compression: str = "zstd",
) -> dict:
    """Ingest one raw NEEDS ZIP into the Hive-partitioned Parquet store.

    Cleans the ZIP with :func:`tse_tick.create_df` and writes per-ticker Parquet
    files under ``output_dir/<data_type>/date=YYYYMMDD/ticker=NNNN.parquet``.

    .. warning::
        This writes ONE ZIP's rows, overwriting any existing per-ticker file for
        the same date. NEEDS splits a trading day across numbered parts that
        repeat tickers (the closing-appendix part especially), so calling this
        per part loses data — use :func:`ingest_directory` /
        :func:`ingest_period`, which ingest a whole day's parts as one unit.

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
    out_path = write_partitioned_parquet(df, output_dir, data_type, compression=compression)

    return {
        "zip_path": str(path.resolve()),
        "data_type": data_type,
        "year": year,
        "rows": rows,
        "output_path": out_path,
    }


def _ingest_single_zip_safe(zip_path, output_dir, data_type, language, ticker_filter, compression):
    """Module-level ingest-one-ZIP task for ``ingest_directory``'s process pool.

    A local closure cannot be pickled under the ``spawn`` start method (Windows/macOS),
    which silently broke ``ingest_directory(..., max_workers>1)``; a module-level function
    pickles fine. A one-shot OOM aborts loudly (never recorded as a skipped ZIP); any
    other error is recorded as ``{"zip_path", "error"}``.
    """
    try:
        return ingest_single_zip(
            str(zip_path), output_dir, data_type=data_type,
            language=language, ticker_filter=ticker_filter, compression=compression,
        )
    except OneShotMemoryError:
        raise
    except Exception as exc:
        return {"zip_path": str(zip_path), "error": str(exc)}


def _flat_day_units(zip_files: list) -> "tuple[list, list]":
    """Group a flat folder's ZIPs into per-day units: ``(day_units, singles)``.

    ``day_units`` is a list of ``(date_token, [paths])`` — one unit per
    (type-prefix, date token), the parts natural-sorted — so all numbered parts of
    a trading day are ingested as ONE unit. ``singles`` are ZIPs whose filename
    has no recognizable date token; they keep the old one-ZIP-one-write path.
    """
    groups: dict = {}
    singles: list = []
    for zf in sorted(zip_files, key=_zip_sort_key):
        tok = _zip_date_token(Path(zf).name)
        if tok is None:
            singles.append(zf)
        else:
            prefix = Path(zf).name.split(".", 1)[0]
            groups.setdefault((prefix, tok), []).append(zf)
    day_units = [(tok, parts) for (_pfx, tok), parts in groups.items()]
    return day_units, singles


def _ingest_flat_day_safe(date_str, zip_paths, output_dir, data_type, language, ticker_filter, compression):
    """Module-level ingest-one-day task for ``ingest_directory``'s process pool.

    All ZIP parts of the day are read and concatenated before the single write —
    the same unit :func:`_ingest_date_group` gives the structured paths. The old
    per-ZIP flat path wrote each part independently, so a later part (NEEDS'
    closing-appendix part in particular, which repeats tickers from earlier
    parts) overwrote the earlier parts' per-ticker files: the store kept only the
    few appendix rows and silently lost the session (audit finding H1). Errors
    are recorded, not raised, matching the flat path's per-unit error contract;
    a one-shot OOM still aborts loudly.
    """
    try:
        dtype = data_type or _detect_data_type_from_path(str(zip_paths[0]))
        year = int(date_str[:4])
        meta = _ingest_date_group(
            date_str, zip_paths, output_dir, dtype, year, language, ticker_filter,
            compression=compression,
        )
        meta["data_type"] = dtype
        meta["year"] = year
        meta["zip_paths"] = [str(Path(p).resolve()) for p in zip_paths]
        return meta
    except OneShotMemoryError:
        raise
    except Exception as exc:
        return {
            "date": date_str,
            "zip_path": str(zip_paths[0]),
            "zip_paths": [str(Path(p).resolve()) for p in zip_paths],
            "error": str(exc),
        }


def _log_flat_progress(done: int, total: int, meta: dict) -> None:
    label = meta.get("date") or Path(meta.get("zip_path", "")).name
    rows = meta.get("rows", "error")
    logger.info("[%d/%d] %s -> %s rows", done, total, label, rows)


def ingest_directory(
    input_dir: str,
    output_dir: str,
    data_type: Optional[str] = None,
    language: str = "en",
    max_workers: Union[int, str, None] = None,
    progress: bool = True,
    ticker_filter: Optional[set] = None,
    compression: str = "zstd",
) -> list[dict]:
    """Ingest every ``.zip`` in a single flat directory into the Parquet store.

    Globs ``*.zip`` **directly** under ``input_dir`` (non-recursive) and groups
    the ZIPs by their filename date token, ingesting each trading day as ONE unit
    (all numbered parts read and concatenated before the write, exactly like
    :func:`ingest_period`) — NEEDS repeats tickers across a day's parts (the
    closing-appendix part especially), so writing per ZIP silently overwrote
    earlier parts' data. ZIPs with no recognizable date token fall back to the
    per-ZIP path via :func:`ingest_single_zip`. For a structured
    ``{year}/{yearmonth}/`` NEEDS root use :func:`ingest_period` /
    :func:`ingest_year_from_root` instead.

    Args:
        input_dir: Directory containing the NEEDS ``.zip`` files.
        output_dir: Store root to write under.
        data_type: NEEDS type; auto-detected per day/ZIP from the filename when
            ``None``.
        language: Output column-name language (``"en"`` / ``"jp"``).
        max_workers: Parallel worker processes — an ``int``, ``"auto"`` (this machine's
            logical cores), or ``None`` (the default): ``None`` reads the
            ``TSE_TICK_MAX_WORKERS`` env var, runs auto in an interactive
            session (Jupyter/REPL — spawn-safe there), and serially from a
            script. Parallel workers are capped by the machine's logical cores
            AND available RAM (each worker holds a whole day's frame).
            **When running parallel from a script, the call must be inside
            ``if __name__ == "__main__":`` — workers are started with the
            ``spawn`` method, which re-imports your script in every worker.**
        progress: Log a per-unit progress line.
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per ingested **day** (``{"date", "parts", "rows",
        "output_path", "data_type", "year", "zip_paths"}``, plus ``"errors"``
        when parts failed) and one per date-token-less ZIP (see
        :func:`ingest_single_zip`); a failed unit contributes
        ``{"zip_path": ..., "error": ...}`` instead.
    """
    _reject_bootstrap_reimport()  # actionable error for an unguarded top-level call (B1)

    max_workers = _resolve_max_workers(max_workers)

    in_path = Path(input_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    day_units, singles = _flat_day_units(list(in_path.glob("*.zip")))
    total = len(day_units) + len(singles)
    results: list[dict] = []

    # RAM-aware worker cap: a flat-directory worker holds one day's frame. Estimate
    # per worker from the largest unit (full-frame individual_stock) and clamp to
    # cores + RAM.
    max_workers = _cap_workers(
        max_workers,
        per_worker_gb=_estimate_worker_gb(
            day_units + [(Path(zf).name, [zf]) for zf in singles],
            data_type, ticker_filter,
        ),
    )

    if max_workers > 1 and total > 1:
        with _bounded_polars_threads(max_workers, total):
            with ProcessPoolExecutor(max_workers=max_workers, mp_context=_MP_SPAWN) as executor:
                futures = {}
                for tok, unit_parts in day_units:
                    futures[executor.submit(
                        _ingest_flat_day_safe, tok, unit_parts, output_dir,
                        data_type, language, ticker_filter, compression,
                    )] = tok
                for zf in singles:
                    futures[executor.submit(
                        _ingest_single_zip_safe, zf, output_dir,
                        data_type, language, ticker_filter, compression,
                    )] = zf
                done = 0
                for future in as_completed(futures):
                    # a one-shot OOM propagates and aborts; a *killed* worker only
                    # yields BrokenProcessPool, so convert it to an actionable error.
                    try:
                        meta = future.result()
                    except BrokenProcessPool as exc:
                        raise _worker_died_error(done, total, max_workers) from exc
                    done += 1
                    results.append(meta)
                    if progress:
                        _log_flat_progress(done, total, meta)
    else:
        done = 0
        for tok, unit_parts in day_units:
            done += 1
            meta = _ingest_flat_day_safe(
                tok, unit_parts, output_dir, data_type, language, ticker_filter, compression
            )
            results.append(meta)
            if progress:
                _log_flat_progress(done, total, meta)
        for zf in singles:
            done += 1
            meta = _ingest_single_zip_safe(zf, output_dir, data_type, language, ticker_filter, compression)
            results.append(meta)
            if progress:
                _log_flat_progress(done, total, meta)

    return results


def ingest_year(
    input_dir: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    max_workers: Union[int, str, None] = 1,
    ticker_filter: Optional[set] = None,
    compression: str = "zstd",
) -> list[dict]:
    """Ingest every ``.zip`` for one ``year`` from a **flat** directory.

    Globs ``*.zip`` directly under ``input_dir`` (non-recursive) and keeps those
    whose filename **date token** falls in ``year`` (a plain substring match used
    to over-select — ``"20201207"`` contains ``"2012"``, so ``year=2012`` ingested
    December-2020 files). The kept ZIPs are grouped by date token and each
    trading day ingested as ONE unit (all numbered parts read and concatenated
    before the write), so a day's later parts no longer overwrite the earlier
    ones. For a structured ``{year}/{yearmonth}/`` NEEDS root use
    :func:`ingest_year_from_root` (or :func:`ingest_period`) instead.

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
        One result dict per ingested **date** (``{"date", "parts", "rows",
        "output_path"}``, plus ``"errors"`` when parts failed); a failed date
        contributes ``{"date": ..., "error": ...}`` instead.
    """
    validate_data_type(data_type)

    # Serial path: resolve quietly ("auto"/None -> 1, no hint) so only an
    # explicit int request triggers the ignored-workers warning.
    max_workers = _resolve_max_workers(max_workers, allow_default_auto=False)
    if max_workers > 1:
        logger.warning(
            "ingest_year runs the flat-directory path serially; max_workers=%d is "
            "ignored. Use ingest_period / ingest_year_from_root (a structured root) for "
            "parallel per-date ingestion.", max_workers,
        )

    in_path = Path(input_dir)
    if not in_path.exists():
        raise FileNotFoundError(f"Input directory not found: {input_dir}")

    year_zips = [
        f for f in in_path.glob("*.zip")
        if (_zip_date_token(f.name) or "")[:4] == str(year)
    ]
    day_units, _singles = _flat_day_units(year_zips)  # token filter leaves no singles
    results: list[dict] = []

    for tok, unit_parts in day_units:
        try:
            meta = _ingest_date_group(
                tok, unit_parts, output_dir, data_type, year, language, ticker_filter,
                compression=compression,
            )
        except OneShotMemoryError:
            raise
        except Exception as exc:
            meta = {
                "date": tok,
                "zip_paths": [str(p) for p in unit_parts],
                "error": str(exc),
            }
        results.append(meta)

    return results


def _ingest_date_group(date_str, zip_paths, output_dir, data_type, year, language, ticker_filter,
                       compression="zstd"):
    """Read every ZIP part of one date, concat, and write each ticker file once.

    This is the multi-part-per-day unit: NEEDS splits a trading day across parts
    by ticker range (plus a closing tail), so all parts must be read and
    concatenated before writing — otherwise later parts get skipped (resume) or
    overwrite earlier ones.
    """
    # Root the requested codes to their 4-char families (72031 -> 7203) so the
    # filter, the prune, and the coverage marker all record the same family
    # semantics (idempotent — the parent paths root too, but the flat path and a
    # direct call reach here un-rooted).
    if ticker_filter and data_type == "individual_stock":
        ticker_filter = _stock_family_roots(ticker_filter)
    # Prune the day's parts to the requested ticker(s) HERE, not on the parent, so
    # pruning stays interleaved with this date's write (issue #39: a partition lands
    # per day and a resumed run prunes only the dates it ingests) and, under
    # parallelism, each worker prunes its own date concurrently. `_prune_parts_by_ticker`
    # groups by day internally, so per-date pruning selects the identical parts.
    if ticker_filter and data_type == "individual_stock":
        zip_paths = _prune_parts_by_ticker(zip_paths, ticker_filter)
    # Stream each part straight into the store when a ticker filter bounds how many
    # output files a day can have. Holding every part and concatenating the day is
    # what made a 4.67M-row day (7203+9984, 20250409) peak at 24.5 GB and OOM a
    # 34 GB box; appending part-by-part keeps the peak at ~one part, and NEEDS
    # size-splits parts at ~55 MB, so that bound holds as volume grows. Everything
    # else — full-frame days (thousands of ticker files, one writer each) and the
    # summary/index types (small daily frames) — keeps the proven concat path.
    streaming = (
        data_type == "individual_stock"
        and bool(ticker_filter)
        and len(ticker_filter) <= _MAX_STREAM_TICKERS
    )
    appender = (
        PartitionedParquetAppender(output_dir, data_type, compression=compression)
        if streaming
        else None
    )
    parts: list = []
    errors: list = []
    try:
        for zp in zip_paths:
            try:
                df = create_df(
                    str(zp), language=language, auto_detect=False,
                    data_type=data_type, year=year, ticker_filter=ticker_filter,
                    # Streaming: each cleaned morsel goes straight to its ticker's
                    # Parquet writer and is dropped, so neither the part's frame nor
                    # the day's ever exists. create_df then returns a typed-empty
                    # frame (the sink owns the rows) and `rows` comes from the
                    # appender. This is the whole point: peak ~= one morsel.
                    on_morsel=(appender.write if appender is not None else None),
                )
            except (zipfile.BadZipFile, EOFError) as exc:
                logger.error("Corrupt zip %s: %s", Path(zp).name, exc)
                errors.append({"zip_path": str(zp), "error": str(exc)})
                continue
            except OneShotMemoryError:
                # A one-shot OOM must abort the date group, not silently write a partial
                # day that resume then marks complete (alpha-review finding 1).
                raise
            except Exception as exc:
                logger.error("Error reading %s: %s", Path(zp).name, exc)
                errors.append({"zip_path": str(zp), "error": str(exc)})
                continue
            # create_df's ticker_filter only drives the individual_stock raw-byte fast
            # path; for the other types prune here so ingest honors ticker_filter too.
            if ticker_filter and data_type != "individual_stock":
                df = _filter_codes(df, data_type, {str(t).strip() for t in ticker_filter})
            if appender is not None:
                # on_morsel already streamed this part's rows to the writers; the
                # returned frame is the typed-empty placeholder, never the data.
                pass
            elif not df.is_empty():
                parts.append(df)
            del df
    except BaseException:
        # Publish nothing on the way out: the temp files are dropped, so a failed
        # day stays fully re-ingestable rather than half-written.
        if appender is not None:
            appender.abort()
        raise

    if appender is not None:
        rows = appender.rows_written
        if rows == 0:
            appender.abort()
        else:
            out_path = appender.commit()
        gc.collect()
        if rows == 0:
            meta = {"date": date_str, "parts": len(zip_paths), "rows": 0, "output_path": None}
            if errors:
                meta["errors"] = errors
            else:
                date_dir = Path(output_dir) / data_type / f"date={date_str}"
                date_dir.mkdir(parents=True, exist_ok=True)
                _write_coverage_marker(date_dir, ticker_filter, complete=True)
            return meta
        _write_coverage_marker(
            Path(output_dir) / data_type / f"date={date_str}",
            ticker_filter,
            complete=not errors,
        )
        meta = {"date": date_str, "parts": len(zip_paths), "rows": rows, "output_path": out_path}
        if errors:
            meta["errors"] = errors
        return meta

    if not parts:
        gc.collect()
        meta = {"date": date_str, "parts": len(zip_paths), "rows": 0, "output_path": None}
        if errors:
            # Lost parts are recorded (audit finding M1), never silently dropped.
            # No marker either: the day must stay fully re-ingestable.
            meta["errors"] = errors
        else:
            # A cleanly-read day that yielded no rows for this request (the
            # filtered ticker never traded) is DONE for this coverage — record
            # that in a marker so resume can skip it. Without one, every resumed
            # run re-probed and re-scanned the day's parts forever.
            date_dir = Path(output_dir) / data_type / f"date={date_str}"
            date_dir.mkdir(parents=True, exist_ok=True)
            _write_coverage_marker(date_dir, ticker_filter, complete=True)
        return meta
    combined = pl.concat(parts, how="vertical")
    # Keep the per-date gc.collect() (issue #43 proposed removing them as "pure waste"):
    # a full-frame individual_stock day holds every part of the day at once and peaks
    # within ~0.6 GB of the RAM ceiling on a 34 GB box (measured; HEAD OOM-crashes there
    # too), so prompt collection between the concat and the write gives real headroom at
    # a cost dwarfed by the multi-second-per-day I/O. `del` drops the reference first.
    del parts
    gc.collect()
    rows = len(combined)
    out_path = write_partitioned_parquet(combined, output_dir, data_type, compression=compression)
    # Record what this write covered (full or which tickers) so resume can skip
    # only requests the partition actually satisfies (audit finding H2). A day
    # that lost parts is marked incomplete so resume re-ingests it (finding M1)
    # instead of trusting a permanently partial day.
    _write_coverage_marker(
        Path(output_dir) / data_type / f"date={date_str}",
        ticker_filter,
        complete=not errors,
    )
    del combined
    gc.collect()
    meta = {"date": date_str, "parts": len(zip_paths), "rows": rows, "output_path": out_path}
    if errors:
        meta["errors"] = errors
    return meta


def _ingest_grouped(zip_paths, output_dir, data_type, year, language, resume, ticker_filter,
                    max_workers=1, compression="zstd"):
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
    _reject_bootstrap_reimport()  # actionable error for an unguarded top-level call (B1)

    # Family-root the request up front so the per-date resume check compares the
    # same codes the coverage markers record (workers re-root; it is idempotent).
    if ticker_filter and data_type == "individual_stock":
        ticker_filter = _stock_family_roots(ticker_filter)

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
            # Skip a date only if its partition files exist AND all pass the
            # Parquet footer probe — an interrupted older-version write leaves a
            # truncated file that a bare existence check would trust forever
            # (audit finding B11). Invalid files are deleted (they are unreadable
            # by any Parquet reader) and the date re-ingested. `*.parquet` also
            # matches the summary types' `<date>.parquet` files, which the old
            # `ticker=*.parquet` glob missed (their daily-token dates never
            # resume-skipped).
            date_dir = output_root_path / f"date={date_str}"
            existing = list(date_dir.glob("*.parquet")) if date_dir.exists() else []
            invalid = [p for p in existing if not _valid_parquet_file(p)]
            for p in invalid:
                logger.warning(
                    "Resume: %s is not a complete Parquet file (interrupted write?) — "
                    "deleting it and re-ingesting date %s", p, date_str,
                )
                try:
                    p.unlink()
                except OSError as exc:
                    logger.warning("Resume: could not delete %s: %s", p, exc)
            # Files existing (and valid) is necessary but NOT sufficient: the
            # partition must also have been written with coverage that includes
            # THIS request's ticker_filter — a store built for ticker A used to
            # resume-skip a later request for ticker B and silently return
            # nothing for it (audit finding H2). A marker ALONE (zero parquet
            # files) also satisfies: a filtered day whose ticker never traded
            # writes only the marker, and used to be re-scanned on every resume.
            # A marker-less empty dir still re-ingests (legacy semantics).
            has_marker = _read_coverage_marker(date_dir) is not None
            if (existing or has_marker) and not invalid and _coverage_satisfied(date_dir, ticker_filter):
                continue
        tasks.append((date_str, parts))

    if resume and len(tasks) < len(groups):
        logger.info(
            "Resume: skipped %d of %d date(s) already in the store",
            len(groups) - len(tasks), len(groups),
        )

    # RAM-aware: each worker holds one whole date's frame. Estimate the largest day's
    # per-worker peak (from its part sizes for a full-frame individual_stock ingest; small
    # for filtered / summary / index) and let _cap_workers clamp to what RAM allows.
    workers = _cap_workers(
        max_workers, per_worker_gb=_estimate_worker_gb(tasks, data_type, ticker_filter)
    )

    if workers <= 1 or len(tasks) <= 1:
        results: list[dict] = []
        for done, (date_str, parts) in enumerate(tasks, 1):
            meta = _ingest_date_group(date_str, parts, output_dir, data_type, year, language,
                                      ticker_filter, compression=compression)
            results.append(meta)
            logger.info("  [%d/%d] %s (%d parts) -> %s rows",
                        done, len(tasks), date_str, meta["parts"], meta["rows"])
        return results

    results = []
    with _bounded_polars_threads(workers, len(tasks)):
        with ProcessPoolExecutor(max_workers=workers, mp_context=_MP_SPAWN) as executor:
            futures = {
                executor.submit(
                    _ingest_date_group, date_str, parts, output_dir,
                    data_type, year, language, ticker_filter, compression,
                ): date_str
                for date_str, parts in tasks
            }
            done = 0
            for future in as_completed(futures):
                # A one-shot OOM in any worker propagates here and aborts the whole
                # ingest rather than silently leaving a partial period behind. A
                # *killed* worker can't raise at all, so its BrokenProcessPool
                # becomes an actionable IngestWorkerError instead.
                try:
                    meta = future.result()
                except BrokenProcessPool as exc:
                    raise _worker_died_error(done, len(tasks), workers) from exc
                results.append(meta)
                done += 1
                logger.info("  [%d/%d] %s (%d parts) -> %s rows",
                            done, len(tasks), meta["date"], meta["parts"], meta["rows"])
    results.sort(key=lambda m: m["date"])
    return results


def ingest_year_from_root(
    input_root: str,
    output_dir: str,
    year: int,
    data_type: str,
    language: str = "en",
    resume: bool = True,
    max_workers: Union[int, str, None] = None,
    ticker_filter: Optional[set] = None,
    compression: str = "zstd",
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
        resume: Skip dates already present in the store (default ``True``). A date
            is skipped only if its Parquet files pass a footer integrity probe;
            a truncated partition (interrupted write) is deleted and re-ingested.
        max_workers: Parallel worker processes for the independent per-date
            ingests — an ``int``, ``"auto"`` (this machine's
            logical cores), or ``None`` (the default): ``None`` reads the
            ``TSE_TICK_MAX_WORKERS`` env var, runs auto in an interactive
            session (Jupyter/REPL — spawn-safe there), and serially from a
            script. Parallel workers are capped by the machine's logical cores
            AND available RAM (each worker holds a whole day's frame).
            **When running parallel from a script, the call must be inside
            ``if __name__ == "__main__":`` — worker processes are started with
            the ``spawn`` method (a Polars ``fork`` deadlock workaround), which
            re-imports your script in every worker.**
        ticker_filter: Optional ``set`` of stock/index codes to keep.

    Returns:
        One result dict per ingested date (``{"date", "parts", "rows",
        "output_path"}``).
    """
    validate_data_type(data_type)
    _require_input_root(input_root)
    max_workers = _resolve_max_workers(max_workers)

    zip_paths = discover_zips(input_root, data_type, [year])
    if not zip_paths:
        _warn_zero_discovery(input_root, data_type, f"year {year}")
    return _ingest_grouped(
        zip_paths, output_dir, data_type, year, language, resume, ticker_filter, max_workers,
        compression=compression,
    )


def ingest_period(
    input_root: str,
    output_dir: str,
    period: str,
    data_type: str,
    language: str = "en",
    resume: bool = True,
    max_workers: Union[int, str, None] = None,
    ticker_filter: Optional[set] = None,
    compression: str = "zstd",
) -> list[dict]:
    """Ingest a whole period from a structured NEEDS root into the Parquet store.

    Resolves ``period`` with :func:`tse_tick.parse_period`, discovers the ZIPs
    with :func:`tse_tick.discover_zips`, and ingests each via
    :func:`ingest_single_zip`.

    Args:
        input_root: Root of the ``{year}/{yearmonth}/`` NEEDS hierarchy.
        output_dir: Store root to write under.
        period: ``"YYYY"``, a single ``"YYYYMM"`` or ``"YYYYMMDD"``, or a
            ``"YYYY-YYYY"`` / ``"YYYYMM-YYYYMM"`` / ``"YYYYMMDD-YYYYMMDD"`` range
            (the same forms :func:`tse_tick.parse_period` accepts).
        data_type: NEEDS type to ingest.
        language: Output column-name language (``"en"`` / ``"jp"``).
        resume: Skip dates whose Parquet output already exists (default ``True``).
            A date is skipped only if its Parquet files pass a footer integrity
            probe; a truncated partition (interrupted write) is deleted and
            re-ingested.
        max_workers: Parallel worker processes for the independent per-date
            ingests — an ``int``, ``"auto"`` (this machine's
            logical cores), or ``None`` (the default): ``None`` reads the
            ``TSE_TICK_MAX_WORKERS`` env var, runs auto in an interactive
            session (Jupyter/REPL — spawn-safe there), and serially from a
            script. Parallel workers are capped by the machine's logical cores
            AND available RAM (each worker holds a whole day's frame).
            Wired through every granularity (year / month / date). **When
            running parallel from a script, the call must be inside
            ``if __name__ == "__main__":`` — worker processes are started with
            the ``spawn`` method (a Polars ``fork`` deadlock workaround), which
            re-imports your script in every worker.**
        ticker_filter: Optional ``set`` of string stock codes
            (``individual_stock`` only).

    Returns:
        One result dict per processed ZIP (see :func:`ingest_single_zip`); a
        failed ZIP contributes ``{"zip_path": ..., "error": ...}`` instead.
    """
    validate_data_type(data_type)
    _require_input_root(input_root)
    max_workers = _resolve_max_workers(max_workers)

    parsed = parse_period(period)
    granularity = parsed["granularity"]
    years = parsed["years"]

    if granularity == "year":
        results: list[dict] = []
        for year in years:
            results.extend(
                ingest_year_from_root(
                    input_root, output_dir, year, data_type, language, resume,
                    max_workers=max_workers, ticker_filter=ticker_filter,
                    compression=compression,
                )
            )
        return results

    if granularity == "month":
        results = []
        discovered = 0
        months_by_year: dict = parsed["months_by_year"]
        for year, months in months_by_year.items():
            zip_paths = discover_zips(input_root, data_type, [year], months=list(months))
            discovered += len(zip_paths)
            results.extend(
                _process_zips(zip_paths, output_dir, data_type, year, language, resume,
                              max_workers=max_workers, ticker_filter=ticker_filter,
                              compression=compression)
            )
        if not discovered:
            _warn_zero_discovery(input_root, data_type, f"period {period}")
        return results

    if granularity == "date":
        results = []
        discovered = 0
        dates: list = parsed["dates"]
        date_years = sorted(set(int(d[:4]) for d in dates))
        for year in date_years:
            year_dates = [d for d in dates if d.startswith(str(year))]
            year_months = sorted(set(int(d[4:6]) for d in year_dates))
            zip_paths = discover_zips(input_root, data_type, [year], months=year_months, dates=year_dates)
            discovered += len(zip_paths)
            results.extend(
                _process_zips(zip_paths, output_dir, data_type, year, language, resume,
                              max_workers=max_workers, ticker_filter=ticker_filter,
                              compression=compression)
            )
        if not discovered:
            _warn_zero_discovery(input_root, data_type, f"period {period}")
        return results

    raise ValueError(f"Unknown granularity: {granularity}")


def _period_date_bounds(period: str) -> "tuple[str, str]":
    """Inclusive ``(first, last)`` ``YYYYMMDD`` bounds of a period string.

    ``parse_period`` only produces contiguous ranges, so a pair of bounds
    represents any accepted period form exactly. Used to scope
    :func:`extract_to_store`'s Stage-2 query to ``period`` on a reused store
    (audit finding B5: it used to return every stored day).
    """
    parsed = parse_period(period)
    granularity = parsed["granularity"]
    if granularity == "year":
        years = cast("list[int]", parsed["years"])
        return f"{min(years)}0101", f"{max(years)}1231"
    if granularity == "month":
        months_by_year = cast("dict[int, list[int]]", parsed["months_by_year"])
        pairs = [(y, m) for y, months in months_by_year.items() for m in months]
        y0, m0 = min(pairs)
        y1, m1 = max(pairs)
        return f"{y0}{m0:02d}01", f"{y1}{m1:02d}{calendar.monthrange(y1, m1)[1]:02d}"
    dates = cast("list[str]", parsed["dates"])
    return min(dates), max(dates)


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
    max_workers: Union[int, str, None] = None,
    compression: str = "zstd",
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
            ``individual_stock`` codes select their whole share-class **family**:
            ``"7203"`` (and equally ``"72031"``) extracts the parent plus its
            suffixed classes — the same rows a filtered ingest has always stored.
        data_type: One of the four NEEDS types (default ``"individual_stock"``).
        start_time: Optional intraday lower bound ``"HH:MM:SS"`` (tick types only;
            the two ``*_summary`` types are daily aggregates and raise if given).
        end_time: Optional intraday upper bound ``"HH:MM:SS"``.
        language: Output column-name language (``"en"`` / ``"jp"``).
        resume: Skip dates already in the store (default ``True``); truncated
            partitions (interrupted writes) are detected and re-ingested.
        max_workers: Parallel worker processes for the Stage-1 per-date
            ingests — an ``int``, ``"auto"`` (this machine's
            logical cores), or ``None`` (the default): ``None`` reads the
            ``TSE_TICK_MAX_WORKERS`` env var, runs auto in an interactive
            session (Jupyter/REPL — spawn-safe there), and serially from a
            script. Parallel workers are capped by the machine's logical cores
            AND available RAM (each worker holds a whole day's frame).
            **When running parallel from a script, the call must be inside
            ``if __name__ == "__main__":`` — workers are started with the
            ``spawn`` method, which re-imports your script in every worker.**

    Returns:
        The queried Polars DataFrame for the requested ticker(s) — columns match
        :func:`tse_tick.query_ticks` (the read columns plus a ``date`` column). With
        several tickers the per-ticker frames are concatenated (in sorted code
        order). **All** matching rows are returned as ONE in-memory DataFrame —
        this queries with ``limit=None``, so unlike a bare :func:`query_ticks` call
        it is not subject to the default 10M-row cap (a whole month of a very
        active ticker exceeds it). The flip side: a long period of an active
        ticker can be tens of GB in RAM. Past ~10M rows a capturable
        :class:`tse_tick.LargeResultWarning` is emitted first — the store is
        already built at that point, so the memory-safe pattern is to ignore the
        returned frame and read the store in bounded :func:`tse_tick.query_ticks`
        slices (per day / per month). The result is scoped to ``period`` even when
        ``output_dir`` is a reused store holding other days.

    Requires the optional ``[query]`` extra (DuckDB). Example::

        >>> df = tse_tick.extract_to_store("G:/NEEDS", "toyota_sb_store",
        ...                                "202201", ["7203", "9984"])
    """
    # Import (and so the DuckDB dependency check) stays FIRST: Stage 1 can run
    # for hours, and a missing [query] extra must fail before it, not after.
    try:
        from tse_tick.query import _query_extract_batch
    except ImportError as exc:
        raise ImportError(
            "extract_to_store requires DuckDB (Stage 2 queries the built store). "
            "Install the query extra: pip install tse-tick[query]"
        ) from exc

    # Validate arguments UP FRONT — before the (potentially hours-long) Stage-1
    # ingest — so a bad data_type or an unsupported summary time filter fails in
    # 0.00 s with a clear ValueError instead of after a wasted ingest with a raw
    # DuckDB bind error and a partial store on disk (round-16 finding 1).
    validate_data_type(data_type)
    validate_time_filter_support(data_type, start_time, end_time)

    max_workers = _resolve_max_workers(max_workers)
    tickers = _normalize_ticker_filter(ticker)
    if not tickers:
        raise ValueError("extract_to_store: at least one ticker is required")
    if data_type == "individual_stock":
        # Family semantics: root each code to its 4-char family (72031 -> 7203) so
        # Stage 1's filter, the resume coverage, and Stage 2's file selection all
        # agree — Stage 1 has always ingested the whole family for a 4-char code,
        # and Stage 2 used to silently drop the suffixed classes' rows.
        tickers = _stock_family_roots(tickers)

    # Stage 1 — ingest every ticker in one part-pruned pass into the reusable store.
    stage1_results = ingest_period(
        input_root, output_dir, period, data_type,
        language=language, resume=resume, ticker_filter=tickers,
        max_workers=max_workers, compression=compression,
    )
    # Stage 1 records lost parts / failed dates in its results instead of raising
    # (corrupt ZIPs are per-unit, not fatal) — but THIS call is about to return
    # the queried frame as if it were complete, so surface the loss loudly. The
    # affected days stay resume-eligible: fixing the raw files and re-running
    # re-ingests exactly them.
    lossy = sorted(
        str(r.get("date") or r.get("zip_path", "?"))
        for r in stage1_results
        if r.get("error") or r.get("errors")
    )
    if lossy:
        shown = ", ".join(lossy[:10]) + (", …" if len(lossy) > 10 else "")
        warnings.warn(
            f"extract_to_store: Stage 1 lost data on {len(lossy)} date(s) "
            f"({shown}) — see the logged errors. The returned DataFrame may be "
            f"missing those rows; the affected days remain resume-eligible, so "
            f"re-running after fixing the raw files re-ingests exactly them.",
            PartialIngestWarning,
            stacklevel=2,
        )
    # Stage 2 — one DuckDB connection and one scan for ALL tickers (issue #44), replacing
    # the per-ticker query_ticks(limit=None) loop + concat (and, for the two summary types,
    # N full-store scans with one). Returns the same multiset of rows in the same
    # (code, Data Date, time) order; the summary types are byte-identical. The query is
    # scoped to period's date bounds so a REUSED store returns exactly `period`, not every
    # stored day (audit finding B5). limit is None — extract returns ALL rows (a whole
    # month of a very active ticker exceeds query_ticks' default 10M exploratory cap).
    date_from, date_to = _period_date_bounds(period)

    # The result is materialized as ONE in-memory frame; past the threshold, warn
    # first (audit finding B2). The per-ticker-day row counts come free from the
    # Parquet footers of the files the query is about to read.
    if data_type == "individual_stock" or data_type == "indices":
        import pyarrow.parquet as pq

        type_dir = Path(output_dir) / data_type
        total_rows = 0
        if type_dir.exists():
            for code in tickers:
                # ticker={code}* + the family predicate: count the suffixed
                # share-class files the query below will read too.
                for f in type_dir.glob(f"date=*/ticker={code}*.parquet"):
                    if not _code_matches_family(f.stem[len("ticker="):], code):
                        continue
                    if date_from <= f.parent.name[len("date="):] <= date_to:
                        try:
                            total_rows += pq.ParquetFile(f).metadata.num_rows
                        except Exception:  # unreadable file: the query itself will surface it
                            continue
        if total_rows > _LARGE_EXTRACT_ROWS:
            warnings.warn(
                f"extract_to_store is about to return ~{total_rows:,} rows as one "
                f"in-memory DataFrame. The Parquet store at {output_dir!r} is already "
                f"built — for large periods, ignore the returned frame and read the "
                f"store in bounded query_ticks(...) slices (per day / per month) "
                f"instead.",
                LargeResultWarning,
                stacklevel=2,
            )

    return _query_extract_batch(
        output_dir, data_type, tickers,
        date_from=date_from, date_to=date_to,
        start_time=start_time, end_time=end_time,
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
    compression: str = "zstd",
) -> list[dict]:
    return _ingest_grouped(
        zip_paths, output_dir, data_type, year, language, resume, ticker_filter, max_workers,
        compression=compression,
    )


def ingest_event_windows_period(
    input_root: str,
    output_dir: str,
    period: str,
    filter_csv: str,
    window_minutes: int = 120,
    resume: bool = True,
    max_workers: Union[int, str, None] = 1,
    compression: str = "zstd",
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
            ``"YYYY-YYYY"`` / ``"YYYYMM-YYYYMM"`` / ``"YYYYMMDD-YYYYMMDD"`` range.
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
    max_workers = _resolve_max_workers(max_workers, allow_default_auto=False)
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
                write_event_window_parquet(combined, output_dir, compression=compression)
                del combined
                gc.collect()
                logger.info("  %s: %s event-window ticks written", date_str, f"{combined_rows:,}")
            else:
                logger.info("  %s: no matching ticks after filtering", date_str)

    if skipped_by_resume:
        logger.info("Resume: skipped %d already-processed dates", skipped_by_resume)
