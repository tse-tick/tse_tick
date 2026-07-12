# tse_tick/io/parquet.py
import datetime
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Optional

import polars as pl

import pyarrow.dataset as ds

from tse_tick.constants import INDEX_TYPES, validate_data_type

logger = logging.getLogger(__name__)

# Tick types partition by (date, code): each ticker-day holds thousands of ticks,
# so a per-ticker file is large and prunes well. The two *daily-aggregate* summary
# types hold ~1 row per (date, code), so a per-ticker file there is one tiny row —
# tens of thousands of files, ~160x size amplification, minutes to build. They
# partition by date only (one file per date) and keep the code as a column;
# query_ticks / get_available_tickers prune that column via row-group statistics.
_DEFAULT_PARTITION_COLS: dict[str, list[str]] = {
    "individual_stock": ["Data Date", "Stock Code"],
    "stock_summary": ["Data Date"],
    "indices": ["Data Date", "Index Code"],
    "indices_summary": ["Data Date"],
}

_INDEX_DATA_TYPES = INDEX_TYPES
_UNKNOWN_CODE_RE = re.compile(r"^Unknown \((.+)\)$")

# Per-date ticker-file fan-out: write concurrently only when a date produces at
# least this many files (a full-frame individual_stock day writes thousands;
# tiny fan-outs would pay more in thread setup than they save). The thread
# count is deliberately small and NOT scaled to os.cpu_count(): the writes
# release the GIL in Polars' Rust writer, but per-date process workers may
# already be running several of these loops at once.
_PARALLEL_WRITE_MIN_FILES = 16
_PARALLEL_WRITE_THREADS = 8


def _index_code_lookup() -> dict[str, str]:
    """Map a decoded Index Code display value back to its raw code.

    ``clean_data`` categorically decodes Index Code (e.g. "101" -> "Nikkei 225")
    for display *before* partitioning, which would otherwise be baked into the
    ``ticker=`` filename. Reverse-map the decoded name (English or Japanese) back
    to the raw code so the store partitions on the raw code while the in-file
    Index Code column keeps its decoded display value.
    """
    from tse_tick.core import get_schemas_categorical

    catalogue = get_schemas_categorical().get("Index Code", {})
    lookup: dict[str, str] = {}
    for code, info in catalogue.items():
        for key in ("name", "jp"):
            display = info.get(key)
            if display is not None:
                lookup[display] = code
    return lookup


def _partition_value(raw_value: object, code_lookup: Optional[dict[str, str]]):
    """Resolve the partition filename value for one ticker/index group.

    For index data types the group value is the decoded display name; map it back
    to the raw code (handling ``Unknown (NNN)`` too), truncated to the 4-char code
    width. Stock codes are kept **whole**: truncating to 4 chars made a suffixed
    5-char code (e.g. New Shares ``"72031"``) collide with its parent (``"7203"``)
    — two groups then targeted the same ticker= file, crashing the write's replace
    loop (or silently mislabelling the suffixed rows as the parent).
    """
    text = str(raw_value).strip()
    if code_lookup is not None:
        mapped = code_lookup.get(text)
        if mapped is None:
            unknown = _UNKNOWN_CODE_RE.match(text)
            if unknown is not None:
                mapped = unknown.group(1).strip()
        if mapped is not None:
            text = mapped
        value = text[:4]
    else:
        value = text
    try:
        return int(value)
    except ValueError:
        return value


def _drop_null_date_rows(df: pl.DataFrame, date_col: str) -> pl.DataFrame:
    """Drop rows whose derived ``_date_str`` partition key is null, with a warning.

    A null key comes from a raw ``Data Date`` that ``clean_data``'s non-strict
    parse could not read — garbage rows with no date to file under. Left in,
    they would be written to a ``date=None/`` partition that no date-scoped
    query ever matches (audit finding L2).
    """
    null_count = df["_date_str"].null_count()
    if null_count:
        logger.warning(
            "Dropping %d row(s) with an unparseable %r before the partitioned "
            "write — they would land in an unqueryable date=None partition.",
            null_count, date_col,
        )
        df = df.filter(pl.col("_date_str").is_not_null())
    return df


def _coerce_time_cols(df: pl.DataFrame) -> pl.DataFrame:
    """Convert any ``datetime.time``-holding column to ``"HHMMSS"`` strings.

    Only ``pl.Time`` (native) and ``pl.Object`` (opaque Python values) columns
    can hold time-of-day values — a ``pl.String`` column holds ``str``, never
    ``datetime.time``, so the old String-dtype guard could never fire, yet it
    materialized a ``drop_nulls()`` copy of every String column (~90 for
    ``individual_stock``) on every partition write. The pipeline stores times as
    strings throughout, so this normally sees nothing to do.
    """
    exprs = []
    for col, dtype in zip(df.columns, df.dtypes):
        if dtype == pl.Time:
            exprs.append(pl.col(col).dt.to_string("%H%M%S").alias(col))
        elif dtype == pl.Object:
            sample_vals = df[col].drop_nulls()
            if len(sample_vals) > 0 and isinstance(sample_vals[0], datetime.time):
                exprs.append(
                    pl.col(col).map_elements(
                        lambda t: t.strftime("%H%M%S") if isinstance(t, datetime.time) else t,
                        return_dtype=pl.String,
                    ).alias(col)
                )
    return df.with_columns(exprs) if exprs else df


def write_partitioned_parquet(
    df: pl.DataFrame,
    output_dir: str,
    data_type: str,
    partition_cols: Optional[list[str]] = None,
    compression: str = "zstd",
) -> str:
    """Write a cleaned frame to the Hive-partitioned Parquet store.

    Writes under ``output_dir/<data_type>/``. The tick types
    (``individual_stock``, ``indices``) partition by ``(Data Date, code)`` —
    ``date=YYYYMMDD/ticker=CODE.parquet`` — so each per-ticker-day file is large
    and prunes well. The daily-aggregate summary types partition by date only —
    ``date=YYYYMMDD/<date>.parquet``, the code kept as a column — to avoid a
    tens-of-thousands-of-tiny-files fan-out. Index codes are stored as the raw
    numeric code in the ``ticker=`` filename.

    Args:
        df: The cleaned DataFrame to write (must contain the partition columns).
        output_dir: Store root; ``<data_type>/`` is created under it.
        data_type: One of the four NEEDS types (selects the default partitioning).
        partition_cols: Override the default partition columns (advanced use).
        compression: Parquet codec (default ``"zstd"`` — ~30% smaller and ~3x
            faster to read than ``"snappy"`` on this data per the tracked format
            benchmark; pass ``"snappy"`` to match pre-0.14 stores). The codec is
            per-file Parquet metadata, so mixed-codec stores read fine — no
            re-ingest needed.

    Returns:
        The absolute path of the ``<output_dir>/<data_type>`` directory.
    """
    validate_data_type(data_type)

    pcols = partition_cols if partition_cols is not None else _DEFAULT_PARTITION_COLS[data_type]
    for col in pcols:
        if col not in df.columns:
            raise ValueError(f"Partition column {col!r} not in DataFrame")

    type_dir = Path(output_dir) / data_type
    type_dir.mkdir(parents=True, exist_ok=True)

    df = _coerce_time_cols(df)

    date_col = pcols[0]
    ticker_col = pcols[1] if len(pcols) > 1 else None
    code_lookup = _index_code_lookup() if data_type in _INDEX_DATA_TYPES else None

    if df.schema[date_col].is_temporal():
        df = df.with_columns(
            pl.col(date_col).dt.strftime("%Y%m%d").alias("_date_str")
        )
    else:
        df = df.with_columns(
            pl.col(date_col).cast(pl.String).str.replace_all("-", "", literal=True).alias("_date_str")
        )

    # clean_data parses dates non-strictly, so a malformed raw Data Date is null
    # here; its group key would stringify to "None" and write an unqueryable
    # date=None/ partition that resume then treats as a real date (audit finding
    # L2). Drop such rows loudly instead of hiding them in a dead partition.
    df = _drop_null_date_rows(df, date_col)

    grouped = df.group_by("_date_str", maintain_order=True)
    for (date_str,), date_group in grouped:
        date_str_val = str(date_str)
        date_dir = type_dir / f"date={date_str_val}"
        date_dir.mkdir(parents=True, exist_ok=True)

        # Two-phase write per date: every file goes to a hidden temp name first,
        # then each is os.replace()d into place (atomic on Windows and POSIX). A
        # process killed mid-write can no longer leave a truncated final file —
        # or, for a multi-file date, a partial subset of final files — that the
        # existence-keyed resume would then trust forever (audit finding B11,
        # observed live as an unreadable partition). Temp names start with "."
        # so `*.parquet` globs and pyarrow/DuckDB dataset scans never see them;
        # the PID suffix keeps concurrent flat-path writers apart.
        pending: list[tuple[Path, Path]] = []
        try:
            if ticker_col is not None:
                # Merge groups whose resolved partition value coincides (e.g. two
                # display forms of one index code) BEFORE writing: duplicate
                # targets used to share one tmp name, and the second os.replace
                # of the same tmp crashed the whole date write.
                by_target: dict[str, list[pl.DataFrame]] = {}
                target_order: list[str] = []
                ticker_groups = date_group.group_by(ticker_col, maintain_order=True)
                for (ticker_val,), ticker_group in ticker_groups:
                    key = str(_partition_value(ticker_val, code_lookup))
                    if key not in by_target:
                        by_target[key] = []
                        target_order.append(key)
                    by_target[key].append(ticker_group.drop(["_date_str"]))
                tasks: list[tuple[pl.DataFrame, Path]] = []
                for key in target_order:
                    frames = by_target[key]
                    out_df = frames[0] if len(frames) == 1 else pl.concat(frames, how="vertical")
                    fpath = date_dir / f"ticker={key}.parquet"
                    tmp = date_dir / f".{fpath.name}.{os.getpid()}.tmp"
                    pending.append((tmp, fpath))
                    tasks.append((out_df, tmp))
                # The per-file writes are independent and release the GIL in
                # Polars' Rust writer, so a big fan-out (a full-frame day is
                # thousands of ticker files) runs them across a small thread
                # pool. Only the WRITES are concurrent; the commit loop below
                # stays sequential, so the all-or-nothing cleanup contract is
                # unchanged. Small fan-outs skip the pool (thread setup would
                # dominate).
                if len(tasks) >= _PARALLEL_WRITE_MIN_FILES:
                    workers = min(_PARALLEL_WRITE_THREADS, len(tasks))
                    with ThreadPoolExecutor(max_workers=workers) as pool:
                        futures = [
                            pool.submit(out_df.write_parquet, tmp, compression=compression)
                            for out_df, tmp in tasks
                        ]
                        for f in futures:
                            f.result()
                else:
                    for out_df, tmp in tasks:
                        out_df.write_parquet(tmp, compression=compression)
            else:
                out_df = date_group.drop(["_date_str"])
                fpath = date_dir / f"{date_str_val}.parquet"
                tmp = date_dir / f".{fpath.name}.{os.getpid()}.tmp"
                pending.append((tmp, fpath))
                out_df.write_parquet(tmp, compression=compression)
            for tmp, fpath in pending:
                os.replace(tmp, fpath)
        except BaseException:
            for tmp, _ in pending:
                try:
                    tmp.unlink(missing_ok=True)
                except OSError:
                    pass
            raise

    return str(type_dir.resolve())


def read_parquet_partition(
    data_dir: str,
    data_type: str,
    date: Optional[str] = None,
    ticker: Optional[int] = None,
    columns: Optional[list[str]] = None,
) -> pl.DataFrame:
    """Read from the **main** Parquet store with PyArrow only (no DuckDB).

    A dependency-light alternative to :func:`tse_tick.query_ticks`: it reads the
    same ``ingest_*`` store (``<data_dir>/<data_type>/date=…/ticker=….parquet``),
    filters by ``date`` / ``ticker`` and projects ``columns`` — but has **no**
    time filter and **no** ordering. For time-range queries use
    :func:`tse_tick.query_ticks` (the ``[query]`` / DuckDB extra).

    Contrast :func:`read_partitioned_parquet`, which reads the separate
    *event-window* store.

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        data_type: Which store to read.
        date: ``"YYYYMMDD"`` day filter; ``None`` for all days.
        ticker: Stock/index code filter (matched on the in-file code column);
            ``None`` for all.
        columns: Column projection; ``None`` selects all.

    Returns:
        A Polars DataFrame of the matching rows.
    """
    type_dir = Path(data_dir) / data_type
    if not type_dir.exists():
        raise FileNotFoundError(f"Parquet store not found: {type_dir}")

    dataset = ds.dataset(str(type_dir), format="parquet", partitioning="hive")

    # The Hive "date" partition is inferred as an integer; cast it to string so
    # the comparison against the "YYYYMMDD" argument has a matching kernel. The
    # ticker is encoded in the filename (ticker=NNNN.parquet), not a directory,
    # so it is not a partition column — filter the in-file code column instead.
    code_col = "Index Code" if data_type in _INDEX_DATA_TYPES else "Stock Code"

    expr = None
    if date is not None:
        expr = ds.field("date").cast("string") == date
    if ticker is not None:
        ticker_expr = ds.field(code_col).cast("string") == str(ticker)
        expr = ticker_expr if expr is None else (expr & ticker_expr)

    table = dataset.to_table(filter=expr, columns=columns)
    df = pl.from_arrow(table)

    return df


def write_event_window_parquet(
    df: pl.DataFrame, output_dir: str, compression: str = "zstd"
) -> None:
    """Append event-window ticks to the **event-window** Parquet store.

    Writes the separate event-window store (laid out as
    ``<output_dir>/year=YYYY/month=MM/<date>.parquet``) that
    :func:`tse_tick.read_partitioned_parquet` reads — distinct from the main
    per-ticker tick store written by :func:`write_partitioned_parquet`. Rows are
    grouped by ``Data Date``; if a date's file already exists the new rows are
    concatenated onto it (so multiple ZIP parts of a day accumulate).

    Args:
        df: Event-window ticks; must contain a ``Data Date`` column.
        output_dir: Root of the event-window store.
    """
    out_root = Path(output_dir)
    df = _coerce_time_cols(df)

    if "Data Date" not in df.columns:
        raise ValueError("DataFrame must contain 'Data Date' column")

    if df.schema["Data Date"].is_temporal():
        date_strs = df["Data Date"].dt.strftime("%Y%m%d")
    else:
        date_strs = (
            df["Data Date"].cast(pl.String).str.replace_all("-", "", literal=True)
        )

    df = df.with_columns(date_strs.alias("_date_str"))

    # Same null-date guard as write_partitioned_parquet (audit finding L2): a
    # malformed Data Date must not create a dead year=/month= partition.
    df = _drop_null_date_rows(df, "Data Date")

    grouped = df.group_by("_date_str", maintain_order=True)
    for (date_str,), group in grouped:
        date_str_val = str(date_str)
        year = date_str_val[:4]
        month = date_str_val[4:6]

        part_dir = out_root / f"year={year}" / f"month={month}"
        part_dir.mkdir(parents=True, exist_ok=True)
        fpath = part_dir / f"{date_str_val}.parquet"

        out_df = group.drop(["_date_str"])

        if fpath.exists():
            existing = pl.read_parquet(fpath)
            out_df = pl.concat([existing, out_df], how="vertical")

        # This writer REWRITES an existing date file to append rows, so a death
        # mid-write would otherwise destroy everything accumulated so far. Write
        # to a hidden temp and os.replace() it into place (atomic; see B11 note
        # in write_partitioned_parquet).
        tmp = part_dir / f".{fpath.name}.{os.getpid()}.tmp"
        try:
            out_df.write_parquet(tmp, compression=compression)
            os.replace(tmp, fpath)
        except BaseException:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise


def read_partitioned_parquet(
    data_dir: str,
    year: Optional[int] = None,
    month: Optional[int] = None,
) -> pl.DataFrame:
    """Read from the **event-window** Parquet store (``year=`` / ``month=``).

    Reads the store written by :func:`write_event_window_parquet` /
    ``ingest_event_windows_period`` (laid out as
    ``<data_dir>/year=YYYY/month=MM/<date>.parquet``), optionally restricted to a
    ``year`` / ``month``.

    Not to be confused with :func:`read_parquet_partition`, which reads the main
    per-ticker tick store.

    Args:
        data_dir: Root of the event-window store.
        year: Restrict to this year; ``None`` for all.
        month: Restrict to this month; ``None`` for all.

    Returns:
        A Polars DataFrame of the matching event-window ticks.
    """
    root = Path(data_dir)
    if not root.exists():
        raise FileNotFoundError(f"Event window Parquet store not found: {root}")

    dataset = ds.dataset(str(root), format="parquet", partitioning="hive")

    expr = None
    if year is not None:
        year_expr = ds.field("year") == year
        expr = year_expr if expr is None else (expr & year_expr)
    if month is not None:
        month_expr = ds.field("month") == month
        expr = month_expr if expr is None else (expr & month_expr)

    table = dataset.to_table(filter=expr)
    return pl.from_arrow(table)
