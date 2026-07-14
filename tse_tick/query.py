# tse_tick/query.py
import re
import tempfile
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union, cast

import polars as pl
import pyarrow.parquet as pq
import duckdb

from .constants import SUMMARY_TYPES, validate_data_type, validate_time_filter_support
from .enhanced import (
    NoDataWarning,
    QueryMemoryError,
    TruncationWarning,
    _code_matches_family,
    parse_period,
)
from .io.parquet import EFFECTIVE_TIME_COL

# Column names are interpolated as double-quoted identifiers (f'"{c}"'), so the
# only injection risk is a character that closes the quote (") or escapes it
# (\), plus statement terminators / control chars. NEEDS column names legitimately
# contain spaces (e.g. "Execution Time"), so a strict word-only regex would reject
# every real column; block the dangerous characters instead.
_FORBIDDEN_IDENTIFIER_CHARS = frozenset('"\\;`\r\n\t\x00')
_MAX_QUERY_ROWS = 10_000_000

# Shared tail for the query-path out-of-memory guidance. The built store is fine;
# the fix is to pull less of it into RAM per call. Mirrors enhanced.py's
# _TWO_STAGE_GUIDANCE (the read-path escape hatch) for the query path.
_QUERY_MEMORY_GUIDANCE = (
    "The query result is too large to materialize as one in-memory DataFrame. The "
    "built store is fine; read it back in bounded slices instead: narrow date= to "
    "a month or day, pass a smaller limit=, or loop query_ticks over sub-periods "
    "(per day / per month) and process each slice before reading the next."
)


# The pre-#65 effective-time expression. Stores written before the materialized
# column existed keep working on this fallback — the store change is additive, so
# no re-ingest is required (same compatibility contract as the coverage marker).
_EFFECTIVE_TIME_CASE = (
    'CASE WHEN "Execution Time" IS NULL OR "Execution Time" = \'\' '
    'THEN substr("Update Time", 1, 6) ELSE "Execution Time" END'
)


def _store_has_effective_time(sample_file: Optional[str]) -> bool:
    """Does this store carry the materialized ``Effective Time`` column?

    Read from one file's Parquet footer (schema only, no row groups) so a store
    written before the column existed transparently keeps the CASE fallback.
    Any unreadable footer answers "no": the fallback is always correct, just
    slower — the same never-less-correct degradation contract ``partscan`` uses
    when it cannot confirm the ascending layout.
    """
    if sample_file is None:
        return False
    try:
        return EFFECTIVE_TIME_COL in pq.ParquetFile(sample_file).schema_arrow.names
    except Exception:
        return False


def _time_expr_for(data_type: str, stored: bool) -> str:
    """The SQL expression a time predicate / ORDER BY should use."""
    if data_type != "individual_stock":
        # The other types never blank Execution Time, so it is already a stored,
        # prunable column for them.
        return '"Execution Time"'
    return f'"{EFFECTIVE_TIME_COL}"' if stored else _EFFECTIVE_TIME_CASE


def _time_literal(hhmmss: str, stored: bool, data_type: str) -> str:
    """Render a ``"HHMMSS"`` bound to match the column the predicate compares.

    The materialized column is ``Int32`` HHMMSS; the string forms compare
    lexicographically (equivalent for fixed-width digits).
    """
    if stored and data_type == "individual_stock":
        return str(int(hhmmss))
    return f"'{hhmmss}'"


def _select_clause(col_select: str, stored: bool) -> str:
    """Keep the materialized key out of the projection.

    ``Effective Time`` is an internal index, not data: individual_stock's output
    is a locked 95 columns, and a whole-day query with no time filter would
    otherwise pay to read a column it never uses.
    """
    if col_select == "*" and stored:
        return f'* EXCLUDE ("{EFFECTIVE_TIME_COL}")'
    return col_select


def _drop_effective_time(df: pl.DataFrame) -> pl.DataFrame:
    """Drop the stored key from a frame read outside the SQL builders."""
    if EFFECTIVE_TIME_COL in df.columns:
        return df.drop(EFFECTIVE_TIME_COL)
    return df


def _duckdb_connect() -> "duckdb.DuckDBPyConnection":
    """An in-memory DuckDB connection tuned for large, ordered scans.

    Two best-effort session settings (an old DuckDB missing either still works):

    * ``preserve_insertion_order = false`` — the two structured query builders
      (:func:`query_ticks`, :func:`_query_extract_batch`) always impose an explicit
      ``ORDER BY``, so not preserving DuckDB's input order changes nothing they
      promise (the within-same-timestamp tick tie order is already
      non-deterministic — see PR #45), while letting the engine avoid buffering the
      whole result just to keep insertion order, which lowers peak memory on a big
      ``limit=None`` scan.
    * ``temp_directory`` — an in-memory DuckDB spills large sorts to a ``.tmp/``
      folder **in the caller's working directory** by default; a whole-store
      ``query_ticks`` was observed dumping 31 GB there and orphaning it when
      interrupted (audit finding B3). The system temp dir is the right home."""
    con = duckdb.connect()
    try:
        con.execute("SET preserve_insertion_order = false")
        spill = Path(tempfile.gettempdir()) / "tse_tick_duckdb_spill"
        spill.mkdir(parents=True, exist_ok=True)
        escaped = str(spill).replace("'", "''")
        con.execute(f"SET temp_directory = '{escaped}'")
    except Exception:
        pass
    return con


def _execute_to_polars(con: "duckdb.DuckDBPyConnection", sql: str) -> pl.DataFrame:
    """Run ``sql`` and return a Polars frame, converting a DuckDB out-of-memory
    failure into a catchable :class:`QueryMemoryError`.

    DuckDB raises ``duckdb.OutOfMemoryException`` — which is *not* a
    :class:`MemoryError` subclass, and whose message advises engine settings
    (``SET threads=…`` / ``SET memory_limit=…``) the caller cannot reach through
    this API — when it cannot materialize a result. For ``query_ticks`` that is a
    ``limit=None`` scan of a multi-year active ticker whose assembled frame
    overflows RAM at the Arrow conversion. Re-raise it (and a bare
    :class:`MemoryError` from the host) as :class:`QueryMemoryError`, which carries
    tse_tick's own slice-the-store remedy and lets callers ``except MemoryError``
    uniformly across the read and query paths. Other DuckDB errors propagate
    unchanged."""
    try:
        return con.execute(sql).pl()
    except (duckdb.OutOfMemoryException, MemoryError) as exc:
        raise QueryMemoryError(
            f"{_QUERY_MEMORY_GUIDANCE} (underlying error: {type(exc).__name__}: {exc})"
        ) from exc


def _resolve_type_dir(data_dir: str, data_type: str) -> Path:
    resolved = Path(data_dir).resolve()
    type_dir = (resolved / data_type).resolve()
    if not str(type_dir).startswith(str(resolved)):
        raise ValueError(f"Path traversal detected in data_dir: {data_dir!r}")
    if not type_dir.exists():
        raise FileNotFoundError(
            f"No Parquet store for {data_type!r} under {data_dir!r}. "
            f"query_ticks / get_available_dates / get_available_tickers read a "
            f"built Parquet store, not raw NEEDS files — run "
            f"ingest_period(...) or ingest_year_from_root(...) first. To discover "
            f"codes straight from raw data, read the period with read_ticks (no "
            f"ticker_filter) and inspect the 'Stock Code' / 'Index Code' column."
        )
    return type_dir


def _validate_identifier(name: str) -> None:
    if not name or any(c in _FORBIDDEN_IDENTIFIER_CHARS or ord(c) < 0x20 for c in name):
        raise ValueError(f"Invalid identifier: {name!r}")


def _validate_date(date_str: str) -> None:
    if not re.match(r"^\d{8}$", date_str):
        raise ValueError(f"Invalid date format (expected YYYYMMDD): {date_str!r}")


def _date_range_bounds(date_str: str) -> "tuple[str, str]":
    """Inclusive ``(lo, hi)`` ``YYYYMMDD`` bounds for a flexible ``date=`` argument.

    Accepts the same forms ``read_ticks`` / ``ingest_period`` do — ``YYYY`` /
    ``YYYYMM`` / ``YYYYMMDD`` / ``start-end`` — by delegating to
    :func:`tse_tick.parse_period`, so the accepted syntax and the error messages
    stay identical across the read and query paths (report B3). The bounds map onto
    the store's Hive ``date`` partition: a lone day gives ``lo == hi``; a month or
    year widens to that unit's first/last calendar slot (``01``..``31`` /
    ``0101``..``1231`` — safe inclusive upper bounds, since no ``YYYYMMDD`` exceeds
    them)."""
    info = parse_period(date_str)
    granularity = cast(str, info["granularity"])
    if granularity == "date":
        dates = cast(List[str], info["dates"])  # ascending & contiguous; a day is [d]
        return dates[0], dates[-1]
    if granularity == "month":
        months_by_year = cast(Dict[int, List[int]], info["months_by_year"])
        pairs = [(y, m) for y, months in months_by_year.items() for m in months]
        (lo_y, lo_m), (hi_y, hi_m) = min(pairs), max(pairs)
        return f"{lo_y}{lo_m:02d}01", f"{hi_y}{hi_m:02d}31"
    years = cast(List[int], info["years"])  # granularity == "year"
    return f"{min(years)}0101", f"{max(years)}1231"


def _validate_time(time_str: str) -> None:
    if not re.match(r"^\d{2}:\d{2}:\d{2}$", time_str):
        raise ValueError(f"Invalid time format (expected HH:MM:SS): {time_str!r}")


def _normalize_ticker(ticker: Union[int, str]) -> str:
    """Normalize an int/str ticker to the code token used in ``ticker=….parquet``.

    Accepts an ``int`` (e.g. ``7203``) or an alphanumeric ``str`` code (e.g.
    ``"7203"`` or ``"130A"``) and returns the bare token. Anything else raises a
    ``ValueError`` naming the expected type. The token is interpolated into a
    filename glob, so non-alphanumeric input (glob/path metacharacters) is
    rejected rather than silently globbed.
    """
    if isinstance(ticker, bool) or not isinstance(ticker, (int, str)):
        raise ValueError(f"Invalid ticker (expected int or str code): {ticker!r}")
    token = str(ticker).strip()
    if not re.fullmatch(r"[A-Za-z0-9]+", token):
        raise ValueError(
            f"Invalid ticker (expected an alphanumeric stock/index code): {ticker!r}"
        )
    return token


def _warn_query_no_data(data_type: str, ticker, date) -> None:
    """Emit the same capturable ``NoDataWarning`` :func:`read_ticks` does when a
    store query resolves to zero rows — a date not in the store, a code never
    ingested, or filters that exclude every row — so the two documented read
    paths signal "no data" the same way. ``stacklevel=3`` reports the caller's
    ``query_ticks(...)`` line (helper -> query_ticks -> user)."""
    warnings.warn(
        f"query_ticks: 0 rows for the requested filters (data_type={data_type!r}, "
        f"ticker={ticker!r}, date={date!r}). Possible causes: a date not in the "
        f"store, a ticker/index code never ingested, or time filters that exclude "
        f"every row.",
        NoDataWarning,
        stacklevel=3,
    )


def _drop_partition_ticker_column(df: pl.DataFrame) -> pl.DataFrame:
    """Drop a stray ``ticker`` column if the query surfaced one.

    The store encodes the code in the Parquet *filename* (``ticker=NNNN.parquet``)
    and the date in the directory. Modern DuckDB (the ``duckdb>=1.1.0`` floor)
    exposes only the directory key (``date``) as a Hive column, but older DuckDB
    also derived a ``ticker`` column from the filename and leaked it into the
    result. No NEEDS output schema has a literal ``ticker`` column (codes are
    ``Stock Code`` / ``Index Code``), so dropping it is always safe and keeps the
    store path robust across DuckDB versions regardless of the declared floor
    (report A2)."""
    return df.drop("ticker") if "ticker" in df.columns else df


def query_ticks(
    data_dir: str,
    data_type: str = "individual_stock",
    ticker: Optional[Union[int, str]] = None,
    date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    columns: Optional[list[str]] = None,
    limit: Optional[int] = _MAX_QUERY_ROWS,
) -> pl.DataFrame:
    """Query a **pre-built Parquet store** for ticks, with optional filters.

    Reads the Hive-partitioned Parquet store produced by the ``ingest_*``
    functions; it does **not** read raw ZIPs (use :func:`tse_tick.create_df` or
    :func:`tse_tick.read_ticks` for those). Requires the ``[query]`` extra —
    DuckDB — installed via ``pip install tse-tick[query]``.

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        data_type: One of ``"individual_stock"``, ``"stock_summary"``,
            ``"indices"``, ``"indices_summary"`` (a :class:`DataType` works too).
        ticker: Stock code (``individual_stock``) or index code (``indices``) as
            ``int`` or ``str`` — e.g. ``7203`` or ``"7203"``; ``None`` for all.
            A 4-char stock code selects its whole share-class family (``"7203"``
            also returns New Shares ``72031``, matching what a filtered ingest
            stores for it); a 5-char code reads exactly that share class.
        date: A day ``"YYYYMMDD"``, month ``"YYYYMM"``, year ``"YYYY"``, or a
            ``"start-end"`` range — the same flexible forms :func:`read_ticks` and
            ``ingest_period`` accept, matched against the store's ``date``
            partition. ``None`` for every stored date.
        start_time: Inclusive lower bound on time-of-day (``"HH:MM:SS"``). For
            ``individual_stock``, quote-only rows (blank ``Execution Time``) are
            matched on their ``Update Time`` instead, so an in-window order book
            is kept whole rather than reduced to trade rows.
        end_time: Inclusive upper bound on time-of-day (``"HH:MM:SS"``).
        columns: Column projection; ``None`` selects all columns.
        limit: Maximum rows returned (default 10,000,000); ``None`` for no cap.
            When more rows match than the cap the result is truncated and a
            :class:`TruncationWarning` is emitted (capturable via ``warnings``); a
            result that exactly fills the cap with nothing dropped does **not** warn.
            Pass a larger ``limit=`` or ``limit=None`` for the full result. Note that
            ``limit=None`` over a multi-year range of an active ticker can assemble a
            frame far larger than RAM (Toyota 7203 for 2017–2019 is ~136M rows × 95
            cols ≈ 100 GB) — see *Raises*.

    Returns:
        A Polars DataFrame ordered by ``Data Date`` then ``Execution Time``
        (empty if no file matches the requested ``ticker``). It includes an extra
        ``date`` partition column (``i64`` ``YYYYMMDD``) that Hive partitioning
        derives from the ``date=`` directory, so this store path returns one more
        column than the one-shot :func:`tse_tick.read_ticks`.

    Raises:
        QueryMemoryError: If the result is too large to materialize as one in-memory
            DataFrame (a :class:`MemoryError` subclass, so ``except MemoryError``
            catches both this and the read path's :class:`OneShotMemoryError`). The
            built store is unaffected — read it back in bounded slices instead (narrow
            ``date=`` to a month or day, pass a smaller ``limit=``, or loop per day /
            per month). Replaces DuckDB's raw ``OutOfMemoryException``, whose engine
            hints are not reachable through this API.

    Example:
        >>> df = query_ticks(store, data_type=DataType.INDIVIDUAL_STOCK,
        ...                  ticker=7203, date="20240201",
        ...                  start_time="09:00:00", end_time="11:30:00")
    """
    validate_data_type(data_type)
    validate_time_filter_support(data_type, start_time, end_time)

    type_dir = _resolve_type_dir(data_dir, data_type)

    if columns:
        for c in columns:
            _validate_identifier(c)
        col_select = ", ".join(f'"{c}"' for c in columns)
    else:
        col_select = "*"

    # The ticker is encoded in the Parquet *filename* (ticker=NNNN.parquet),
    # which Hive partitioning does not expose as a column (it derives columns
    # only from directory names, e.g. date=YYYYMMDD). Prune by selecting the
    # matching per-ticker files directly. This is also robust to the in-file
    # code column being categorically decoded (e.g. Index Code "101" -> "Nikkei
    # 225"), which would defeat a value-based filter.
    summary = data_type in SUMMARY_TYPES
    code_condition: Optional[str] = None
    # One file of the selected set answers whether this store carries the
    # materialized time key (issue #65); every file of a store is written by the
    # same writer, so any of them is representative.
    sample_file: Optional[str] = None
    if ticker is not None:
        ticker_token = _normalize_ticker(ticker)
        if summary:
            # Summary stores partition by date only (no per-ticker files), so the
            # code lives in a column — prune it instead, matching on its first 4
            # chars to mirror the old ticker= partition value. An unknown code then
            # yields a typed-empty frame naturally (the SELECT keeps the schema).
            code_col = "Index Code" if data_type == "indices_summary" else "Stock Code"
            glob_pattern = str(type_dir / "**" / "*.parquet").replace("\\", "/")
            source = f"'{glob_pattern}'"
            code_condition = f"substr(\"{code_col}\", 1, 4) = '{ticker_token}'"
        else:
            # Family matching: a 4-char code also selects its suffixed share
            # classes' files (ticker=72031.parquet for "7203") — the same rows
            # Stage 1 ingests for that request. A longer code stays an exact
            # match, so a single share class remains directly addressable.
            ticker_files = sorted(
                str(p).replace("\\", "/")
                for p in type_dir.glob(f"**/ticker={ticker_token}*.parquet")
                if _code_matches_family(p.stem[len("ticker="):], ticker_token)
            )
            if not ticker_files:
                # Unknown ticker: return the store schema with 0 rows so chained
                # column access doesn't raise (instead of a schemaless (0, 0) frame).
                _warn_query_no_data(data_type, ticker, date)
                any_file = next(type_dir.glob("**/*.parquet"), None)
                if any_file is None:
                    return pl.DataFrame()
                # The stored time key is not part of the output schema, so the
                # typed-empty frame must not expose it either.
                empty = _drop_effective_time(pl.read_parquet(any_file, n_rows=0))
                if columns:
                    empty = empty.select([c for c in columns if c in empty.columns])
                return empty
            source = "[" + ", ".join(f"'{f}'" for f in ticker_files) + "]"
            sample_file = ticker_files[0]
    else:
        glob_pattern = str(type_dir / "**" / "*.parquet").replace("\\", "/")
        source = f"'{glob_pattern}'"
        any_file = next(type_dir.glob("**/*.parquet"), None)
        sample_file = str(any_file) if any_file is not None else None

    # individual_stock quote-only book rows have a blank Execution Time but a real
    # Update Time (stored "HHMMSSssssss"); fall back to its first 6 chars so a time
    # window keeps in-window order-book rows, not just trade-coincident snapshots
    # (~94% of a liquid day is quotes). Scoped to individual_stock: the other types
    # have no Update Time column (and indices' Execution Time is never blank).
    # Stores written since #65 materialize that value as an Int32 column, so the
    # predicate hits row-group statistics instead of a per-row CASE; older stores
    # fall back to the CASE and keep working unchanged.
    stored_time = data_type == "individual_stock" and _store_has_effective_time(sample_file)

    base_conditions: list[str] = []
    if date is not None:
        lo, hi = _date_range_bounds(date)
        base_conditions.append(
            f"date = '{lo}'" if lo == hi else f"date >= '{lo}' AND date <= '{hi}'"
        )
    if code_condition is not None:
        base_conditions.append(code_condition)
    if start_time is not None:
        _validate_time(start_time)
    if end_time is not None:
        _validate_time(end_time)

    # Fetch one row beyond the cap so an exact-fit result (height == limit, nothing
    # dropped) isn't mistaken for truncation — only a genuine overflow warns
    # (alpha-review finding 7).
    limit_clause = f"LIMIT {limit + 1}" if limit is not None else ""

    def _build(stored: bool, exclude_key: bool, union: bool) -> str:
        time_expr = _time_expr_for(data_type, stored)
        conditions = list(base_conditions)
        # Execution Time is stored as a 6-digit "HHMMSS" string; the public API
        # accepts validated "HH:MM:SS" values, so strip the colons before the
        # comparison so it matches the stored format (the materialized key
        # compares as the equivalent Int32).
        if start_time is not None:
            lit = _time_literal(start_time.replace(":", ""), stored, data_type)
            conditions.append(f"{time_expr} >= {lit}")
        if end_time is not None:
            lit = _time_literal(end_time.replace(":", ""), stored, data_type)
            conditions.append(f"{time_expr} <= {lit}")
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        # Summary types are daily aggregates with no "Execution Time" column, so
        # order only by the columns this data_type actually has (a hard-coded
        # ORDER BY "Execution Time" makes query_ticks unusable for both
        # summaries). individual_stock orders by the same effective time so quote
        # rows interleave chronologically rather than all sorting first on a
        # blank Execution Time.
        order_cols = ['"Data Date"']
        if data_type == "individual_stock":
            order_cols.append(time_expr)
        elif data_type == "indices":
            order_cols.append('"Execution Time"')
        union_arg = ", union_by_name=true" if union else ""
        return (
            f"SELECT {_select_clause(col_select, exclude_key)} "
            f"FROM read_parquet({source}, hive_partitioning=true{union_arg}) "
            f"{where_clause} "
            f"ORDER BY {', '.join(order_cols)} "
            f"{limit_clause}"
        )

    con = _duckdb_connect()
    try:
        try:
            df = _execute_to_polars(con, _build(stored_time, stored_time, False))
        except duckdb.InvalidInputException:
            # A store can be MIXED: resume ingests new dates (which carry the
            # key) into a store whose older dates predate it, and DuckDB rejects
            # a file list whose first file has a column a later one lacks. Only
            # the fast path can trip this — retry on the CASE, unioning the
            # schemas by name so the key is simply null on the older files (the
            # CASE never reads it) and EXCLUDE-ing it from the output. Correct,
            # just unaccelerated; re-ingest the older dates to get the speedup.
            if not stored_time:
                raise
            df = _execute_to_polars(con, _build(False, True, True))
    finally:
        con.close()

    # Defensive: keep the store path robust to a DuckDB that derives a `ticker`
    # column from the ticker=NNNN.parquet filename (older versions did — report A2).
    df = _drop_partition_ticker_column(df)

    # We fetched limit+1 above; if more than `limit` rows came back the result was
    # truncated — trim to `limit` and surface the same capturable TruncationWarning
    # read_ticks emits on its row cap, so callers learn rows were dropped instead of
    # silently receiving a partial frame. An exact-fit result (nothing dropped) does
    # not warn.
    if limit is not None and df.height > limit:
        df = df.head(limit)
        warnings.warn(
            f"Result truncated at {limit} rows. Pass a larger limit= or use "
            f"limit=None for all rows.",
            TruncationWarning,
            stacklevel=2,
        )

    # Same "no data" signal read_ticks emits — a date not in the store, a code
    # never ingested, or filters that excluded every row. (The unknown-ticker
    # early return above already warned, so it won't reach here.)
    if df.height == 0:
        _warn_query_no_data(data_type, ticker, date)

    return df


def export_query(
    data_dir: str,
    output_path: str,
    data_type: str = "individual_stock",
    ticker: Optional[Union[int, str]] = None,
    date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    columns: Optional[list[str]] = None,
    compression: str = "zstd",
    overwrite: bool = False,
) -> Dict[str, object]:
    """Stream a **pre-built Parquet store** slice to a single Parquet file, without
    ever holding the whole result in memory.

    The memory-safe counterpart to :func:`query_ticks` for results too large to
    assemble as one in-memory DataFrame: where ``query_ticks(..., limit=None)`` over
    a multi-year active ticker raises :class:`QueryMemoryError` (~100 GB for Toyota
    7203 / 2017–2019), this walks the store's ``date=`` partitions **in order** and
    appends each stored day as a Parquet row group, so peak memory stays ~one trading
    day regardless of period length. It reuses :func:`query_ticks` per day, so the
    written rows are identical to concatenating ``query_ticks(..., limit=None)`` over
    the same slice (modulo the already non-deterministic same-timestamp tick tie
    order — see PR #45). Requires the ``[query]`` extra (DuckDB).

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        output_path: Destination ``.parquet`` file. Parent directories are created.
        data_type: One of the four NEEDS types (see :func:`query_ticks`).
        ticker: Stock/index code (``int`` or ``str``) with the same family semantics
            as :func:`query_ticks` (a 4-char code exports its whole share-class
            family); ``None`` exports every code.
        date: A day / month / year / ``start-end`` range (the flexible forms
            :func:`query_ticks` accepts); ``None`` exports every stored date.
        start_time: Inclusive ``"HH:MM:SS"`` lower bound (tick types only).
        end_time: Inclusive ``"HH:MM:SS"`` upper bound (tick types only).
        columns: Column projection; ``None`` writes all columns.
        compression: Parquet codec for the output file (default ``"zstd"``).
        overwrite: If ``output_path`` already exists, replace it only when ``True``;
            otherwise raise :class:`FileExistsError` (so an export can't silently
            clobber a file).

    Returns:
        A small manifest ``dict`` — ``{"path", "rows", "dates", "data_type",
        "ticker"}`` — where ``rows`` is the total rows written and ``dates`` the
        number of stored days that contributed rows. It deliberately does **not**
        return the data, so a huge export stays memory-bounded.

    Raises:
        FileExistsError: If ``output_path`` exists and ``overwrite`` is ``False``.

    Example:
        >>> export_query(store, "toyota_2017_2019.parquet",
        ...              data_type="individual_stock", ticker=7203, date="2017-2019")
        {'path': 'toyota_2017_2019.parquet', 'rows': 136436016, 'dates': 733, ...}
    """
    validate_data_type(data_type)
    validate_time_filter_support(data_type, start_time, end_time)
    _resolve_type_dir(data_dir, data_type)  # store-exists + path-traversal validation

    out_path = Path(output_path)
    if out_path.exists() and not overwrite:
        raise FileExistsError(
            f"{output_path!r} already exists; pass overwrite=True to replace it."
        )
    out_path.parent.mkdir(parents=True, exist_ok=True)

    lo, hi = _date_range_bounds(date) if date is not None else (None, None)
    days = [
        d
        for d in get_available_dates(data_dir, data_type)
        if (lo is None or d >= lo) and (hi is None or d <= hi)
    ]

    writer: Optional["pq.ParquetWriter"] = None
    total_rows = 0
    dates_written = 0
    try:
        # Per-day queries on days a filtered ticker never traded resolve to 0 rows and
        # would each emit a NoDataWarning; suppress those and warn once, below, only if
        # the whole export is empty — matching query_ticks's single "no data" signal.
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", NoDataWarning)
            for day in days:
                frame = query_ticks(
                    data_dir,
                    data_type=data_type,
                    ticker=ticker,
                    date=day,
                    start_time=start_time,
                    end_time=end_time,
                    columns=columns,
                    limit=None,
                )
                if frame.height == 0:
                    continue
                table = frame.to_arrow()
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(out_path), table.schema, compression=compression
                    )
                elif table.schema != writer.schema:
                    # Defensive: the store's per-day schema is stable, but align an
                    # all-null column that a single day might type differently.
                    table = table.cast(writer.schema)
                writer.write_table(table)
                total_rows += frame.height
                dates_written += 1
    finally:
        if writer is not None:
            writer.close()

    if writer is None:
        # Nothing matched across the range (unknown code, a period with no stored
        # days, or filters that excluded every row). Write query_ticks's typed-empty
        # frame so the output keeps the full schema, and let its single NoDataWarning
        # surface — the same "no data" signal both read paths emit.
        empty_table = query_ticks(
            data_dir,
            data_type=data_type,
            ticker=ticker,
            date=date,
            start_time=start_time,
            end_time=end_time,
            columns=columns,
            limit=None,
        ).to_arrow()
        empty_writer = pq.ParquetWriter(
            str(out_path), empty_table.schema, compression=compression
        )
        try:
            empty_writer.write_table(empty_table)
        finally:
            empty_writer.close()

    return {
        "path": str(out_path),
        "rows": total_rows,
        "dates": dates_written,
        "data_type": data_type,
        "ticker": ticker,
    }


def _query_extract_batch(
    data_dir: str,
    data_type: str,
    tickers,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
) -> pl.DataFrame:
    """Byte-identical, lower-overhead replacement for concatenating
    ``query_ticks(..., limit=None)`` over ``sorted(tickers)`` — the
    :func:`tse_tick.extract_to_store` Stage-2 query without the per-ticker N+1 (issue #44).

    A single DuckDB connection and a single scan replace the per-ticker loop's fresh
    connection + whole-store glob + scan + Arrow->Polars conversion *per ticker* (and, for
    the two ``*_summary`` types, N full-store scans). Every ticker's file list is built from
    ONE store walk.

    The result is the identical **multiset** of rows in the same ``(code, Data Date,
    effective-time)`` order as the old loop, with the same columns (incl. the Hive ``date``)
    and the same all-absent return shape per type (tick: 0 rows *without* ``date``; summary:
    0 rows *with* ``date``). The ``*_summary`` types are daily aggregates (one row per
    (code, date)), so their order is total and deterministic — byte-identical. For the tick
    types the order WITHIN a same-(date, time) tie is arbitrary, exactly as in the loop it
    replaces: both order via DuckDB, whose parallel sort does not fix a tie order, so the
    current per-ticker ``query_ticks`` already returns a run-dependent within-tie order on
    real data (two runs differ only in the position of same-timestamp rows).

    Fixed to ``limit=None`` — extract_to_store's mode. Do NOT use for general
    :func:`query_ticks`, whose finite ``limit`` is a per-call total, not a whole-result cap.

    ``date_from`` / ``date_to`` are inclusive ``YYYYMMDD`` bounds: extract_to_store
    passes its period's bounds so a REUSED store returns exactly the requested
    period, not every stored day (audit finding B5). For the tick types the bounds
    prune the per-ticker file list (each file is one date); for the date-partitioned
    summary types they become a SQL condition on the Hive ``date`` column.
    """
    validate_data_type(data_type)
    validate_time_filter_support(data_type, start_time, end_time)
    type_dir = _resolve_type_dir(data_dir, data_type)

    # sorted(tickers) is the concat block order of the old loop; normalise each code the
    # same way query_ticks does (so the per-ticker file glob / IN-list match exactly).
    ordered_codes = [_normalize_ticker(t) for t in sorted(tickers)]
    summary = data_type in SUMMARY_TYPES

    if date_from is not None:
        _validate_date(date_from)
    if date_to is not None:
        _validate_date(date_to)

    if summary:
        # Summary stores partition by date only; the code lives in a column. One scan of
        # the whole store with an IN-list over the first 4 chars replaces the per-ticker
        # loop's N full-store scans. Summary types are daily aggregates (one row per
        # (code, date)), so ORDER BY that same substr then Data Date is a *total* order
        # that reproduces the per-ticker block order exactly — no ties to break. An
        # all-absent request yields a 0-row frame that keeps the schema (incl. date).
        code_col = "Index Code" if data_type == "indices_summary" else "Stock Code"
        code_sql = f'substr("{code_col}", 1, 4)'
        in_list = ", ".join(f"'{c}'" for c in ordered_codes)
        # validate_time_filter_support above rejects start_time/end_time for the
        # summary types, so there is never a time condition to carry here.
        conds = [f"{code_sql} IN ({in_list})"]
        # Summary partitions are one file per date with the Hive `date` column
        # carrying YYYYMMDD — bound it to the requested period (B5).
        if date_from is not None:
            conds.append(f"date >= '{date_from}'")
        if date_to is not None:
            conds.append(f"date <= '{date_to}'")
        glob_pattern = str(type_dir / "**" / "*.parquet").replace("\\", "/")
        sql = (
            f"SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true) "
            f"WHERE {' AND '.join(conds)} "
            f'ORDER BY {code_sql}, "Data Date"'
        )
        con = _duckdb_connect()
        try:
            summary_result = _execute_to_polars(con, sql)
        finally:
            con.close()
        return _drop_partition_ticker_column(summary_result)

    # Tick types: ONE scan over all the requested tickers' files (the file list is built
    # from a SINGLE store walk, vs the loop's fresh connection + whole-store glob per
    # ticker — the dominant cost on a large reused store). Tag each row with the code from
    # its filename and ORDER BY (code, Data Date, effective time) — the same ordering the
    # per-ticker loop produces.
    #
    # The row order WITHIN a same-(date, time) tick tie is arbitrary here, exactly as in
    # the loop it replaces: both rely on DuckDB's ORDER BY, whose parallel sort does not
    # fix a tie order. The current per-ticker query_ticks is itself non-deterministic on
    # real data — two runs of the same query differ only in the position of same-timestamp
    # rows (verified: 16 of ~912k rows for one ticker-month). So this returns the identical
    # multiset in the identical (code, date, time) order; only that already-nondeterministic
    # within-tie order may differ. `EXCLUDE (filename)` keeps the columns byte-identical
    # (file columns + the Hive `date`), with no `filename`/`_code` artifact.
    files_by_code: dict[str, list[str]] = {}
    for p in type_dir.glob("**/ticker=*.parquet"):
        # Each per-ticker file holds exactly one date (its date= parent dir), so
        # the period bounds prune here — a reused store's out-of-period days are
        # never even opened (B5).
        if date_from is not None or date_to is not None:
            fdate = p.parent.name[len("date="):]
            if (date_from is not None and fdate < date_from) or (
                date_to is not None and fdate > date_to
            ):
                continue
        files_by_code.setdefault(p.stem[len("ticker="):], []).append(
            str(p).replace("\\", "/")
        )
    files: list[str] = []
    seen: set[str] = set()
    for code in ordered_codes:
        # Family expansion: a 4-char code also collects its suffixed share-class
        # files (Stage 1 ingests the whole family for such a request; keying on
        # the exact stem silently dropped those rows). Dedupe defends a caller
        # passing overlapping codes.
        matched = sorted(
            f
            for stem, stem_files in files_by_code.items()
            if _code_matches_family(stem, code)
            for f in stem_files
        )
        for f in matched:
            if f not in seen:
                seen.add(f)
                files.append(f)
    if not files:
        # Every requested ticker absent -> mirror query_ticks's tick all-absent shape
        # (store schema, 0 rows, WITHOUT the Hive date column).
        any_file = next(type_dir.glob("**/*.parquet"), None)
        if any_file is None:
            return pl.DataFrame()
        # The stored time key is an internal index, not part of the output schema.
        return _drop_effective_time(pl.read_parquet(any_file, n_rows=0))

    # Effective time — identical resolution to query_ticks: the materialized Int32
    # column on stores written since #65, the CASE fallback on older ones.
    stored_time = data_type == "individual_stock" and _store_has_effective_time(files[0])

    if start_time is not None:
        _validate_time(start_time)
    if end_time is not None:
        _validate_time(end_time)

    source = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    # Order by the 4-char FAMILY root of the filename code: a family (7203 +
    # 72031) is one requested block, ordered by (date, time) within — exactly
    # what a per-ticker query_ticks("7203") returns for it. Ordering by the full
    # stem would split the family into per-class blocks.
    code_sql = "substr(regexp_extract(filename, 'ticker=([A-Za-z0-9]+)\\.parquet', 1), 1, 4)"

    def _build(stored: bool, exclude_key: bool, union: bool) -> str:
        time_expr = _time_expr_for(data_type, stored)
        conditions: list[str] = []
        if start_time is not None:
            lit = _time_literal(start_time.replace(":", ""), stored, data_type)
            conditions.append(f"{time_expr} >= {lit}")
        if end_time is not None:
            lit = _time_literal(end_time.replace(":", ""), stored, data_type)
            conditions.append(f"{time_expr} <= {lit}")
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        order_cols = [code_sql, '"Data Date"']
        if data_type == "individual_stock":
            order_cols.append(time_expr)
        elif data_type == "indices":
            order_cols.append('"Execution Time"')
        # `filename` drives the code ordering and the stored key drives the time
        # ordering; neither is output — EXCLUDE keeps the columns byte-identical
        # to the pre-#65 result.
        excluded = "filename" + (f', "{EFFECTIVE_TIME_COL}"' if exclude_key else "")
        union_arg = ", union_by_name=true" if union else ""
        return (
            f"SELECT * EXCLUDE ({excluded}) "
            f"FROM read_parquet({source}, hive_partitioning=true, filename=true{union_arg}) "
            f"{where_clause} "
            f"ORDER BY {', '.join(order_cols)}"
        )

    con = _duckdb_connect()
    try:
        try:
            tick_result = _execute_to_polars(con, _build(stored_time, stored_time, False))
        except duckdb.InvalidInputException:
            # Mixed store (older dates predate the key) — see query_ticks.
            if not stored_time:
                raise
            tick_result = _execute_to_polars(con, _build(False, True, True))
    finally:
        con.close()
    return _drop_partition_ticker_column(tick_result)


def query_sql(
    data_dir: str,
    sql: str,
    data_type: str = "individual_stock",
) -> pl.DataFrame:
    """Execute arbitrary SQL against the partitioned Parquet store via DuckDB.

    WARNING: This is a privileged escape hatch. The SQL is passed directly
    to DuckDB with no sanitization. Only use with trusted SQL input.
    Destructive operations (DROP, DELETE, ALTER) will affect the in-memory
    database only but could still cause denial of service.

    Before running ``sql``, a DuckDB view named ``ticks`` is registered
    backed by the glob ``data_dir/{data_type}/**/*.parquet``.
    """
    type_dir = _resolve_type_dir(data_dir, data_type)

    glob_pattern = str(type_dir / "**" / "*.parquet").replace("\\", "/")

    con = _duckdb_connect()
    try:
        con.execute(
            f"CREATE VIEW ticks AS "
            f"SELECT * FROM read_parquet('{glob_pattern}', hive_partitioning=true)"
        )
        df = con.execute(sql).pl()
    finally:
        con.close()

    return df


def get_available_dates(
    data_dir: str,
    data_type: str = "individual_stock",
) -> list[str]:
    """List the trading days present in a Parquet store.

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        data_type: Which store to inspect (see :func:`query_ticks`).

    Returns:
        Sorted ``"YYYYMMDD"`` date strings (from the ``date=`` partition dirs).
    """
    type_dir = _resolve_type_dir(data_dir, data_type)

    dates = []
    for entry in type_dir.iterdir():
        # A date dir holding only the ingest coverage marker (a filtered day on
        # which the ticker never traded) has no data — not a trading day here.
        if (
            entry.is_dir()
            and entry.name.startswith("date=")
            and next(entry.glob("*.parquet"), None) is not None
        ):
            dates.append(entry.name[5:])

    return sorted(dates)


def get_available_tickers(
    data_dir: str,
    data_type: str = "individual_stock",
    date: Optional[str] = None,
) -> list[str]:
    """List the ticker/index codes present in a Parquet store.

    Args:
        data_dir: Store root: the directory that contains ``<data_type>/``.
        data_type: Which store to inspect (see :func:`query_ticks`).
        date: Restrict to a day ``"YYYYMMDD"``, month ``"YYYYMM"``, year ``"YYYY"``,
            or a ``"start-end"`` range (the same flexible forms :func:`query_ticks`
            and :func:`read_ticks` accept); ``None`` scans all days.

    Returns:
        Sorted **string** codes (e.g. ``["6758", "7203", "9984"]``) — ready to
        pass straight to ``read_ticks(ticker_filter=...)`` with no conversion.
        For the tick types they come from the ``ticker=CODE.parquet`` filenames;
        for the date-only summary stores they are read from the in-file code
        column. Strings (not ints) so modern alphanumeric codes (e.g. ``"130A"``)
        are preserved instead of silently dropped; pure-digit codes sort
        numerically, ahead of any alphanumeric ones.
    """
    type_dir = _resolve_type_dir(data_dir, data_type)

    all_date_dirs = [
        d for d in type_dir.iterdir() if d.is_dir() and d.name.startswith("date=")
    ]
    if date is None:
        date_dirs = all_date_dirs
    else:
        lo, hi = _date_range_bounds(date)
        date_dirs = [d for d in all_date_dirs if lo <= d.name[len("date=") :] <= hi]

    def _sort_key(code: str):
        # Pure-digit codes sort numerically ("9984" < "10000"); alphanumeric
        # codes sort lexically, after all the numeric ones.
        return (0, int(code)) if code.isdigit() else (1, code)

    if data_type in SUMMARY_TYPES:
        # Summary stores partition by date only, so codes live in a column rather
        # than per-ticker filenames; read it (first 4 chars, mirroring the
        # partition value the tick types encode in the filename).
        code_col = "Index Code" if data_type == "indices_summary" else "Stock Code"
        codes: set[str] = set()
        for date_dir in date_dirs:
            if not date_dir.exists():
                continue
            for fpath in date_dir.glob("*.parquet"):
                try:
                    series = pl.read_parquet(fpath, columns=[code_col]).to_series()
                except Exception:
                    continue
                codes.update(
                    str(v).strip()[:4] for v in series.unique().to_list() if v is not None
                )
        return sorted(codes, key=_sort_key)

    tickers: set[str] = set()
    prefix = "ticker="
    for date_dir in date_dirs:
        if not date_dir.exists():
            continue
        for fpath in date_dir.iterdir():
            if fpath.suffix == ".parquet" and fpath.stem.startswith(prefix):
                tickers.add(fpath.stem[len(prefix):])
    return sorted(tickers, key=_sort_key)
