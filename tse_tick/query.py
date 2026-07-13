# tse_tick/query.py
import re
import tempfile
import warnings
from pathlib import Path
from typing import Dict, List, Optional, Union, cast

import polars as pl
import duckdb

from .constants import SUMMARY_TYPES, validate_data_type, validate_time_filter_support
from .enhanced import NoDataWarning, TruncationWarning, _code_matches_family, parse_period

# Column names are interpolated as double-quoted identifiers (f'"{c}"'), so the
# only injection risk is a character that closes the quote (") or escapes it
# (\), plus statement terminators / control chars. NEEDS column names legitimately
# contain spaces (e.g. "Execution Time"), so a strict word-only regex would reject
# every real column; block the dangerous characters instead.
_FORBIDDEN_IDENTIFIER_CHARS = frozenset('"\\;`\r\n\t\x00')
_MAX_QUERY_ROWS = 10_000_000


def _duckdb_connect() -> "duckdb.DuckDBPyConnection":
    """An in-memory DuckDB connection with its spill directory pointed at the
    system temp dir.

    An in-memory DuckDB spills large sorts to a ``.tmp/`` folder **in the
    caller's working directory** by default — a whole-store ``query_ticks``
    was observed dumping 31 GB there and leaving it orphaned when interrupted
    (audit finding B3). The system temp dir is the right home for scratch
    files; configuring it is best-effort (an old DuckDB without the setting
    still works, just with its default spill location)."""
    con = duckdb.connect()
    try:
        spill = Path(tempfile.gettempdir()) / "tse_tick_duckdb_spill"
        spill.mkdir(parents=True, exist_ok=True)
        escaped = str(spill).replace("'", "''")
        con.execute(f"SET temp_directory = '{escaped}'")
    except Exception:
        pass
    return con


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
            Pass a larger ``limit=`` or ``limit=None`` for the full result.

    Returns:
        A Polars DataFrame ordered by ``Data Date`` then ``Execution Time``
        (empty if no file matches the requested ``ticker``). It includes an extra
        ``date`` partition column (``i64`` ``YYYYMMDD``) that Hive partitioning
        derives from the ``date=`` directory, so this store path returns one more
        column than the one-shot :func:`tse_tick.read_ticks`.

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
                empty = pl.read_parquet(any_file, n_rows=0)
                if columns:
                    empty = empty.select([c for c in columns if c in empty.columns])
                return empty
            source = "[" + ", ".join(f"'{f}'" for f in ticker_files) + "]"
    else:
        glob_pattern = str(type_dir / "**" / "*.parquet").replace("\\", "/")
        source = f"'{glob_pattern}'"

    # individual_stock quote-only book rows have a blank Execution Time but a real
    # Update Time (stored "HHMMSSssssss"); fall back to its first 6 chars so a time
    # window keeps in-window order-book rows, not just trade-coincident snapshots
    # (~94% of a liquid day is quotes). Scoped to individual_stock: the other types
    # have no Update Time column (and indices' Execution Time is never blank).
    if data_type == "individual_stock":
        time_expr = (
            'CASE WHEN "Execution Time" IS NULL OR "Execution Time" = \'\' '
            'THEN substr("Update Time", 1, 6) ELSE "Execution Time" END'
        )
    else:
        time_expr = '"Execution Time"'

    conditions: list[str] = []
    if date is not None:
        lo, hi = _date_range_bounds(date)
        conditions.append(
            f"date = '{lo}'" if lo == hi else f"date >= '{lo}' AND date <= '{hi}'"
        )
    if code_condition is not None:
        conditions.append(code_condition)
    # Execution Time is stored as a 6-digit "HHMMSS" string; the public API
    # accepts validated "HH:MM:SS" values, so strip the colons before the
    # lexicographic comparison so it matches the stored format.
    if start_time is not None:
        _validate_time(start_time)
        conditions.append(f"{time_expr} >= '{start_time.replace(':', '')}'")
    if end_time is not None:
        _validate_time(end_time)
        conditions.append(f"{time_expr} <= '{end_time.replace(':', '')}'")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # Fetch one row beyond the cap so an exact-fit result (height == limit, nothing
    # dropped) isn't mistaken for truncation — only a genuine overflow warns
    # (alpha-review finding 7).
    limit_clause = f"LIMIT {limit + 1}" if limit is not None else ""

    # Summary types are daily aggregates with no "Execution Time" column, so order
    # only by the columns this data_type actually has (a hard-coded ORDER BY
    # "Execution Time" makes query_ticks unusable for both summaries).
    # individual_stock orders by the same effective time so quote rows interleave
    # chronologically rather than all sorting first on a blank Execution Time.
    order_cols = ['"Data Date"']
    if data_type == "individual_stock":
        order_cols.append(time_expr)
    elif data_type == "indices":
        order_cols.append('"Execution Time"')
    order_clause = "ORDER BY " + ", ".join(order_cols)

    sql = (
        f"SELECT {col_select} "
        f"FROM read_parquet({source}, hive_partitioning=true) "
        f"{where_clause} "
        f"{order_clause} "
        f"{limit_clause}"
    )

    con = _duckdb_connect()
    try:
        df = con.execute(sql).pl()
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

    # Effective time column — individual_stock falls back to Update Time for quote-only
    # rows (blank Execution Time); identical to query_ticks.
    if data_type == "individual_stock":
        time_expr = (
            'CASE WHEN "Execution Time" IS NULL OR "Execution Time" = \'\' '
            'THEN substr("Update Time", 1, 6) ELSE "Execution Time" END'
        )
    else:
        time_expr = '"Execution Time"'

    if date_from is not None:
        _validate_date(date_from)
    if date_to is not None:
        _validate_date(date_to)

    conditions: list[str] = []
    if start_time is not None:
        _validate_time(start_time)
        conditions.append(f"{time_expr} >= '{start_time.replace(':', '')}'")
    if end_time is not None:
        _validate_time(end_time)
        conditions.append(f"{time_expr} <= '{end_time.replace(':', '')}'")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

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
        conds = conditions + [f"{code_sql} IN ({in_list})"]
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
            summary_result = con.execute(sql).pl()
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
        return pl.read_parquet(any_file, n_rows=0)

    source = "[" + ", ".join(f"'{f}'" for f in files) + "]"
    # Order by the 4-char FAMILY root of the filename code: a family (7203 +
    # 72031) is one requested block, ordered by (date, time) within — exactly
    # what a per-ticker query_ticks("7203") returns for it. Ordering by the full
    # stem would split the family into per-class blocks.
    code_sql = "substr(regexp_extract(filename, 'ticker=([A-Za-z0-9]+)\\.parquet', 1), 1, 4)"
    order_cols = [code_sql, '"Data Date"']
    if data_type == "individual_stock":
        order_cols.append(time_expr)
    elif data_type == "indices":
        order_cols.append('"Execution Time"')
    sql = (
        f"SELECT * EXCLUDE (filename) "
        f"FROM read_parquet({source}, hive_partitioning=true, filename=true) "
        f"{where_clause} "
        f"ORDER BY {', '.join(order_cols)}"
    )
    con = _duckdb_connect()
    try:
        tick_result = con.execute(sql).pl()
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
