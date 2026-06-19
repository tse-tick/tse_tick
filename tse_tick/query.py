# tse_tick/query.py
import re
from pathlib import Path
from typing import Optional, Union

import polars as pl
import duckdb

from .constants import SUMMARY_TYPES, validate_data_type

# Column names are interpolated as double-quoted identifiers (f'"{c}"'), so the
# only injection risk is a character that closes the quote (") or escapes it
# (\), plus statement terminators / control chars. NEEDS column names legitimately
# contain spaces (e.g. "Execution Time"), so a strict word-only regex would reject
# every real column; block the dangerous characters instead.
_FORBIDDEN_IDENTIFIER_CHARS = frozenset('"\\;`\r\n\t\x00')
_MAX_QUERY_ROWS = 10_000_000


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
        date: Trading day as ``"YYYYMMDD"``; ``None`` for every stored date.
        start_time: Inclusive lower bound on time-of-day (``"HH:MM:SS"``). For
            ``individual_stock``, quote-only rows (blank ``Execution Time``) are
            matched on their ``Update Time`` instead, so an in-window order book
            is kept whole rather than reduced to trade rows.
        end_time: Inclusive upper bound on time-of-day (``"HH:MM:SS"``).
        columns: Column projection; ``None`` selects all columns.
        limit: Maximum rows returned (default 10,000,000); ``None`` for no cap.

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
            ticker_files = sorted(
                str(p).replace("\\", "/")
                for p in type_dir.glob(f"**/ticker={ticker_token}.parquet")
            )
            if not ticker_files:
                # Unknown ticker: return the store schema with 0 rows so chained
                # column access doesn't raise (instead of a schemaless (0, 0) frame).
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
        _validate_date(date)
        conditions.append(f"date = '{date}'")
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

    limit_clause = f"LIMIT {limit}" if limit is not None else ""

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

    con = duckdb.connect()
    try:
        df = con.execute(sql).pl()
    finally:
        con.close()

    return df


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

    con = duckdb.connect()
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
        if entry.is_dir() and entry.name.startswith("date="):
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
        date: Restrict to a single ``"YYYYMMDD"`` day; ``None`` scans all days.

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

    date_dirs = (
        [type_dir / f"date={date}"]
        if date is not None
        else [d for d in type_dir.iterdir() if d.is_dir() and d.name.startswith("date=")]
    )

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
