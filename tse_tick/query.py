# tse_tick/query.py
import re
from pathlib import Path
from typing import Optional

import polars as pl
import duckdb

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
        raise FileNotFoundError(f"No Parquet store for {data_type!r} under {data_dir!r}")
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


def query_ticks(
    data_dir: str,
    data_type: str = "individual_stock",
    ticker: Optional[int] = None,
    date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    columns: Optional[list[str]] = None,
    limit: Optional[int] = _MAX_QUERY_ROWS,
) -> pl.DataFrame:
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if data_type not in valid_types:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(valid_types)}"
        )

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
    if ticker is not None:
        ticker_files = sorted(
            str(p).replace("\\", "/")
            for p in type_dir.glob(f"**/ticker={ticker}.parquet")
        )
        if not ticker_files:
            return pl.DataFrame()
        source = "[" + ", ".join(f"'{f}'" for f in ticker_files) + "]"
    else:
        glob_pattern = str(type_dir / "**" / "*.parquet").replace("\\", "/")
        source = f"'{glob_pattern}'"

    conditions: list[str] = []
    if date is not None:
        _validate_date(date)
        conditions.append(f"date = '{date}'")
    # Execution Time is stored as a 6-digit "HHMMSS" string; the public API
    # accepts validated "HH:MM:SS" values, so strip the colons before the
    # lexicographic comparison so it matches the stored format.
    if start_time is not None:
        _validate_time(start_time)
        conditions.append(f'"Execution Time" >= \'{start_time.replace(":", "")}\'')
    if end_time is not None:
        _validate_time(end_time)
        conditions.append(f'"Execution Time" <= \'{end_time.replace(":", "")}\'')

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    limit_clause = f"LIMIT {limit}" if limit is not None else ""

    sql = (
        f"SELECT {col_select} "
        f"FROM read_parquet({source}, hive_partitioning=true) "
        f"{where_clause} "
        f'ORDER BY "Data Date", "Execution Time" '
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
) -> list[int]:
    type_dir = _resolve_type_dir(data_dir, data_type)

    tickers: set[int] = set()

    date_dirs = (
        [type_dir / f"date={date}"]
        if date is not None
        else [d for d in type_dir.iterdir() if d.is_dir() and d.name.startswith("date=")]
    )

    for date_dir in date_dirs:
        if not date_dir.exists():
            continue
        for fpath in date_dir.iterdir():
            if fpath.suffix == ".parquet" and fpath.stem.startswith("ticker="):
                try:
                    tickers.add(int(fpath.stem[7:]))
                except ValueError:
                    pass

    return sorted(tickers)
