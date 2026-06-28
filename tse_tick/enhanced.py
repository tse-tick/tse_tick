# tse_tick/enhanced.py
import datetime
import gc
import re
import zipfile
import io
import glob
import logging
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Optional, Literal, Tuple, List, Dict, Union

import polars as pl

from .core import clean_data, parse_line, _tick_datetime_expr
from .constants import INDEX_TYPES, SUMMARY_TYPES, validate_data_type
from .schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_15,
    get_schema_indices_23,
    get_japanese_column_mapping,
)

logger = logging.getLogger(__name__)


class NoDataWarning(UserWarning):
    """Warned when a read resolves to zero rows (a typed-empty frame is returned).

    Emitted by :func:`read_ticks` whenever nothing matches — a non-trading day
    (e.g. a holiday), a ticker/index code not present, or filters that exclude
    every row — for all four data types alike. Being a ``UserWarning`` subclass
    it is capturable with ``warnings.catch_warnings(record=True)`` and silenceable
    with ``warnings.filterwarnings("ignore", category=tse_tick.NoDataWarning)``.
    """


def _warn_no_data(message: str) -> None:
    # stacklevel=3: report the user's read_ticks(...) call site, not this helper
    # or its internal caller.
    warnings.warn(message, NoDataWarning, stacklevel=3)


class TruncationWarning(UserWarning):
    """Warned when :func:`read_ticks` hits its ``rows`` cap and truncates output.

    A ``UserWarning`` subclass so it surfaces through the same ``warnings``
    channel as :class:`NoDataWarning` (capturable with
    ``warnings.catch_warnings()``, silenceable / escalatable by category) rather
    than via ``logging`` — hitting the cap means "build a Parquet store and use
    :func:`tse_tick.query_ticks` instead".
    """


_MAX_DECOMPRESSED_BYTES = 5 * 1024 * 1024 * 1024
_MAX_ZIP_ENTRIES = 5

# Cumulative decompressed-size ceiling for the one-shot (create_df) path. The
# per-entry _MAX_DECOMPRESSED_BYTES guard above is checked per ZIP *member*, so it
# can't see memory adding up across the many numbered parts of one trading day (a
# normal individual_stock day is ~9 parts / tens of millions of rows). When the
# running total of decompressed bytes crosses this ceiling we raise a catchable
# MemoryError *before* the load rather than letting Polars panic uncatchably
# (PanicException subclasses BaseException). Default 5 GB; override by setting
# tse_tick.enhanced._MAX_ONESHOT_DECOMPRESSED_BYTES.
_MAX_ONESHOT_DECOMPRESSED_BYTES = 5 * 1024 * 1024 * 1024

# Shared tail for the one-shot OOM guidance: the two-stage escape hatch.
_TWO_STAGE_GUIDANCE = (
    "Use the two-stage path instead: ingest_single_zip() then query_ticks()."
)

_CODE_TYPE_MAP = {
    "individual_stock": "HTICST120",
    "stock_summary": "HTICSS110",
    "indices": "HTICIT110",
    "indices_summary": "HTICIS110",
}


def _expand_date_range(from_date: str, to_date: str) -> List[str]:
    start = datetime.datetime.strptime(from_date, "%Y%m%d")
    end = datetime.datetime.strptime(to_date, "%Y%m%d")
    if start > end:
        raise ValueError(f"Start date {from_date} is after end date {to_date}")
    dates = []
    current = start
    while current <= end:
        dates.append(current.strftime("%Y%m%d"))
        current += datetime.timedelta(days=1)
    return dates


def _expand_month_range(from_str: str, to_str: str) -> List[Tuple[int, int]]:
    from_year, from_month = int(from_str[:4]), int(from_str[4:6])
    to_year, to_month = int(to_str[:4]), int(to_str[4:6])
    if (from_year, from_month) > (to_year, to_month):
        raise ValueError(f"Start month {from_str} is after end month {to_str}")
    months = []
    year, month = from_year, from_month
    while (year < to_year) or (year == to_year and month <= to_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months


# Single source of truth for the accepted period forms, so every parse_period
# error message lists the same complete set (incl. the bare YYYYMM / YYYYMMDD
# forms the code accepts and read_ticks documents).
_PERIOD_FORMATS_HELP = (
    "Expected YYYY, a single YYYYMM or YYYYMMDD, or a "
    "YYYYMM-YYYYMM / YYYYMMDD-YYYYMMDD range"
)


def parse_period(period_str: str) -> Dict[str, Union[str, List[int], Dict[int, List[int]], List[str]]]:
    """Parse a period string into structured parameters.

    Accepted formats:
        YYYY                         -> entire year
        YYYYMM                       -> a single month
        YYYYMMDD                     -> a single trading day
        YYYYMM-YYYYMM                -> all trading days from start month to end month
        YYYYMMDD-YYYYMMDD            -> all trading days from start date to end date

    The single ``YYYYMM`` and ``YYYYMMDD`` forms are the same ones
    ``read_ticks(date=…)`` accepts — a lone month or day needs no ``start-end``
    range.

    Returns dict with keys:
        granularity: "year" | "month" | "date"
        years: list of years covered
        months_by_year (month): {year: [month, ...]}
        dates (date): list of YYYYMMDD strings
    """
    period_str = str(period_str).strip()

    if "-" in period_str:
        parts = period_str.split("-")
        if len(parts) != 2:
            raise ValueError(f"Invalid period format: {period_str!r}. {_PERIOD_FORMATS_HELP}")
        from_part, to_part = parts[0].strip(), parts[1].strip()

        if len(from_part) == 8 and len(to_part) == 8 and from_part.isdigit() and to_part.isdigit():
            dates = _expand_date_range(from_part, to_part)
            years = sorted(set(int(d[:4]) for d in dates))
            return {"granularity": "date", "years": years, "dates": dates}

        elif len(from_part) == 6 and len(to_part) == 6 and from_part.isdigit() and to_part.isdigit():
            months = _expand_month_range(from_part, to_part)
            months_by_year: Dict[int, List[int]] = defaultdict(list)
            for y, m in months:
                months_by_year[y].append(m)
            years = sorted(months_by_year.keys())
            return {"granularity": "month", "years": years, "months_by_year": dict(months_by_year)}

        else:
            raise ValueError(f"Invalid period format: {period_str!r}. {_PERIOD_FORMATS_HELP}")
    else:
        if len(period_str) == 4 and period_str.isdigit():
            return {"granularity": "year", "years": [int(period_str)]}
        elif len(period_str) == 6 and period_str.isdigit():
            y, m = int(period_str[:4]), int(period_str[4:6])
            return {"granularity": "month", "years": [y], "months_by_year": {y: [m]}}
        elif len(period_str) == 8 and period_str.isdigit():
            return {"granularity": "date", "years": [int(period_str[:4])], "dates": [period_str]}
        else:
            raise ValueError(f"Invalid period: {period_str!r}. {_PERIOD_FORMATS_HELP}")


def _detect_year_from_path(folder_path: str) -> Optional[int]:
    """The first ``20xx`` year token found in the path parts, or ``None``."""
    for part in Path(folder_path).parts:
        match = re.search(r"(20\d{2})", part)
        if match:
            return int(match.group(1))
    return None


def _detect_data_type_from_path(folder_path: str) -> str:
    """Detect the NEEDS ``data_type`` from path keywords, or a sample filename.

    Raises ``ValueError`` (with the same messages :func:`detect_data_type_and_year`
    has always used) when the type can't be determined.
    """
    path = Path(folder_path)
    path_str = str(path).lower()

    if any(kw in path_str for kw in ["individual_stock", "ticst", "stock_tick"]):
        return "individual_stock"
    elif any(kw in path_str for kw in ["stock_summary", "ticss", "stock_daily"]):
        return "stock_summary"
    elif any(kw in path_str for kw in ["indices_tick", "ticit", "index_tick"]) and "summary" not in path_str:
        return "indices"
    elif any(kw in path_str for kw in ["indices_summary", "ticis", "index_daily", "index_summary"]):
        return "indices_summary"

    if path.exists() and path.is_dir():
        files = list(path.glob("*.zip")) + list(path.glob("*.csv"))
        if files:
            sample_file = files[0].name.upper()
            if "TICST" in sample_file:
                return "individual_stock"
            elif "TICSS" in sample_file:
                return "stock_summary"
            elif "TICIT" in sample_file:
                return "indices"
            elif "TICIS" in sample_file:
                return "indices_summary"
            raise ValueError(f"Could not detect data type from files in: {folder_path}")
        raise ValueError(f"No ZIP or CSV files found in: {folder_path}")
    raise ValueError(f"Could not detect data type from path: {folder_path}")


def detect_data_type_and_year(folder_path: str) -> Tuple[str, int]:
    """Detect both the NEEDS ``data_type`` and ``year`` from a path.

    Year comes from a ``20xx`` token in the path; data type from path keywords or
    a sample filename. Raises ``ValueError`` when either can't be determined (year
    is reported first, preserving the original behavior). :func:`create_df` calls
    the two underlying detectors independently so an explicitly-passed
    ``year=``/``data_type=`` is honored without forcing the other to be detectable.
    """
    year = _detect_year_from_path(folder_path)
    if year is None:
        raise ValueError(f"Could not detect year from path: {folder_path}")
    data_type = _detect_data_type_from_path(folder_path)
    return data_type, year


def _zip_date_token(name: str) -> Optional[str]:
    """The YYYYMMDD (daily) or YYYYMM (monthly) date token in a NEEDS filename."""
    for part in name.split("."):
        if part.isdigit() and part.startswith("20") and len(part) in (6, 8):
            return part
    return None


def _zip_sort_key(path) -> Tuple[str, int]:
    """Chronological natural sort key: (date token, part number) from the filename.

    Sorts ``…20240104.1.zip``, ``…20240104.2.zip``, ``…20240104.10.zip`` in numeric
    order rather than lexical (1, 10, 2, …).
    """
    parts = Path(path).name.split(".")
    date_tok, part_num = "", 0
    for i, p in enumerate(parts):
        if p.isdigit() and p.startswith("20") and len(p) in (6, 8):
            date_tok = p
            if i + 1 < len(parts) and parts[i + 1].isdigit():
                part_num = int(parts[i + 1])
            break
    return (date_tok, part_num)


def _discover_zips_recursive(
    root: Path,
    prefixes: Union[str, List[str]],
    years: List[int],
    months: Optional[List[int]],
    dates: Optional[List[str]],
) -> List[Path]:
    """Fallback discovery: recursively find ``<PREFIX>.*.zip`` anywhere under
    ``root`` (robust to nested delivery trees such as
    ``個別株式{year}/TICST120/{yyyymm}/`` and the 2016 ``…010`` index code),
    filtered by the requested year/month/date tokens parsed from each filename.
    """
    if isinstance(prefixes, str):  # tolerate a single prefix
        prefixes = [prefixes]
    year_set = {str(y) for y in years}
    month_set = {f"{m:02d}" for m in (months if months is not None else range(1, 13))}
    date_set = set(dates) if dates else None
    out: List[Path] = []
    for prefix in prefixes:
        for p in glob.glob(str(root / "**" / f"{prefix}.*.zip"), recursive=True):
            tok = _zip_date_token(Path(p).name)
            if tok is None or tok[:4] not in year_set or tok[4:6] not in month_set:
                continue
            # Daily files must match a requested date; monthly files match by month.
            if date_set is not None and len(tok) == 8 and tok not in date_set:
                continue
            out.append(Path(p))
    return sorted(out)


def discover_zips(
    input_root: str,
    data_type: str,
    years: List[int],
    months: Optional[List[int]] = None,
    dates: Optional[List[str]] = None,
) -> List[Path]:
    """Find NEEDS ZIPs for the requested year/month/date under ``input_root``.

    Tries two fast-path layouts — the documented ``{year}/{yearmonth}/`` tree and
    a ``{yearmonth}/`` directory directly under ``input_root`` (e.g. when it
    already points at a ``.../TICST120`` type folder) — then, if neither matched,
    falls back to a recursive search that handles deeper real-world delivery
    trees such as ``個別株式{year}/TICST120/{yyyymm}/``. Uses the data type's
    record prefix (e.g. ``HTICST120`` for ``individual_stock``).

    Args:
        input_root: Root of (or a folder above) the NEEDS delivery tree.
        data_type: One of the four NEEDS types (selects the filename prefix).
        years: Years to scan.
        months: Months (1-12) to scan; ``None`` means all twelve.
        dates: Restrict to these exact ``"YYYYMMDD"`` days; ``None`` for all.

    Returns:
        A sorted list of matching ZIP paths (``pathlib.Path``).
    """
    prefix = _CODE_TYPE_MAP.get(data_type)
    if prefix is None:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {list(_CODE_TYPE_MAP.keys())}"
        )
    # Index types changed record code across eras: 2017+ uses …110, 2016 uses
    # …010 (HTICIT010 / HTICIS010). Search both so 2016 index data is reachable.
    prefixes = [prefix]
    if data_type in INDEX_TYPES:
        prefixes.append(prefix[:-3] + "010")

    if months is None:
        months = list(range(1, 13))

    root = Path(input_root)
    all_zips: List[Path] = []

    for year in years:
        for month in months:
            month_str = f"{year}{month:02d}"
            # Two fast-path layouts: the documented {year}/{yearmonth}/ tree, and
            # a {yearmonth}/ folder directly under root (e.g. input_root already
            # points at a .../TICST120 type folder, as in G:\NEEDS\個別株式2023\TICST120).
            subdirs = [Path(str(year)) / month_str, Path(month_str)]
            targets = [d for d in dates if d[:6] == month_str] if dates is not None else [None]
            for sub in subdirs:
                for date_str in targets:
                    for pfx in prefixes:
                        fname = f"{pfx}.{date_str}.*.zip" if date_str else f"{pfx}.*.zip"
                        all_zips.extend(Path(p) for p in glob.glob(str(root / sub / fname)))

    # The fast paths cover the common deliveries; if none matched, fall back to a
    # full recursive search so deeper nested trees still work
    # (e.g. 個別株式{year}/TICST120/{yyyymm}/).
    if not all_zips:
        all_zips = _discover_zips_recursive(root, prefixes, years, months, dates)

    # Dedupe (a file can match more than one fast-path subdir) and natural-sort.
    seen: set = set()
    unique: List[Path] = []
    for z in all_zips:
        if str(z) not in seen:
            seen.add(str(z))
            unique.append(z)
    return sorted(unique, key=_zip_sort_key)


def _raw_width(kind: str, year: int) -> int:
    """Expected raw (pre-clean) column count for a NEEDS data type/era."""
    if kind == "indices":
        return 15 if year == 2016 else 23
    if kind in SUMMARY_TYPES:
        return 83
    return 95  # individual_stock


def _guard_polars_oom(load):
    """Run a Polars load, converting an *uncatchable* panic into a catchable error.

    A failed allocation on a huge one-shot read surfaces as a Polars
    ``PanicException``, which subclasses ``BaseException`` (not ``Exception``) — so
    ordinary ``except Exception`` can't catch it and it tears the caller down. Run
    ``load`` and convert any such non-``Exception`` ``BaseException`` into a
    ``MemoryError`` carrying the two-stage guidance; ordinary exceptions and real
    interrupts (``KeyboardInterrupt`` / ``SystemExit``) pass through untouched.
    """
    try:
        return load()
    except (Exception, KeyboardInterrupt, SystemExit):
        raise
    except BaseException as exc:  # PanicException & co. — not an Exception subclass
        raise MemoryError(
            f"Loading this data exhausted memory (Polars panicked). {_TWO_STAGE_GUIDANCE}"
        ) from exc


def get_1y_dataframe(
    folder_path: str,
    year: int,
    kind: str,
    rows: Optional[int] = None,
    ticker_filter: Optional[set] = None,
) -> pl.DataFrame:
    path = Path(folder_path)

    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {folder_path}")

    if path.is_file() and path.suffix.lower() == ".zip":
        zip_files = [path]
    elif path.is_dir():
        zip_files = sorted(path.glob("*.zip"), key=_zip_sort_key)
    else:
        raise ValueError(
            f"Path must be either a directory containing ZIP files or a ZIP file: {folder_path}"
        )

    if not zip_files:
        raise FileNotFoundError(f"No ZIP files found in: {folder_path}")

    logger.debug("Found %d ZIP file(s) in %s", len(zip_files), folder_path)

    dfs = []
    total_rows_read = 0
    cumulative_decompressed = 0

    schema_override = {f"column_{col+1}": pl.String for col in range(95)}

    for zip_file in zip_files:
        try:
            with zipfile.ZipFile(zip_file, "r") as zf:
                if len(zf.namelist()) > _MAX_ZIP_ENTRIES:
                    raise ValueError(
                        f"ZIP has {len(zf.namelist())} entries, max {_MAX_ZIP_ENTRIES}"
                    )
                file_name = zf.namelist()[0]
                info = zf.getinfo(file_name)
                decompressed_size = info.file_size
                compressed_size = info.compress_size
                if compressed_size > 0 and decompressed_size / compressed_size > 100:
                    raise ValueError(
                        f"Suspicious compression ratio ({decompressed_size / compressed_size:.0f}:1) "
                        f"in {zip_file}"
                    )
                if decompressed_size > _MAX_DECOMPRESSED_BYTES:
                    raise ValueError(
                        f"ZIP entry decompressed size ({decompressed_size:,} bytes) "
                        f"exceeds max ({_MAX_DECOMPRESSED_BYTES:,} bytes)"
                    )
                # Cumulative across parts: the per-entry guard above can't see
                # memory accumulating over a day's many numbered ZIPs. Stop with a
                # clear, catchable error *before* the load rather than letting the
                # concat OOM-panic uncatchably.
                cumulative_decompressed += decompressed_size
                if cumulative_decompressed > _MAX_ONESHOT_DECOMPRESSED_BYTES:
                    raise MemoryError(
                        f"Estimated decompressed size "
                        f"({cumulative_decompressed / 1024**3:.1f} GB) exceeds the "
                        f"{_MAX_ONESHOT_DECOMPRESSED_BYTES / 1024**3:.0f} GB one-shot "
                        f"limit. {_TWO_STAGE_GUIDANCE}"
                    )
                with zf.open(file_name) as f:
                    rows_to_read = None
                    if rows is not None:
                        remaining_rows = rows - total_rows_read
                        if remaining_rows <= 0:
                            break
                        rows_to_read = remaining_rows

                    if (year == 2016) and (kind == "indices_summary"):
                        parsed_rows = []
                        n_lines = 0
                        for line in f:
                            if rows_to_read is not None and n_lines >= rows_to_read:
                                break
                            parsed_rows.append(parse_line(line))
                            n_lines += 1
                        df_chunk = pl.DataFrame(parsed_rows)

                    elif (year == 2016) and (kind == "indices"):
                        parsed_rows = []
                        n_lines = 0
                        for line in f:
                            if rows_to_read is not None and n_lines >= rows_to_read:
                                break
                            parsed_rows.append(parse_line(line, kind="indices"))
                            n_lines += 1
                        df_chunk = pl.DataFrame(parsed_rows)

                    elif ticker_filter and kind == "individual_stock":
                        kept_lines = []
                        for raw_line in f:
                            pos = 0
                            for _ in range(5):
                                idx = raw_line.find(b'","', pos)
                                if idx == -1:
                                    break
                                pos = idx + 3
                            else:
                                end = raw_line.find(b'"', pos)
                                if end != -1:
                                    stock_code = raw_line[pos:end].strip()[:4].decode("ascii")
                                    if stock_code in ticker_filter:
                                        kept_lines.append(raw_line)

                        if kept_lines:
                            raw_bytes = b"".join(kept_lines)
                            df_chunk = _guard_polars_oom(
                                lambda: pl.read_csv(
                                    io.BytesIO(raw_bytes),
                                    has_header=False,
                                    schema_overrides=schema_override,
                                    truncate_ragged_lines=True,
                                )
                            )
                            if rows_to_read is not None:
                                df_chunk = df_chunk.slice(0, rows_to_read)
                        else:
                            df_chunk = pl.DataFrame()

                    else:
                        df_chunk = _guard_polars_oom(
                            lambda: pl.read_csv(
                                f,
                                has_header=False,
                                schema_overrides=schema_override,
                                truncate_ragged_lines=True,
                            )
                        )
                        if rows_to_read is not None:
                            df_chunk = df_chunk.slice(0, rows_to_read)

                    if not df_chunk.is_empty():
                        dfs.append(df_chunk)
                        total_rows_read += len(df_chunk)

                    if rows is not None and total_rows_read >= rows:
                        break

        except (zipfile.BadZipFile, EOFError, MemoryError):
            # MemoryError is the cumulative one-shot guard (and converted Polars
            # panics) — propagate it instead of swallowing it as a skipped part.
            raise
        except Exception as e:
            logger.warning("Error reading %s: %s", zip_file, e)
            continue

    if not dfs:
        if ticker_filter:
            # No matching rows: return a 0-row frame of the correct raw width so
            # the cleaning pipeline still yields a fully-typed empty result.
            return pl.DataFrame(
                schema={f"column_{i + 1}": pl.String for i in range(_raw_width(kind, year))}
            )
        raise ValueError("No data was successfully read")

    result = _guard_polars_oom(lambda: pl.concat(dfs, how="vertical"))
    logger.debug("Total rows read: %d", len(result))

    return result


def set_columns(df: pl.DataFrame, kind: str, language: Literal["en", "jp"] = "en") -> pl.DataFrame:
    if kind == "individual_stock":
        if len(df.columns) == 23:
            col_names_en = get_schema_indices_23()
        elif len(df.columns) == 95:
            col_names_en = get_schema_individual_stock_95()
        else:
            raise ValueError(f"Unexpected number of columns for {kind}: {len(df.columns)}")
    elif kind in SUMMARY_TYPES:
        if len(df.columns) == 83:
            col_names_en = get_schema_summary_83()
            if kind == "indices_summary":
                # The shared 83-column summary layout names column 5
                # "Stock Code", but for index summary data that field holds
                # the index identifier; get_final_columns() and the Parquet
                # partition key select it as "Index Code".
                col_names_en = [
                    "Index Code" if c == "Stock Code" else c for c in col_names_en
                ]
        else:
            raise ValueError(
                f"Unexpected number of columns for {kind}: {len(df.columns)}, expected 83"
            )
    elif kind == "indices":
        if len(df.columns) == 23:
            col_names_en = get_schema_indices_23()
        elif len(df.columns) == 15:
            col_names_en = get_schema_indices_15()
        else:
            raise ValueError(
                f"Unexpected number of columns for {kind}: {len(df.columns)}, expected 15 or 23"
            )
    else:
        raise ValueError(f"Unknown kind: {kind}")

    if len(df.columns) != len(col_names_en):
        raise ValueError(
            f"Column count mismatch: DataFrame has {len(df.columns)} columns but schema has {len(col_names_en)}"
        )

    rename_map = dict(zip(df.columns, col_names_en))
    df = df.rename(rename_map)

    if language == "jp":
        jp_mapping = get_japanese_column_mapping()
        col_names_jp = [jp_mapping.get(col, col) for col in col_names_en]
        rename_jp = dict(zip(col_names_en, col_names_jp))
        df = df.rename(rename_jp)

    return df


def get_final_columns(data_type):
    if data_type == "indices_summary":
        return [
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Index Code",
            "AM Opening Price", "AM Opening Time", "AM High Price", "AM Low Price",
            "AM Close Price", "AM Close Time", "PM Opening Price", "PM Opening Time",
            "PM High Price", "PM Low Price", "PM Close Price", "PM Close Time",
        ]
    elif data_type == "indices":
        return [
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Session",
            "Index Code", "Execution Time", "Index Value", "Execution Type", "Ayumi Flag",
        ]
    else:
        return [
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Stock Code",
            "Trading Unit", "Issued Shares", "Executions ≤3 units",
            "Executions 3<x≤6 units", "Executions 6<x≤9 units", "Executions 9<x≤29 units",
            "Executions 29<x≤49 units", "Executions 49<x≤99 units", "Executions 99<x≤199 units",
            "Executions 199<x≤299 units", "AM Opening Price", "AM Opening Time",
            "AM Opening Volume", "AM High Price", "AM Low Price", "AM Close Price",
            "AM Close Time", "AM Close Volume", "AM UpTick Volume", "AM UpTick Amount",
            "AM UpTick Count", "AM DownTick Volume", "AM DownTick Amount", "AM DownTick Count",
            "AM Total Volume", "AM Total Amount", "AM Execution Count", "AM VWAP", "AM Std Dev",
            "AM Sell Quote Time", "AM Buy Quote Time", "AM Spread Time", "AM Avg Sell Quote Vol",
            "AM Avg Buy Quote Vol", "AM Avg Spread", "PM Opening Price", "PM Opening Time",
            "PM Opening Volume", "PM High Price", "PM Low Price", "PM Close Price",
            "PM Close Time", "PM Close Volume", "PM UpTick Volume", "PM UpTick Amount",
            "PM UpTick Count", "PM DownTick Volume", "PM DownTick Amount", "PM DownTick Count",
            "PM Total Volume", "PM Total Amount", "PM Execution Count", "PM VWAP", "PM Std Dev",
            "PM Sell Quote Time", "PM Buy Quote Time", "PM Spread Time", "PM Avg Sell Quote Vol",
            "PM Avg Buy Quote Vol", "PM Avg Spread", "Daily VWAP", "Daily Std Dev",
            "Daily Weighted Avg Sell Quote", "Daily Weighted Avg Buy Quote", "Daily Avg Spread",
            "AM Sell Quote Execution Vol", "AM Sell Quote Execution Amt",
            "AM Sell Quote Execution Cnt", "AM Buy Quote Execution Vol",
            "AM Buy Quote Execution Amt", "AM Buy Quote Execution Cnt",
            "PM Sell Quote Execution Vol", "PM Sell Quote Execution Amt",
            "PM Sell Quote Execution Cnt", "PM Buy Quote Execution Vol",
            "PM Buy Quote Execution Amt", "PM Buy Quote Execution Cnt",
        ]


def _finalize_raw(
    df_raw: pl.DataFrame,
    data_type: str,
    language: Literal["en", "jp"] = "en",
) -> pl.DataFrame:
    """Name, clean, and project a raw NEEDS frame into the final cleaned frame.

    Shared by :func:`create_df` (populated frames) and :func:`_empty_typed_frame`
    (a 0-row frame), so a no-data read carries a schema byte-identical to a
    populated read instead of drifting from it.
    """
    df_with_columns = set_columns(df_raw, data_type, language)

    if language == "jp":
        jp_mapping = get_japanese_column_mapping()
        en_to_jp = {v: k for k, v in jp_mapping.items()}
        jp_cols = df_with_columns.columns
        en_cols = [en_to_jp.get(col, col) for col in jp_cols]
        df_with_columns = df_with_columns.rename(dict(zip(jp_cols, en_cols)))
        df_cleaned = clean_data(df_with_columns, data_type, language)
        df_cleaned = df_cleaned.rename(dict(zip(en_cols, jp_cols)))
        if data_type != "individual_stock":
            final_cols = get_final_columns(data_type)
            final_cols_jp = [jp_mapping.get(c, c) for c in final_cols]
            available = [c for c in final_cols_jp if c in df_cleaned.columns]
            return df_cleaned.select(available)
        return df_cleaned

    df_cleaned = clean_data(df_with_columns, data_type, language)
    if data_type != "individual_stock":
        final_cols = get_final_columns(data_type)
        available = [c for c in final_cols if c in df_cleaned.columns]
        return df_cleaned.select(available)
    return df_cleaned


def _empty_typed_frame(
    data_type: str,
    language: Literal["en", "jp"] = "en",
    year: Optional[int] = None,
) -> pl.DataFrame:
    """A 0-row frame whose schema matches :func:`create_df`'s output for
    ``data_type`` — so a "no ZIPs found" read returns full typed columns rather
    than a schemaless ``(0, 0)`` frame (consistent with the no-match path).
    """
    if year is None:
        year = datetime.datetime.now().year
    width = _raw_width(data_type, year)
    raw = pl.DataFrame(schema={f"column_{i + 1}": pl.String for i in range(width)})
    return _finalize_raw(raw, data_type, language)


def _year_hint_from_date(date: Optional[str]) -> Optional[int]:
    """A representative 4-digit year parsed from a date/period string, used only
    to size an empty frame; ``None`` when none is present."""
    if date is None:
        return None
    for tok in re.split(r"[-\s]+", str(date).strip()):
        if len(tok) >= 4 and tok[:4].isdigit() and tok.startswith("20"):
            return int(tok[:4])
    return None


def _normalize_ticker_filter(ticker_filter) -> Optional[set]:
    """Normalize ``ticker_filter`` to a set of string codes (or ``None``).

    A bare single code is an easy novice mistake: a ``str`` like ``"101"`` would be
    iterated into characters (``{'1', '0', '1'}`` — matching nothing, a silent
    empty result) and an ``int`` like ``101`` would raise ``TypeError`` when
    iterated. Treat a lone ``str``/``int`` as a one-element filter; otherwise
    coerce each item of the iterable to a stripped string.
    """
    if ticker_filter is None:
        return None
    if isinstance(ticker_filter, (str, int)):  # a single bare code, not an iterable of codes
        ticker_filter = {ticker_filter}
    return {str(t).strip() for t in ticker_filter}


def create_df(
    folder_path: str,
    language: Literal["en", "jp"] = "en",
    rows: Optional[int] = None,
    auto_detect: bool = True,
    data_type: Optional[str] = None,
    year: Optional[int] = None,
    ticker_filter: Optional[set] = None,
) -> pl.DataFrame:
    """Read raw NEEDS ZIP(s) into a cleaned Polars DataFrame.

    Loads a single ``.zip`` or every ``.zip`` in a directory, parses the
    headerless NEEDS CSV, applies column names and categorical decoding, and
    returns the cleaned frame. This reads **raw files** — no Parquet store.

    Args:
        folder_path: A ``.zip`` file, or a directory of NEEDS ZIPs.
        language: Output column-name language, ``"en"`` or ``"jp"`` (a
            :class:`Language` works too).
        rows: Optional cap on rows read (a fast sample of the first N).
        auto_detect: When ``True`` (default) detect ``data_type``/``year`` from
            the path — but only for whichever you leave as ``None``. An explicitly
            passed ``data_type=``/``year=`` is always honored (no longer
            overwritten by detection), so a correctly-named ZIP in a folder whose
            path has no year still reads when you pass ``year=``. When ``False``
            you **must** pass both ``data_type`` and ``year``.
        data_type: One of the four NEEDS types (a :class:`DataType` works too).
            Auto-detected from the path when omitted; required when
            ``auto_detect=False``.
        year: Selects era-specific parsing. Auto-detected from the path when
            omitted; required when ``auto_detect=False``.
        ticker_filter: A ``set`` of **string** stock codes (e.g. ``{"7203"}``)
            kept via the raw-byte fast path; a bare ``"7203"`` / ``7203`` is also
            accepted as a single code. Applied for ``individual_stock`` **only** —
            ignored for the other data types. Note a single numbered ZIP holds
            only part of a day's code range, so filtering a lone part may yield 0
            rows; pass the day's directory for complete coverage.

    Returns:
        The cleaned DataFrame (empty if ``ticker_filter`` matched no rows).
    """
    ticker_filter = _normalize_ticker_filter(ticker_filter)

    # Explicit year/data_type always win; auto-detection (when on) only fills in
    # whichever was left as None. This lets a correctly-named ZIP in a year-less
    # folder be read by passing year= even under the default auto_detect=True (it
    # previously raised "Could not detect year from path": alpha-test Bug #3).
    if auto_detect:
        if year is None:
            year = _detect_year_from_path(folder_path)
            if year is None:
                raise ValueError(f"Could not detect year from path: {folder_path}")
        if data_type is None:
            data_type = _detect_data_type_from_path(folder_path)
        logger.debug("Resolved: %s, Year: %s", data_type, year)
    else:
        if data_type is None or year is None:
            raise ValueError(
                "When auto_detect=False, data_type and year must be explicitly provided"
            )
        logger.debug("Manual: %s, Year: %s", data_type, year)

    df_raw = get_1y_dataframe(
        folder_path,
        year,
        data_type,
        rows,
        ticker_filter=ticker_filter if data_type == "individual_stock" else None,
    )

    df_final = _finalize_raw(df_raw, data_type, language)
    logger.debug("Data successfully created")
    return df_final


def export_to_csv(
    folder_path: str,
    output_path: Optional[str] = None,
    language: Literal["en", "jp"] = "en",
    rows: Optional[int] = None,
) -> str:
    """Read raw NEEDS ZIP(s) with :func:`create_df` and write the result to CSV.

    Convenience wrapper that cleans the **whole** file/directory and writes it
    out. There is **no** ``ticker_filter`` parameter — call :func:`create_df`
    (or :func:`tse_tick.read_ticks`) directly when you need ticker pruning.

    Args:
        folder_path: A ``.zip`` file, or a directory of NEEDS ZIPs.
        output_path: Destination ``.csv``; when ``None`` a name is derived as
            ``<data_type>_<year>_{en|jp}_cleaned.csv``.
        language: Output column-name language, ``"en"`` or ``"jp"``.
        rows: Optional cap on rows read.

    Returns:
        The path the CSV was written to.
    """
    df = create_df(folder_path, language, rows)

    if output_path is None:
        data_type, year = detect_data_type_and_year(folder_path)
        lang_suffix = "_jp" if language == "jp" else "_en"
        output_path = f"{data_type}_{year}{lang_suffix}_cleaned.csv"

    df.write_csv(output_path)
    logger.info("Data exported to: %s", output_path)
    logger.debug("Shape: %s", df.shape)

    return output_path


# --------------------------------------------------------------------------- #
# read_ticks — one-shot path: raw ZIPs -> ticker/time-filtered DataFrame
# --------------------------------------------------------------------------- #

def _zip_year(zip_path: Path) -> int:
    """Best-effort era year for a ZIP: the YYYY of the date token in its name."""
    for part in zip_path.name.split("."):
        if len(part) == 8 and part.isdigit() and part.startswith("20"):
            return int(part[:4])
    try:  # fall back to scanning the path (handles {year}/{yearmonth}/ roots)
        _, year = detect_data_type_and_year(str(zip_path))
        return year
    except Exception as exc:
        raise ValueError(
            f"read_ticks: could not determine the year for {zip_path} "
            f"(needed for era-specific parsing)"
        ) from exc


def _requested_days(date: Optional[str]) -> Optional[set]:
    """The exact ``YYYYMMDD`` days a date request resolves to, or ``None`` when
    the request is month/year-level (no per-day pruning intended).

    Monthly NEEDS ZIPs (the summary and index types) hold a whole month, so a
    single-day or day-range request must prune the result to those days —
    otherwise the caller silently gets the whole month, inconsistent with the
    daily ``individual_stock`` files and with ``query_ticks`` (both day-scoped).
    """
    if date is None:
        return None
    token = str(date).strip()
    if "-" not in token and token.isdigit() and len(token) == 8:
        return {token}
    parsed = parse_period(token)
    if parsed.get("granularity") == "date":
        return set(parsed["dates"])
    return None


def _discover_root_zips(input_root: str, data_type: str, date: Optional[str]) -> List[Path]:
    """Discover ZIPs under a structured ``{year}/{yearmonth}/`` NEEDS root."""
    if date is None:
        raise ValueError(
            "read_ticks: 'date' is required when 'source' is a structured NEEDS "
            "root (a day 'YYYYMMDD', month 'YYYYMM', year 'YYYY', or a "
            "'start-end' range)"
        )
    token = str(date).strip()
    if "-" not in token and token.isdigit() and len(token) == 8:
        return discover_zips(input_root, data_type, [int(token[:4])],
                             months=[int(token[4:6])], dates=[token])
    if "-" not in token and token.isdigit() and len(token) == 6:
        return discover_zips(input_root, data_type, [int(token[:4])], months=[int(token[4:6])])

    parsed = parse_period(token)
    gran = parsed["granularity"]
    zips: List[Path] = []
    if gran == "year":
        for y in parsed["years"]:
            zips.extend(discover_zips(input_root, data_type, [y]))
    elif gran == "month":
        for y, months in parsed["months_by_year"].items():
            zips.extend(discover_zips(input_root, data_type, [y], months=list(months)))
    else:  # gran == "date"
        dates = parsed["dates"]
        for y in sorted({int(d[:4]) for d in dates}):
            ydates = [d for d in dates if d.startswith(str(y))]
            ymonths = sorted({int(d[4:6]) for d in ydates})
            zips.extend(discover_zips(input_root, data_type, [y], months=ymonths, dates=ydates))
    return zips


def _resolve_source_zips(source: str, data_type: str, date: Optional[str]) -> List[Path]:
    """Resolve a ``source`` (single ZIP, flat dir, or structured root) to ZIPs."""
    p = Path(source)
    if not p.exists():
        raise FileNotFoundError(f"read_ticks: source does not exist: {source}")
    if p.is_file():
        if p.suffix.lower() != ".zip":
            raise ValueError(f"read_ticks: source file must be a .zip, got {source!r}")
        return [p]
    if p.is_dir():
        # With no date, a flat folder reads all its ZIPs (a structured root needs a
        # date — the call below then raises a clear message).
        if date is None:
            direct = sorted(p.glob("*.zip"), key=_zip_sort_key)
            if direct:
                return direct
        # With a date, resolve via discover_zips for BOTH a flat folder and a
        # structured root: it maps a single *day* onto its containing *monthly*
        # file and matches daily-packaged files by day. (The old flat-folder branch
        # matched the date token as a filename substring, so a single-day date never
        # matched a "…YYYYMM.zip" monthly file — a month-folder + single-day read
        # came back falsely empty: run11 Finding 1.)
        return _discover_root_zips(str(p), data_type, date)
    raise ValueError(f"read_ticks: source must be a .zip file or a directory: {source!r}")


def _parse_hms(value: str) -> datetime.time:
    try:
        return datetime.time.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid time format (expected HH:MM:SS): {value!r}") from exc


def _resolve_col(df: pl.DataFrame, en_name: str) -> Optional[str]:
    """Return the column matching an English concept, honoring jp output.

    ``read_ticks(language="jp")`` renames columns to Japanese, so ticker- and
    time-filters keyed on the English name must also accept the Japanese
    equivalent (otherwise the filter silently finds no column and is skipped).
    """
    if en_name in df.columns:
        return en_name
    jp_name = get_japanese_column_mapping().get(en_name)  # mapping is {en: jp}
    if jp_name is not None and jp_name in df.columns:
        return jp_name
    return None


def _filter_time_window(
    df: pl.DataFrame, start_time: Optional[str], end_time: Optional[str]
) -> pl.DataFrame:
    time_col = _resolve_col(df, "Execution Time")
    if time_col is None:
        raise ValueError(
            "read_ticks: start_time/end_time require an 'Execution Time' column"
        )
    date_col = _resolve_col(df, "Data Date") or "Data Date"

    # individual_stock quote-only book updates have a blank Execution Time but a
    # real Update Time; fall back to it so a time window keeps in-window order-book
    # rows, not just trade-coincident snapshots (~94% of a liquid day is quotes).
    # Scoped automatically: only individual_stock carries an Update Time column
    # (the indices/summary projections drop it), so the fallback is inactive there.
    update_col = _resolve_col(df, "Update Time")
    work = df
    eff_col = time_col
    if update_col is not None:
        exec_raw = pl.col(time_col).cast(pl.String).str.strip_chars()
        work = df.with_columns(
            pl.when(exec_raw.is_null() | (exec_raw == ""))
            .then(pl.col(update_col))
            .otherwise(pl.col(time_col))
            .alias("__eff_time")
        )
        eff_col = "__eff_time"

    time_of_day = _tick_datetime_expr(date_col, eff_col).dt.time()
    expr = None
    if start_time is not None:
        cond = time_of_day >= pl.lit(_parse_hms(start_time))
        expr = cond if expr is None else (expr & cond)
    if end_time is not None:
        cond = time_of_day <= pl.lit(_parse_hms(end_time))
        expr = cond if expr is None else (expr & cond)

    result = work.filter(expr) if expr is not None else work
    if eff_col == "__eff_time":
        result = result.drop("__eff_time")
    return result


def _filter_codes(df: pl.DataFrame, data_type: str, wanted: set) -> pl.DataFrame:
    """Post-parse ticker filter for the non-individual_stock types (en or jp)."""
    if data_type == "stock_summary":
        col = _resolve_col(df, "Stock Code")
        if col is None:
            return df
        codes = pl.col(col).cast(pl.String).str.strip_chars().str.slice(0, 4)
        return df.filter(codes.is_in(list(wanted)))
    # indices / indices_summary: Index Code is categorically decoded to a display
    # name (e.g. "101" -> "Nikkei 225" / "日経平均株価"); accept the raw code or
    # either-language name. _index_code_lookup() maps both en and jp names -> code.
    col = _resolve_col(df, "Index Code")
    if col is None:
        return df
    from .io.parquet import _index_code_lookup

    # Index Code is now the raw code in-column, but accept a display name as
    # input too (and still match old stores that hold names): map names->code
    # and code->name(s) so wanted matches whichever form the column holds.
    lookup = _index_code_lookup()  # display name (en+jp) -> raw code
    expanded = set(wanted)
    for w in list(wanted):
        if w in lookup:
            expanded.add(lookup[w])
    for display, code in lookup.items():
        if code in expanded:
            expanded.add(display)
    disp = pl.col(col).cast(pl.String).str.strip_chars()
    return df.filter(disp.is_in(list(expanded)))


def read_ticks(
    source: str,
    *,
    data_type: str = "individual_stock",
    ticker_filter: Optional[set] = None,
    date: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    columns: Optional[List[str]] = None,
    rows: Optional[int] = 10_000_000,
    language: Literal["en", "jp"] = "en",
) -> pl.DataFrame:
    """One-shot read: raw NEEDS ZIPs -> a ticker/time-filtered DataFrame (no store).

    Answers the common exploratory question — *"ticker 7203 on 2024-02-01,
    09:00-11:30"* — in a single call, with no Parquet store to build first. It
    composes the existing pipeline (:func:`create_df`'s ``individual_stock``
    raw-byte ticker fast path, :func:`discover_zips` / :func:`parse_period`
    discovery, and the shared tick-timestamp helper); it does not reimplement
    parsing, ticker pruning, or time filtering.

    Args:
        source: A single ``.zip``, a flat folder of ZIPs, or a structured NEEDS
            root (``{year}/{yearmonth}/…``) — the same inputs the ``ingest_*``
            functions accept.
        data_type: One of the four NEEDS types (a :class:`DataType` works too).
        ticker_filter: A ``set`` of codes (e.g. ``{"7203"}``); ``int`` codes
            such as ``{7203}`` are accepted too and coerced to strings — so the
            output of :func:`tse_tick.get_available_tickers` feeds straight in. A
            **bare single code** (``"7203"`` or ``7203``) is also accepted and
            treated as a one-element filter (so a stray ``"7203"`` is not split
            into characters). For ``individual_stock`` this drives the
            bounded-memory raw-byte fast path; for ``indices`` it matches the
            index code (``"101"`` == Nikkei 225) after parsing.
        date: A day ``"YYYYMMDD"``, month ``"YYYYMM"``, year ``"YYYY"``, or a
            ``"start-end"`` range. Selects which ZIPs to open. **Required** when
            ``source`` is a structured root; optional for a single ZIP/flat dir.
        start_time: Inclusive lower bound on time-of-day (``"HH:MM:SS"``).
        end_time: Inclusive upper bound on time-of-day (``"HH:MM:SS"``).
        columns: Column projection; ``None`` selects all columns.
        rows: Cap on returned rows (default 10,000,000). On hitting the cap the
            result is truncated **and a** :class:`TruncationWarning` **is
            emitted** (capturable via ``warnings``) — the signal to build a store
            and use :func:`tse_tick.query_ticks` instead.
        language: Output column-name language, ``"en"`` or ``"jp"``.

    Returns:
        The cleaned Polars DataFrame — empty but fully typed if nothing matches.
        Its columns match :func:`create_df`. Note :func:`tse_tick.query_ticks`
        returns these columns **plus** an extra ``date`` partition column
        (``i64`` ``YYYYMMDD``, added by the store's Hive partitioning), so the
        store path has one more column than this one-shot path.

    Caveats:
        * **Time filtering applies to tick types only.** ``individual_stock`` /
          ``indices`` have ``Execution Time``; the two ``*_summary`` types are
          daily aggregates, so passing ``start_time``/``end_time`` for them
          raises — filter on ``date`` only. For ``individual_stock``, quote-only
          book updates have a blank ``Execution Time`` but a real ``Update Time``,
          so the time window falls back to ``Update Time`` for those rows — an
          in-window order book is kept whole, not reduced to trade-coincident
          snapshots.
        * **The fast path is ``individual_stock``-only.** Other types parse in
          full then filter (fine — those files are far smaller).
        * **Not a store replacement at scale.** Even one ticker-day opens **every
          ZIP part** of each requested day, so a one-shot read can take tens of
          seconds per day (the raw-byte fast path still scans all parts). With no
          ``ticker_filter`` over a wide span it re-scans raw ZIPs on every call;
          for repeated or large analyses — or just faster single-ticker lookups —
          ``ingest_*`` once + :func:`tse_tick.query_ticks` is far faster
          (sub-second).
        * **A single numbered ZIP is only part of a day.** NEEDS splits each day
          across numbered parts by ascending code, so one
          ``HTICST120.<date>.N.zip`` holds a code subset (part ``1`` starts at
          1301; Toyota 7203 is in a later part). Pass the directory or structured
          root — not a lone part — for complete ticker coverage.

    Example:
        >>> df = read_ticks("G:/NEEDS_root", ticker_filter={"7203"},
        ...                 date="20240201", start_time="09:00:00",
        ...                 end_time="11:30:00")
    """
    validate_data_type(data_type)

    is_summary = data_type in SUMMARY_TYPES
    if (start_time is not None or end_time is not None) and is_summary:
        raise ValueError(
            f"read_ticks: start_time/end_time are not supported for {data_type!r} "
            f"(daily aggregates have no Execution Time); filter on 'date' only"
        )

    norm_filter = _normalize_ticker_filter(ticker_filter)

    zips = _resolve_source_zips(source, data_type, date)
    if not zips:
        _warn_no_data(
            f"read_ticks: no ZIP files found for data_type={data_type!r} on the "
            f"requested date(s) in {source!r}. Likely causes: a non-trading day "
            "(e.g. an exchange holiday), or data_type not matching the files in "
            "this folder (e.g. pointing 'individual_stock' at an index folder)."
        )
        return _empty_typed_frame(data_type, language, _year_hint_from_date(date))

    # create_df reaches the raw-byte fast path only for individual_stock.
    cdf_filter = norm_filter if data_type == "individual_stock" else None

    parts: List[pl.DataFrame] = []
    total = 0
    schema_frame: Optional[pl.DataFrame] = None
    for zip_path in zips:
        df = None
        try:
            year = _zip_year(zip_path)
            df = create_df(
                str(zip_path),
                language=language,
                auto_detect=False,
                data_type=data_type,
                year=year,
                ticker_filter=cdf_filter,
            )
            if norm_filter is not None and data_type != "individual_stock":
                df = _filter_codes(df, data_type, norm_filter)

            if start_time is not None or end_time is not None:
                df = _filter_time_window(df, start_time, end_time)

            # Capture the typed schema from the first part so a no-match read
            # returns an empty-but-typed frame, not a schemaless (0, 0).
            if schema_frame is None:
                schema_frame = df.clear()
            if df.height:
                parts.append(df)
                total += df.height
        finally:
            # One ZIP at a time, collected between, to bound memory on big files.
            del df
            gc.collect()

        if rows is not None and total >= rows:
            break

    if parts:
        result = pl.concat(parts, how="vertical")
    elif schema_frame is not None:
        result = schema_frame
    else:
        result = _empty_typed_frame(data_type, language, _year_hint_from_date(date))

    # Monthly ZIPs hold a whole month; prune to the requested day(s) so a
    # single-day/day-range request doesn't silently return the entire month.
    days = _requested_days(date)
    if days is not None and result.height:
        date_col = _resolve_col(result, "Data Date")
        if date_col is not None:
            dd = pl.col(date_col).cast(pl.Date).cast(pl.String).str.replace_all("-", "")
            result = result.filter(dd.is_in(list(days)))

    # Project AFTER all filtering — code, time, AND the monthly day-prune above.
    # Projecting per-part earlier let a `columns` subset that drops 'Data Date'
    # skip the day-prune and silently return the whole month (the run10 indices
    # ~20x inflation bug); doing it here keeps every filter on the full schema.
    if columns:
        missing = [c for c in columns if c not in result.columns]
        if missing:
            raise ValueError(f"read_ticks: requested columns not present: {missing}")
        result = result.select(columns)

    # A zero-row result is the same "no data" condition for every data type —
    # signal it the same capturable way (the ZIPs existed but a holiday/unknown
    # code/over-tight filter left nothing), not silently as before.
    if result.height == 0:
        # `is not None` (not truthiness) so an *empty* filter is still named — an
        # accidentally-empty set matches nothing, and omitting it from the message
        # wrongly implied no filter was applied (run10 F4).
        ft = f", ticker_filter={sorted(norm_filter)}" if norm_filter is not None else ""
        _warn_no_data(
            f"read_ticks: 0 rows for the requested filters "
            f"(data_type={data_type!r}, date={date!r}{ft}). Possible causes: a "
            "non-trading day (e.g. a holiday), a ticker/index code not present, or "
            "time/column filters that exclude every row (e.g. start_time after "
            "end_time)."
        )

    if rows is not None and result.height > rows:
        warnings.warn(
            f"read_ticks: row cap ({rows}) reached; result truncated. "
            "Build a Parquet store (ingest_*) and use query_ticks for full coverage.",
            TruncationWarning,
            stacklevel=2,
        )
        result = result.head(rows)
    return result
