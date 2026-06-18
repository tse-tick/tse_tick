# tse_tick/enhanced.py
import datetime
import gc
import re
import zipfile
import io
import glob
import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional, Literal, Tuple, List, Dict, Union

import polars as pl

from .core import clean_data, parse_line, _tick_datetime_expr
from .schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_15,
    get_schema_indices_23,
    get_japanese_column_mapping,
)

logger = logging.getLogger(__name__)

_MAX_DECOMPRESSED_BYTES = 5 * 1024 * 1024 * 1024
_MAX_ZIP_ENTRIES = 5

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


def parse_period(period_str: str) -> Dict[str, Union[str, List[int], Dict[int, List[int]], List[str]]]:
    """Parse a period string into structured parameters.

    Accepted formats:
        YYYY                         -> process entire year
        YYYYMM-YYYYMM                -> process all trading days from start month to end month
        YYYYMMDD-YYYYMMDD            -> process all trading days from start date to end date

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
            raise ValueError(f"Invalid period format: {period_str!r}. Expected YYYY, YYYYMM-YYYYMM, or YYYYMMDD-YYYYMMDD")
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
            raise ValueError(
                f"Invalid period format: {period_str!r}. "
                f"Expected YYYY, YYYYMM-YYYYMM (6-digit months), or YYYYMMDD-YYYYMMDD (8-digit dates)"
            )
    else:
        if len(period_str) == 4 and period_str.isdigit():
            return {"granularity": "year", "years": [int(period_str)]}
        else:
            raise ValueError(
                f"Invalid period: {period_str!r}. "
                f"Expected YYYY, YYYYMM-YYYYMM, or YYYYMMDD-YYYYMMDD"
            )


def detect_data_type_and_year(folder_path: str) -> Tuple[str, int]:
    path = Path(folder_path)

    year = None
    for part in path.parts:
        match = re.search(r"(20\d{2})", part)
        if match:
            year = int(match.group(1))
            break

    if year is None:
        raise ValueError(f"Could not detect year from path: {folder_path}")

    path_str = str(path).lower()

    if any(kw in path_str for kw in ["individual_stock", "ticst", "stock_tick"]):
        data_type = "individual_stock"
    elif any(kw in path_str for kw in ["stock_summary", "ticss", "stock_daily"]):
        data_type = "stock_summary"
    elif any(kw in path_str for kw in ["indices_tick", "ticit", "index_tick"]) and "summary" not in path_str:
        data_type = "indices"
    elif any(kw in path_str for kw in ["indices_summary", "ticis", "index_daily", "index_summary"]):
        data_type = "indices_summary"
    else:
        if path.exists() and path.is_dir():
            files = list(path.glob("*.zip")) + list(path.glob("*.csv"))
            if files:
                sample_file = files[0].name.upper()
                if "TICST" in sample_file:
                    data_type = "individual_stock"
                elif "HTICIS" in sample_file or "TICSS" in sample_file:
                    data_type = "stock_summary"
                elif "TICIT" in sample_file:
                    data_type = "indices"
                elif "TICIS" in sample_file:
                    data_type = "indices_summary"
                else:
                    raise ValueError(f"Could not detect data type from files in: {folder_path}")
            else:
                raise ValueError(f"No ZIP or CSV files found in: {folder_path}")
        else:
            raise ValueError(f"Could not detect data type from path: {folder_path}")

    return data_type, year


def _zip_date_token(name: str) -> Optional[str]:
    """The YYYYMMDD (daily) or YYYYMM (monthly) date token in a NEEDS filename."""
    for part in name.split("."):
        if part.isdigit() and part.startswith("20") and len(part) in (6, 8):
            return part
    return None


def _discover_zips_recursive(
    root: Path,
    prefix: str,
    years: List[int],
    months: Optional[List[int]],
    dates: Optional[List[str]],
) -> List[Path]:
    """Fallback discovery: recursively find ``<PREFIX>.*.zip`` anywhere under
    ``root`` (robust to nested delivery trees such as
    ``個別株式{year}/TICST120/{yyyymm}/``), filtered by the requested
    year/month/date tokens parsed from each filename.
    """
    year_set = {str(y) for y in years}
    month_set = {f"{m:02d}" for m in (months if months is not None else range(1, 13))}
    date_set = set(dates) if dates else None
    pattern = str(root / "**" / f"{prefix}.*.zip")
    out: List[Path] = []
    for p in glob.glob(pattern, recursive=True):
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
    """Find NEEDS ZIPs under a structured ``{year}/{yearmonth}/`` root.

    Globs ``input_root/<year>/<yearmonth>/<PREFIX>.<date>.*.zip`` using the data
    type's record prefix (e.g. ``HTICST120`` for ``individual_stock``).

    Args:
        input_root: Root of the NEEDS ``{year}/{yearmonth}/`` hierarchy.
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

    if months is None:
        months = list(range(1, 13))

    root = Path(input_root)
    all_zips: List[Path] = []

    for year in years:
        for month in months:
            month_str = f"{year}{month:02d}"
            if dates is not None:
                for date_str in dates:
                    if date_str[:6] != month_str:
                        continue
                    pattern = str(root / str(year) / month_str / f"{prefix}.{date_str}.*.zip")
                    matched = sorted(glob.glob(pattern))
                    all_zips.extend(Path(p) for p in matched)
            else:
                pattern = str(root / str(year) / month_str / f"{prefix}.*.zip")
                matched = sorted(glob.glob(pattern))
                all_zips.extend(Path(p) for p in matched)

    # The documented layout above is the fast path; if it matched nothing, fall
    # back to a recursive search so nested real-world trees still work.
    if not all_zips:
        all_zips = _discover_zips_recursive(root, prefix, years, months, dates)

    return all_zips


def _raw_width(kind: str, year: int) -> int:
    """Expected raw (pre-clean) column count for a NEEDS data type/era."""
    if kind == "indices":
        return 15 if year == 2016 else 23
    if kind in ("stock_summary", "indices_summary"):
        return 83
    return 95  # individual_stock


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
        zip_files = sorted(list(path.glob("*.zip")))
    else:
        raise ValueError(
            f"Path must be either a directory containing ZIP files or a ZIP file: {folder_path}"
        )

    if not zip_files:
        raise FileNotFoundError(f"No ZIP files found in: {folder_path}")

    logger.debug("Found %d ZIP file(s) in %s", len(zip_files), folder_path)

    dfs = []
    total_rows_read = 0

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
                            df_chunk = pl.read_csv(
                                io.BytesIO(raw_bytes),
                                has_header=False,
                                schema_overrides=schema_override,
                                truncate_ragged_lines=True,
                            )
                            if rows_to_read is not None:
                                df_chunk = df_chunk.slice(0, rows_to_read)
                        else:
                            df_chunk = pl.DataFrame()

                    else:
                        df_chunk = pl.read_csv(
                            f,
                            has_header=False,
                            schema_overrides=schema_override,
                            truncate_ragged_lines=True,
                        )
                        if rows_to_read is not None:
                            df_chunk = df_chunk.slice(0, rows_to_read)

                    if not df_chunk.is_empty():
                        dfs.append(df_chunk)
                        total_rows_read += len(df_chunk)

                    if rows is not None and total_rows_read >= rows:
                        break

        except (zipfile.BadZipFile, EOFError):
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

    result = pl.concat(dfs, how="vertical")
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
    elif (kind == "stock_summary") or (kind == "indices_summary"):
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
            the path. When ``False`` you **must** pass ``data_type`` and ``year``
            (auto-detect would otherwise overwrite them).
        data_type: Required when ``auto_detect=False``; one of the four NEEDS
            types (a :class:`DataType` works too).
        year: Required when ``auto_detect=False`` (selects era-specific parsing).
        ticker_filter: A ``set`` of **string** stock codes (e.g. ``{"7203"}``)
            kept via the raw-byte fast path. Applied for ``individual_stock``
            **only** — ignored for the other data types.

    Returns:
        The cleaned DataFrame (empty if ``ticker_filter`` matched no rows).
    """
    if auto_detect:
        data_type, year = detect_data_type_and_year(folder_path)
        logger.debug("Auto-detected: %s, Year: %s", data_type, year)
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

    df_with_columns = set_columns(df_raw, data_type, language)

    if language == "jp":
        jp_mapping = get_japanese_column_mapping()
        en_to_jp = {v: k for k, v in jp_mapping.items()}
        jp_cols = df_with_columns.columns
        en_cols = [en_to_jp.get(col, col) for col in jp_cols]
        rename_back_en = dict(zip(jp_cols, en_cols))
        df_with_columns = df_with_columns.rename(rename_back_en)
        df_cleaned = clean_data(df_with_columns, data_type, language)
        rename_to_jp = dict(zip(en_cols, jp_cols))
        df_cleaned = df_cleaned.rename(rename_to_jp)

        if not data_type == "individual_stock":
            final_cols = get_final_columns(data_type)
            final_cols_jp = [jp_mapping.get(c, c) for c in final_cols]
            available = [c for c in final_cols_jp if c in df_cleaned.columns]
            df_final = df_cleaned.select(available)
        else:
            df_final = df_cleaned
    else:
        df_cleaned = clean_data(df_with_columns, data_type, language)

        if not data_type == "individual_stock":
            final_cols = get_final_columns(data_type)
            available = [c for c in final_cols if c in df_cleaned.columns]
            df_final = df_cleaned.select(available)
        else:
            df_final = df_cleaned

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


def _date_prefixes(date: Optional[str]) -> Optional[List[str]]:
    """Expand ``date`` into filename date tokens, or ``None`` for 'all dates'."""
    if date is None:
        return None
    token = str(date).strip()
    if "-" not in token and token.isdigit() and len(token) in (4, 6, 8):
        return [token]
    parsed = parse_period(token)
    gran = parsed["granularity"]
    if gran == "year":
        return [str(y) for y in parsed["years"]]
    if gran == "month":
        out: List[str] = []
        for y, months in parsed["months_by_year"].items():
            out.extend(f"{y}{m:02d}" for m in months)
        return out
    return list(parsed["dates"])


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
        direct = sorted(p.glob("*.zip"))
        if direct:  # flat directory of ZIPs; narrow by date when given
            prefixes = _date_prefixes(date)
            if prefixes is None:
                return direct
            return [z for z in direct if any(pfx in z.name for pfx in prefixes)]
        return _discover_root_zips(str(p), data_type, date)
    raise ValueError(f"read_ticks: source must be a .zip file or a directory: {source!r}")


def _parse_hms(value: str) -> datetime.time:
    try:
        return datetime.time.fromisoformat(str(value))
    except ValueError as exc:
        raise ValueError(f"Invalid time format (expected HH:MM:SS): {value!r}") from exc


def _filter_time_window(
    df: pl.DataFrame, start_time: Optional[str], end_time: Optional[str]
) -> pl.DataFrame:
    if "Execution Time" not in df.columns:
        raise ValueError(
            "read_ticks: start_time/end_time require an 'Execution Time' column"
        )
    time_of_day = _tick_datetime_expr().dt.time()
    expr = None
    if start_time is not None:
        cond = time_of_day >= pl.lit(_parse_hms(start_time))
        expr = cond if expr is None else (expr & cond)
    if end_time is not None:
        cond = time_of_day <= pl.lit(_parse_hms(end_time))
        expr = cond if expr is None else (expr & cond)
    return df.filter(expr) if expr is not None else df


def _filter_codes(df: pl.DataFrame, data_type: str, wanted: set) -> pl.DataFrame:
    """Post-parse ticker filter for the non-individual_stock types."""
    if data_type == "stock_summary":
        if "Stock Code" not in df.columns:
            return df
        codes = pl.col("Stock Code").cast(pl.String).str.strip_chars().str.slice(0, 4)
        return df.filter(codes.is_in(list(wanted)))
    # indices / indices_summary: Index Code is categorically decoded to a display
    # name (e.g. "101" -> "Nikkei 225"); accept either the raw code or the name.
    if "Index Code" not in df.columns:
        return df
    from .io.parquet import _index_code_lookup

    expanded = set(wanted)
    for display, code in _index_code_lookup().items():  # display -> raw code
        if code in wanted:
            expanded.add(display)
    disp = pl.col("Index Code").cast(pl.String).str.strip_chars()
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
        ticker_filter: A ``set`` of **string** codes (e.g. ``{"7203"}``). For
            ``individual_stock`` this drives the bounded-memory raw-byte fast
            path; for ``indices`` it matches the index code (``"101"`` == Nikkei
            225) after parsing.
        date: A day ``"YYYYMMDD"``, month ``"YYYYMM"``, year ``"YYYY"``, or a
            ``"start-end"`` range. Selects which ZIPs to open. **Required** when
            ``source`` is a structured root; optional for a single ZIP/flat dir.
        start_time: Inclusive lower bound on time-of-day (``"HH:MM:SS"``).
        end_time: Inclusive upper bound on time-of-day (``"HH:MM:SS"``).
        columns: Column projection; ``None`` selects all columns.
        rows: Cap on returned rows (default 10,000,000). The cap **silently
            truncates** — hitting it is the signal to build a store and use
            :func:`tse_tick.query_ticks` instead.
        language: Output column-name language, ``"en"`` or ``"jp"``.

    Returns:
        The same cleaned Polars DataFrame shape as :func:`create_df` /
        :func:`tse_tick.query_ticks` (empty if nothing matches).

    Caveats:
        * **Time filtering applies to tick types only.** ``individual_stock`` /
          ``indices`` have ``Execution Time``; the two ``*_summary`` types are
          daily aggregates, so passing ``start_time``/``end_time`` for them
          raises — filter on ``date`` only.
        * **The fast path is ``individual_stock``-only.** Other types parse in
          full then filter (fine — those files are far smaller).
        * **Not a store replacement at scale.** With no ``ticker_filter`` over a
          wide span this re-scans raw ZIPs on every call; for repeated or large
          analyses, ``ingest_*`` once + :func:`tse_tick.query_ticks` is far
          faster.

    Example:
        >>> df = read_ticks("G:/NEEDS_root", ticker_filter={"7203"},
        ...                 date="20240201", start_time="09:00:00",
        ...                 end_time="11:30:00")
    """
    valid_types = {"individual_stock", "stock_summary", "indices", "indices_summary"}
    if data_type not in valid_types:
        raise ValueError(
            f"Unknown data_type {data_type!r}. Must be one of {sorted(valid_types)}"
        )

    is_summary = data_type in ("stock_summary", "indices_summary")
    if (start_time is not None or end_time is not None) and is_summary:
        raise ValueError(
            f"read_ticks: start_time/end_time are not supported for {data_type!r} "
            f"(daily aggregates have no Execution Time); filter on 'date' only"
        )

    norm_filter = None
    if ticker_filter is not None:
        norm_filter = {str(t).strip() for t in ticker_filter}

    zips = _resolve_source_zips(source, data_type, date)
    if not zips:
        return pl.DataFrame()

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

            if columns:
                missing = [c for c in columns if c not in df.columns]
                if missing:
                    raise ValueError(
                        f"read_ticks: requested columns not present: {missing}"
                    )
                df = df.select(columns)

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
        result = pl.DataFrame()

    if rows is not None and result.height > rows:
        result = result.head(rows)
    return result
