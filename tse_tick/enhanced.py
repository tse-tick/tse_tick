# tse_tick/enhanced.py
_MAX_DECOMPRESSED_BYTES = 5 * 1024 * 1024 * 1024
_MAX_ZIP_ENTRIES = 5

import datetime
import re
import zipfile
import io
import glob
import warnings
import gc
import logging
from collections import defaultdict
from pathlib import Path
from typing import Optional, Literal, Tuple, List, Dict, Union

import polars as pl

from .core import clean_data, parse_line
from .schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_15,
    get_schema_indices_23,
    get_schema_indices_summary,
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


def discover_zips(
    input_root: str,
    data_type: str,
    years: List[int],
    months: Optional[List[int]] = None,
    dates: Optional[List[str]] = None,
) -> List[Path]:
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

    return all_zips


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

    print(f"Found {len(zip_files)} ZIP file(s) in {folder_path}")

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
            print(f"Error reading {zip_file}: {e}")
            continue

    if not dfs:
        if ticker_filter:
            return pl.DataFrame()
        raise ValueError("No data was successfully read")

    result = pl.concat(dfs, how="vertical")
    print(f"Total rows read: {len(result)}")

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
            "Record Type", "Data Date", "Exchange Code", "Security Type", "Stock Code",
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
    if auto_detect:
        data_type, year = detect_data_type_and_year(folder_path)
        print(f"Auto-detected: {data_type}, Year: {year}")
    else:
        if data_type is None or year is None:
            raise ValueError(
                "When auto_detect=False, data_type and year must be explicitly provided"
            )
        print(f"Manual: {data_type}, Year: {year}")

    df_raw = get_1y_dataframe(
        folder_path,
        year,
        data_type,
        rows,
        ticker_filter=ticker_filter if data_type == "individual_stock" else None,
    )

    if df_raw.is_empty():
        print("Data successfully created")
        return df_raw

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

    print("Data successfully created")
    return df_final


def export_to_csv(
    folder_path: str,
    output_path: Optional[str] = None,
    language: Literal["en", "jp"] = "en",
    rows: Optional[int] = None,
) -> str:
    df = create_df(folder_path, language, rows)

    if output_path is None:
        data_type, year = detect_data_type_and_year(folder_path)
        lang_suffix = "_jp" if language == "jp" else "_en"
        output_path = f"{data_type}_{year}{lang_suffix}_cleaned.csv"

    df.write_csv(output_path)
    print(f"Data exported to: {output_path}")
    print(f"Shape: {df.shape}")

    return output_path
