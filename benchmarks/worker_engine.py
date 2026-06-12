"""
Subprocess worker: parse + clean a single NEEDS ZIP file.
Called as:  python worker_engine.py <backend> <zip_path> [max_threads] [data_type]
  backend: pandas | pandas-fair | polars
  max_threads: only used for polars (omit or 0 = default)
  data_type: individual_stock | stock_summary | indices | indices_summary
             (default: individual_stock)

Outputs a single JSON line on stdout with timing and memory data.
"""
import sys
import os
import json
import time
import zipfile
import io

zip_path = sys.argv[2]
backend = sys.argv[1]
max_threads = int(sys.argv[3]) if len(sys.argv) > 3 else 0
data_type = sys.argv[4] if len(sys.argv) > 4 else "individual_stock"

if backend == "polars" and max_threads > 0:
    os.environ["POLARS_MAX_THREADS"] = str(max_threads)

import psutil

proc = psutil.Process(os.getpid())

DATA_TYPE_COLS = {
    "individual_stock": 95,
    "stock_summary": 83,
    "indices": 23,
    "indices_summary": 83,
}

n_cols = DATA_TYPE_COLS.get(data_type, 95)


# ---------------------------------------------------------------------------
# Schemas and mappings used by pandas backends
# ---------------------------------------------------------------------------

SCHEMA_95 = [
    "Record Type", "Data Date", "Exchange Code", "Security Type", "Session",
    "Stock Code", "Execution Time", "Sell Quote Time", "Buy Quote Time",
    "Update Time", "Management Number", "Execution Price", "Execution Type",
    "Ayumi Flag", "Volume", "Volume Flag", "Close Quote Flag",
    "Sell Quote 1 Best", "Sell Quote Vol 1", "Sell Quote Flag 1",
    "Buy Quote 1 Best", "Buy Quote Vol 1", "Buy Quote Flag 1",
    "Sell Limit Quote", "Sell Limit Vol", "Sell Limit Flag",
    "Sell Market Quote", "Sell Market Vol", "Sell Market Flag",
]
for i in range(2, 11):
    SCHEMA_95.extend([f"Sell Quote {i}", f"Sell Quote Vol {i}", f"Sell Quote Flag {i}"])
SCHEMA_95.extend(["Sell Quote OVER", "Sell Quote Vol OVER", "Sell Quote Flag OVER"])
SCHEMA_95.extend([
    "Buy Limit Quote", "Buy Limit Vol", "Buy Limit Flag",
    "Buy Market Quote", "Buy Market Vol", "Buy Market Flag",
])
for i in range(2, 11):
    SCHEMA_95.extend([f"Buy Quote {i}", f"Buy Quote Vol {i}", f"Buy Quote Flag {i}"])
SCHEMA_95.extend(["Buy Quote UNDER", "Buy Quote Vol UNDER", "Buy Quote Flag UNDER"])

CATEGORICAL = {
    "Record Type": {
        "DB13": "Stocks", "DB23": "Indices", "DB33": "Futures",
        "DB43": "Options", "DB53": "Convertible Bonds",
        "1100": "Stocks - Best Quote", "1200": "Stocks - Multiple Quote",
        "2100": "Indices - Execution",
    },
    "Exchange Code": {
        "11": "Tokyo Stock Exchange (TSE)", "31": "Nagoya Stock Exchange (NSE)",
        "61": "Fukuoka Stock Exchange (FSE)", "81": "Sapporo Securities Exchange (SSE)",
        "21": "Osaka Securities Exchange (OSE)", "91": "JASDAQ", "A1": "Hercules",
    },
    "Security Type": {
        "1": "First Section", "2": "Second Section", "3": "Foreign Stocks",
        "4": "TSE Mothers", "5": "TOKYO PRO Market (Domestic)",
        "6": "TOKYO PRO Market (Foreign)", "7": "TSE JASDAQ (Domestic)",
        "11": "TSE JASDAQ (Foreign)", "8": "Under Supervision",
        "9": "Delisting", "10": "Cash Index",
    },
    "Session": {"1": "Morning / Day", "2": "Afternoon"},
    "Execution Type Stocks": {
        "1": "Opening", "16": "At Buy Quote", "32": "Between Quotes",
        "48": "At Sell Quote", "64": "Outside Quotes", "0": "Other",
    },
    "Execution Type Indices": {
        "1": "Opening", "2": "Post-Closing", "0": "Other",
    },
    "Ayumi Flag Stocks": {
        "0": "Regular", "4": "System Halt", "8": "Temporary Suspension",
        "12": "Interruption", "16": "Call Auction", "17": "Auction Released",
        "18": "Circuit Breaker", "19": "CB Released", "22": "Reference",
        "33": "Discontinuous", "64": "Suspension Released",
        "128": "Closing (volume>0)", "160": "Closing (volume=0)",
    },
    "Ayumi Flag Indices": {
        "0": "Regular", "128": "Closing",
    },
    "Volume Flag": {"0": "Final", "128": "Estimated"},
    "Quote Flag": {
        "0": "No Quote", "1": "Special Quote Cancelled",
        "8": "Pre-Suspension Special", "16": "Quote Omitted",
        "32": "Special Quote", "33": "Special Quote Opposite",
        "64": "Market Order", "66": "Continuous Execution",
        "67": "Continuous Exec Opposite", "68": "Pre-Suspension Continuous",
        "111": "Pre-Opening Expected", "112": "Pre-Opening",
        "127": "Market at Same Price", "128": "Regular Quote",
        "129": "Attention Quote", "130": "Final Quote",
        "131": "Regular (Improving)",
    },
    "Stock Code Suffix": {
        " ": "Parent Stock", "1": "New Shares", "2": "Second New Shares",
        "3": "Third New Shares", "5": "Preferred Stock",
        "6": "Preferred New Shares", "7": "Deferred Stock",
        "8": "Deferred New Shares", "9": "Stock Subscription Warrants",
    },
}

INT_COLS_95 = [14, 15, 18, 19, 21, 22, 24, 25, 27, 28, 30, 31, 33, 34,
               36, 37, 39, 40, 42, 43, 45, 46, 48, 49, 51, 52, 54, 55,
               57, 58, 60, 61, 63, 64, 66, 67, 69, 70, 72, 73, 75, 76,
               78, 79, 81, 82, 84, 85, 87, 88, 90, 91, 93, 94]
FLOAT_COLS_95 = [11, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53,
                 56, 59, 62, 65, 68, 71, 74, 77, 80, 83, 86, 89, 92]
TIME_COL_IDXS_95 = [6, 7, 8]


def _get_schema_names(dt):
    """Return (column_names, n_cols) for the given data type."""
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from tse_tick.schemas import (
        get_schema_individual_stock_95,
        get_schema_summary_83,
        get_schema_indices_23,
        get_schema_indices_summary,
    )
    if dt == "individual_stock":
        return get_schema_individual_stock_95(), 95
    elif dt == "stock_summary":
        return get_schema_summary_83(), 83
    elif dt == "indices":
        return get_schema_indices_23(), 23
    elif dt == "indices_summary":
        return get_schema_summary_83(), 83
    else:
        raise ValueError(f"Unknown data_type: {dt}")


# ---------------------------------------------------------------------------
# pandas (original prototype) — Python CSV engine
# ---------------------------------------------------------------------------
def pandas_pipeline_individual_stock(zpath):
    import pandas as pd
    dtype_dict = {col: str for col in range(10)}
    with zipfile.ZipFile(zpath, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pd.read_csv(f, header=None, dtype=dtype_dict, engine="python")
    df.columns = SCHEMA_95
    df["Buy Quote 1 Best"] = df["Buy Quote 1 Best"].astype(float)
    df["Buy Quote Vol 1"] = df["Buy Quote Vol 1"].astype(float)
    for col_idx in TIME_COL_IDXS_95:
        col = df.columns[col_idx]
        df[col] = df[col].fillna("")
    for i in INT_COLS_95:
        col = df.columns[i]
        df[col] = df[col].fillna(0).astype(int)
    for i in FLOAT_COLS_95:
        col = df.columns[i]
        df[col] = df[col].fillna(0.0)
    df["Data Date"] = pd.to_datetime(df["Data Date"], format="%Y%m%d")
    for tc in ["Execution Time", "Sell Quote Time", "Buy Quote Time"]:
        df[tc] = df[tc].str.slice(0, 6)
    df["Update Time"] = df["Update Time"].str.slice(0, 12)
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()
    for col in df.columns:
        if df[col].dtype == "float64":
            continue
        if "Time" in col or col == "Data Date" or "Vol" in col:
            continue
        if col == "Management Number":
            continue
        if ("Buy" in col) or ("Sell" in col):
            continue
        if col in ("Record Type", "Exchange Code"):
            mapping = CATEGORICAL[col]
            df[col] = df[col].map(lambda v, m=mapping: m.get(v, f"Unknown ({v})") if pd.notna(v) else v)
        elif col == "Stock Code":
            suffix_map = CATEGORICAL["Stock Code Suffix"]
            def decode_stock(v):
                if pd.isna(v) or len(v) == 4:
                    return v
                return v + suffix_map.get(v[-1], "")
            df[col] = df[col].map(decode_stock)
        elif col == "Execution Type":
            m = CATEGORICAL["Execution Type Stocks"]
            df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
        elif col == "Ayumi Flag":
            m = CATEGORICAL["Ayumi Flag Stocks"]
            df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
        elif col == "Volume Flag":
            m = CATEGORICAL["Volume Flag"]
            df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
        elif "Flag" in col:
            m = CATEGORICAL["Quote Flag"]
            df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
        elif col in ("Security Type", "Session"):
            m = CATEGORICAL[col]
            df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
    return df


def pandas_pipeline_generic(zpath, dt):
    """Pandas prototype pipeline for non-individual_stock types. Python CSV engine."""
    import pandas as pd
    col_names, nc = _get_schema_names(dt)
    dtype_dict = {col: str for col in range(min(10, nc))}
    with zipfile.ZipFile(zpath, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pd.read_csv(f, header=None, dtype=dtype_dict, engine="python")
    if len(df.columns) != len(col_names):
        col_names = col_names[:len(df.columns)]
    df.columns = col_names

    if dt == "stock_summary" or dt == "indices_summary":
        time_idxs = [17, 22, 42, 47]
        for idx in time_idxs:
            if idx < len(df.columns):
                col = df.columns[idx]
                s = df[col].astype(str).str.strip().str.replace('"', '')
                df[col] = s.str.slice(0, 12)
        if dt == "indices_summary":
            price_cols = [c for c in df.columns if "Price" in c]
            for c in price_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce") * 0.01
    elif dt == "indices":
        df["Index Value"] = pd.to_numeric(df["Index Value"], errors="coerce") * 0.01
        for tc in ["Execution Time"]:
            if tc in df.columns:
                df[tc] = df[tc].astype(str).str.strip().str.slice(0, 6)
        if "Update Time" in df.columns:
            df["Update Time"] = df["Update Time"].astype(str).str.strip().str.slice(0, 12)

    df["Data Date"] = pd.to_datetime(df["Data Date"].astype(str).str.strip().str.replace('"', ''), format="%Y%m%d", errors="coerce")
    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    cat_cols = list(df.columns[:5]) if len(df.columns) >= 5 else list(df.columns)
    for col in cat_cols:
        if col == "Data Date" or col == "Identification Flag":
            continue
        if col in ("Record Type", "Exchange Code"):
            mapping = CATEGORICAL.get(col, {})
            if isinstance(mapping, dict) and "all" not in mapping:
                df[col] = df[col].map(lambda v, m=mapping: m.get(v, f"Unknown ({v})") if pd.notna(v) else v)
        elif col in ("Security Type", "Session"):
            m = CATEGORICAL.get(col, {})
            df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)

    return df


# ---------------------------------------------------------------------------
# pandas-fair — C engine, forced N columns, all-string, same downstream work
# ---------------------------------------------------------------------------
def pandas_fair_pipeline(zpath, dt):
    """Fair pandas baseline: C engine with forced columns, all-string dtype,
    then identical downstream casting as the Polars path."""
    import pandas as pd
    col_names, nc = _get_schema_names(dt)

    with zipfile.ZipFile(zpath, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pd.read_csv(
                f, header=None, names=range(nc), dtype=str,
                engine="c", on_bad_lines="warn",
            )

    if len(df.columns) != len(col_names):
        col_names = col_names[:len(df.columns)]
    df.columns = col_names

    if dt == "individual_stock":
        df["Buy Quote 1 Best"] = pd.to_numeric(df["Buy Quote 1 Best"], errors="coerce")
        df["Buy Quote Vol 1"] = pd.to_numeric(df["Buy Quote Vol 1"], errors="coerce")
        for col_idx in TIME_COL_IDXS_95:
            col = df.columns[col_idx]
            df[col] = df[col].fillna("")
        for i in INT_COLS_95:
            col = df.columns[i]
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
        for i in FLOAT_COLS_95:
            col = df.columns[i]
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        df["Data Date"] = pd.to_datetime(df["Data Date"].str.strip(), format="%Y%m%d", errors="coerce")
        for tc in ["Execution Time", "Sell Quote Time", "Buy Quote Time"]:
            df[tc] = df[tc].str.slice(0, 6)
        df["Update Time"] = df["Update Time"].str.slice(0, 12)

    elif dt == "stock_summary" or dt == "indices_summary":
        time_idxs = [17, 22, 42, 47]
        for idx in time_idxs:
            if idx < len(df.columns):
                col = df.columns[idx]
                df[col] = df[col].fillna("").str.slice(0, 12)
        if dt == "indices_summary":
            price_cols = [c for c in df.columns if "Price" in c]
            for c in price_cols:
                df[c] = pd.to_numeric(df[c], errors="coerce") * 0.01
        df["Data Date"] = pd.to_datetime(df["Data Date"].str.strip(), format="%Y%m%d", errors="coerce")

    elif dt == "indices":
        df["Index Value"] = pd.to_numeric(df["Index Value"], errors="coerce") * 0.01
        if "Execution Time" in df.columns:
            df["Execution Time"] = df["Execution Time"].fillna("").str.slice(0, 6)
        if "Update Time" in df.columns:
            df["Update Time"] = df["Update Time"].fillna("").str.slice(0, 12)
        df["Data Date"] = pd.to_datetime(df["Data Date"].str.strip(), format="%Y%m%d", errors="coerce")

    str_cols = df.select_dtypes(include=["object"]).columns
    for col in str_cols:
        df[col] = df[col].str.strip()

    if dt == "individual_stock":
        for col in df.columns:
            if df[col].dtype == "float64":
                continue
            if "Time" in col or col == "Data Date" or "Vol" in col:
                continue
            if col == "Management Number":
                continue
            if ("Buy" in col) or ("Sell" in col):
                continue
            if col in ("Record Type", "Exchange Code"):
                mapping = CATEGORICAL[col]
                df[col] = df[col].map(lambda v, m=mapping: m.get(v, f"Unknown ({v})") if pd.notna(v) else v)
            elif col == "Stock Code":
                suffix_map = CATEGORICAL["Stock Code Suffix"]
                def decode_stock(v):
                    if pd.isna(v) or len(v) == 4:
                        return v
                    return v + suffix_map.get(v[-1], "")
                df[col] = df[col].map(decode_stock)
            elif col == "Execution Type":
                m = CATEGORICAL["Execution Type Stocks"]
                df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
            elif col == "Ayumi Flag":
                m = CATEGORICAL["Ayumi Flag Stocks"]
                df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
            elif col == "Volume Flag":
                m = CATEGORICAL["Volume Flag"]
                df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
            elif "Flag" in col:
                m = CATEGORICAL["Quote Flag"]
                df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
            elif col in ("Security Type", "Session"):
                m = CATEGORICAL[col]
                df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)
    else:
        cat_cols = list(df.columns[:5]) if len(df.columns) >= 5 else list(df.columns)
        for col in cat_cols:
            if col == "Data Date" or col == "Identification Flag":
                continue
            if col in ("Record Type", "Exchange Code"):
                mapping = CATEGORICAL.get(col, {})
                if isinstance(mapping, dict) and "all" not in mapping:
                    df[col] = df[col].map(lambda v, m=mapping: m.get(v, f"Unknown ({v})") if pd.notna(v) else v)
            elif col in ("Security Type", "Session"):
                m = CATEGORICAL.get(col, {})
                df[col] = df[col].map(lambda v, m=m: m.get(str(v), f"Unknown ({v})") if pd.notna(v) else v)

    return df


# ---------------------------------------------------------------------------
# Polars pipeline
# ---------------------------------------------------------------------------
def polars_pipeline(zpath, dt):
    import polars as pl
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from tse_tick.schemas import (
        get_schema_individual_stock_95,
        get_schema_summary_83,
        get_schema_indices_23,
    )
    from tse_tick.core import clean_data

    if dt == "individual_stock":
        nc = 95
        kind = "individual_stock"
    elif dt == "stock_summary":
        nc = 83
        kind = "stock_summary"
    elif dt == "indices":
        nc = 23
        kind = "indices"
    elif dt == "indices_summary":
        nc = 83
        kind = "indices_summary"
    else:
        raise ValueError(f"Unknown data_type: {dt}")

    schema_override = {f"column_{col+1}": pl.String for col in range(nc)}
    with zipfile.ZipFile(zpath, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pl.read_csv(
                f, has_header=False,
                schema_overrides=schema_override,
                truncate_ragged_lines=True,
            )

    if dt == "individual_stock":
        col_names = get_schema_individual_stock_95()
    elif dt == "stock_summary" or dt == "indices_summary":
        col_names = get_schema_summary_83()
    elif dt == "indices":
        col_names = get_schema_indices_23()

    rename_map = dict(zip(df.columns, col_names))
    df = df.rename(rename_map)
    df = clean_data(df, kind=kind, language="en")
    return df


# ---------------------------------------------------------------------------
# Main dispatch
# ---------------------------------------------------------------------------
if backend == "pandas":
    import pandas as pd
    import numpy as np
    t0 = time.perf_counter()
    if data_type == "individual_stock":
        df = pandas_pipeline_individual_stock(zip_path)
    else:
        df = pandas_pipeline_generic(zip_path, data_type)
    elapsed = time.perf_counter() - t0
    rows = len(df)
    cols = len(df.columns)

elif backend == "pandas-fair":
    import pandas as pd
    import numpy as np
    t0 = time.perf_counter()
    df = pandas_fair_pipeline(zip_path, data_type)
    elapsed = time.perf_counter() - t0
    rows = len(df)
    cols = len(df.columns)

elif backend == "polars":
    t0 = time.perf_counter()
    df = polars_pipeline(zip_path, data_type)
    elapsed = time.perf_counter() - t0
    rows = len(df)
    cols = len(df.columns)

else:
    print(json.dumps({"error": f"Unknown backend: {backend}"}))
    sys.exit(1)

peak_rss = proc.memory_info().peak_wset
result = {
    "backend": backend,
    "max_threads": max_threads,
    "data_type": data_type,
    "elapsed_s": round(elapsed, 4),
    "peak_rss_mb": round(peak_rss / (1024 * 1024), 1),
    "rows": rows,
    "cols": cols,
}
print(json.dumps(result))
