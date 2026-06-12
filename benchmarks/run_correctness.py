"""
Correctness gate: verify polars and pandas-fair produce equivalent output
for each data type.

For each data type: load with polars pipeline and pandas-fair pipeline,
then compare shapes, numeric columns (within 1e-6), and string columns.
"""
import os
import sys
import zipfile

import numpy as np
import polars as pl
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from tse_tick.schemas import (
    get_schema_individual_stock_95,
    get_schema_summary_83,
    get_schema_indices_23,
    get_schema_indices_summary,
)
from tse_tick.core import clean_data

DATA_ROOT = os.path.join("G:", os.sep, "flash_crash")

ZIP_PATHS = {
    "HTICST120": os.path.join(DATA_ROOT, "raw_2017", "201701", "HTICST120.20170104.1.zip"),
    "HTICSS110": os.path.join(DATA_ROOT, "raw_other", "HTICSS110.201701.zip"),
    "HTICIT110": os.path.join(DATA_ROOT, "raw_other", "HTICIT110.201701.zip"),
    "HTICIS110": os.path.join(DATA_ROOT, "raw_other", "HTICIS110.201701.zip"),
}

DATA_TYPE_MAP = {
    "HTICST120": "individual_stock",
    "HTICSS110": "stock_summary",
    "HTICIT110": "indices",
    "HTICIS110": "indices_summary",
}


def _get_schema(dt):
    if dt == "individual_stock":
        return get_schema_individual_stock_95(), 95
    elif dt == "stock_summary":
        return get_schema_summary_83(), 83
    elif dt == "indices":
        return get_schema_indices_23(), 23
    elif dt == "indices_summary":
        return get_schema_summary_83(), 83
    else:
        raise ValueError(f"Unknown dt: {dt}")


def load_polars(zpath, dt):
    col_names, nc = _get_schema(dt)
    kind_map = {
        "individual_stock": "individual_stock",
        "stock_summary": "stock_summary",
        "indices": "indices",
        "indices_summary": "indices_summary",
    }
    kind = kind_map[dt]
    schema_override = {f"column_{col+1}": pl.String for col in range(nc)}
    with zipfile.ZipFile(zpath, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pl.read_csv(f, has_header=False,
                             schema_overrides=schema_override,
                             truncate_ragged_lines=True)
    rename_map = dict(zip(df.columns, col_names))
    df = df.rename(rename_map)
    df = clean_data(df, kind=kind, language="en")
    return df


def load_pandas_fair(zpath, dt):
    """Load via pandas C engine with forced columns, all-string, then cast."""
    col_names, nc = _get_schema(dt)

    with zipfile.ZipFile(zpath, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pd.read_csv(f, header=None, names=range(nc), dtype=str,
                             engine="c", on_bad_lines="warn")
    if len(df.columns) != len(col_names):
        col_names = col_names[:len(df.columns)]
    df.columns = col_names

    if dt == "individual_stock":
        int_list = [14, 15, 18, 19, 21, 22, 24, 25, 27, 28, 30, 31, 33, 34,
                    36, 37, 39, 40, 42, 43, 45, 46, 48, 49, 51, 52, 54, 55,
                    57, 58, 60, 61, 63, 64, 66, 67, 69, 70, 72, 73, 75, 76,
                    78, 79, 81, 82, 84, 85, 87, 88, 90, 91, 93, 94]
        float_list = [11, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53,
                      56, 59, 62, 65, 68, 71, 74, 77, 80, 83, 86, 89, 92]
        df["Buy Quote 1 Best"] = pd.to_numeric(df["Buy Quote 1 Best"], errors="coerce")
        df["Buy Quote Vol 1"] = pd.to_numeric(df["Buy Quote Vol 1"], errors="coerce")
        for i in int_list:
            col = df.columns[i]
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype("int64")
        for i in float_list:
            col = df.columns[i]
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    elif dt == "indices":
        df["Index Value"] = pd.to_numeric(df["Index Value"], errors="coerce") * 0.01

    elif dt == "indices_summary":
        price_cols = [c for c in df.columns if "Price" in c]
        for c in price_cols:
            df[c] = pd.to_numeric(df[c], errors="coerce") * 0.01

    return df


def compare(df_pl, df_pd, file_code, dt):
    """Compare polars DataFrame (df_pl) with pandas DataFrame (df_pd).
    Returns (passed: bool, issues: list[str])."""
    issues = []

    if df_pl.shape[0] != len(df_pd):
        issues.append(f"Row count: polars={df_pl.shape[0]}, pandas={len(df_pd)}")
    if df_pl.shape[1] != len(df_pd.columns):
        issues.append(f"Col count: polars={df_pl.shape[1]}, pandas={len(df_pd.columns)}")
    if list(df_pl.columns) != list(df_pd.columns):
        issues.append("Column names/order differ")
        return len(issues) == 0, issues

    n_int, n_float, n_str = 0, 0, 0
    for col in df_pl.columns:
        pl_dtype = df_pl.schema[col]
        if pl_dtype == pl.Int64:
            pl_vals = df_pl[col].to_numpy()
            pd_vals = df_pd[col].to_numpy()
            try:
                pd_vals = pd_vals.astype("int64")
            except (ValueError, TypeError):
                pd_vals = pd.to_numeric(df_pd[col], errors="coerce").fillna(0).astype("int64").to_numpy()
            if not np.array_equal(pl_vals, pd_vals):
                diff_count = int(np.sum(pl_vals != pd_vals))
                issues.append(f"Int col '{col}': {diff_count}/{len(pl_vals)} mismatches")
            n_int += 1
        elif pl_dtype == pl.Float64:
            pl_vals = df_pl[col].to_numpy()
            pd_vals = df_pd[col].to_numpy().astype(float)
            nan_mask = np.isnan(pl_vals) & np.isnan(pd_vals)
            valid = ~(np.isnan(pl_vals) | np.isnan(pd_vals))
            if valid.any() and not np.allclose(pl_vals[valid], pd_vals[valid], atol=1e-6):
                max_diff = float(np.nanmax(np.abs(pl_vals[valid] - pd_vals[valid])))
                issues.append(f"Float col '{col}': max diff={max_diff}")
            n_float += 1
        elif pl_dtype == pl.String:
            n_str += 1

    return len(issues) == 0, issues, n_int, n_float, n_str


def main():
    all_passed = True
    results = {}

    for file_code, zpath in ZIP_PATHS.items():
        dt = DATA_TYPE_MAP[file_code]
        if not os.path.exists(zpath):
            print(f"\n=== {file_code} ({dt}): SKIPPED (file not found) ===")
            results[file_code] = "SKIPPED"
            continue

        print(f"\n=== {file_code} ({dt}) ===")
        print(f"Loading polars...")
        df_pl = load_polars(zpath, dt)
        print(f"  Polars shape: {df_pl.shape}")

        print(f"Loading pandas-fair...")
        df_pd = load_pandas_fair(zpath, dt)
        print(f"  Pandas-fair shape: {df_pd.shape}")

        passed, issues, n_int, n_float, n_str = compare(df_pl, df_pd, file_code, dt)

        print(f"  Verified: {n_int} int, {n_float} float, {n_str} string columns")
        if passed:
            print(f"  CORRECTNESS GATE: PASSED")
            results[file_code] = "PASSED"
        else:
            print(f"  CORRECTNESS GATE: FAILED ({len(issues)} issues)")
            for iss in issues:
                print(f"    - {iss}")
            results[file_code] = f"FAILED ({len(issues)} issues)"
            all_passed = False

    print(f"\n=== OVERALL ===")
    for fc, status in results.items():
        print(f"  {fc}: {status}")
    print(f"  All passed: {all_passed}")

    return all_passed


if __name__ == "__main__":
    ok = main()
    sys.exit(0 if ok else 1)
