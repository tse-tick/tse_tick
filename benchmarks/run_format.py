"""
Format comparison benchmark: CSV / Parquet / Feather / Pickle.
Also includes query benchmark with Hive-partitioned Parquet vs CSV scan.

Usage: python run_format.py
"""
import csv
import os
import shutil
import sys
import time
import zipfile
from pathlib import Path
from statistics import median

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import pyarrow as pa
import pyarrow.feather as pf
import pyarrow.parquet as pq

BENCHMARKS_DIR = Path(__file__).parent
RAW_ROOT = Path(r"G:\flash_crash_pilot")
REPS = 5
WARMUP = 1

sys.path.insert(0, str(BENCHMARKS_DIR.parent))
from tse_tick.schemas import get_schema_individual_stock_95
from tse_tick.core import clean_data


def load_cleaned_df(zip_path):
    schema_override = {f"column_{col+1}": pl.String for col in range(95)}
    col_names = get_schema_individual_stock_95()
    with zipfile.ZipFile(zip_path, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pl.read_csv(f, has_header=False,
                             schema_overrides=schema_override,
                             truncate_ragged_lines=True)
    df = df.rename(dict(zip(df.columns, col_names)))
    df = clean_data(df, kind="individual_stock", language="en")
    return df


def find_multi_day_zips(year=2017, n_days=3):
    """Find the last (smallest) partition per day to keep memory manageable."""
    results = []
    seen_dates = set()
    for month in range(1, 13):
        month_dir = RAW_ROOT / f"raw_{year}" / f"{year}{month:02d}"
        if not month_dir.exists():
            continue
        all_zips = sorted(month_dir.glob("HTICST120.*.zip"))
        by_date = {}
        for z in all_zips:
            date_str = z.stem.split(".")[1]
            by_date.setdefault(date_str, []).append(z)
        for date_str in sorted(by_date.keys()):
            if date_str in seen_dates:
                continue
            seen_dates.add(date_str)
            partitions = by_date[date_str]
            results.append(partitions[-1])
            if len(seen_dates) >= n_days:
                return results
    return results


def timed_median(fn, reps=REPS, warmup=WARMUP):
    times = []
    result = None
    for _ in range(reps):
        t0 = time.perf_counter()
        result = fn()
        times.append(time.perf_counter() - t0)
    return round(median(times[warmup:]), 4), result


def main():
    zip_path = RAW_ROOT / "raw_2017" / "201701" / "HTICST120.20170104.1.zip"
    print(f"Loading cleaned DataFrame from {zip_path}...")
    df_pl = load_cleaned_df(zip_path)
    n_rows, n_cols = df_pl.shape
    print(f"  Loaded {n_rows:,} rows x {n_cols} cols")

    tmpdir = BENCHMARKS_DIR / "_tmp_formats"
    tmpdir.mkdir(exist_ok=True)

    sel_cols = ["Stock Code", "Execution Price", "Volume"]
    results = {}
    import gc

    # ── Parquet (Snappy) ── polars writes directly
    print("\nParquet (Snappy)...")
    pqs_path = tmpdir / "data_snappy.parquet"
    w_t, _ = timed_median(lambda: df_pl.write_parquet(pqs_path, compression="snappy"))
    size = pqs_path.stat().st_size
    r_t, _ = timed_median(lambda: pl.read_parquet(pqs_path))
    s_t, _ = timed_median(lambda: pl.read_parquet(pqs_path, columns=sel_cols))
    results["Parquet (Snappy)"] = {"size_mb": round(size / 1048576, 2), "write_s": w_t,
                                    "read_s": r_t, "selective_s": s_t}
    print(f"  {results['Parquet (Snappy)']}")

    # ── Parquet (Zstd) ──
    print("Parquet (Zstd)...")
    pqz_path = tmpdir / "data_zstd.parquet"
    w_t, _ = timed_median(lambda: df_pl.write_parquet(pqz_path, compression="zstd"))
    size = pqz_path.stat().st_size
    r_t, _ = timed_median(lambda: pl.read_parquet(pqz_path))
    s_t, _ = timed_median(lambda: pl.read_parquet(pqz_path, columns=sel_cols))
    results["Parquet (Zstd)"] = {"size_mb": round(size / 1048576, 2), "write_s": w_t,
                                  "read_s": r_t, "selective_s": s_t}
    print(f"  {results['Parquet (Zstd)']}")

    # ── Feather / Arrow IPC ── via polars→arrow
    print("Feather (Arrow IPC)...")
    ftr_path = tmpdir / "data.feather"
    arrow_table = df_pl.to_arrow()
    w_t, _ = timed_median(lambda: pf.write_feather(arrow_table, ftr_path))
    size = ftr_path.stat().st_size
    r_t, _ = timed_median(lambda: pf.read_table(ftr_path))
    s_t, _ = timed_median(lambda: pf.read_table(ftr_path, columns=sel_cols))
    results["Feather (IPC)"] = {"size_mb": round(size / 1048576, 2), "write_s": w_t,
                                 "read_s": r_t, "selective_s": s_t}
    print(f"  {results['Feather (IPC)']}")
    del arrow_table; gc.collect()

    # Convert to pandas for CSV/Pickle tests, release polars
    print("\nConverting to pandas for CSV/Pickle tests...")
    df_pd = df_pl.to_pandas()
    del df_pl; gc.collect()

    # ── CSV ──
    print("CSV (uncompressed)...")
    csv_path = tmpdir / "data.csv"
    w_t, _ = timed_median(lambda: df_pd.to_csv(csv_path, index=False))
    size = csv_path.stat().st_size
    r_t, _ = timed_median(lambda: pd.read_csv(csv_path))
    s_t, _ = timed_median(lambda: pd.read_csv(csv_path, usecols=sel_cols))
    results["CSV"] = {"size_mb": round(size / 1048576, 2), "write_s": w_t,
                       "read_s": r_t, "selective_s": s_t}
    print(f"  {results['CSV']}")

    # ── CSV.gz ──
    print("CSV.gz...")
    csvgz_path = tmpdir / "data.csv.gz"
    w_t, _ = timed_median(lambda: df_pd.to_csv(csvgz_path, index=False, compression="gzip"))
    size = csvgz_path.stat().st_size
    r_t, _ = timed_median(lambda: pd.read_csv(csvgz_path, compression="gzip"))
    s_t, _ = timed_median(lambda: pd.read_csv(csvgz_path, usecols=sel_cols, compression="gzip"))
    results["CSV.gz"] = {"size_mb": round(size / 1048576, 2), "write_s": w_t,
                          "read_s": r_t, "selective_s": s_t}
    print(f"  {results['CSV.gz']}")

    # ── Pickle ──
    print("Pickle...")
    pkl_path = tmpdir / "data.pkl"
    w_t, _ = timed_median(lambda: df_pd.to_pickle(pkl_path))
    size = pkl_path.stat().st_size
    r_t, _ = timed_median(lambda: pd.read_pickle(pkl_path))
    results["Pickle"] = {"size_mb": round(size / 1048576, 2), "write_s": w_t,
                          "read_s": r_t, "selective_s": None}
    print(f"  {results['Pickle']}")
    del df_pd; gc.collect()

    # Save format results
    fmt_csv = BENCHMARKS_DIR / "results_format.csv"
    with open(fmt_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["format", "size_mb", "write_median_s",
                                          "read_median_s", "selective_3col_median_s"])
        w.writeheader()
        for name, vals in results.items():
            w.writerow({"format": name, "size_mb": vals["size_mb"],
                        "write_median_s": vals["write_s"],
                        "read_median_s": vals["read_s"],
                        "selective_3col_median_s": vals["selective_s"]})
    print(f"\nFormat results saved to {fmt_csv}")

    # Cleanup format temp files
    shutil.rmtree(tmpdir, ignore_errors=True)

    # ─────────────────────────────────────────────────────────────
    # QUERY BENCHMARK
    # ─────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print("QUERY BENCHMARK: Hive Parquet + DuckDB vs CSV scan")
    print("=" * 60)

    multi_zips = find_multi_day_zips(2017, n_days=5)
    MAX_ROWS_PER_DAY = 500_000
    print(f"Loading {len(multi_zips)} ZIP files for multi-day store "
          f"(max {MAX_ROWS_PER_DAY:,} rows/day)...")

    schema_override = {f"column_{col+1}": pl.String for col in range(95)}
    col_names = get_schema_individual_stock_95()
    all_dfs = []
    for zp in multi_zips:
        print(f"  {zp.name}...")
        with zipfile.ZipFile(zp, "r") as zf:
            fname = zf.namelist()[0]
            with zf.open(fname) as f:
                df = pl.read_csv(f, has_header=False,
                                 schema_overrides=schema_override,
                                 truncate_ragged_lines=True,
                                 n_rows=MAX_ROWS_PER_DAY)
        df = df.rename(dict(zip(df.columns, col_names)))
        df = clean_data(df, kind="individual_stock", language="en")
        all_dfs.append(df)

    combined = pl.concat(all_dfs)
    total_rows = len(combined)
    print(f"Combined: {total_rows:,} rows")

    combined = combined.with_columns([
        pl.col("Data Date").dt.strftime("%Y%m%d").alias("date"),
        pl.col("Stock Code").str.slice(0, 4).alias("ticker"),
    ])

    tickers_7203 = combined.filter(pl.col("ticker") == "7203")
    if len(tickers_7203) > 0:
        target_ticker = "7203"
    else:
        target_ticker = combined["ticker"].unique().sort()[0]
    dates_sorted = combined["date"].unique().sort()
    target_date = dates_sorted[0]
    print(f"Query target: ticker={target_ticker}, date={target_date}, time 09:00-10:00")

    qdir = BENCHMARKS_DIR / "_tmp_query"
    qdir.mkdir(exist_ok=True)
    hive_root = qdir / "hive_store"
    csv_mono = qdir / "monolithic.csv"

    # Write Hive-partitioned Parquet
    print("Writing Hive-partitioned Parquet...")
    pa_table = combined.to_arrow()
    pq.write_to_dataset(pa_table, root_path=str(hive_root),
                        partition_cols=["date", "ticker"])
    hive_size = sum(f.stat().st_size for f in hive_root.rglob("*.parquet"))
    print(f"  Hive store: {hive_size / 1048576:.1f} MB")

    # Write monolithic CSV
    print("Writing monolithic CSV...")
    combined.write_csv(csv_mono)
    csv_size = csv_mono.stat().st_size
    print(f"  CSV: {csv_size / 1048576:.1f} MB")

    query_results = {}

    # DuckDB on Hive Parquet
    print("\nDuckDB + Hive Parquet...")
    hive_pattern = str(hive_root).replace("\\", "/") + "/**/*.parquet"
    q_times = []
    q_rows = 0
    for _ in range(REPS):
        con = duckdb.connect()
        t0 = time.perf_counter()
        r = con.execute(f"""
            SELECT * FROM read_parquet('{hive_pattern}', hive_partitioning=true)
            WHERE ticker = '{target_ticker}'
              AND date = '{target_date}'
              AND "Execution Time" >= '090000'
              AND "Execution Time" < '100000'
        """).fetchdf()
        q_times.append(time.perf_counter() - t0)
        q_rows = len(r)
        con.close()
    med_hive = round(median(q_times[WARMUP:]), 4)
    query_results["DuckDB+Hive Parquet"] = {"median_s": med_hive, "rows": q_rows}
    print(f"  Median: {med_hive}s  Rows: {q_rows}")

    # DuckDB scan CSV
    print("DuckDB + CSV scan...")
    csv_str = str(csv_mono).replace("\\", "/")
    q_times = []
    for _ in range(REPS):
        con = duckdb.connect()
        t0 = time.perf_counter()
        r = con.execute(f"""
            SELECT * FROM read_csv_auto('{csv_str}')
            WHERE ticker = '{target_ticker}'
              AND date = '{target_date}'
              AND "Execution Time" >= '090000'
              AND "Execution Time" < '100000'
        """).fetchdf()
        q_times.append(time.perf_counter() - t0)
        q_rows_csv = len(r)
        con.close()
    med_csv_ddb = round(median(q_times[WARMUP:]), 4)
    query_results["DuckDB+CSV scan"] = {"median_s": med_csv_ddb, "rows": q_rows_csv}
    print(f"  Median: {med_csv_ddb}s  Rows: {q_rows_csv}")

    # pandas CSV scan
    print("pandas CSV scan...")
    q_times = []
    for _ in range(REPS):
        t0 = time.perf_counter()
        df_csv = pd.read_csv(csv_mono, dtype={"date": str, "ticker": str,
                                               "Execution Time": str})
        mask = ((df_csv["ticker"] == target_ticker) &
                (df_csv["date"] == target_date) &
                (df_csv["Execution Time"] >= "090000") &
                (df_csv["Execution Time"] < "100000"))
        r_pd = df_csv[mask]
        q_times.append(time.perf_counter() - t0)
        q_rows_pd = len(r_pd)
    med_pd = round(median(q_times[WARMUP:]), 4)
    query_results["pandas CSV scan"] = {"median_s": med_pd, "rows": q_rows_pd}
    print(f"  Median: {med_pd}s  Rows: {q_rows_pd}")

    # Save query results
    q_csv = BENCHMARKS_DIR / "results_query.csv"
    with open(q_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["method", "median_s", "rows_returned",
                                          "total_rows", "speedup_vs_pandas_scan"])
        w.writeheader()
        for method, vals in query_results.items():
            speedup = round(med_pd / vals["median_s"], 1) if vals["median_s"] > 0 else None
            w.writerow({"method": method, "median_s": vals["median_s"],
                        "rows_returned": vals["rows"], "total_rows": total_rows,
                        "speedup_vs_pandas_scan": speedup})
    print(f"\nQuery results saved to {q_csv}")

    shutil.rmtree(qdir, ignore_errors=True)
    print("\nFormat + query benchmarks complete.")


if __name__ == "__main__":
    main()
