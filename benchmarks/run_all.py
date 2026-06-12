"""
tse_tick benchmark suite.

Runs:
  Part 1: Input discovery + validation
  Part 2: Polars vs pandas engine benchmark (subprocess-isolated)
  Part 3: Format comparison (CSV / Parquet / Feather / Pickle) + query bench
  Part 4: Saves all CSV results

Usage:  python run_all.py
"""
import csv
import gzip
import io
import json
import os
import pickle
import shutil
import subprocess
import sys
import tempfile
import time
import zipfile
from pathlib import Path
from statistics import median

import duckdb
import numpy as np
import pandas as pd
import polars as pl
import psutil
import pyarrow as pa
import pyarrow.feather as pf
import pyarrow.parquet as pq

BENCHMARKS_DIR = Path(__file__).parent
RAW_ROOT = Path(r"G:\flash_crash_pilot")
WORKER = BENCHMARKS_DIR / "worker_engine.py"
RESULTS_DIR = BENCHMARKS_DIR
REPS = 7
WARMUP = 1
FORMAT_REPS = 5
FORMAT_WARMUP = 1

sys.path.insert(0, str(BENCHMARKS_DIR.parent))
from tse_tick.schemas import get_schema_individual_stock_95
from tse_tick.core import clean_data


def find_representative_zip(year=2017):
    """Find first partition of a normal trading day."""
    for month in range(1, 13):
        month_dir = RAW_ROOT / f"raw_{year}" / f"{year}{month:02d}"
        if not month_dir.exists():
            continue
        zips = sorted(month_dir.glob("HTICST120.*.1.zip"))
        if zips:
            return zips[0]
    return None


def find_multi_day_zips(year=2017, n_days=5):
    """Find first partition for several consecutive trading days."""
    results = []
    for month in range(1, 13):
        month_dir = RAW_ROOT / f"raw_{year}" / f"{year}{month:02d}"
        if not month_dir.exists():
            continue
        all_zips = sorted(month_dir.glob("HTICST120.*.1.zip"))
        for z in all_zips:
            results.append(z)
            if len(results) >= n_days:
                return results
    return results


def validate_zip(zip_path):
    """Test ZIP integrity and return row count."""
    schema_override = {f"column_{col+1}": pl.String for col in range(95)}
    with zipfile.ZipFile(zip_path, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pl.read_csv(
                f, has_header=False,
                schema_overrides=schema_override,
                truncate_ragged_lines=True,
            )
    return len(df), len(df.columns)


def run_worker(backend, zip_path, max_threads=0):
    """Run a single engine benchmark in a subprocess. Returns dict."""
    cmd = [sys.executable, str(WORKER), backend, str(zip_path)]
    if max_threads > 0:
        cmd.append(str(max_threads))
    result = subprocess.run(
        cmd, capture_output=True, text=True, timeout=600,
        cwd=str(BENCHMARKS_DIR),
    )
    if result.returncode != 0:
        print(f"  WORKER ERROR: {result.stderr[:500]}")
        return None
    try:
        return json.loads(result.stdout.strip().split("\n")[-1])
    except (json.JSONDecodeError, IndexError):
        print(f"  PARSE ERROR: {result.stdout[:500]}")
        return None


def load_polars_df(zip_path):
    """Load and clean a ZIP via the polars pipeline (in-process)."""
    schema_override = {f"column_{col+1}": pl.String for col in range(95)}
    with zipfile.ZipFile(zip_path, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pl.read_csv(
                f, has_header=False,
                schema_overrides=schema_override,
                truncate_ragged_lines=True,
            )
    col_names = get_schema_individual_stock_95()
    rename_map = dict(zip(df.columns, col_names))
    df = df.rename(rename_map)
    df = clean_data(df, kind="individual_stock", language="en")
    return df


# ─────────────────────────────────────────────────────────────────────
# PART 1: Input discovery
# ─────────────────────────────────────────────────────────────────────
def part1():
    print("=" * 60)
    print("PART 1: Input Discovery")
    print("=" * 60)

    zip_path = find_representative_zip(2017)
    if zip_path is None:
        print("ABORT: No HTICST120 ZIP found for 2017")
        sys.exit(1)

    print(f"Representative ZIP: {zip_path}")
    print(f"  Size: {zip_path.stat().st_size / (1024*1024):.1f} MB")

    rows, cols = validate_zip(zip_path)
    print(f"  Rows: {rows:,}  Cols: {cols}")

    if cols != 95:
        print(f"  WARNING: Expected 95 columns, got {cols}")

    print()
    print("Data type coverage:")
    print("  HTICST120 (individual stock, 95 cols): AVAILABLE")
    print("  TICSS110  (stock summary, 82 cols):    NOT AVAILABLE (no files)")
    print("  TICIT110  (index ticks, 10 cols):      NOT AVAILABLE (no files)")
    print("  TICIS110  (index summary, 17 cols):    NOT AVAILABLE (no files)")
    print()
    print("NOTE: Only HTICST120 data exists under G:\\flash_crash_pilot\\raw_*.")
    print("      Benchmarks will run on this single data type.")
    print()
    return zip_path


# ─────────────────────────────────────────────────────────────────────
# PART 2: Engine benchmark (polars vs pandas)
# ─────────────────────────────────────────────────────────────────────
def part2(zip_path):
    print("=" * 60)
    print("PART 2: Polars vs Pandas Engine Benchmark")
    print("=" * 60)

    conditions = [
        ("pandas", 0),
        ("polars", 0),
        ("polars", 1),
    ]

    raw_rows = []
    for backend, threads in conditions:
        label = f"{backend}" + (f"-{threads}thread" if threads else "-default")
        print(f"\n  Condition: {label}  ({REPS} reps, discard first {WARMUP})")

        for rep in range(REPS):
            result = run_worker(backend, zip_path, threads)
            if result is None:
                print(f"    Rep {rep+1}: FAILED")
                raw_rows.append({
                    "data_type": "HTICST120",
                    "condition": label,
                    "rep": rep + 1,
                    "elapsed_s": None,
                    "peak_rss_mb": None,
                    "rows": None,
                    "cols": None,
                })
                continue
            print(f"    Rep {rep+1}: {result['elapsed_s']:.3f}s  "
                  f"RSS={result['peak_rss_mb']:.0f}MB  "
                  f"rows={result['rows']:,}")
            raw_rows.append({
                "data_type": "HTICST120",
                "condition": label,
                "rep": rep + 1,
                "elapsed_s": result["elapsed_s"],
                "peak_rss_mb": result["peak_rss_mb"],
                "rows": result["rows"],
                "cols": result["cols"],
            })

    raw_csv = RESULTS_DIR / "results_engine.csv"
    with open(raw_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data_type", "condition", "rep",
                                          "elapsed_s", "peak_rss_mb", "rows", "cols"])
        w.writeheader()
        w.writerows(raw_rows)
    print(f"\n  Raw results saved to {raw_csv}")

    summary_rows = []
    for backend, threads in conditions:
        label = f"{backend}" + (f"-{threads}thread" if threads else "-default")
        times = [r["elapsed_s"] for r in raw_rows
                 if r["condition"] == label and r["elapsed_s"] is not None]
        mems = [r["peak_rss_mb"] for r in raw_rows
                if r["condition"] == label and r["peak_rss_mb"] is not None]
        if len(times) <= WARMUP:
            continue
        times_trimmed = times[WARMUP:]
        mems_trimmed = mems[WARMUP:]
        summary_rows.append({
            "data_type": "HTICST120",
            "condition": label,
            "median_s": round(median(times_trimmed), 3),
            "min_s": round(min(times_trimmed), 3),
            "max_s": round(max(times_trimmed), 3),
            "median_rss_mb": round(median(mems_trimmed), 1),
            "n_measured": len(times_trimmed),
        })

    pd_time = next((r["median_s"] for r in summary_rows if r["condition"] == "pandas-default"), None)
    for r in summary_rows:
        if pd_time and pd_time > 0:
            r["speedup_vs_pandas"] = round(pd_time / r["median_s"], 2)
        else:
            r["speedup_vs_pandas"] = None

    sum_csv = RESULTS_DIR / "results_engine_summary.csv"
    with open(sum_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data_type", "condition", "median_s",
                                          "min_s", "max_s", "median_rss_mb",
                                          "n_measured", "speedup_vs_pandas"])
        w.writeheader()
        w.writerows(summary_rows)
    print(f"  Summary saved to {sum_csv}")

    print("\n  SUMMARY:")
    for r in summary_rows:
        print(f"    {r['condition']:20s}  median={r['median_s']:.3f}s  "
              f"RSS={r['median_rss_mb']:.0f}MB  speedup={r['speedup_vs_pandas']}")

    return summary_rows


# ─────────────────────────────────────────────────────────────────────
# CORRECTNESS GATE
# ─────────────────────────────────────────────────────────────────────
def correctness_gate(zip_path):
    print("\n" + "=" * 60)
    print("CORRECTNESS GATE")
    print("=" * 60)

    schema_override = {f"column_{col+1}": pl.String for col in range(95)}
    col_names = get_schema_individual_stock_95()

    with zipfile.ZipFile(zip_path, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df_pl = pl.read_csv(
                f, has_header=False,
                schema_overrides=schema_override,
                truncate_ragged_lines=True,
            )
    df_pl = df_pl.rename(dict(zip(df_pl.columns, col_names)))
    df_pl = clean_data(df_pl, kind="individual_stock", language="en")

    dtype_dict = {col: str for col in range(10)}
    with zipfile.ZipFile(zip_path, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df_pd = pd.read_csv(f, header=None, dtype=dtype_dict, engine="python")
    df_pd.columns = col_names

    df_pd["Buy Quote 1 Best"] = df_pd["Buy Quote 1 Best"].astype(float)
    df_pd["Buy Quote Vol 1"] = df_pd["Buy Quote Vol 1"].astype(float)
    for ci in [6, 7, 8]:
        col = df_pd.columns[ci]
        df_pd[col] = df_pd[col].fillna(pd.NaT)
    int_list = [14, 15, 18, 19, 21, 22, 24, 25, 27, 28, 30, 31, 33, 34,
                36, 37, 39, 40, 42, 43, 45, 46, 48, 49, 51, 52, 54, 55,
                57, 58, 60, 61, 63, 64, 66, 67, 69, 70, 72, 73, 75, 76,
                78, 79, 81, 82, 84, 85, 87, 88, 90, 91, 93, 94]
    for i in int_list:
        col = df_pd.columns[i]
        df_pd[col] = df_pd[col].fillna(0).astype(int)
    float_list = [11, 17, 20, 23, 26, 29, 32, 35, 38, 41, 44, 47, 50, 53,
                  56, 59, 62, 65, 68, 71, 74, 77, 80, 83, 86, 89, 92]
    for i in float_list:
        col = df_pd.columns[i]
        df_pd[col] = df_pd[col].fillna(0.0)

    passed = True
    issues = []

    if df_pl.shape[0] != df_pd.shape[0]:
        issues.append(f"Row count mismatch: polars={df_pl.shape[0]}, pandas={df_pd.shape[0]}")
        passed = False

    if df_pl.shape[1] != df_pd.shape[1]:
        issues.append(f"Column count mismatch: polars={df_pl.shape[1]}, pandas={df_pd.shape[1]}")
        passed = False

    if list(df_pl.columns) != list(df_pd.columns):
        issues.append("Column order mismatch")
        passed = False

    n_checked = 0
    for col in df_pl.columns:
        pl_dtype = df_pl.schema[col]
        if pl_dtype in (pl.Float64,):
            pl_vals = df_pl[col].to_numpy()
            pd_vals = df_pd[col].to_numpy().astype(float)
            mask = ~(np.isnan(pl_vals) & np.isnan(pd_vals))
            if not np.allclose(pl_vals[mask], pd_vals[mask], atol=1e-6, equal_nan=True):
                issues.append(f"Float column '{col}' values differ")
                passed = False
            n_checked += 1
        elif pl_dtype in (pl.Int64,):
            pl_vals = df_pl[col].to_numpy()
            pd_vals = df_pd[col].to_numpy()
            if not np.array_equal(pl_vals, pd_vals):
                issues.append(f"Int column '{col}' values differ")
                passed = False
            n_checked += 1

    if passed:
        print(f"  PASSED — shape {df_pl.shape}, {n_checked} numeric columns verified.")
    else:
        print(f"  FAILED — {len(issues)} issue(s):")
        for iss in issues:
            print(f"    - {iss}")

    return passed


# ─────────────────────────────────────────────────────────────────────
# PART 3: Format comparison
# ─────────────────────────────────────────────────────────────────────
def part3_formats(zip_path):
    print("\n" + "=" * 60)
    print("PART 3a: Format Size / Write / Read Comparison")
    print("=" * 60)

    df_pl = load_polars_df(zip_path)
    df_pd = df_pl.to_pandas()
    arrow_table = df_pl.to_arrow()
    n_rows = len(df_pl)
    print(f"  Loaded {n_rows:,} rows × {df_pl.shape[1]} cols for format tests.")

    tmpdir = BENCHMARKS_DIR / "_tmp_formats"
    tmpdir.mkdir(exist_ok=True)

    format_results = []

    def bench_write_read(fmt_name, write_fn, read_fn, selective_fn=None):
        write_times = []
        for _ in range(FORMAT_REPS):
            t0 = time.perf_counter()
            write_fn()
            write_times.append(time.perf_counter() - t0)
        write_times = write_times[FORMAT_WARMUP:]

        fpath = write_fn.__defaults__(None) if hasattr(write_fn, "__defaults__") else None
        size_bytes = 0

        read_times = []
        for _ in range(FORMAT_REPS):
            t0 = time.perf_counter()
            read_fn()
            read_times.append(time.perf_counter() - t0)
        read_times = read_times[FORMAT_WARMUP:]

        sel_times = []
        if selective_fn:
            for _ in range(FORMAT_REPS):
                t0 = time.perf_counter()
                selective_fn()
                sel_times.append(time.perf_counter() - t0)
            sel_times = sel_times[FORMAT_WARMUP:]

        return {
            "write_median": round(median(write_times), 4),
            "read_median": round(median(read_times), 4),
            "selective_median": round(median(sel_times), 4) if sel_times else None,
        }

    sel_cols = ["Stock Code", "Execution Price", "Volume"]

    csv_path = tmpdir / "data.csv"
    csvgz_path = tmpdir / "data.csv.gz"
    parquet_snappy_path = tmpdir / "data_snappy.parquet"
    parquet_zstd_path = tmpdir / "data_zstd.parquet"
    feather_path = tmpdir / "data.feather"
    pickle_path = tmpdir / "data.pkl"

    formats = {}

    # CSV
    print("  Testing CSV...")
    def write_csv():
        df_pd.to_csv(csv_path, index=False)
    def read_csv():
        return pd.read_csv(csv_path)
    def sel_csv():
        return pd.read_csv(csv_path, usecols=sel_cols)
    write_csv()
    size_csv = csv_path.stat().st_size
    times_w, times_r, times_s = [], [], []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); write_csv(); times_w.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); read_csv(); times_r.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); sel_csv(); times_s.append(time.perf_counter() - t0)
    formats["CSV"] = {
        "size_mb": round(size_csv / (1024*1024), 2),
        "write_median": round(median(times_w[FORMAT_WARMUP:]), 4),
        "read_median": round(median(times_r[FORMAT_WARMUP:]), 4),
        "selective_median": round(median(times_s[FORMAT_WARMUP:]), 4),
    }
    print(f"    Size={formats['CSV']['size_mb']} MB  "
          f"Write={formats['CSV']['write_median']:.3f}s  "
          f"Read={formats['CSV']['read_median']:.3f}s  "
          f"Selective={formats['CSV']['selective_median']:.3f}s")

    # CSV.gz
    print("  Testing CSV.gz...")
    def write_csvgz():
        df_pd.to_csv(csvgz_path, index=False, compression="gzip")
    def read_csvgz():
        return pd.read_csv(csvgz_path, compression="gzip")
    def sel_csvgz():
        return pd.read_csv(csvgz_path, usecols=sel_cols, compression="gzip")
    write_csvgz()
    size_csvgz = csvgz_path.stat().st_size
    times_w, times_r, times_s = [], [], []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); write_csvgz(); times_w.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); read_csvgz(); times_r.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); sel_csvgz(); times_s.append(time.perf_counter() - t0)
    formats["CSV.gz"] = {
        "size_mb": round(size_csvgz / (1024*1024), 2),
        "write_median": round(median(times_w[FORMAT_WARMUP:]), 4),
        "read_median": round(median(times_r[FORMAT_WARMUP:]), 4),
        "selective_median": round(median(times_s[FORMAT_WARMUP:]), 4),
    }
    print(f"    Size={formats['CSV.gz']['size_mb']} MB  "
          f"Write={formats['CSV.gz']['write_median']:.3f}s  "
          f"Read={formats['CSV.gz']['read_median']:.3f}s  "
          f"Selective={formats['CSV.gz']['selective_median']:.3f}s")

    # Parquet (Snappy)
    print("  Testing Parquet (Snappy)...")
    def write_pq_snappy():
        df_pl.write_parquet(parquet_snappy_path, compression="snappy")
    def read_pq_snappy():
        return pl.read_parquet(parquet_snappy_path)
    def sel_pq_snappy():
        return pl.read_parquet(parquet_snappy_path, columns=sel_cols)
    write_pq_snappy()
    size_pqs = parquet_snappy_path.stat().st_size
    times_w, times_r, times_s = [], [], []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); write_pq_snappy(); times_w.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); read_pq_snappy(); times_r.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); sel_pq_snappy(); times_s.append(time.perf_counter() - t0)
    formats["Parquet (Snappy)"] = {
        "size_mb": round(size_pqs / (1024*1024), 2),
        "write_median": round(median(times_w[FORMAT_WARMUP:]), 4),
        "read_median": round(median(times_r[FORMAT_WARMUP:]), 4),
        "selective_median": round(median(times_s[FORMAT_WARMUP:]), 4),
    }
    print(f"    Size={formats['Parquet (Snappy)']['size_mb']} MB  "
          f"Write={formats['Parquet (Snappy)']['write_median']:.3f}s  "
          f"Read={formats['Parquet (Snappy)']['read_median']:.3f}s  "
          f"Selective={formats['Parquet (Snappy)']['selective_median']:.3f}s")

    # Parquet (Zstd)
    print("  Testing Parquet (Zstd)...")
    def write_pq_zstd():
        df_pl.write_parquet(parquet_zstd_path, compression="zstd")
    def read_pq_zstd():
        return pl.read_parquet(parquet_zstd_path)
    def sel_pq_zstd():
        return pl.read_parquet(parquet_zstd_path, columns=sel_cols)
    write_pq_zstd()
    size_pqz = parquet_zstd_path.stat().st_size
    times_w, times_r, times_s = [], [], []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); write_pq_zstd(); times_w.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); read_pq_zstd(); times_r.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); sel_pq_zstd(); times_s.append(time.perf_counter() - t0)
    formats["Parquet (Zstd)"] = {
        "size_mb": round(size_pqz / (1024*1024), 2),
        "write_median": round(median(times_w[FORMAT_WARMUP:]), 4),
        "read_median": round(median(times_r[FORMAT_WARMUP:]), 4),
        "selective_median": round(median(times_s[FORMAT_WARMUP:]), 4),
    }
    print(f"    Size={formats['Parquet (Zstd)']['size_mb']} MB  "
          f"Write={formats['Parquet (Zstd)']['write_median']:.3f}s  "
          f"Read={formats['Parquet (Zstd)']['read_median']:.3f}s  "
          f"Selective={formats['Parquet (Zstd)']['selective_median']:.3f}s")

    # Feather / Arrow IPC
    print("  Testing Feather (Arrow IPC)...")
    def write_feather():
        pf.write_feather(arrow_table, feather_path)
    def read_feather():
        return pf.read_table(feather_path)
    def sel_feather():
        return pf.read_table(feather_path, columns=sel_cols)
    write_feather()
    size_ftr = feather_path.stat().st_size
    times_w, times_r, times_s = [], [], []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); write_feather(); times_w.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); read_feather(); times_r.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); sel_feather(); times_s.append(time.perf_counter() - t0)
    formats["Feather (IPC)"] = {
        "size_mb": round(size_ftr / (1024*1024), 2),
        "write_median": round(median(times_w[FORMAT_WARMUP:]), 4),
        "read_median": round(median(times_r[FORMAT_WARMUP:]), 4),
        "selective_median": round(median(times_s[FORMAT_WARMUP:]), 4),
    }
    print(f"    Size={formats['Feather (IPC)']['size_mb']} MB  "
          f"Write={formats['Feather (IPC)']['write_median']:.3f}s  "
          f"Read={formats['Feather (IPC)']['read_median']:.3f}s  "
          f"Selective={formats['Feather (IPC)']['selective_median']:.3f}s")

    # Pickle
    print("  Testing Pickle...")
    def write_pickle():
        df_pd.to_pickle(pickle_path)
    def read_pickle():
        return pd.read_pickle(pickle_path)
    write_pickle()
    size_pkl = pickle_path.stat().st_size
    times_w, times_r = [], []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); write_pickle(); times_w.append(time.perf_counter() - t0)
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter(); read_pickle(); times_r.append(time.perf_counter() - t0)
    formats["Pickle"] = {
        "size_mb": round(size_pkl / (1024*1024), 2),
        "write_median": round(median(times_w[FORMAT_WARMUP:]), 4),
        "read_median": round(median(times_r[FORMAT_WARMUP:]), 4),
        "selective_median": None,
    }
    print(f"    Size={formats['Pickle']['size_mb']} MB  "
          f"Write={formats['Pickle']['write_median']:.3f}s  "
          f"Read={formats['Pickle']['read_median']:.3f}s  "
          f"Selective=N/A")

    # Save results
    fmt_csv = RESULTS_DIR / "results_format.csv"
    with open(fmt_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["format", "size_mb", "write_median_s",
                                          "read_median_s", "selective_3col_median_s"])
        w.writeheader()
        for name, vals in formats.items():
            w.writerow({
                "format": name,
                "size_mb": vals["size_mb"],
                "write_median_s": vals["write_median"],
                "read_median_s": vals["read_median"],
                "selective_3col_median_s": vals["selective_median"],
            })
    print(f"\n  Format results saved to {fmt_csv}")

    # Cleanup
    shutil.rmtree(tmpdir, ignore_errors=True)
    return formats


# ─────────────────────────────────────────────────────────────────────
# PART 3b: Query benchmark (Parquet + DuckDB vs CSV scan)
# ─────────────────────────────────────────────────────────────────────
def part3_query(zip_paths_multi):
    print("\n" + "=" * 60)
    print("PART 3b: Query Benchmark (Partition Pruning + Pushdown)")
    print("=" * 60)

    tmpdir = BENCHMARKS_DIR / "_tmp_query"
    tmpdir.mkdir(exist_ok=True)
    hive_root = tmpdir / "hive_store"
    csv_mono = tmpdir / "monolithic.csv"

    print(f"  Loading {len(zip_paths_multi)} days of HTICST120 data...")

    schema_override = {f"column_{col+1}": pl.String for col in range(95)}
    col_names = get_schema_individual_stock_95()
    all_dfs = []
    date_ticker_sample = None

    for zp in zip_paths_multi:
        with zipfile.ZipFile(zp, "r") as zf:
            fname = zf.namelist()[0]
            with zf.open(fname) as f:
                df = pl.read_csv(
                    f, has_header=False,
                    schema_overrides=schema_override,
                    truncate_ragged_lines=True,
                )
        df = df.rename(dict(zip(df.columns, col_names)))
        df = clean_data(df, kind="individual_stock", language="en")
        all_dfs.append(df)

    combined = pl.concat(all_dfs)
    total_rows = len(combined)
    print(f"  Combined: {total_rows:,} rows across {len(zip_paths_multi)} days")

    dates = combined["Data Date"].unique().sort()
    tickers_7203 = combined.filter(pl.col("Stock Code").str.starts_with("7203"))
    if len(tickers_7203) > 0:
        target_ticker = "7203"
    else:
        first_ticker = combined["Stock Code"].head(1)[0][:4]
        target_ticker = first_ticker
        print(f"  Ticker 7203 not found, using {target_ticker} instead.")

    target_date = dates[0]
    target_date_str = target_date.strftime("%Y-%m-%d") if hasattr(target_date, 'strftime') else str(target_date)[:10]
    print(f"  Query target: ticker={target_ticker}, date={target_date_str}, 09:00-10:00")

    combined_with_parts = combined.with_columns([
        pl.col("Data Date").dt.strftime("%Y%m%d").alias("date"),
        pl.col("Stock Code").str.slice(0, 4).alias("ticker"),
    ])

    # Write Hive-partitioned Parquet
    print("  Writing Hive-partitioned Parquet store...")
    combined_with_parts.write_parquet(
        hive_root / "data.parquet",
        use_pyarrow=True,
    )
    pa_table = combined_with_parts.to_arrow()
    pq.write_to_dataset(
        pa_table,
        root_path=str(hive_root),
        partition_cols=["date", "ticker"],
    )
    hive_size = sum(f.stat().st_size for f in hive_root.rglob("*.parquet"))
    print(f"    Hive store size: {hive_size / (1024*1024):.1f} MB")

    # Write monolithic CSV
    print("  Writing monolithic CSV...")
    combined_with_parts.write_csv(csv_mono)
    csv_size = csv_mono.stat().st_size
    print(f"    CSV size: {csv_size / (1024*1024):.1f} MB")

    # Remove the flat parquet file (keep only hive structure)
    flat_pq = hive_root / "data.parquet"
    if flat_pq.exists():
        flat_pq.unlink()

    query_results = {}
    target_date_str_compact = target_date.strftime("%Y%m%d") if hasattr(target_date, 'strftime') else str(target_date)[:10].replace("-", "")

    # Query via DuckDB on Hive Parquet
    print("\n  Querying: DuckDB on Hive-partitioned Parquet...")
    q_times = []
    q_rows_out = 0
    for _ in range(FORMAT_REPS):
        con = duckdb.connect()
        t0 = time.perf_counter()
        result = con.execute(f"""
            SELECT *
            FROM read_parquet('{str(hive_root)}/**/*.parquet', hive_partitioning=true)
            WHERE ticker = '{target_ticker}'
              AND date = '{target_date_str_compact}'
              AND "Execution Time" >= '090000'
              AND "Execution Time" < '100000'
        """).fetchdf()
        q_times.append(time.perf_counter() - t0)
        q_rows_out = len(result)
        con.close()
    q_times = q_times[FORMAT_WARMUP:]
    query_results["DuckDB+Hive Parquet"] = {
        "median_s": round(median(q_times), 4),
        "rows_returned": q_rows_out,
    }
    print(f"    Median: {median(q_times):.4f}s  Rows returned: {q_rows_out}")

    # Query via DuckDB scanning monolithic CSV
    print("  Querying: DuckDB scanning monolithic CSV...")
    q_times = []
    for _ in range(FORMAT_REPS):
        con = duckdb.connect()
        t0 = time.perf_counter()
        result = con.execute(f"""
            SELECT *
            FROM read_csv_auto('{str(csv_mono)}')
            WHERE ticker = '{target_ticker}'
              AND date = '{target_date_str_compact}'
              AND "Execution Time" >= '090000'
              AND "Execution Time" < '100000'
        """).fetchdf()
        q_times.append(time.perf_counter() - t0)
        q_rows_csv = len(result)
        con.close()
    q_times = q_times[FORMAT_WARMUP:]
    query_results["DuckDB+CSV scan"] = {
        "median_s": round(median(q_times), 4),
        "rows_returned": q_rows_csv,
    }
    print(f"    Median: {median(q_times):.4f}s  Rows returned: {q_rows_csv}")

    # Query via pandas CSV scan
    print("  Querying: pandas full CSV scan...")
    q_times = []
    for _ in range(FORMAT_REPS):
        t0 = time.perf_counter()
        df_csv = pd.read_csv(csv_mono)
        mask = (
            (df_csv["ticker"] == target_ticker) &
            (df_csv["date"] == int(target_date_str_compact)) &
            (df_csv["Execution Time"] >= "090000") &
            (df_csv["Execution Time"] < "100000")
        )
        result_pd = df_csv[mask]
        q_times.append(time.perf_counter() - t0)
        q_rows_pdscan = len(result_pd)
    q_times = q_times[FORMAT_WARMUP:]
    query_results["pandas CSV scan"] = {
        "median_s": round(median(q_times), 4),
        "rows_returned": q_rows_pdscan,
    }
    print(f"    Median: {median(q_times):.4f}s  Rows returned: {q_rows_pdscan}")

    # Save query results
    q_csv = RESULTS_DIR / "results_query.csv"
    with open(q_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["method", "median_s", "rows_returned",
                                          "total_rows", "speedup_vs_csv_scan"])
        w.writeheader()
        csv_scan_time = query_results["pandas CSV scan"]["median_s"]
        for method, vals in query_results.items():
            speedup = round(csv_scan_time / vals["median_s"], 2) if vals["median_s"] > 0 else None
            w.writerow({
                "method": method,
                "median_s": vals["median_s"],
                "rows_returned": vals["rows_returned"],
                "total_rows": total_rows,
                "speedup_vs_csv_scan": speedup,
            })
    print(f"\n  Query results saved to {q_csv}")

    # Cleanup
    shutil.rmtree(tmpdir, ignore_errors=True)
    return query_results, total_rows


# ─────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    t_start = time.perf_counter()

    zip_path = part1()

    engine_summary = part2(zip_path)

    gate_passed = correctness_gate(zip_path)

    format_results = part3_formats(zip_path)

    multi_zips = find_multi_day_zips(2017, n_days=5)
    if len(multi_zips) >= 2:
        query_results, total_query_rows = part3_query(multi_zips)
    else:
        print("\nSKIPPING query benchmark — not enough multi-day files found.")
        query_results = {}
        total_query_rows = 0

    elapsed_total = time.perf_counter() - t_start
    print(f"\n{'='*60}")
    print(f"ALL BENCHMARKS COMPLETE in {elapsed_total:.0f}s")
    print(f"{'='*60}")
    print(f"  Correctness gate: {'PASSED' if gate_passed else 'FAILED'}")
    print(f"  Results files:")
    for f in RESULTS_DIR.glob("results_*.csv"):
        print(f"    {f.name}")
