"""
Re-run query benchmark using the FIRST partition (which has morning trades).
Loads 3 days × first partition, builds Hive store, queries for Toyota (7203).
"""
import csv, os, shutil, sys, time, zipfile
from pathlib import Path
from statistics import median

import duckdb, pandas as pd, polars as pl
import pyarrow.parquet as pq

BENCH = Path(__file__).parent
RAW = Path(r"G:\flash_crash")
REPS = 5
WARMUP = 1

sys.path.insert(0, str(BENCH.parent))
from tse_tick.schemas import get_schema_individual_stock_95
from tse_tick.core import clean_data

schema_override = {f"column_{col+1}": pl.String for col in range(95)}
col_names = get_schema_individual_stock_95()

zips = [
    RAW / "raw_2017/201701/HTICST120.20170104.1.zip",
    RAW / "raw_2017/201701/HTICST120.20170105.1.zip",
    RAW / "raw_2017/201701/HTICST120.20170106.1.zip",
]

MAX_ROWS = 500_000

print(f"Loading {len(zips)} partitions (max {MAX_ROWS:,} rows each)...")
all_dfs = []
for zp in zips:
    print(f"  {zp.name}...")
    with zipfile.ZipFile(zp, "r") as zf:
        fname = zf.namelist()[0]
        with zf.open(fname) as f:
            df = pl.read_csv(f, has_header=False, schema_overrides=schema_override,
                             truncate_ragged_lines=True, n_rows=MAX_ROWS)
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

for tk in ["7203", "9984", "6758", "8306"]:
    n = combined.filter(pl.col("ticker") == tk).shape[0]
    if n > 0:
        target_ticker = tk
        print(f"  Found ticker {tk}: {n} rows")
        break
else:
    target_ticker = combined["ticker"].unique().sort()[0]
    print(f"  Using first ticker: {target_ticker}")

target_date = combined["date"].unique().sort()[0]

sample = combined.filter(
    (pl.col("ticker") == target_ticker) &
    (pl.col("date") == target_date) &
    (pl.col("Execution Time") >= "090000") &
    (pl.col("Execution Time") < "100000")
)
print(f"Query target: ticker={target_ticker}, date={target_date}, 09:00-10:00 -> {len(sample)} rows")

qdir = BENCH / "_tmp_query2"
qdir.mkdir(exist_ok=True)
hive_root = qdir / "hive_store"
csv_mono = qdir / "monolithic.csv"

print("Writing Hive-partitioned Parquet...")
pa_table = combined.to_arrow()
pq.write_to_dataset(pa_table, root_path=str(hive_root), partition_cols=["date", "ticker"])
hive_size = sum(f.stat().st_size for f in hive_root.rglob("*.parquet"))
print(f"  Hive store: {hive_size / 1048576:.1f} MB")

print("Writing monolithic CSV...")
combined.write_csv(csv_mono)
csv_size = csv_mono.stat().st_size
print(f"  CSV: {csv_size / 1048576:.1f} MB")

query_results = {}
hive_pattern = str(hive_root).replace("\\", "/") + "/**/*.parquet"

print("\nDuckDB + Hive Parquet...")
q_times, q_rows = [], 0
for _ in range(REPS):
    con = duckdb.connect()
    t0 = time.perf_counter()
    r = con.execute(f"""
        SELECT * FROM read_parquet('{hive_pattern}', hive_partitioning=true)
        WHERE ticker = '{target_ticker}' AND date = '{target_date}'
          AND "Execution Time" >= '090000' AND "Execution Time" < '100000'
    """).fetchdf()
    q_times.append(time.perf_counter() - t0)
    q_rows = len(r)
    con.close()
med_hive = round(median(q_times[WARMUP:]), 4)
query_results["DuckDB+Hive Parquet"] = {"median_s": med_hive, "rows": q_rows}
print(f"  Median: {med_hive}s  Rows: {q_rows}")

print("DuckDB + CSV scan...")
csv_str = str(csv_mono).replace("\\", "/")
q_times = []
for _ in range(REPS):
    con = duckdb.connect()
    t0 = time.perf_counter()
    r = con.execute(f"""
        SELECT * FROM read_csv_auto('{csv_str}')
        WHERE ticker = '{target_ticker}' AND date = '{target_date}'
          AND "Execution Time" >= '090000' AND "Execution Time" < '100000'
    """).fetchdf()
    q_times.append(time.perf_counter() - t0)
    con.close()
med_csv_ddb = round(median(q_times[WARMUP:]), 4)
query_results["DuckDB+CSV scan"] = {"median_s": med_csv_ddb, "rows": len(r)}
print(f"  Median: {med_csv_ddb}s  Rows: {len(r)}")

print("pandas CSV scan...")
q_times = []
for _ in range(REPS):
    t0 = time.perf_counter()
    df_csv = pd.read_csv(csv_mono, dtype={"date": str, "ticker": str, "Execution Time": str})
    mask = ((df_csv["ticker"] == target_ticker) & (df_csv["date"] == target_date) &
            (df_csv["Execution Time"] >= "090000") & (df_csv["Execution Time"] < "100000"))
    r_pd = df_csv[mask]
    q_times.append(time.perf_counter() - t0)
    q_rows_pd = len(r_pd)
med_pd = round(median(q_times[WARMUP:]), 4)
query_results["pandas CSV scan"] = {"median_s": med_pd, "rows": q_rows_pd}
print(f"  Median: {med_pd}s  Rows: {q_rows_pd}")

q_csv = BENCH / "results_query.csv"
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
print("Done.")
