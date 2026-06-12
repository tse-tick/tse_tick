"""
Standalone engine benchmark: Polars vs Pandas (original + fair) for all four data types.
Runs all repetitions and saves results to CSV files.
Designed to run as a detached process (may take 30-60+ minutes).
"""
import csv
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from statistics import median

BENCHMARKS_DIR = Path(__file__).parent
WORKER = BENCHMARKS_DIR / "worker_engine.py"

DATA_ROOT = Path(r"G:\flash_crash")

ZIP_PATHS = {
    "HTICST120": DATA_ROOT / "raw_2017" / "201701" / "HTICST120.20170104.1.zip",
    "HTICSS110": DATA_ROOT / "raw_other" / "HTICSS110.201701.zip",
    "HTICIT110": DATA_ROOT / "raw_other" / "HTICIT110.201701.zip",
    "HTICIS110": DATA_ROOT / "raw_other" / "HTICIS110.201701.zip",
}

DATA_TYPE_MAP = {
    "HTICST120": "individual_stock",
    "HTICSS110": "stock_summary",
    "HTICIT110": "indices",
    "HTICIS110": "indices_summary",
}

REPS = 7
WARMUP = 1
DONE_FLAG = BENCHMARKS_DIR / "_engine_done.flag"


def run_worker(backend, zip_path, max_threads=0, data_type="individual_stock"):
    cmd = [sys.executable, str(WORKER), backend, str(zip_path)]
    if max_threads > 0:
        cmd.append(str(max_threads))
    else:
        cmd.append("0")
    cmd.append(data_type)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=900,
                                cwd=str(BENCHMARKS_DIR))
    except subprocess.TimeoutExpired:
        return {"error": "subprocess timed out after 900s"}
    if result.returncode != 0:
        return {"error": result.stderr[:500]}
    try:
        return json.loads(result.stdout.strip().split("\n")[-1])
    except (json.JSONDecodeError, IndexError):
        return {"error": result.stdout[:500]}


def main():
    log = open(BENCHMARKS_DIR / "engine_benchmark.log", "w")
    def p(msg):
        log.write(msg + "\n")
        log.flush()
        print(msg)

    p(f"Engine benchmark started at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    p(f"Reps: {REPS}, Warmup: {WARMUP}")
    p("")

    conditions = [
        ("polars", 0, "polars-default"),
        ("polars", 1, "polars-1thread"),
        ("pandas-fair", 0, "pandas-fair"),
        ("pandas", 0, "pandas-prototype"),
    ]

    raw_rows = []

    for file_code, zip_path in ZIP_PATHS.items():
        dt = DATA_TYPE_MAP[file_code]
        if not zip_path.exists():
            p(f"=== SKIPPING {file_code}: {zip_path} not found ===\n")
            continue

        p(f"=== {file_code} ({dt}) ===")
        p(f"ZIP: {zip_path}")
        p(f"Size: {zip_path.stat().st_size / (1024*1024):.1f} MB")
        p("")

        for backend, threads, label in conditions:
            p(f"--- {file_code} / {label} ---")
            for rep in range(REPS):
                t0 = time.perf_counter()
                result = run_worker(backend, zip_path, threads, dt)
                wall = time.perf_counter() - t0

                if "error" in result:
                    p(f"  Rep {rep+1}/{REPS}: FAILED ({result['error'][:100]})")
                    raw_rows.append({
                        "data_type": file_code, "condition": label,
                        "rep": rep + 1, "elapsed_s": None,
                        "peak_rss_mb": None, "rows": None, "cols": None,
                    })
                else:
                    p(f"  Rep {rep+1}/{REPS}: {result['elapsed_s']:.3f}s  "
                      f"RSS={result['peak_rss_mb']:.0f}MB  rows={result['rows']:,}  "
                      f"cols={result['cols']}  (wall={wall:.1f}s)")
                    raw_rows.append({
                        "data_type": file_code, "condition": label,
                        "rep": rep + 1, "elapsed_s": result["elapsed_s"],
                        "peak_rss_mb": result["peak_rss_mb"],
                        "rows": result["rows"], "cols": result["cols"],
                    })
            p("")

    # Write raw results
    raw_csv = BENCHMARKS_DIR / "results_engine.csv"
    with open(raw_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data_type", "condition", "rep",
                                          "elapsed_s", "peak_rss_mb", "rows", "cols"])
        w.writeheader()
        w.writerows(raw_rows)
    p(f"Raw results: {raw_csv}")

    # Compute summaries with TWO speedup factors
    summary_rows = []
    data_types_seen = list(dict.fromkeys(r["data_type"] for r in raw_rows))

    for file_code in data_types_seen:
        for _, _, label in conditions:
            times = [r["elapsed_s"] for r in raw_rows
                     if r["data_type"] == file_code and r["condition"] == label
                     and r["elapsed_s"] is not None]
            mems = [r["peak_rss_mb"] for r in raw_rows
                    if r["data_type"] == file_code and r["condition"] == label
                    and r["peak_rss_mb"] is not None]
            row_counts = [r["rows"] for r in raw_rows
                          if r["data_type"] == file_code and r["condition"] == label
                          and r["rows"] is not None]
            col_counts = [r["cols"] for r in raw_rows
                          if r["data_type"] == file_code and r["condition"] == label
                          and r["cols"] is not None]
            if len(times) <= WARMUP:
                continue
            t_trimmed = times[WARMUP:]
            m_trimmed = mems[WARMUP:]
            summary_rows.append({
                "data_type": file_code, "condition": label,
                "median_s": round(median(t_trimmed), 3),
                "min_s": round(min(t_trimmed), 3),
                "max_s": round(max(t_trimmed), 3),
                "median_rss_mb": round(median(m_trimmed), 1),
                "n_measured": len(t_trimmed),
                "rows": row_counts[0] if row_counts else None,
                "cols": col_counts[0] if col_counts else None,
                "speedup_vs_prototype": None,
                "speedup_vs_fair": None,
            })

    # Compute speedups per data_type
    for file_code in data_types_seen:
        proto_time = next((r["median_s"] for r in summary_rows
                           if r["data_type"] == file_code
                           and r["condition"] == "pandas-prototype"), None)
        fair_time = next((r["median_s"] for r in summary_rows
                          if r["data_type"] == file_code
                          and r["condition"] == "pandas-fair"), None)
        for r in summary_rows:
            if r["data_type"] != file_code:
                continue
            if proto_time and r["median_s"] > 0:
                r["speedup_vs_prototype"] = round(proto_time / r["median_s"], 2)
            if fair_time and r["median_s"] > 0:
                r["speedup_vs_fair"] = round(fair_time / r["median_s"], 2)

    sum_csv = BENCHMARKS_DIR / "results_engine_summary.csv"
    with open(sum_csv, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=[
            "data_type", "condition", "median_s", "min_s", "max_s",
            "median_rss_mb", "n_measured", "rows", "cols",
            "speedup_vs_prototype", "speedup_vs_fair",
        ])
        w.writeheader()
        w.writerows(summary_rows)
    p(f"Summary: {sum_csv}")

    p("\n=== SUMMARY ===")
    for r in summary_rows:
        p(f"  {r['data_type']:12s} {r['condition']:20s}  "
          f"median={r['median_s']:.3f}s  RSS={r['median_rss_mb']:.0f}MB  "
          f"vs_proto={r['speedup_vs_prototype']}  vs_fair={r['speedup_vs_fair']}")

    p(f"\nCompleted at {time.strftime('%Y-%m-%d %H:%M:%S')}")
    log.close()

    with open(DONE_FLAG, "w") as f:
        f.write("done")


if __name__ == "__main__":
    main()
