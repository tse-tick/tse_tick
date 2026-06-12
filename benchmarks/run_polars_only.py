"""Quick re-run of just the polars conditions (clean, no concurrent load)."""
import csv, json, os, subprocess, sys, time
from pathlib import Path
from statistics import median

BENCHMARKS_DIR = Path(__file__).parent
WORKER = BENCHMARKS_DIR / "worker_engine.py"
ZIP_PATH = Path(r"G:\flash_crash_pilot\raw_2017\201701\HTICST120.20170104.1.zip")
REPS = 7
WARMUP = 1

def run_worker(backend, zip_path, max_threads=0):
    cmd = [sys.executable, str(WORKER), backend, str(zip_path)]
    if max_threads > 0:
        cmd.append(str(max_threads))
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120,
                                cwd=str(BENCHMARKS_DIR))
    except subprocess.TimeoutExpired:
        return {"error": "timeout"}
    if result.returncode != 0:
        return {"error": result.stderr[:200]}
    try:
        return json.loads(result.stdout.strip().split("\n")[-1])
    except Exception:
        return {"error": "parse error"}

conditions = [("polars", 0, "polars-default"), ("polars", 1, "polars-1thread")]
raw_rows = []

for backend, threads, label in conditions:
    print(f"--- {label} ---")
    for rep in range(REPS):
        r = run_worker(backend, ZIP_PATH, threads)
        if "error" in r:
            print(f"  Rep {rep+1}: FAILED ({r['error']})")
            raw_rows.append({"data_type": "HTICST120", "condition": label,
                             "rep": rep+1, "elapsed_s": None, "peak_rss_mb": None,
                             "rows": None, "cols": None})
        else:
            print(f"  Rep {rep+1}: {r['elapsed_s']:.3f}s  RSS={r['peak_rss_mb']:.0f}MB")
            raw_rows.append({"data_type": "HTICST120", "condition": label,
                             "rep": rep+1, "elapsed_s": r["elapsed_s"],
                             "peak_rss_mb": r["peak_rss_mb"],
                             "rows": r["rows"], "cols": r["cols"]})

# Save just the polars rows to a temp file
out = BENCHMARKS_DIR / "results_polars_clean.csv"
with open(out, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=["data_type","condition","rep",
                                       "elapsed_s","peak_rss_mb","rows","cols"])
    w.writeheader()
    w.writerows(raw_rows)
print(f"\nSaved to {out}")
