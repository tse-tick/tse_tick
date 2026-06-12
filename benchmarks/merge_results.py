"""
Merge clean polars results + pandas results into final engine CSV files.
Run this after both run_polars_only.py and run_engine.py complete.
"""
import csv
import sys
from pathlib import Path
from statistics import median

BENCH = Path(__file__).parent

def main():
    polars_csv = BENCH / "results_polars_clean.csv"
    engine_csv = BENCH / "results_engine.csv"

    if not polars_csv.exists():
        print(f"ERROR: {polars_csv} not found. Run run_polars_only.py first.")
        sys.exit(1)
    if not engine_csv.exists():
        print(f"ERROR: {engine_csv} not found. Run run_engine.py first.")
        sys.exit(1)

    with open(polars_csv, newline="") as f:
        polars_rows = list(csv.DictReader(f))
    with open(engine_csv, newline="") as f:
        engine_rows = list(csv.DictReader(f))

    pandas_rows = [r for r in engine_rows if "pandas" in r["condition"]]
    combined = polars_rows + pandas_rows

    out_raw = BENCH / "results_engine.csv"
    with open(out_raw, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data_type", "condition", "rep",
                                          "elapsed_s", "peak_rss_mb", "rows", "cols"])
        w.writeheader()
        w.writerows(combined)
    print(f"Wrote {out_raw} ({len(combined)} rows)")

    WARMUP = 1
    summary = []
    for label in ["polars-default", "polars-1thread", "pandas-default"]:
        rows = [r for r in combined if r["condition"] == label and r["elapsed_s"]]
        times = [float(r["elapsed_s"]) for r in rows if r["elapsed_s"]]
        mems = [float(r["peak_rss_mb"]) for r in rows if r["peak_rss_mb"]]
        if len(times) <= WARMUP:
            continue
        t = times[WARMUP:]
        m = mems[WARMUP:]
        summary.append({
            "data_type": "HTICST120", "condition": label,
            "median_s": round(median(t), 3),
            "min_s": round(min(t), 3),
            "max_s": round(max(t), 3),
            "median_rss_mb": round(median(m), 1),
            "n_measured": len(t),
        })

    pd_time = next((r["median_s"] for r in summary if "pandas" in r["condition"]), None)
    for r in summary:
        r["speedup_vs_pandas"] = (round(pd_time / r["median_s"], 2)
                                   if pd_time and r["median_s"] > 0 else None)

    out_sum = BENCH / "results_engine_summary.csv"
    with open(out_sum, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["data_type", "condition", "median_s",
                                          "min_s", "max_s", "median_rss_mb",
                                          "n_measured", "speedup_vs_pandas"])
        w.writeheader()
        w.writerows(summary)
    print(f"Wrote {out_sum}")

    print("\nSummary:")
    for r in summary:
        print(f"  {r['condition']:20s}  median={r['median_s']:.3f}s  "
              f"RSS={r['median_rss_mb']:.0f}MB  speedup={r['speedup_vs_pandas']}")

if __name__ == "__main__":
    main()
