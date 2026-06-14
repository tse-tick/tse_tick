"""
Reproduce all tse_tick benchmarks.

Runs the four canonical benchmark scripts in sequence:
  run_engine.py       - Polars vs pandas (prototype + fair) for all 4 data types
  run_format.py       - storage-format comparison (CSV/Parquet/Feather/Pickle)
  run_query_fix.py    - Hive-Parquet + DuckDB vs pandas CSV scan (query latency)
  run_correctness.py  - verifies Polars output is identical to pandas-fair

Results are written to benchmarks/results_*.csv. See ENVIRONMENT.md for the
reference machine and package versions; regenerate the paper figure afterward
via generate_assets.generate_figure(...).

Note: run_format.py also writes results_query.csv, and run_query_fix.py runs
after it, so the canonical (morning-trade, Toyota) query result is the one that
persists. The full sequence takes a few hours (the pandas Python-engine
prototype on the 4.8M-row file is ~10 min per repetition).

Usage:  python run_all.py
"""
import subprocess
import sys
import time
from pathlib import Path

BENCH = Path(__file__).parent
SCRIPTS = [
    "run_engine.py",
    "run_format.py",
    "run_query_fix.py",
    "run_correctness.py",
]


def main():
    t0 = time.perf_counter()
    failures = []
    for script in SCRIPTS:
        print("\n" + "=" * 64)
        print(f"  {script}")
        print("=" * 64, flush=True)
        result = subprocess.run([sys.executable, str(BENCH / script)], cwd=str(BENCH))
        if result.returncode != 0:
            failures.append((script, result.returncode))
            print(f"  WARNING: {script} exited with code {result.returncode}")

    print("\n" + "=" * 64)
    print(f"ALL BENCHMARKS COMPLETE in {time.perf_counter() - t0:.0f}s")
    print("  Results written:")
    for f in sorted(BENCH.glob("results_*.csv")):
        if not f.stem.endswith("_prev"):
            print(f"    {f.name}")
    if failures:
        print("  Failures:")
        for s, rc in failures:
            print(f"    {s} (exit {rc})")
        sys.exit(1)
    print("  Correctness gate and all benchmarks passed.")


if __name__ == "__main__":
    main()
