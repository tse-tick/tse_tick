"""One parallel-ingest scaling condition, in an isolated process.

Usage:  python worker_parallel_ingest.py <workers> <period> <out_dir> [input_root]

Prints one JSON line: version, workers_requested, workers_effective, elapsed_s,
days, rows. The store is left on disk for the driver to compare and delete.

Isolated per condition for the same reason ``worker_engine.py`` is: a fresh
process means a fresh Polars thread pool and no allocator state carried between
worker counts. Requires real NEEDS archives; timings and row counts only, never
raw records.
"""

import json
import logging
import os
import shutil
import sys
import time

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

import tse_tick  # noqa: E402
from tse_tick.ingest import _STREAM_WORKER_GB, _cap_workers, ingest_period  # noqa: E402

DEFAULT_INPUT_ROOT = os.environ.get("TSE_TICK_DATA_ROOT", r"G:\flash_crash")
TICKER = {"7203"}


def main() -> None:
    workers = int(sys.argv[1])
    period = sys.argv[2]
    out_dir = sys.argv[3]
    input_root = sys.argv[4] if len(sys.argv) > 4 else DEFAULT_INPUT_ROOT

    logging.basicConfig(level=logging.WARNING, stream=sys.stderr, format="%(message)s")

    # A fresh store per condition: resume=False alone would still let an existing
    # store's coverage markers change the work, and the point is equal work each run.
    if os.path.isdir(out_dir):
        shutil.rmtree(out_dir)
    os.makedirs(out_dir, exist_ok=True)

    t0 = time.perf_counter()
    results = ingest_period(
        input_root,
        out_dir,
        period,
        "individual_stock",
        ticker_filter=set(TICKER),
        max_workers=workers,
        resume=False,
    )
    elapsed = time.perf_counter() - t0

    rows = sum(int(r.get("rows", 0)) for r in results)
    # The streaming path bounds a worker at a flat _STREAM_WORKER_GB whatever the
    # day's size, so that is the cap's input for a ticker-filtered individual_stock
    # ingest. Recomputed here to record what the run was actually allowed.
    effective = _cap_workers(workers, _STREAM_WORKER_GB)
    print(
        json.dumps(
            {
                "version": tse_tick.__version__,
                "workers_requested": workers,
                "workers_effective": effective,
                "elapsed_s": round(elapsed, 3),
                "days": len(results),
                "rows": rows,
            }
        )
    )


if __name__ == "__main__":
    main()
