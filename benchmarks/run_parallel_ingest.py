"""Parallel-ingest scaling benchmark (writes results_parallel_ingest.csv).

Re-measures what CHANGELOG 0.13.0 measured — a ticker-filtered multi-day
``individual_stock`` ingest at increasing worker counts — on the current engine,
which has since gained part-pruning for alphanumeric-code days (0.14.5) and the
morsel-bounded streaming write path (0.14.6).

Design:
  * one isolated subprocess per condition (``worker_parallel_ingest.py``);
  * conditions are interleaved and the second repetition runs them in reverse
    order, so thermal or page-cache drift cannot load onto one condition;
  * ``resume=False`` and a fresh store per run, so no run can skip work;
  * the *effective* worker count is recorded per run — the RAM-aware cap
    legitimately clamps a request that does not fit, and that clamp is part of
    the result, not an error;
  * the serial and highest-worker stores of the first repetition are compared
    byte-for-byte, because parallel ingest promises an identical store.

Needs real NEEDS archives (``TSE_TICK_DATA_ROOT``, default ``G:\\flash_crash``).
Emits timings and row counts only.
"""

import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
PERIOD = "20230104-20230131"  # January 2023: 19 trading days
CONDITIONS = [1, 2, 4, 8]
REPS = 2
WORK_DIR = HERE.parent / "_perf_work" / "parallel_scaling"


def main() -> None:
    WORK_DIR.mkdir(parents=True, exist_ok=True)
    rows = []
    keep = {}

    for rep in range(1, REPS + 1):
        order = CONDITIONS if rep % 2 == 1 else list(reversed(CONDITIONS))
        for workers in order:
            out = WORK_DIR / f"store_r{rep}_w{workers}"
            t0 = time.perf_counter()
            proc = subprocess.run(
                [
                    sys.executable,
                    str(HERE / "worker_parallel_ingest.py"),
                    str(workers),
                    PERIOD,
                    str(out),
                ],
                capture_output=True,
                text=True,
                cwd=str(HERE.parent),
            )
            wall = time.perf_counter() - t0
            if proc.returncode != 0:
                print(f"FAILED rep{rep} w{workers}: {proc.stderr[-2000:]}", flush=True)
                continue
            payload = json.loads(proc.stdout.strip().splitlines()[-1])
            clamp = [ln for ln in proc.stderr.splitlines() if "Limiting workers" in ln]
            payload.update(rep=rep, wall_s=round(wall, 3), clamped=bool(clamp))
            rows.append(payload)
            print(json.dumps(payload), flush=True)

            if rep == 1 and workers in (CONDITIONS[0], CONDITIONS[-1]):
                keep[workers] = out
            else:
                shutil.rmtree(out, ignore_errors=True)

    with (HERE / "results_parallel_ingest.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh,
            fieldnames=[
                "version",
                "rep",
                "workers_requested",
                "workers_effective",
                "elapsed_s",
                "wall_s",
                "days",
                "rows",
                "clamped",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    # Store identity: serial and parallel must produce the same bytes.
    lo, hi = CONDITIONS[0], CONDITIONS[-1]
    if lo in keep and hi in keep:
        a, b = keep[lo], keep[hi]
        fa = sorted(p.relative_to(a).as_posix() for p in a.rglob("*.parquet"))
        fb = sorted(p.relative_to(b).as_posix() for p in b.rglob("*.parquet"))
        same_names = fa == fb
        same_bytes = same_names and all((a / n).read_bytes() == (b / n).read_bytes() for n in fa)
        print(f"IDENTITY files={len(fa)} same_names={same_names} byte_identical={same_bytes}")

    print("DONE")


if __name__ == "__main__":
    main()
