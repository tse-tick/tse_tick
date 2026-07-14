"""Regression tests for round-19: TSE alphanumeric stock codes break part-pruning,
and a killed ingest worker surfaces as a bare ``BrokenProcessPool``.

Found by a real-data run: ``ingest_period("2023-2025", ticker_filter={"7203","9984"})``
died with ``BrokenProcessPool`` after 13 minutes. Probing ``G:\\NEEDS`` showed ~half of
all 2024/2025 trading days carry a part whose first record is an **alphanumeric** code
(e.g. ``162A`` — TSE issues these from 2024). ``part_start_code`` returned ``None`` for
those, which made ``select_parts_for_day`` disable pruning for the WHOLE day: 27 parts
opened instead of 2 — measured at **5.14 GB / 204 s per worker-day** vs **0.18 GB /
13 s** when pruned. 2017-2019 and 2023 probe clean, which is why earlier runs worked.
"""

from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import pytest

import tse_tick
from tse_tick import ingest as ingest_mod
from tse_tick import read_ticks
from tse_tick.ingest import _TICKER_WORKER_GB, _filtered_worker_gb
from tse_tick.partscan import select_parts_for_day
from tests.synthetic_data import individual_stock_csv, write_zip


def _alnum_day(root: Path, date: str = "20240403") -> Path:
    """A structured NEEDS tree for one day whose parts ascend in 4-char-string
    order and include an alphanumeric-coded part in the middle (real 2024 shape)."""
    month = root / f"個別株式{date[:4]}" / "TICST120" / date[:6]
    month.mkdir(parents=True, exist_ok=True)
    for n, codes in enumerate([["1301"], ["162A"], ["2036"], ["7203"], ["9999"]], 1):
        write_zip(
            month / f"HTICST120.{date}.{n}.zip",
            f"HTICST120.{date}.{n}.csv",
            individual_stock_csv(date, codes, rows_per_ticker=10),
        )
    return root


# --------------------------------------------------------------------------- #
# A — pruning must stay row-for-row identical to a full scan (the hard rule)
# --------------------------------------------------------------------------- #
def test_pruned_read_is_row_identical_to_full_scan_with_alphanumeric_codes(tmp_path):
    """The locked contract: part-pruning only shrinks I/O, never the result —
    now also on a day containing TSE's 2024+ alphanumeric codes."""
    root = _alnum_day(tmp_path)
    pruned = read_ticks(
        str(root),
        data_type="individual_stock",
        ticker_filter={"7203"},
        date="20240403",
        prune_parts=True,
    )
    full = read_ticks(
        str(root),
        data_type="individual_stock",
        ticker_filter={"7203"},
        date="20240403",
        prune_parts=False,
    )
    assert pruned.height == 10  # the ticker's rows, nothing lost
    assert pruned.shape == full.shape
    assert pruned.equals(full)


def test_alphanumeric_ticker_read_is_row_identical_to_full_scan(tmp_path):
    """Reading an alphanumeric code itself (e.g. 162A) prunes and stays identical."""
    root = _alnum_day(tmp_path)
    pruned = read_ticks(
        str(root),
        data_type="individual_stock",
        ticker_filter={"162A"},
        date="20240403",
        prune_parts=True,
    )
    full = read_ticks(
        str(root),
        data_type="individual_stock",
        ticker_filter={"162A"},
        date="20240403",
        prune_parts=False,
    )
    assert pruned.height == 10
    assert pruned.equals(full)


def test_alphanumeric_part_no_longer_forces_a_full_scan(tmp_path):
    """The core defect: ONE unprobeable (alphanumeric) part disabled pruning for
    the whole day. The day must now prune to fewer than all 5 parts."""
    root = _alnum_day(tmp_path)
    parts = sorted(
        (root / "個別株式2024" / "TICST120" / "202404").glob("*.zip"),
        key=lambda p: int(p.name.split(".")[2]),
    )
    chosen = select_parts_for_day(parts, {"7203"})
    assert chosen is not None  # was None -> "open all parts"
    assert len(chosen) < len(parts)  # genuinely pruned


# --------------------------------------------------------------------------- #
# C — a killed worker surfaces as an actionable error, not raw BrokenProcessPool
# --------------------------------------------------------------------------- #
class _DeadFuture:
    def result(self):
        raise BrokenProcessPool("A process in the process pool was terminated abruptly")


class _DeadExecutor:
    """Stands in for ProcessPoolExecutor: every task's worker dies."""

    def __init__(self, *a, **k):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def submit(self, *a, **k):
        return _DeadFuture()


def test_killed_worker_raises_actionable_ingest_error(tmp_path, monkeypatch):
    """A dead worker must name the likely cause (OOM), say the finished dates are
    resume-safe, and point at max_workers — not just "terminated abruptly"."""
    root = tmp_path / "raw"
    for date in ("20240403", "20240404"):  # >1 task -> parallel path
        month = root / "個別株式2024" / "TICST120" / "202404"
        month.mkdir(parents=True, exist_ok=True)
        write_zip(
            month / f"HTICST120.{date}.1.zip",
            f"HTICST120.{date}.1.csv",
            individual_stock_csv(date, ["7203"], rows_per_ticker=4),
        )

    # Pin cores/RAM so the parallel path is taken regardless of what this machine
    # happens to have free (otherwise _cap_workers can clamp to 1 -> serial path).
    monkeypatch.setattr(ingest_mod, "_cpu_cap", lambda: 4)
    monkeypatch.setattr(ingest_mod, "_available_ram_gb", lambda: 32.0)
    monkeypatch.setattr("tse_tick.ingest.ProcessPoolExecutor", _DeadExecutor)
    monkeypatch.setattr("tse_tick.ingest.as_completed", lambda fs: list(fs))

    with pytest.raises(tse_tick.IngestWorkerError) as exc:
        tse_tick.ingest_period(
            str(root),
            str(tmp_path / "store"),
            "20240403-20240404",
            "individual_stock",
            ticker_filter={"7203"},
            max_workers=2,
        )
    msg = str(exc.value)
    assert "max_workers" in msg  # the actionable knob
    assert "resume" in msg.lower()  # finished dates are kept
    assert "memory" in msg.lower()  # the likely cause


def test_ingest_worker_error_is_importable_and_catchable():
    """Importable from the package root (core install, no [query] extra needed)."""
    assert issubclass(tse_tick.IngestWorkerError, RuntimeError)


# --------------------------------------------------------------------------- #
# B — the RAM guard must actually bind on a ticker-filtered ingest
# --------------------------------------------------------------------------- #
def test_filtered_worker_estimate_scales_with_ticker_count():
    """The guard assumed ANY ticker_filter meant 0.5 GB/worker, so it never
    clamped: 16 Jupyter workers x a measured ~2.2 GB (7203+9984, one real day)
    overcommitted a 34 GB box and a worker was killed.

    Round-19 fixed that by scaling per kept code. Round-20 **supersedes** that for
    the streaming path — a streamed day's peak is a bounded constant, so it no
    longer scales with codes at all — and the per-code scaling now applies only to
    a filter too wide to stream, which still holds the whole day.
    """
    full = 11.8  # a real 2024 day: 1.47 GB compressed x _FULLFRAME_EXPANSION
    wide = ingest_mod._MAX_STREAM_TICKERS + 1
    assert _filtered_worker_gb(full, wide) == min(full, _TICKER_WORKER_GB * wide)
    assert _TICKER_WORKER_GB >= 1.0, "measured ~1.1 GB/code; the old flat 0.5 caused the OOM"
    # The streaming path (the common case) is bounded, not data-dependent.
    assert _filtered_worker_gb(full, 1) == _filtered_worker_gb(full, 2) == ingest_mod._STREAM_WORKER_GB


def test_filtered_worker_estimate_never_exceeds_the_whole_day():
    """A filtered frame is a subset of the day's frame, so it can never be bigger —
    this also keeps a small/synthetic day's estimate at the floor."""
    assert _filtered_worker_gb(0.5, 20) == 0.5


def test_two_ticker_filtered_ingest_no_longer_runs_one_worker_per_core(monkeypatch):
    """End of the actual crash: on a 16-core / ~24.6 GB-available box, a 2-code
    filtered ingest must now clamp below the core count instead of running 16."""
    monkeypatch.setattr(ingest_mod, "_cpu_cap", lambda: 16)
    monkeypatch.setattr(ingest_mod, "_available_ram_gb", lambda: 24.6)
    gb = _filtered_worker_gb(11.8, 2)  # 3.0 GB/worker
    workers = ingest_mod._cap_workers(16, per_worker_gb=gb)
    assert workers < 16  # was 16: 0.5 GB/worker => ram_cap 34 => never clamped
    assert workers * 2.21 < 24.6  # measured real peak/worker now fits in RAM
