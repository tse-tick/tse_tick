"""Parallel per-date ingest (issue #43).

``ingest_period(..., max_workers=N)`` dispatches the independent per-date units across
a process pool. The store must be byte-identical to the serial (``max_workers=1``)
path — each date writes its own ``date=`` dir — and the returned results list must be
deterministic (sorted by date) regardless of worker completion order. These also
exercise the process-pool path end to end (pickling the module-level worker, spawn).
"""
from pathlib import Path

import polars as pl

import tse_tick
import tse_tick.ingest as ingest_mod

DAYS = ["20240104", "20240105", "20240108", "20240109"]


def _seed_stock(root):
    # Two tickers in part 1, a third in part 2, per day (multi-part-per-day unit).
    for day in DAYS:
        leaf = root / f"個別株式{day[:4]}" / "TICST120" / day[:6]
        leaf.mkdir(parents=True, exist_ok=True)
        from tests.synthetic_data import individual_stock_csv, write_zip
        write_zip(leaf / f"HTICST120.{day}.1.zip", f"HTICST120.{day}.1.csv",
                  individual_stock_csv(day, ["1301", "7203"], rows_per_ticker=8))
        write_zip(leaf / f"HTICST120.{day}.2.zip", f"HTICST120.{day}.2.csv",
                  individual_stock_csv(day, ["9984"], rows_per_ticker=8))


def _seed_stock_multipart_ticker(root):
    # 7203 spans parts 2 & 3 with an appendix in the last part — exercises the
    # per-date prune running INSIDE each worker under parallelism.
    from tests.synthetic_data import individual_stock_csv, write_zip
    for day in DAYS:
        leaf = root / f"個別株式{day[:4]}" / "TICST120" / day[:6]
        leaf.mkdir(parents=True, exist_ok=True)
        for n, codes in {1: ["1301"], 2: ["7203"], 3: ["7203"], 4: ["9999", "7203"]}.items():
            write_zip(leaf / f"HTICST120.{day}.{n}.zip", f"HTICST120.{day}.{n}.csv",
                      individual_stock_csv(day, codes, rows_per_ticker=6))


def _seed_indices(root):
    from tests.synthetic_data import indices_csv, write_zip
    for day in DAYS:
        leaf = root / f"個別株式{day[:4]}" / "TICIT110" / day[:6]
        leaf.mkdir(parents=True, exist_ok=True)
        write_zip(leaf / f"HTICIT110.{day}.1.zip", f"HTICIT110.{day}.1.csv",
                  indices_csv(day, ["101", "113"], rows_per_code=10))


def _snapshot(store, data_type):
    """rel-path -> frame, for logical byte-identity comparison of two stores."""
    base = Path(store) / data_type
    return {
        p.relative_to(base).as_posix(): pl.read_parquet(p)
        for p in sorted(base.rglob("*.parquet"))
    }


def _assert_stores_identical(a: dict, b: dict):
    assert set(a) == set(b) and len(a) > 0, (sorted(a), sorted(b))
    for key in a:
        assert a[key].equals(b[key]), f"content differs for {key}"


def test_parallel_full_frame_equals_serial(tmp_path):
    src = tmp_path / "src"
    _seed_stock(src)
    serial = tmp_path / "serial"
    par = tmp_path / "par"
    tse_tick.ingest_period(str(src), str(serial), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", max_workers=1)
    tse_tick.ingest_period(str(src), str(par), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", max_workers=4)
    _assert_stores_identical(_snapshot(serial, "individual_stock"),
                             _snapshot(par, "individual_stock"))


def test_parallel_ticker_filtered_equals_serial(tmp_path):
    # Prune runs inside each worker; the pruned+ingested store must match serial.
    src = tmp_path / "src"
    _seed_stock_multipart_ticker(src)
    serial = tmp_path / "serial"
    par = tmp_path / "par"
    tse_tick.ingest_period(str(src), str(serial), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={"7203"}, max_workers=1)
    tse_tick.ingest_period(str(src), str(par), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", ticker_filter={"7203"}, max_workers=4)
    a = _snapshot(serial, "individual_stock")
    b = _snapshot(par, "individual_stock")
    _assert_stores_identical(a, b)
    # 7203 lives in parts 2, 3 and the part-4 appendix -> 3 * 6 rows per day.
    for key, frame in a.items():
        assert frame.height == 18, key


def test_parallel_indices_equals_serial(tmp_path):
    src = tmp_path / "src"
    _seed_indices(src)
    serial = tmp_path / "serial"
    par = tmp_path / "par"
    tse_tick.ingest_period(str(src), str(serial), f"{DAYS[0]}-{DAYS[-1]}",
                           "indices", max_workers=1)
    tse_tick.ingest_period(str(src), str(par), f"{DAYS[0]}-{DAYS[-1]}",
                           "indices", max_workers=3)
    _assert_stores_identical(_snapshot(serial, "indices"), _snapshot(par, "indices"))


def test_parallel_results_sorted_by_date(tmp_path):
    src = tmp_path / "src"
    _seed_stock(src)
    par = tmp_path / "par"
    results = tse_tick.ingest_period(str(src), str(par), f"{DAYS[0]}-{DAYS[-1]}",
                                     "individual_stock", max_workers=4)
    dates = [m["date"] for m in results]
    assert dates == sorted(DAYS), dates  # deterministic order despite async completion


def test_cap_workers_clamps_to_core_count(caplog):
    import logging
    cap = ingest_mod._cpu_cap()
    with caplog.at_level(logging.WARNING):
        capped = ingest_mod._cap_workers(cap + 100)
    assert capped == cap
    assert any("exceeds this machine" in r.message for r in caplog.records)
    assert ingest_mod._cap_workers(1) == 1
    assert ingest_mod._cap_workers(0) == 1


def test_ingest_directory_parallel_equals_serial(tmp_path):
    # ingest_directory's flat parallel path uses a module-level worker (picklable under
    # spawn); it must succeed with max_workers>1 (previously crashed) and match serial.
    from tests.synthetic_data import individual_stock_csv, write_zip
    flat = tmp_path / "flat"
    flat.mkdir()
    for day in DAYS:
        write_zip(flat / f"HTICST120.{day}.1.zip", f"HTICST120.{day}.1.csv",
                  individual_stock_csv(day, ["7203", "9984"], rows_per_ticker=6))
    serial = tse_tick.ingest_directory(str(flat), str(tmp_path / "s"),
                                       data_type="individual_stock", max_workers=1, progress=False)
    par = tse_tick.ingest_directory(str(flat), str(tmp_path / "p"),
                                    data_type="individual_stock", max_workers=4, progress=False)
    assert all("error" not in m for m in serial), serial
    assert all("error" not in m for m in par), par
    _assert_stores_identical(_snapshot(tmp_path / "s", "individual_stock"),
                             _snapshot(tmp_path / "p", "individual_stock"))


def test_estimate_worker_gb(tmp_path):
    # Filtered / non-stock ingests estimate a small per-worker frame.
    assert ingest_mod._estimate_worker_gb([("d", [])], "individual_stock", {"7203"}) \
        == ingest_mod._FILTERED_WORKER_GB
    assert ingest_mod._estimate_worker_gb([("d", [])], "stock_summary", None) \
        == ingest_mod._FILTERED_WORKER_GB
    # A full-frame individual_stock estimate scales with the largest day's part bytes.
    f = tmp_path / "part.zip"
    f.write_bytes(b"x" * 200_000_000)  # 200 MB
    est = ingest_mod._estimate_worker_gb([("d", [str(f)])], "individual_stock", None)
    assert est == max(ingest_mod._FILTERED_WORKER_GB, 0.2 * ingest_mod._FULLFRAME_EXPANSION)
    # a missing/unreadable part is skipped, NOT allowed to zero the whole day's estimate
    # (a zeroed estimate would weaken the RAM cap in the OOM-dangerous direction).
    est_missing = ingest_mod._estimate_worker_gb(
        [("d", [str(f), str(tmp_path / "gone.zip")])], "individual_stock", None)
    assert est_missing == est


def test_cap_workers_ram_aware(caplog):
    import logging
    cap = ingest_mod._cpu_cap()
    avail = ingest_mod._available_ram_gb()
    if cap > 1 and avail > 0:
        # A per-worker estimate as large as all available RAM leaves budget for <1 worker,
        # so even a 2-worker request is clamped to 1 with a RAM warning.
        with caplog.at_level(logging.WARNING):
            w = ingest_mod._cap_workers(2, per_worker_gb=avail * 2)
        assert w == 1, w
        assert any("available RAM" in r.message for r in caplog.records)
    # A tiny per-worker estimate never RAM-caps.
    caplog.clear()
    with caplog.at_level(logging.WARNING):
        w2 = ingest_mod._cap_workers(min(2, cap), per_worker_gb=0.01)
    assert w2 == min(2, cap)
    assert not any("available RAM" in r.message for r in caplog.records)


# --- max_workers="auto" resolution (0.14.0) -----------------------------------

def test_resolve_max_workers_sentinels(monkeypatch):
    import pytest

    resolve = ingest_mod._resolve_max_workers
    monkeypatch.delenv(ingest_mod._WORKERS_ENV, raising=False)
    assert resolve("auto") == ingest_mod._cpu_cap()
    assert resolve(" AUTO ") == ingest_mod._cpu_cap()   # case/space tolerant
    assert resolve(3) == 3
    assert resolve(0) == 1                              # ints clamp to >= 1
    with pytest.raises(ValueError):
        resolve("bogus")
    # serial-only paths resolve the sentinels quietly to 1
    assert resolve("auto", allow_default_auto=False) == 1
    assert resolve(None, allow_default_auto=False) == 1
    assert resolve(4, allow_default_auto=False) == 4    # explicit int survives


def test_resolve_max_workers_env_var(monkeypatch):
    resolve = ingest_mod._resolve_max_workers
    monkeypatch.setenv(ingest_mod._WORKERS_ENV, "3")
    assert resolve(None) == 3
    monkeypatch.setenv(ingest_mod._WORKERS_ENV, "auto")
    assert resolve(None) == ingest_mod._cpu_cap()
    monkeypatch.setenv(ingest_mod._WORKERS_ENV, "2")
    assert resolve(5) == 5                              # explicit arg beats env


def test_resolve_max_workers_interactive_vs_script(monkeypatch, caplog):
    import logging
    import sys
    import types

    resolve = ingest_mod._resolve_max_workers
    monkeypatch.delenv(ingest_mod._WORKERS_ENV, raising=False)

    # Interactive (__main__ without __file__, e.g. Jupyter/REPL): spawn has
    # nothing to re-import, so the default goes parallel.
    fake_main = types.ModuleType("__main__")
    monkeypatch.setitem(sys.modules, "__main__", fake_main)
    assert resolve(None) == ingest_mod._cpu_cap()

    # Script (__main__ with __file__): default stays serial, with a one-time hint.
    fake_main.__file__ = "/some/script.py"
    monkeypatch.setattr(ingest_mod, "_workers_hint_emitted", False)
    monkeypatch.setattr(ingest_mod, "_cpu_cap", lambda: 8)
    with caplog.at_level(logging.INFO, logger="tse_tick.ingest"):
        assert resolve(None) == 1
        assert resolve(None) == 1
    hints = [r for r in caplog.records if "max_workers" in r.getMessage()]
    assert len(hints) == 1                              # hint logged exactly once


def test_ingest_period_auto_equals_serial(tmp_path):
    src = tmp_path / "src"
    _seed_stock(src)
    serial = tmp_path / "serial"
    auto = tmp_path / "auto"
    tse_tick.ingest_period(str(src), str(serial), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", max_workers=1)
    tse_tick.ingest_period(str(src), str(auto), f"{DAYS[0]}-{DAYS[-1]}",
                           "individual_stock", max_workers="auto")
    _assert_stores_identical(_snapshot(serial, "individual_stock"),
                             _snapshot(auto, "individual_stock"))


def test_cli_parallel_accepts_auto_and_int():
    import pytest
    from tse_tick.cli import _build_parser, _parse_parallel

    assert _parse_parallel("auto") == "auto"
    assert _parse_parallel("2") == 2
    with pytest.raises(Exception):
        _parse_parallel("zero-ish")
    parser = _build_parser()
    args = parser.parse_args(["ingest", "--data-type", "individual_stock",
                              "--input-root", "x", "--output-root", "y",
                              "--period", "2024"])
    assert args.parallel == "auto"                      # the CLI default
    args = parser.parse_args(["ingest", "--data-type", "individual_stock",
                              "--input-root", "x", "--output-root", "y",
                              "--period", "2024", "--parallel", "2"])
    assert args.parallel == 2
