# tests/test_audit_fixes.py
"""Regression tests for the 7203 two-stage extraction audit findings (B1-B11).

Each test reproduces a defect from the 0.13.0 audit
(``benchmark_extraction_7203/run_7203_2021-2023_twostage_v0.13.0/BUGS_AND_INEFFICIENCIES.md``)
and locks in its fix. Synthetic NEEDS-format fixtures only — no proprietary data.
"""
from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tse_tick.io.parquet import write_event_window_parquet, write_partitioned_parquet
from tests.synthetic_data import individual_stock_csv, stock_summary_csv, write_zip

DAY1, DAY2 = "20240104", "20240105"


def _seed_stock(root, days=(DAY1, DAY2), codes=("1301", "7203")):
    for day in days:
        leaf = root / f"個別株式{day[:4]}" / "TICST120" / day[:6]
        leaf.mkdir(parents=True, exist_ok=True)
        write_zip(
            leaf / f"HTICST120.{day}.1.zip",
            f"HTICST120.{day}.1.csv",
            individual_stock_csv(day, list(codes), rows_per_ticker=6),
        )


def _frame(tmp_path, day=DAY1, codes=("1301", "7203")):
    """A real cleaned individual_stock frame (via the actual parse pipeline)."""
    z = tmp_path / f"HTICST120.{day}.1.zip"
    write_zip(
        z, f"HTICST120.{day}.1.csv", individual_stock_csv(day, list(codes), rows_per_ticker=4)
    )
    return tse_tick.create_df(
        str(z), auto_detect=False, data_type="individual_stock", year=int(day[:4])
    )


# --------------------------------------------------------------------------
# B1 — unguarded top-level parallel ingest crashed with the raw
#      freeze_support RuntimeError instead of an actionable message
# --------------------------------------------------------------------------


def test_ingest_during_spawn_bootstrap_raises_actionable_error(tmp_path, monkeypatch):
    """An ingest call re-executed while a spawn worker is bootstrapping (i.e. the
    user's call sits at module top level, so every worker re-imports and re-runs
    it) must fail with a clear pointer to the ``if __name__ == "__main__":``
    guard — not the stdlib's raw ``freeze_support`` RuntimeError (B1)."""
    import multiprocessing

    src, store = tmp_path / "src", tmp_path / "store"
    _seed_stock(src, days=(DAY1,))
    proc = multiprocessing.current_process()
    monkeypatch.setattr(proc, "_inheriting", True, raising=False)
    with pytest.raises(RuntimeError, match=r"if __name__ == .__main__."):
        tse_tick.ingest_period(str(src), str(store), DAY1, "individual_stock", max_workers=2)
    with pytest.raises(RuntimeError, match=r"if __name__ == .__main__."):
        tse_tick.ingest_directory(str(src), str(store), data_type="individual_stock")


def test_unguarded_toplevel_parallel_script_gets_actionable_error(tmp_path):
    """End-to-end B1 reproduction: a plain script calling
    ``ingest_period(..., max_workers=2)`` at module top level (no ``__main__``
    guard) must surface tse_tick's actionable error in its output."""
    import subprocess
    import sys

    src, store = tmp_path / "src", tmp_path / "store"
    _seed_stock(src)
    script = tmp_path / "run_unguarded.py"
    script.write_text(
        "import tse_tick\n"
        f"tse_tick.ingest_period({str(src)!r}, {str(store)!r}, "
        f"'{DAY1}-{DAY2}', 'individual_stock', max_workers=2)\n",
        encoding="utf-8",
    )
    result = subprocess.run(
        [sys.executable, str(script)], capture_output=True, text=True, timeout=300
    )
    combined = result.stdout + result.stderr
    assert result.returncode != 0, "the unguarded script cannot succeed under spawn"
    assert 'if __name__ == "__main__":' in combined, (
        "expected tse_tick's actionable guard message in the crash output, got:\n" + combined
    )


# --------------------------------------------------------------------------
# B11 — non-atomic partition write + existence-only resume = trusted corruption
# --------------------------------------------------------------------------


def test_resume_reingests_truncated_partition(tmp_path):
    """A truncated (killed-mid-write) partition must be re-ingested on resume,
    not trusted forever (B11 — observed live as an unreadable date=20220511)."""
    src, store = tmp_path / "src", tmp_path / "store"
    _seed_stock(src)
    tse_tick.ingest_period(
        str(src), str(store), f"{DAY1}-{DAY2}", "individual_stock", ticker_filter={"7203"}
    )
    f = store / "individual_stock" / f"date={DAY1}" / "ticker=7203.parquet"
    good = pl.read_parquet(f)
    data = f.read_bytes()
    f.write_bytes(data[: len(data) // 2])  # simulate the observed kill-mid-write
    with pytest.raises(Exception):
        pl.read_parquet(f)  # sanity: the planted file IS unreadable

    results = tse_tick.ingest_period(
        str(src), str(store), f"{DAY1}-{DAY2}", "individual_stock", ticker_filter={"7203"}
    )
    redone = [m["date"] for m in results if "date" in m]
    assert DAY1 in redone, "truncated date must be re-ingested, not resume-skipped"
    assert DAY2 not in redone, "intact dates must still resume-skip"
    assert pl.read_parquet(f).equals(good), "store must be healed"


def test_failed_write_leaves_no_final_partition(tmp_path, monkeypatch):
    """A write that dies partway may not leave a half-written FINAL file that
    resume / query globs would trust (B11: temp file + atomic os.replace)."""
    df = _frame(tmp_path)
    store = tmp_path / "store"

    def dying_write(self, path, *a, **kw):
        Path(path).write_bytes(b"NOT A PARQUET FILE")  # half-written garbage
        raise OSError("simulated crash mid-write")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", dying_write)
    with pytest.raises(OSError):
        write_partitioned_parquet(df, str(store), "individual_stock")
    assert list((store / "individual_stock").rglob("*.parquet")) == []
    assert list((store / "individual_stock").rglob("*.tmp")) == []  # temps cleaned up


def test_multi_ticker_date_write_is_all_or_nothing(tmp_path, monkeypatch):
    """If the writer dies after ticker A's file but before ticker B's, NO final
    file may appear for that date — otherwise the existence-keyed resume trusts
    a partial date forever (B11, multi-file variant)."""
    df = _frame(tmp_path, codes=("1301", "7203"))
    store = tmp_path / "store"
    real = pl.DataFrame.write_parquet
    calls = {"n": 0}

    def second_write_dies(self, path, *a, **kw):
        calls["n"] += 1
        if calls["n"] >= 2:
            raise OSError("simulated crash between ticker files")
        return real(self, path, *a, **kw)

    monkeypatch.setattr(pl.DataFrame, "write_parquet", second_write_dies)
    with pytest.raises(OSError):
        write_partitioned_parquet(df, str(store), "individual_stock")
    assert list((store / "individual_stock").rglob("*.parquet")) == []


def test_event_window_write_failure_keeps_existing_file(tmp_path, monkeypatch):
    """write_event_window_parquet accumulates a date file by rewriting it; a
    mid-write death must not destroy the previously accumulated rows (B11)."""
    df = _frame(tmp_path)
    store = tmp_path / "ew"
    write_event_window_parquet(df, str(store))
    fpath = next(Path(store).rglob("*.parquet"))
    before = pl.read_parquet(fpath)

    def dying_write(self, path, *a, **kw):
        Path(path).write_bytes(b"GARBAGE")
        raise OSError("simulated crash mid-write")

    monkeypatch.setattr(pl.DataFrame, "write_parquet", dying_write)
    with pytest.raises(OSError):
        write_event_window_parquet(df, str(store))
    assert pl.read_parquet(fpath).equals(before), "existing accumulated file destroyed"


# --------------------------------------------------------------------------
# B3 — DuckDB spilled tens of GB into ./.tmp in the caller's cwd (and left it
#      orphaned on interruption): no temp_directory was configured
# --------------------------------------------------------------------------


def test_duckdb_connections_spill_to_system_temp(tmp_path, monkeypatch):
    """Query connections must set a temp_directory under the system temp dir, so
    a big sort spills there — not into an orphaned ./.tmp in the user's cwd (B3,
    observed as 31 GB left in the repo root)."""
    import tempfile

    from tse_tick.query import _duckdb_connect

    con = _duckdb_connect()
    try:
        val = con.execute("SELECT current_setting('temp_directory')").fetchone()[0]
    finally:
        con.close()
    assert val, "temp_directory must be configured"
    sys_tmp = Path(tempfile.gettempdir()).resolve()
    assert sys_tmp in Path(val).resolve().parents, val

    # and query_ticks must route through the configured connection helper
    import tse_tick.query as q

    calls = {"n": 0}
    real = q._duckdb_connect

    def spy():
        calls["n"] += 1
        return real()

    monkeypatch.setattr(q, "_duckdb_connect", spy)
    _seed_stock(tmp_path / "src", days=(DAY1,))
    tse_tick.ingest_period(str(tmp_path / "src"), str(tmp_path / "store"), DAY1, "individual_stock")
    df = q.query_ticks(str(tmp_path / "store"), ticker="7203", date=DAY1)
    assert df.height > 0
    assert calls["n"] >= 1, "query_ticks must use the temp-configured connection"


# --------------------------------------------------------------------------
# B8 — parse_period rejected the intuitive YYYY-YYYY multi-year range
# --------------------------------------------------------------------------


def test_parse_period_accepts_year_range():
    parsed = tse_tick.parse_period("2021-2023")
    assert parsed["granularity"] == "year"
    assert parsed["years"] == [2021, 2022, 2023]


def test_parse_period_single_year_range_and_reversed():
    assert tse_tick.parse_period("2021-2021")["years"] == [2021]
    with pytest.raises(ValueError):
        tse_tick.parse_period("2023-2021")  # reversed range
    with pytest.raises(ValueError):
        tse_tick.parse_period("2021-202")  # mixed widths stay rejected


def test_ingest_period_year_range_end_to_end(tmp_path):
    """A YYYY-YYYY period must drive a real multi-year ingest (B8)."""
    src = tmp_path / "src"
    d23, d24 = "20231228", "20240104"
    _seed_stock(src, days=(d23, d24))
    store = tmp_path / "store"
    results = tse_tick.ingest_period(
        str(src), str(store), "2023-2024", "individual_stock", ticker_filter={"7203"}
    )
    assert sorted(m["date"] for m in results) == [d23, d24]
    assert (store / "individual_stock" / f"date={d23}" / "ticker=7203.parquet").exists()
    assert (store / "individual_stock" / f"date={d24}" / "ticker=7203.parquet").exists()


# --------------------------------------------------------------------------
# B4 — extract_to_store had no max_workers (the recommended one-liner was
#      pinned serial); B5 — on a reused store it returned EVERY stored day;
# B2 — it materializes everything with no warning
# --------------------------------------------------------------------------


def test_extract_to_store_passes_max_workers_through(tmp_path, monkeypatch):
    """extract_to_store must forward max_workers to ingest_period (B4)."""
    _seed_stock(tmp_path / "src", days=(DAY1,))
    import tse_tick.ingest as ingest_mod

    seen = {}
    real = ingest_mod.ingest_period

    def spy(*args, **kwargs):
        seen.update(kwargs)
        return real(*args, **kwargs)

    monkeypatch.setattr(ingest_mod, "ingest_period", spy)
    df = tse_tick.extract_to_store(
        str(tmp_path / "src"), str(tmp_path / "store"), DAY1, "7203", max_workers=3
    )
    assert seen.get("max_workers") == 3
    assert df.height > 0


def test_extract_to_store_parallel_equals_serial(tmp_path):
    """A parallel extract_to_store must return the same rows and build the same
    store as the serial one (B4 end-to-end)."""
    _seed_stock(tmp_path / "src")
    src = str(tmp_path / "src")
    serial = tse_tick.extract_to_store(src, str(tmp_path / "s1"), f"{DAY1}-{DAY2}", "7203")
    parallel = tse_tick.extract_to_store(
        src, str(tmp_path / "s2"), f"{DAY1}-{DAY2}", "7203", max_workers=2
    )
    assert parallel.equals(serial)


def test_extract_to_store_scopes_reused_store_to_period(tmp_path):
    """On a REUSED store, extract_to_store must return only `period`'s days —
    not every day the store happens to hold (B5)."""
    src = tmp_path / "src"
    jan, feb = "20240104", "20240205"
    _seed_stock(src, days=(jan, feb))
    store = str(tmp_path / "store")
    first = tse_tick.extract_to_store(str(src), store, "202401", "7203")
    assert set(first["date"].unique().to_list()) == {int(jan)}
    second = tse_tick.extract_to_store(str(src), store, "202402", "7203")
    assert set(second["date"].unique().to_list()) == {
        int(feb)
    }, "the second period's extract must not leak the first period's days"


def test_extract_to_store_scopes_year_and_range_periods(tmp_path):
    """Period scoping must hold for year and date-range forms too (B5)."""
    src = tmp_path / "src"
    d23, d24a, d24b = "20231228", "20240104", "20240105"
    _seed_stock(src, days=(d23, d24a, d24b))
    store = str(tmp_path / "store")
    got23 = tse_tick.extract_to_store(str(src), store, "2023", "7203")
    assert set(got23["date"].unique().to_list()) == {int(d23)}
    got_range = tse_tick.extract_to_store(str(src), store, f"{d24a}-{d24a}", "7203")
    assert set(got_range["date"].unique().to_list()) == {int(d24a)}


def test_extract_to_store_warns_on_large_result(tmp_path, monkeypatch):
    """Past a large-row threshold extract_to_store must emit a capturable
    warning before materializing the whole result (B2 mitigation — the no-cap
    return itself is intentional and stays)."""
    import warnings

    import tse_tick.ingest as ingest_mod

    _seed_stock(tmp_path / "src", days=(DAY1,))
    src, store = str(tmp_path / "src"), str(tmp_path / "store")

    monkeypatch.setattr(ingest_mod, "_LARGE_EXTRACT_ROWS", 5)  # synthetic-scale threshold
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = tse_tick.extract_to_store(src, store, DAY1, "7203")
    assert df.height > 5
    assert any(
        issubclass(w.category, tse_tick.LargeResultWarning) for w in caught
    ), "expected a LargeResultWarning past the threshold"

    # under the threshold: no warning
    monkeypatch.setattr(ingest_mod, "_LARGE_EXTRACT_ROWS", 10_000_000)
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        tse_tick.extract_to_store(src, str(tmp_path / "store2"), DAY1, "7203")
    assert not any(issubclass(w.category, tse_tick.LargeResultWarning) for w in caught)


def test_resume_skips_daily_token_summary_date(tmp_path):
    """Summary stores hold ``<date>.parquet`` (no ``ticker=`` files); the resume
    check must still recognise an ingested daily-token date (new finding while
    fixing B11: the skip glob only matched ``ticker=*.parquet``)."""
    src = tmp_path / "src"
    day = "20230508"
    leaf = src / "個別株式2023" / "TICSS110" / day[:6]
    leaf.mkdir(parents=True)
    write_zip(
        leaf / f"HTICSS110.{day}.zip", f"HTICSS110.{day}.csv", stock_summary_csv(day, ["7203"])
    )
    store = tmp_path / "store"
    first = tse_tick.ingest_period(str(src), str(store), day, "stock_summary")
    assert any(m.get("rows") for m in first), "seed ingest must write rows"
    second = tse_tick.ingest_period(str(src), str(store), day, "stock_summary")
    assert second == [], "an intact summary date must resume-skip, not re-ingest"
