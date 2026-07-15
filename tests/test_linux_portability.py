"""Issue #72: Linux portability fixes, verified on WSL2 Ubuntu 24.04 / py3.12.

The package core already ran correctly on Linux at 0.15.0 — ``spawn`` is forced
explicitly, coverage markers are portable JSON, DuckDB globs are normalized, the
Windows console shim no-ops off Windows. What did not hold up was everything
around the edges, and each item below is one of those:

* **F1** — the CLI tests spawned a literal ``"python"``, which stock Debian /
  Ubuntu does not ship (only ``python3``), so the suite showed 2 failures that
  looked like package breakage. Guarded here by a source scan, the way
  ``test_consolidation.py`` guards the type-classification SSOT.
* **F2** — ``_available_ram_gb()`` read ``SC_AVPHYS_PAGES`` (MemFree) on Linux,
  which excludes the reclaimable page cache, while the Windows branch reports
  MemAvailable-equivalent. A cache-warm 128 GB box looked like it had ~1 GB and
  ``_cap_workers()`` throttled a parallel ingest toward serial.
* **F3** — ZIP discovery matched case-sensitively off Windows, so a ``….ZIP``
  delivery was silently invisible on Linux while Windows ingested it.
* **F5** — the CLI's ``--tickers @file`` decoded with the platform locale, so a
  file of Japanese index display names parsed differently per OS.

Synthetic-first per repo convention: no real NEEDS data is touched.
"""

import logging
import os
import re
import sys
from pathlib import Path

import pytest

from tse_tick.cli import _parse_tickers
from tse_tick.enhanced import _ci_glob, _dedupe_ci, create_df, discover_zips
from tse_tick.ingest import (
    _available_ram_gb,
    _cap_workers,
    _meminfo_available_gb,
    ingest_directory,
)
from tests.synthetic_data import individual_stock_csv, write_zip

_WINDOWS_CASE_INSENSITIVE = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows paths compare case-insensitively — case variants are one file there",
)


# individual_stock_csv splits rows evenly across the morning/afternoon sessions,
# so a seeded part holds exactly this many rows per code.
_ROWS_PER_CODE = 4


def _seed_zip(directory: Path, name: str, day: str = "20230104", codes=("7203",)) -> Path:
    """Write one valid synthetic TICST120 part named exactly ``name``."""
    directory.mkdir(parents=True, exist_ok=True)
    return write_zip(
        directory / name,
        f"HTICST120.{day}.1.csv",
        individual_stock_csv(day, list(codes), rows_per_ticker=_ROWS_PER_CODE),
    )


# ===================================================================
# F1 — no test may spawn a bare "python"
# ===================================================================

def test_no_test_spawns_bare_python():
    """No test may subprocess a literal "python": stock Debian/Ubuntu has none.

    Stock Debian/Ubuntu ships ``python3`` only (``python`` needs the
    ``python-is-python3`` package), so ``subprocess.run(["python", ...])`` is a
    FileNotFoundError there unless a venv happens to be activated — which is why
    this survived until someone ran the suite as ``venv/bin/python -m pytest``.
    ``sys.executable`` is portable AND pins the interpreter running pytest.
    """
    bare_python = re.compile(r"""\[\s*["']python["']\s*,""")
    offenders = [
        p.name
        for p in Path(__file__).parent.glob("test_*.py")
        if p.name != Path(__file__).name  # this file quotes the pattern it forbids
        and bare_python.search(p.read_text(encoding="utf-8"))
    ]
    assert offenders == [], (
        f"{offenders} spawn a literal 'python'; use sys.executable so the test "
        f"runs on a stock Linux box (and under the interpreter pytest is using)"
    )


# ===================================================================
# F2 — MemAvailable, not MemFree, on Linux
# ===================================================================

_MEMINFO_SAMPLE = (
    "MemTotal:       16316532 kB\n"
    "MemFree:         8957744 kB\n"
    "MemAvailable:   15641744 kB\n"
    "Buffers:          123456 kB\n"
)


def test_meminfo_available_gb_reads_memavailable(tmp_path):
    """The MemAvailable line is parsed, not MemFree or MemTotal."""
    f = tmp_path / "meminfo"
    f.write_text(_MEMINFO_SAMPLE, encoding="ascii")
    # 15641744 kB (KiB) -> GB
    assert _meminfo_available_gb(str(f)) == pytest.approx(15641744 * 1024 / 1e9)


def test_meminfo_available_gb_ignores_memfree_prefix_collision(tmp_path):
    """The MemFree line must not be mistaken for the MemAvailable one."""
    f = tmp_path / "meminfo"
    f.write_text(_MEMINFO_SAMPLE, encoding="ascii")
    got = _meminfo_available_gb(str(f))
    assert got != pytest.approx(8957744 * 1024 / 1e9)  # not MemFree
    assert got != pytest.approx(16316532 * 1024 / 1e9)  # not MemTotal


@pytest.mark.parametrize(
    "content",
    [
        "MemTotal:       16316532 kB\nMemFree:         8957744 kB\n",  # pre-3.14 kernel
        "MemAvailable:\n",                                             # truncated line
        "MemAvailable:   not-a-number kB\n",                           # malformed value
        "",                                                            # empty
    ],
)
def test_meminfo_available_gb_returns_zero_when_unusable(tmp_path, content):
    """Anything unparseable yields 0.0 (the "fall back" signal), never raises."""
    f = tmp_path / "meminfo"
    f.write_text(content, encoding="ascii")
    assert _meminfo_available_gb(str(f)) == 0.0


def test_meminfo_available_gb_missing_file_returns_zero(tmp_path):
    assert _meminfo_available_gb(str(tmp_path / "nope")) == 0.0


def test_available_ram_uses_meminfo_on_linux(monkeypatch):
    """On Linux the MemAvailable reader wins over the sysconf (MemFree) path."""
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr("tse_tick.ingest._meminfo_available_gb", lambda: 64.0)
    monkeypatch.setattr(os, "sysconf", lambda name: 1, raising=False)  # sysconf -> ~0 GB
    assert _available_ram_gb() == 64.0


def test_available_ram_falls_back_to_sysconf_without_memavailable(monkeypatch):
    """A kernel with no MemAvailable (reader -> 0.0) keeps the old arithmetic.

    This is also the macOS path, which has no /proc at all.
    """
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr("tse_tick.ingest._meminfo_available_gb", lambda: 0.0)
    monkeypatch.setattr(
        os, "sysconf",
        lambda name: {"SC_AVPHYS_PAGES": 2_000_000, "SC_PAGE_SIZE": 4096}[name],
        raising=False,
    )
    assert _available_ram_gb() == pytest.approx(2_000_000 * 4096 / 1e9)


def test_available_ram_windows_branch_does_not_read_meminfo(monkeypatch):
    """Windows must not touch /proc — its ctypes branch already reports MemAvailable."""
    def _boom():
        raise AssertionError("/proc/meminfo must not be read on Windows")

    monkeypatch.setattr(sys, "platform", "win32")
    monkeypatch.setattr("tse_tick.ingest._meminfo_available_gb", _boom)
    _available_ram_gb()  # must not raise (returns 0.0 off real Windows)


def test_cap_workers_uses_memavailable_not_memfree(monkeypatch):
    """The point of F2: a cache-warm box must not be throttled toward serial.

    Models the live WSL measurement in issue #72 — MemFree 9.0 GB vs MemAvailable
    15.6 GB with 6 GB of page cache — but scaled to a big research box, where the
    gap decides between 1 worker and a real pool.
    """
    monkeypatch.setattr(sys, "platform", "linux")
    monkeypatch.setattr("tse_tick.ingest._cpu_cap", lambda: 16)

    # MemFree on a cache-warm 128 GB box: nearly everything is in the page cache.
    monkeypatch.setattr("tse_tick.ingest._meminfo_available_gb", lambda: 1.0)
    assert _cap_workers(16, per_worker_gb=3.0) == 1  # old behavior: serial

    # MemAvailable: the cache is reclaimable, so the ingest can really have it.
    monkeypatch.setattr("tse_tick.ingest._meminfo_available_gb", lambda: 120.0)
    assert _cap_workers(16, per_worker_gb=3.0) == 16


# ===================================================================
# F3 — discovery is case-insensitive on every platform
# ===================================================================

def test_ci_glob_expands_cased_characters():
    assert _ci_glob("*.zip") == "*.[zZ][iI][pP]"
    assert _ci_glob("HTICST120.*.zip") == "[hH][tT][iI][cC][sS][tT]120.*.[zZ][iI][pP]"


def test_ci_glob_passes_through_uncased_characters():
    """Digits, dots and glob metacharacters are untouched; so is Japanese."""
    assert _ci_glob("120.*.?") == "120.*.?"
    assert _ci_glob("個別株式2023") == "個別株式2023"


@pytest.mark.parametrize("name", [
    "HTICST120.20230104.1.ZIP",   # uppercase extension (the filed evidence)
    "HTICST120.20230104.1.Zip",   # mixed
    "hticst120.20230104.1.zip",   # lowercase prefix
])
def test_discover_zips_fast_path_is_case_insensitive(tmp_path, name):
    """The {year}/{yearmonth}/ fast path finds oddly-cased deliveries."""
    _seed_zip(tmp_path / "2023" / "202301", name)
    found = discover_zips(str(tmp_path), "individual_stock", [2023], [1])
    assert [p.name for p in found] == [name]


@pytest.mark.parametrize("name", [
    "HTICST120.20230104.1.ZIP",
    "hticst120.20230104.1.zip",
])
def test_discover_zips_recursive_fallback_is_case_insensitive(tmp_path, name):
    """The nested-tree fallback (個別株式{year}/TICST120/{yyyymm}/) too."""
    _seed_zip(tmp_path / "個別株式2023" / "TICST120" / "202301", name)
    found = discover_zips(str(tmp_path), "individual_stock", [2023], [1])
    assert [p.name for p in found] == [name]


def test_discover_zips_mixed_case_days_all_found(tmp_path):
    """The exact divergence from issue #72: one odd-cased day among normal ones.

    Windows found both; Linux silently found only the lowercase one — and a
    *partial* miss never trips _warn_zero_discovery, so it was silent.
    """
    day_dir = tmp_path / "2023" / "202301"
    _seed_zip(day_dir, "HTICST120.20230104.1.ZIP", day="20230104")
    _seed_zip(day_dir, "HTICST120.20230105.1.zip", day="20230105")
    found = discover_zips(str(tmp_path), "individual_stock", [2023], [1])
    assert [p.name for p in found] == [
        "HTICST120.20230104.1.ZIP",
        "HTICST120.20230105.1.zip",
    ]


def test_discover_zips_date_filter_is_case_insensitive(tmp_path):
    """The dated fast path (HTICST120.{date}.*.zip) matches ….ZIP as well."""
    _seed_zip(tmp_path / "2023" / "202301", "HTICST120.20230104.1.ZIP")
    _seed_zip(tmp_path / "2023" / "202301", "HTICST120.20230105.1.ZIP", day="20230105")
    found = discover_zips(str(tmp_path), "individual_stock", [2023], [1], dates=["20230104"])
    assert [p.name for p in found] == ["HTICST120.20230104.1.ZIP"]


def test_create_df_reads_uppercase_zip_from_flat_dir(tmp_path):
    """The flat-directory read path (create_df on a folder) matches ….ZIP too."""
    _seed_zip(tmp_path, "HTICST120.20230104.1.ZIP")
    df = create_df(str(tmp_path), data_type="individual_stock", year=2023)
    assert df.height == _ROWS_PER_CODE
    assert len(df.columns) == 95


def test_ingest_directory_reads_uppercase_zip(tmp_path):
    """ingest_directory's flat glob matches ….ZIP, so a store is actually built."""
    src = tmp_path / "in"
    _seed_zip(src, "HTICST120.20230104.1.ZIP")
    out = tmp_path / "store"
    results = ingest_directory(str(src), str(out), data_type="individual_stock")
    assert len(results) == 1 and "error" not in results[0]
    assert list((out / "individual_stock").glob("date=*/ticker=*.parquet"))


# --- dedupe: two spellings of one name must resolve to one file --------------

def test_dedupe_ci_collapses_case_variants_deterministically():
    """Case variants collapse to one, and the survivor does not depend on
    directory order (glob returns arbitrary order on ext4)."""
    a, b = Path("d/HTICST120.20230104.1.ZIP"), Path("d/HTICST120.20230104.1.zip")
    assert _dedupe_ci([a, b]) == _dedupe_ci([b, a])
    assert len(_dedupe_ci([a, b])) == 1


def test_dedupe_ci_keeps_genuinely_different_names():
    names = [Path("d/HTICST120.20230104.1.zip"), Path("d/HTICST120.20230105.1.zip")]
    assert _dedupe_ci(names) == names


def test_dedupe_ci_drops_exact_duplicates_quietly(caplog):
    """One file matched by two fast-path patterns is the normal case, not an anomaly."""
    p = Path("d/HTICST120.20230104.1.zip")
    with caplog.at_level(logging.WARNING, logger="tse_tick.enhanced"):
        assert _dedupe_ci([p, p]) == [p]
    assert caplog.records == []


@_WINDOWS_CASE_INSENSITIVE
def test_dedupe_ci_warns_on_case_variant_collision(caplog):
    """Dropping a file silently is what F3 is about — say so.

    Two case-variant copies of one NEEDS part would otherwise be concatenated as
    two parts of the same trading day, double-counting it.
    """
    a, b = Path("d/HTICST120.20230104.1.ZIP"), Path("d/HTICST120.20230104.1.zip")
    with caplog.at_level(logging.WARNING, logger="tse_tick.enhanced"):
        kept = _dedupe_ci([a, b])
    assert len(kept) == 1
    assert "differs only by letter case" in caplog.text


@_WINDOWS_CASE_INSENSITIVE
def test_discover_zips_dedupes_coexisting_case_variants(tmp_path):
    """On a case-sensitive filesystem both really exist; only one is ingested."""
    day_dir = tmp_path / "2023" / "202301"
    _seed_zip(day_dir, "HTICST120.20230104.1.zip")
    _seed_zip(day_dir, "HTICST120.20230104.1.ZIP")
    assert len(list(day_dir.iterdir())) == 2  # the fixture really is two files
    found = discover_zips(str(tmp_path), "individual_stock", [2023], [1])
    assert len(found) == 1


# ===================================================================
# F5 — --tickers @file is UTF-8 on every platform
# ===================================================================

def test_parse_tickers_file_reads_utf8_japanese(tmp_path):
    """A Japanese index display name survives; under the cp1252 locale default it
    mojibaked on Windows, and a cp932 file raised UnicodeDecodeError on Linux."""
    f = tmp_path / "tickers.txt"
    f.write_text("日経平均株価\nTOPIX\n7203\n", encoding="utf-8")
    assert _parse_tickers(f"@{f}") == {"日経平均株価", "TOPIX", "7203"}


def test_parse_tickers_file_strips_utf8_bom(tmp_path):
    """utf-8-sig: Notepad-style BOMs must not end up glued to the first ticker."""
    f = tmp_path / "tickers.txt"
    f.write_text("7203\n9984\n", encoding="utf-8-sig")
    assert _parse_tickers(f"@{f}") == {"7203", "9984"}


def test_parse_tickers_inline_list_unchanged(tmp_path):
    """Control: the comma-separated form is untouched by the encoding fix."""
    assert _parse_tickers("7203, 9984 ,6758") == {"7203", "9984", "6758"}
