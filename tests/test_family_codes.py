"""Share-class family semantics across the two-stage individual_stock path.

NEEDS suffixes a share class onto its parent's 4-char code ("72031" = Toyota
New Shares). Stage 1's field-5 filter has always kept the whole family for a
4-char request (it compares first-4-chars), writing separate ticker=7203 and
ticker=72031 files — but Stage 2 used to select files by exact stem, silently
dropping the suffixed classes' rows (extract_to_store returned fewer rows than
read_ticks), and a raw 5-char request matched nothing at all. These tests pin
the fix: a 4-char code selects the family everywhere; a 5-char code is rooted
to its family by the raw-read/two-stage entry points and reads exactly that
class via query_ticks on a built store.
"""
import polars as pl
import pytest

import tse_tick
from tse_tick.enhanced import _code_matches_family, _stock_family_roots
from tests.synthetic_data import seed_structured_day

pytest.importorskip("duckdb")

# De-tie the family members' timestamps: DuckDB's parallel sort has an
# arbitrary within-tie order, so frame comparisons need distinct times.
_OFFSETS = {"72031": 1, "9999": 2}
_DAY = "20240104"
_MAPPING = {1: ["1301"], 2: ["7203", "72031"], 3: ["9999"]}


def _seed(root):
    seed_structured_day(root, _DAY, _MAPPING, minute_offsets=_OFFSETS)


def _codes(df):
    return set(
        df.select(pl.col("Stock Code").cast(pl.String).str.strip_chars().unique())
        .to_series()
        .to_list()
    )


# --- helpers -----------------------------------------------------------------

def test_stock_family_roots():
    assert _stock_family_roots(None) is None
    assert _stock_family_roots({"7203"}) == {"7203"}
    assert _stock_family_roots({"72031"}) == {"7203"}
    assert _stock_family_roots({7203, "72031", " 9984 "}) == {"7203", "9984"}
    assert _stock_family_roots({"130A"}) == {"130A"}  # alphanumeric 4-char untouched


def test_code_matches_family():
    assert _code_matches_family("7203", "7203")
    assert _code_matches_family("72031", "7203")   # 4-char request: family prefix
    assert not _code_matches_family("7204", "7203")
    assert _code_matches_family("72031", "72031")  # 5-char request: exact only
    assert not _code_matches_family("7203", "72031")
    assert not _code_matches_family("1010", "101")  # shorter codes stay exact


# --- Stage 1 -----------------------------------------------------------------

def test_ingest_writes_family_files_for_parent_request(tmp_path):
    _seed(tmp_path / "src")
    store = tmp_path / "store"
    tse_tick.ingest_period(
        str(tmp_path / "src"), str(store), _DAY, "individual_stock",
        ticker_filter={"7203"},
    )
    date_dir = store / "individual_stock" / f"date={_DAY}"
    assert (date_dir / "ticker=7203.parquet").exists()
    assert (date_dir / "ticker=72031.parquet").exists()


def test_ingest_suffixed_request_roots_to_family(tmp_path):
    # A raw "72031" request used to match nothing (filter compared 4-char codes
    # against the 5-char request) — it now ingests the whole family.
    _seed(tmp_path / "src")
    store = tmp_path / "store"
    tse_tick.ingest_period(
        str(tmp_path / "src"), str(store), _DAY, "individual_stock",
        ticker_filter={"72031"},
    )
    date_dir = store / "individual_stock" / f"date={_DAY}"
    assert (date_dir / "ticker=7203.parquet").exists()
    assert (date_dir / "ticker=72031.parquet").exists()


def test_read_ticks_suffixed_request_returns_family(tmp_path):
    _seed(tmp_path / "src")
    fam = tse_tick.read_ticks(str(tmp_path / "src"), ticker_filter={"7203"}, date=_DAY)
    suf = tse_tick.read_ticks(str(tmp_path / "src"), ticker_filter={"72031"}, date=_DAY)
    assert suf.height == fam.height > 0
    assert _codes(fam) == {"7203", "72031"}


# --- Stage 2 / extract_to_store ----------------------------------------------

def test_extract_to_store_returns_family_rows(tmp_path):
    # The C1 symptom: extract_to_store("7203") returned fewer rows than
    # read_ticks because Stage 2 never opened ticker=72031.parquet.
    _seed(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    df = tse_tick.extract_to_store(src, store, _DAY, "7203")
    ref = tse_tick.read_ticks(src, ticker_filter={"7203"}, date=_DAY)
    assert df.height == ref.height > 0
    assert _codes(df) == {"7203", "72031"}


def test_extract_to_store_suffixed_equals_parent(tmp_path):
    _seed(tmp_path / "src")
    src = str(tmp_path / "src")
    parent = tse_tick.extract_to_store(src, str(tmp_path / "s1"), _DAY, "7203")
    suffixed = tse_tick.extract_to_store(src, str(tmp_path / "s2"), _DAY, "72031")
    assert suffixed.equals(parent)


def test_query_ticks_family_and_exact(tmp_path):
    _seed(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    tse_tick.extract_to_store(src, store, _DAY, "7203")
    fam = tse_tick.query_ticks(store, ticker="7203", date=_DAY)
    assert _codes(fam) == {"7203", "72031"}
    exact = tse_tick.query_ticks(store, ticker="72031", date=_DAY)
    assert _codes(exact) == {"72031"}          # 5-char: exactly that class
    assert 0 < exact.height < fam.height
    as_int = tse_tick.query_ticks(store, ticker=7203, date=_DAY)
    assert as_int.height == fam.height


def test_resume_skips_family_covered_dates(tmp_path):
    # A store built for "7203" satisfies a later "72031" request (same family
    # root) — the second call must not re-prune or re-read the day's parts.
    _seed(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    first = tse_tick.extract_to_store(src, store, _DAY, "7203")

    import tse_tick.ingest as ingest_mod
    calls = []
    real = ingest_mod._prune_parts_by_ticker

    def spy(zips, tickers):
        calls.append(set(tickers))
        return real(zips, tickers)

    ingest_mod._prune_parts_by_ticker = spy
    try:
        second = tse_tick.extract_to_store(src, store, _DAY, "72031")
    finally:
        ingest_mod._prune_parts_by_ticker = real
    assert calls == [], "resumed family request must not re-prune the day"
    assert second.equals(first)


def test_large_result_estimate_counts_family_rows(tmp_path, monkeypatch):
    # The pre-materialization row estimate must count the suffixed class files
    # the query is about to read, not just the parent's.
    _seed(tmp_path / "src")
    src, store = str(tmp_path / "src"), str(tmp_path / "store")
    ref = tse_tick.read_ticks(src, ticker_filter={"7203"}, date=_DAY)
    import tse_tick.ingest as ingest_mod
    # Parent-only rows would stay under this threshold; family rows exceed it.
    parent_rows = ref.filter(
        pl.col("Stock Code").str.strip_chars() == "7203"
    ).height
    monkeypatch.setattr(ingest_mod, "_LARGE_EXTRACT_ROWS", parent_rows)
    with pytest.warns(tse_tick.LargeResultWarning):
        tse_tick.extract_to_store(src, store, _DAY, "7203")
