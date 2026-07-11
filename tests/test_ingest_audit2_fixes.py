# tests/test_ingest_audit2_fixes.py
"""Regression tests for the ingest / raw-parse audit findings (H1, H2, M1-M4).

Each test reproduces a defect found in the 0.13.1 ingest + process-into-DataFrame
audit and locks in its fix. Synthetic NEEDS-format fixtures only.

H1 - flat-path (ingest_directory / ingest_year) per-ZIP writes overwrote
     multi-part days (the closing-appendix part clobbered the session's rows).
H2 - resume skipped a date whenever ANY parquet existed there, so a store built
     for ticker A silently returned nothing for a later ticker-B request.
M1 - per-part read errors were swallowed: the day was written partial, carried
     no error flag, and resume trusted it forever; the zip-bomb guards' own
     ValueErrors were caught by the same handler.
M2 - ingest_year matched the year as a filename SUBSTRING ("20201207" contains
     "2012"), ingesting wrong-year files.
M3 - the Stock Code suffix decode ("72031" -> "72031New Shares") plus the [:4]
     partition truncation collided a suffixed code with its parent: same target
     file, same tmp name, crash (or silent mislabeling).
M4 - only zf.namelist()[0] was parsed although up to 5 members are allowed:
     members 2..5 were silently dropped.
L1 - read_ticks' row cap broke the ZIP loop at total >= rows but warned only on
     result.height > rows: an exact-fit total with ZIPs still unread returned
     silently incomplete data.
L2 - a raw Data Date that clean_data's non-strict parse nulled was written to an
     unqueryable date=None/ partition instead of being dropped loudly.
"""
import warnings
import zipfile

import polars as pl
import pytest

import tse_tick
from tse_tick.ingest import ingest_directory, ingest_year
from tse_tick.io.parquet import write_partitioned_parquet
from tests.synthetic_data import individual_stock_csv, write_zip

DATE = "20240104"


def _rows(store, date, ticker):
    f = store / "individual_stock" / f"date={date}" / f"ticker={ticker}.parquet"
    return pl.read_parquet(f).height if f.exists() else 0


def _two_part_day(folder, date=DATE):
    """Part 1: tickers 1301 + 1305. Part 2: ticker 7203 + a closing tail for 1301."""
    folder.mkdir(parents=True, exist_ok=True)
    write_zip(
        folder / f"HTICST120.{date}.1.zip", f"HTICST120.{date}.1.csv",
        individual_stock_csv(date, ["1301", "1305"], rows_per_ticker=40,
                             base_prices={"1301": 2000, "1305": 3000}),
    )
    part2 = (
        individual_stock_csv(date, ["7203"], rows_per_ticker=40, base_prices={"7203": 2100})
        + individual_stock_csv(date, ["1301"], rows_per_ticker=4, base_prices={"1301": 2000})
    )
    write_zip(folder / f"HTICST120.{date}.2.zip", f"HTICST120.{date}.2.csv", part2)


def _seed_structured(root, date=DATE):
    leaf = root / date[:4] / date[:6]
    _two_part_day(leaf, date)
    return root


# --------------------------------------------------------------------------
# H1 - flat-path multi-part-day overwrite
# --------------------------------------------------------------------------


def test_flat_directory_collects_all_parts_of_a_day(tmp_path):
    """ingest_directory must ingest a day's parts as ONE unit: the appendix part
    may not overwrite (and the resume may not skip) earlier parts' rows (H1)."""
    flat = tmp_path / "flat"
    _two_part_day(flat)
    store = tmp_path / "store"

    results = ingest_directory(str(flat), str(store), data_type="individual_stock")

    assert all("error" not in m for m in results), results
    assert _rows(store, DATE, 7203) == 40   # part 2 not skipped
    assert _rows(store, DATE, 1305) == 40
    # part 1 (40 rows) + part 2 closing tail (4 rows), NOT overwritten by the tail
    assert _rows(store, DATE, 1301) == 44


def test_flat_directory_parallel_collects_all_parts(tmp_path):
    """The parallel flat path must group parts per day too — with per-ZIP tasks
    the appendix/session winner was completion-order nondeterministic (H1)."""
    flat = tmp_path / "flat"
    _two_part_day(flat)
    store = tmp_path / "store"

    results = ingest_directory(
        str(flat), str(store), data_type="individual_stock",
        max_workers=2, progress=False,
    )

    assert all("error" not in m for m in results), results
    assert _rows(store, DATE, 1301) == 44
    assert _rows(store, DATE, 7203) == 40


def test_flat_directory_auto_detects_type_per_day(tmp_path):
    """data_type=None still auto-detects from the day's filenames (H1 rewrite
    must not regress detection)."""
    flat = tmp_path / "flat"
    _two_part_day(flat)
    store = tmp_path / "store"

    results = ingest_directory(str(flat), str(store))

    assert all(m.get("data_type") == "individual_stock" for m in results), results
    assert _rows(store, DATE, 1301) == 44


def test_ingest_year_collects_all_parts_of_a_day(tmp_path):
    """ingest_year must also ingest per-day units, not per-ZIP writes (H1)."""
    flat = tmp_path / "flat"
    _two_part_day(flat)
    store = tmp_path / "store"

    results = ingest_year(str(flat), str(store), year=2024, data_type="individual_stock")

    assert all("error" not in m for m in results), results
    assert _rows(store, DATE, 1301) == 44
    assert _rows(store, DATE, 7203) == 40


# --------------------------------------------------------------------------
# H2 - resume ignored ticker coverage
# --------------------------------------------------------------------------


def test_extract_to_store_new_ticker_on_reused_store(tmp_path):
    """A reused store built for ticker A must still return ticker B's rows —
    the existence-keyed resume used to skip Stage 1 and return empty (H2)."""
    pytest.importorskip("duckdb")
    src = _seed_structured(tmp_path / "src")
    store = str(tmp_path / "store")

    first = tse_tick.extract_to_store(str(src), store, DATE, "1301")
    assert first.height == 44

    second = tse_tick.extract_to_store(str(src), store, DATE, "7203")
    assert second.height == 40, "ticker B on a reused store must not come back empty"
    codes = set(second["Stock Code"].str.strip_chars().unique().to_list())
    assert codes == {"7203"}


def test_resume_full_ingest_completes_filtered_store(tmp_path):
    """A full ingest over a store previously built with a ticker_filter must
    re-ingest the dates (filtered coverage does not satisfy a full request)."""
    src = _seed_structured(tmp_path / "src")
    store = tmp_path / "store"

    tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                           ticker_filter={"1301"})
    assert _rows(store, DATE, 7203) == 0  # filtered store: no 7203 yet

    tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock", resume=True)
    assert _rows(store, DATE, 7203) == 40, "full resume must complete the store"
    assert _rows(store, DATE, 1305) == 40


def test_resume_same_coverage_still_skips(tmp_path):
    """Coverage-aware resume must keep skipping requests the store satisfies:
    same filter, a subset filter, and anything after a full ingest."""
    src = _seed_structured(tmp_path / "src")
    store = tmp_path / "store"

    tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                           ticker_filter={"1301", "7203"})
    again = tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                                   ticker_filter={"1301", "7203"}, resume=True)
    assert again == [], "identical coverage must resume-skip"
    subset = tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                                    ticker_filter={"7203"}, resume=True)
    assert subset == [], "a subset of the stored coverage must resume-skip"

    full_store = tmp_path / "store_full"
    tse_tick.ingest_period(str(src), str(full_store), DATE, "individual_stock")
    after_full = tse_tick.ingest_period(str(src), str(full_store), DATE,
                                        "individual_stock", ticker_filter={"1305"},
                                        resume=True)
    assert after_full == [], "full coverage must satisfy any filtered request"


def test_legacy_store_without_marker_reingests_new_ticker(tmp_path):
    """A pre-marker (legacy) store must re-ingest a date when a requested
    ticker's file is absent — absence is ambiguous, so err toward re-ingest."""
    src = _seed_structured(tmp_path / "src")
    store = tmp_path / "store"

    tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                           ticker_filter={"1301"})
    marker = store / "individual_stock" / f"date={DATE}" / "_ingest_coverage.json"
    assert marker.exists()
    marker.unlink()  # simulate a store written by an older version

    tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                           ticker_filter={"7203"}, resume=True)
    assert _rows(store, DATE, 7203) == 40


def test_coverage_marker_invisible_to_queries(tmp_path):
    """The marker file may not surface as data in any read path."""
    pytest.importorskip("duckdb")
    src = _seed_structured(tmp_path / "src")
    store = tmp_path / "store"
    tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock")

    df = tse_tick.query_ticks(str(store), ticker="1301", date=DATE)
    assert df.height == 44
    assert tse_tick.get_available_dates(str(store)) == [DATE]
    assert "1301" in tse_tick.get_available_tickers(str(store))


# --------------------------------------------------------------------------
# M1 - swallowed per-part errors
# --------------------------------------------------------------------------


def test_corrupt_part_is_recorded_and_reingested_on_resume(tmp_path):
    """A day that lost a part must carry an ``errors`` record and stay
    resume-eligible; healing the part and resuming must complete the day (M1)."""
    src = tmp_path / "src"
    leaf = src / DATE[:4] / DATE[:6]
    leaf.mkdir(parents=True)
    write_zip(leaf / f"HTICST120.{DATE}.1.zip", f"HTICST120.{DATE}.1.csv",
              individual_stock_csv(DATE, ["1301"], rows_per_ticker=40))
    corrupt = leaf / f"HTICST120.{DATE}.2.zip"
    corrupt.write_bytes(b"not a zip file")
    store = tmp_path / "store"

    results = tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock")
    assert len(results) == 1
    assert results[0]["errors"], "the lost part must be recorded, not silent"
    assert _rows(store, DATE, 1301) == 40  # surviving parts still land

    # Heal the corrupt part; resume must NOT trust the partial day.
    write_zip(corrupt, f"HTICST120.{DATE}.2.csv",
              individual_stock_csv(DATE, ["7203"], rows_per_ticker=40))
    results2 = tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                                      resume=True)
    assert [m["date"] for m in results2] == [DATE], "partial day must re-ingest"
    assert not results2[0].get("errors")
    assert _rows(store, DATE, 7203) == 40
    # and once complete, resume skips again
    assert tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock",
                                  resume=True) == []


def test_zip_bomb_guard_error_propagates(tmp_path):
    """The entry-count guard's error must abort the read, not be logged and
    skipped by the generic per-ZIP handler two lines below it (M1)."""
    z = tmp_path / f"HTICST120.{DATE}.1.zip"
    payload = individual_stock_csv(DATE, ["1301"], rows_per_ticker=2)
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        for i in range(6):  # one more than _MAX_ZIP_ENTRIES
            zf.writestr(f"member{i}.csv", payload)

    with pytest.raises(ValueError, match="entries"):
        tse_tick.create_df(str(z), auto_detect=False,
                           data_type="individual_stock", year=2024)


# --------------------------------------------------------------------------
# M2 - ingest_year substring year match
# --------------------------------------------------------------------------


def test_ingest_year_ignores_substring_year_matches(tmp_path):
    """'20201207' contains '2012': year=2012 used to ingest December-2020 files.
    The filter must match the filename date token's year, not a substring (M2)."""
    flat = tmp_path / "flat"
    flat.mkdir()
    write_zip(flat / "HTICST120.20120104.1.zip", "HTICST120.20120104.1.csv",
              individual_stock_csv("20120104", ["1301"], rows_per_ticker=4))
    write_zip(flat / "HTICST120.20201207.1.zip", "HTICST120.20201207.1.csv",
              individual_stock_csv("20201207", ["1301"], rows_per_ticker=4))
    store = tmp_path / "store"

    results = ingest_year(str(flat), str(store), year=2012, data_type="individual_stock")

    assert [m["date"] for m in results] == ["20120104"]
    dates = {p.name for p in (store / "individual_stock").glob("date=*")}
    assert dates == {"date=20120104"}, "the 2020 file must not be ingested as 2012"


# --------------------------------------------------------------------------
# M3 - suffixed-code partition collision
# --------------------------------------------------------------------------


def test_suffixed_code_gets_its_own_partition(tmp_path):
    """A 5-char suffixed code (New Shares '72031') must neither crash the date
    write nor be merged into (or mislabeled as) its 4-char parent '7203' (M3)."""
    src = tmp_path / "src"
    leaf = src / DATE[:4] / DATE[:6]
    leaf.mkdir(parents=True)
    write_zip(leaf / f"HTICST120.{DATE}.1.zip", f"HTICST120.{DATE}.1.csv",
              individual_stock_csv(DATE, ["7203", "72031"], rows_per_ticker=40))
    store = tmp_path / "store"

    results = tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock")
    assert all("error" not in m and not m.get("errors") for m in results), results

    assert _rows(store, DATE, 7203) == 40
    assert _rows(store, DATE, 72031) == 40
    parent = pl.read_parquet(
        store / "individual_stock" / f"date={DATE}" / "ticker=7203.parquet"
    )
    assert set(parent["Stock Code"].unique().to_list()) == {"7203"}, \
        "the parent partition must not contain the suffixed code's rows"


def test_stock_code_column_keeps_raw_code(tmp_path):
    """clean_data must keep Stock Code raw: '72031', not '72031New Shares'."""
    z = tmp_path / f"HTICST120.{DATE}.1.zip"
    write_zip(z, f"HTICST120.{DATE}.1.csv",
              individual_stock_csv(DATE, ["72031"], rows_per_ticker=4))
    df = tse_tick.create_df(str(z), auto_detect=False,
                            data_type="individual_stock", year=2024)
    assert set(df["Stock Code"].unique().to_list()) == {"72031"}


# --------------------------------------------------------------------------
# M4 - multi-member ZIPs dropped members 2..5
# --------------------------------------------------------------------------


def test_multi_member_zip_reads_all_members(tmp_path):
    """Every file member of a (guard-compliant) multi-member ZIP must be parsed
    — only namelist()[0] used to be read (M4)."""
    z = tmp_path / f"HTICST120.{DATE}.1.zip"
    with zipfile.ZipFile(z, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"HTICST120.{DATE}.1a.csv",
                    individual_stock_csv(DATE, ["1301"], rows_per_ticker=4))
        zf.writestr(f"HTICST120.{DATE}.1b.csv",
                    individual_stock_csv(DATE, ["7203"], rows_per_ticker=4))

    df = tse_tick.create_df(str(z), auto_detect=False,
                            data_type="individual_stock", year=2024)

    codes = set(df["Stock Code"].str.strip_chars().unique().to_list())
    assert codes == {"1301", "7203"}, "member 2 was dropped"
    assert df.height == 8


# --------------------------------------------------------------------------
# L1 - read_ticks exact-fit row cap silently dropped remaining ZIPs
# --------------------------------------------------------------------------


def test_read_ticks_exact_fit_cap_with_unread_zips_warns(tmp_path):
    """A total landing exactly on the cap with more ZIPs unread must emit a
    TruncationWarning — the old `>= rows` break dropped them silently (L1)."""
    flat = tmp_path / "flat"
    flat.mkdir()
    for day in (DATE, "20240105"):
        write_zip(flat / f"HTICST120.{day}.1.zip", f"HTICST120.{day}.1.csv",
                  individual_stock_csv(day, ["1301"], rows_per_ticker=4))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = tse_tick.read_ticks(str(flat), rows=4)

    assert df.height == 4
    assert any(issubclass(w.category, tse_tick.TruncationWarning) for w in caught), \
        "exact-fit truncation must warn, not silently drop the second ZIP"


def test_read_ticks_exact_fit_cap_with_nothing_left_stays_silent(tmp_path):
    """An exact-fit result with NO data left unread is complete — no warning."""
    z = tmp_path / f"HTICST120.{DATE}.1.zip"
    write_zip(z, f"HTICST120.{DATE}.1.csv",
              individual_stock_csv(DATE, ["1301"], rows_per_ticker=4))

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        df = tse_tick.read_ticks(str(z), rows=4)

    assert df.height == 4
    assert not any(issubclass(w.category, tse_tick.TruncationWarning) for w in caught)


# --------------------------------------------------------------------------
# L2 - null Data Date rows landed in an unqueryable date=None partition
# --------------------------------------------------------------------------


def test_null_data_date_rows_are_dropped_not_written_to_date_none(tmp_path, caplog):
    """Rows whose Data Date failed the non-strict parse must be dropped with a
    warning, not filed under date=None/ where no date query finds them (L2)."""
    import logging

    z = tmp_path / f"HTICST120.{DATE}.1.zip"
    write_zip(z, f"HTICST120.{DATE}.1.csv",
              individual_stock_csv(DATE, ["1301"], rows_per_ticker=8))
    df = tse_tick.create_df(str(z), auto_detect=False,
                            data_type="individual_stock", year=2024)
    df = df.with_columns(
        pl.when(pl.int_range(pl.len()) < 2)
        .then(pl.lit(None, dtype=df.schema["Data Date"]))
        .otherwise(pl.col("Data Date"))
        .alias("Data Date")
    )
    store = tmp_path / "store"

    with caplog.at_level(logging.WARNING):
        write_partitioned_parquet(df, str(store), "individual_stock")

    assert not (store / "individual_stock" / "date=None").exists()
    assert _rows(store, DATE, 1301) == 6  # the 6 good rows still land
    assert any("date=None" in r.message for r in caplog.records), \
        "dropped rows must be reported, not silent"


def test_malformed_raw_date_never_creates_date_none_partition(tmp_path):
    """End-to-end: a raw line with a corrupt Data Date field must not surface as
    a date=None partition in an ingested store (L2)."""
    src = tmp_path / "src"
    leaf = src / DATE[:4] / DATE[:6]
    leaf.mkdir(parents=True)
    payload = individual_stock_csv(DATE, ["1301"], rows_per_ticker=8)
    # First occurrence of the quoted date in a row is its Data Date field.
    payload = payload.replace(f'"{DATE}"'.encode("ascii"), b'"2024XX04"', 1)
    write_zip(leaf / f"HTICST120.{DATE}.1.zip", f"HTICST120.{DATE}.1.csv", payload)
    store = tmp_path / "store"

    results = tse_tick.ingest_period(str(src), str(store), DATE, "individual_stock")

    assert not list((store / "individual_stock").glob("date=None*"))
    assert _rows(store, DATE, 1301) == 7  # the corrupt-date row is dropped
    assert all("error" not in m for m in results), results
