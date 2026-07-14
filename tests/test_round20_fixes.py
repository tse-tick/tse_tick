"""Round-20: morsel-bounded parse — peak memory independent of the day's size.

See plans/round20-morsel-bounded-ingest-plan.md.

`create_df` handed a whole filtered part to one `pl.read_csv`, materialising an
all-String frame that stayed alive while `clean_data` cast it: measured peak was
**4.6x the final frame** (9.03 GB for a 1.95 GB / 2,563,684-row result on real
20250409 data). `_ingest_date_group` paid that per part and concatenated the day,
so one April-2025 date needed 24.5 GB and OOM'd a 34 GB box.

The fix reads/cleans the already-filtered bytes in newline-aligned morsels and
concatenates the *cleaned* (4.6x smaller) frames. Identity holds because
`clean_data`/`set_columns` are purely element-wise (no group_by/over/sort/join/
window), so concat(finalize(morsel_i)) == finalize(concat(morsel_i)).
"""

from pathlib import Path

import polars as pl
import pytest

import tse_tick
from tse_tick import enhanced as enh
from tse_tick.enhanced import create_df, _iter_raw_morsels
from tests.synthetic_data import individual_stock_csv, write_zip


@pytest.fixture()
def part(tmp_path: Path) -> Path:
    """One multi-ticker part with enough rows to span several small morsels."""
    return write_zip(
        tmp_path / "HTICST120.20250409.1.zip",
        "HTICST120.20250409.1.csv",
        individual_stock_csv("20250409", ["7203", "9984"], rows_per_ticker=60),
    )


# --------------------------------------------------------------------------- #
# _iter_raw_morsels — newline-aligned slicing must not split/drop/dup a record
# --------------------------------------------------------------------------- #
def test_morsels_reassemble_to_the_original_bytes():
    raw = b"".join(b'"a","b","c%d"\n' % i for i in range(1000))
    for size in (1, 13, 14, 15, 64, 4096, len(raw), len(raw) * 2):
        got = b"".join(_iter_raw_morsels(raw, size))
        assert got == raw, f"morsel size {size} did not reassemble"


def test_every_morsel_ends_on_a_record_boundary():
    raw = b"".join(b'"a","b","c%d"\n' % i for i in range(1000))
    for size in (16, 100, 999):
        for m in _iter_raw_morsels(raw, size):
            assert m.endswith(b"\n"), "a morsel split a record"
            assert m.count(b"\n") >= 1


def test_morsels_handle_a_missing_trailing_newline():
    raw = b'"x","y","1"\n"x","y","2"'  # last line unterminated
    got = b"".join(_iter_raw_morsels(raw, 8))
    assert got == raw
    assert sum(m.count(b"\n") for m in _iter_raw_morsels(raw, 8)) == 1


# --------------------------------------------------------------------------- #
# Parse identity — the property the whole design rests on
# --------------------------------------------------------------------------- #
def test_morselled_read_is_row_identical_to_unbatched(part, monkeypatch):
    """A tiny morsel size (forcing many morsels) must produce exactly the frame
    the single-read path produces — same rows, order, schema and dtypes."""
    big = create_df(
        str(part),
        language="en",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203"},
    )
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 512)  # force many morsels
    small = create_df(
        str(part),
        language="en",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203"},
    )
    assert small.height == big.height == 60
    assert small.schema == big.schema
    assert small.equals(big)


def test_morselled_read_identical_for_multi_ticker_filter(part, monkeypatch):
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 700)
    small = create_df(
        str(part),
        language="en",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203", "9984"},
    )
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 64 * 1024 * 1024)
    big = create_df(
        str(part),
        language="en",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203", "9984"},
    )
    assert small.equals(big)
    assert small.height == 120


def test_morselled_read_identical_in_japanese(part, monkeypatch):
    """The jp path renames around clean_data; morsels must not disturb it."""
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 512)
    small = create_df(
        str(part),
        language="jp",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203"},
    )
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 64 * 1024 * 1024)
    big = create_df(
        str(part),
        language="jp",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203"},
    )
    assert small.equals(big)


def test_morselled_read_honours_the_rows_cap(part, monkeypatch):
    """The rows= cap counts cleaned rows; morselling must not change the cap."""
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 512)
    df = create_df(
        str(part),
        language="en",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"7203"},
        rows=25,
    )
    assert df.height == 25


def test_absent_ticker_still_typed_empty(part, monkeypatch):
    """A filter matching nothing keeps the typed-empty contract (report B1)."""
    monkeypatch.setattr(enh, "_MORSEL_BYTES", 512)
    df = create_df(
        str(part),
        language="en",
        auto_detect=False,
        data_type="individual_stock",
        year=2025,
        ticker_filter={"6758"},
    )
    assert df.height == 0
    assert df.width == 95


def test_morsel_bytes_default_is_bounded():
    """The default morsel must be a bounded constant, not the whole part."""
    assert 1_000_000 <= enh._MORSEL_BYTES <= 256 * 1024 * 1024


# --------------------------------------------------------------------------- #
# Streaming per-ticker write — the piece that actually bounds a DAY
# --------------------------------------------------------------------------- #
def _multi_part_day(root: Path, date: str = "20250409") -> list[Path]:
    """A day split across parts that repeat tickers, incl. a trailing appendix
    part — the real NEEDS shape that forces all parts into one date unit."""
    root.mkdir(parents=True, exist_ok=True)
    paths = []
    for n, codes in enumerate([["1301", "7203"], ["7203", "9984"], ["9984", "9999"]], 1):
        paths.append(
            write_zip(
                root / f"HTICST120.{date}.{n}.zip",
                f"HTICST120.{date}.{n}.csv",
                individual_stock_csv(date, codes, rows_per_ticker=8),
            )
        )
    return paths


def test_streaming_ingest_is_identical_to_the_concat_path(tmp_path, monkeypatch):
    """The streaming write must produce byte-for-byte the same store the concat
    path produces: same rows, order, schema and dtypes, per ticker file."""
    from tse_tick.ingest import _ingest_date_group

    parts = _multi_part_day(tmp_path / "raw")
    zips = [str(p) for p in parts]
    tk = {"7203", "9984"}

    stream_out = tmp_path / "streamed"
    meta_s = _ingest_date_group(
        "20250409", zips, str(stream_out), "individual_stock", 2025, "en", set(tk)
    )

    # Force the legacy concat path by pretending the filter is too wide to stream.
    monkeypatch.setattr("tse_tick.ingest._MAX_STREAM_TICKERS", 0)
    concat_out = tmp_path / "concat"
    meta_c = _ingest_date_group(
        "20250409", zips, str(concat_out), "individual_stock", 2025, "en", set(tk)
    )

    assert meta_s["rows"] == meta_c["rows"] > 0
    for code in ("7203", "9984"):
        rel = Path("individual_stock") / "date=20250409" / f"ticker={code}.parquet"
        a = pl.read_parquet(stream_out / rel)
        b = pl.read_parquet(concat_out / rel)
        assert a.schema == b.schema, code
        assert a.equals(b), f"{code}: streamed store differs from concat store"
    # a filtered ingest writes only the requested family
    assert not (stream_out / "individual_stock" / "date=20250409" / "ticker=9999.parquet").exists()


def test_streaming_ingest_leaves_no_temp_files_and_marks_coverage(tmp_path):
    from tse_tick.ingest import _ingest_date_group, _read_coverage_marker

    zips = [str(p) for p in _multi_part_day(tmp_path / "raw")]
    out = tmp_path / "store"
    _ingest_date_group("20250409", zips, str(out), "individual_stock", 2025, "en", {"7203"})
    date_dir = out / "individual_stock" / "date=20250409"
    assert (date_dir / "ticker=7203.parquet").exists()
    assert not list(date_dir.glob("*.tmp")), "a temp file survived the commit"
    assert not list(date_dir.glob(".*tmp*"))
    assert _read_coverage_marker(date_dir) is not None


def test_streaming_ingest_publishes_nothing_when_a_part_dies(tmp_path, monkeypatch):
    """abort() must drop the temp files, leaving the day fully re-ingestable —
    never a half-published date the existence-keyed resume would trust."""
    from tse_tick import ingest as ing
    from tse_tick.ingest import _ingest_date_group

    zips = [str(p) for p in _multi_part_day(tmp_path / "raw")]
    out = tmp_path / "store"

    real = ing.create_df
    calls = {"n": 0}

    def boom(*a, **k):
        calls["n"] += 1
        if calls["n"] == 2:  # die after part 1 was appended
            raise ing.OneShotMemoryError("simulated one-shot OOM")
        return real(*a, **k)

    monkeypatch.setattr(ing, "create_df", boom)
    with pytest.raises(ing.OneShotMemoryError):
        _ingest_date_group("20250409", zips, str(out), "individual_stock", 2025, "en", {"7203"})

    date_dir = out / "individual_stock" / "date=20250409"
    assert not list(date_dir.glob("*.parquet")), "a partial day was published"
    assert not list(date_dir.glob(".*tmp*")), "a temp file was left behind"


def test_wide_filter_keeps_the_concat_path(tmp_path):
    """>_MAX_STREAM_TICKERS codes would need too many concurrent writers."""
    from tse_tick import ingest as ing

    assert ing._MAX_STREAM_TICKERS >= 2
    zips = [str(p) for p in _multi_part_day(tmp_path / "raw")]
    wide = {str(9000 + i) for i in range(ing._MAX_STREAM_TICKERS + 1)} | {"7203"}
    meta = ing._ingest_date_group(
        "20250409", zips, str(tmp_path / "s"), "individual_stock", 2025, "en", wide
    )
    assert meta["rows"] > 0  # still ingests, via the concat path
