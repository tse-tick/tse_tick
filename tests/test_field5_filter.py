"""Byte-identity gate for the vectorized field-5 ticker filter (issue #38).

The individual_stock ticker fast path used to filter raw lines with a pure-Python
per-line loop calling ``extract_stock_code``; it now uses a vectorized Polars
filter (``_read_individual_stock_matches``). This is a PERF change, not a behavior
change, so these tests pin the vectorized path to the exact byte-loop it replaced:
the kept-line set must be identical, line-for-line, in order.
"""
from __future__ import annotations

import glob
import io
import os
import time
import zipfile
from pathlib import Path

import polars as pl
import pytest

from tse_tick.enhanced import (
    _field5_codes,
    _read_individual_stock_matches,
    get_1y_dataframe,
)
from tse_tick.partscan import extract_stock_code, part_start_code
from tests.synthetic_data import individual_stock_csv, write_zip


# --- the reference byte-loop this feature replaces (kept here forever as the oracle) ---
def _byteloop_matches(stream, tickers: set) -> list[bytes]:
    kept: list[bytes] = []
    for raw in stream:
        code = extract_stock_code(raw)
        if code is not None and code in tickers:
            kept.append(raw)
    return kept


def _norm(lines) -> list[str]:
    """Normalize kept lines to compare sets: strip trailing CR/LF, latin-1 decode."""
    out = []
    for x in lines:
        s = x.decode("latin-1") if isinstance(x, (bytes, bytearray)) else x
        out.append(s.rstrip("\r\n"))
    return out


def _vec_lines(raw_bytes: bytes) -> list[str]:
    if not raw_bytes:
        return []
    text = raw_bytes.decode("latin-1")
    parts = text.split("\n")
    if parts and parts[-1] == "":
        parts = parts[:-1]
    return [p.rstrip("\r") for p in parts]


# ---------------------------------------------------------------------------
# Unit: _field5_codes agrees with extract_stock_code (membership-equivalent)
# ---------------------------------------------------------------------------
def _line(fields: list[str]) -> str:
    return ",".join('"' + f + '"' for f in fields)


def test_field5_codes_membership_matches_extract_stock_code():
    cases = [
        ["A", "B", "C", "D", "E", "7203", "rest", "x"],   # normal
        ["0", "1", "2", "3", "4", "72030", "x"],          # truncate to 4
        ["only", "three", "fields"],                       # < 6 fields -> None
        ["A", "B", "C", "D", "E", "  7203  ", "x"],        # surrounding whitespace
        ["A", "B", "C", "D", "E", "", "x"],                # empty field 5 -> None
        ["A", "B", "C", "D", "E", "12", "x"],              # short code kept as-is
        ["A", "B", "C", "D", "E", "9984", "x"],
    ]
    lines = [_line(c) for c in cases]
    vec = _field5_codes(pl.Series("raw", lines, dtype=pl.String)).to_list()
    ref = [extract_stock_code((ln + "\n").encode("latin-1")) for ln in lines]
    # exact match, including empty/missing field-5 -> None (not "") in both.
    assert vec == ref


# ---------------------------------------------------------------------------
# Byte-identity of the streaming filter vs the byte-loop
# ---------------------------------------------------------------------------
def _multi_ticker_part(tickers, rows=8) -> bytes:
    return individual_stock_csv("20240104", tickers, rows_per_ticker=rows)


def test_vectorized_kept_lines_identical_to_byteloop():
    payload = _multi_ticker_part(["1301", "7203", "8001", "9984", "9999"])
    flt = {"7203", "9984"}
    ref = _byteloop_matches(io.BytesIO(payload), flt)
    vec = _read_individual_stock_matches(io.BytesIO(payload), flt)
    assert len(ref) > 0
    assert _vec_lines(vec) == _norm(ref)


def test_block_boundary_splits_do_not_change_result():
    payload = _multi_ticker_part(["1301", "7203", "9984"], rows=20)
    flt = {"7203"}
    ref = _norm(_byteloop_matches(io.BytesIO(payload), flt))
    # tiny blocks force lines (and the '","' delimiters) to straddle block edges.
    for bs in (1, 3, 7, 13, 64, 4096):
        vec = _read_individual_stock_matches(io.BytesIO(payload), flt, block_bytes=bs)
        assert _vec_lines(vec) == ref, f"block_bytes={bs}"


def test_crlf_line_endings_round_trip():
    lf = _multi_ticker_part(["1301", "7203", "9984"])
    crlf = lf.replace(b"\n", b"\r\n")
    flt = {"7203"}
    ref = _byteloop_matches(io.BytesIO(crlf), flt)
    vec = _read_individual_stock_matches(io.BytesIO(crlf), flt)
    # CR is retained inside the kept line and must survive byte-for-byte.
    assert vec == b"".join(ref)  # exact bytes: CRLF-terminated lines reconstruct identically


def test_no_matches_returns_empty_bytes():
    payload = _multi_ticker_part(["1301", "7203"])
    assert _read_individual_stock_matches(io.BytesIO(payload), {"6758"}) == b""


def test_degenerate_empty_ticker_filter_keeps_nothing_like_oracle():
    # A line whose field-5 is empty: the byte-loop drops it (extract_stock_code ->
    # None); the vectorized filter must too, even for the degenerate filter {""}
    # (which _normalize_ticker_filter can produce from ticker_filter={""}).
    empty5 = _line(["1", "2", "3", "4", "5", "", "x"])
    normal = _line(["1", "2", "3", "4", "5", "7203", "x"])
    payload = (empty5 + "\n" + normal + "\n").encode("latin-1")
    for flt in ({""}, {"", "  "}):
        ref = _byteloop_matches(io.BytesIO(payload), flt)
        vec = _read_individual_stock_matches(io.BytesIO(payload), flt)
        assert ref == []  # oracle keeps nothing for an empty/whitespace code
        assert _vec_lines(vec) == _norm(ref) == []


def test_malformed_field5_parses_like_oracle():
    # Not reachable in a real 95-field TICST120 record (field 5 is never the terminal
    # field, and a stock code never contains a `"`), but the vectorized parser still
    # matches extract_stock_code exactly for these malformed shapes.
    cases = [
        (b'"1","2","3","4","5","72"\n', {"72"}),        # terminal field, <4-char code
        (b'"1","2","3","4","5","7"20","z"\n', {"7"}),   # embedded quote inside field 5
    ]
    for payload, flt in cases:
        ref = _byteloop_matches(io.BytesIO(payload), flt)
        vec = _read_individual_stock_matches(io.BytesIO(payload), flt)
        assert len(ref) == 1  # the oracle keeps it (code read up to the next `"`)
        assert _vec_lines(vec) == _norm(ref)


def test_non_ascii_high_byte_latin1_round_trip():
    # A field carrying non-ASCII / high bytes (>0x7F) must round-trip byte-for-byte
    # through the latin-1 decode/encode; this pins the load-bearing round-trip in CI
    # (the real-data test that exercises it is skipped without NEEDS data).
    hi = b"\x80\xa0\xff\xe3\x83\x88"  # arbitrary high bytes in a non-field-5 column
    a = b'"1","2","3","4","5","7203","' + hi + b'","z"'
    b = b'"1","2","3","4","5","1301","' + hi + b'","z"'
    payload = a + b"\n" + b + b"\n"
    flt = {"7203"}
    ref = _byteloop_matches(io.BytesIO(payload), flt)
    vec = _read_individual_stock_matches(io.BytesIO(payload), flt)
    assert vec == b"".join(ref)  # exact bytes incl. the high-byte column


class _CountingReader:
    """A binary stream that records the largest single ``read`` it served."""

    def __init__(self, data: bytes):
        self._bio = io.BytesIO(data)
        self.max_read = 0
        self.n_reads = 0

    def read(self, n: int = -1) -> bytes:
        chunk = self._bio.read(n)
        self.max_read = max(self.max_read, len(chunk))
        self.n_reads += 1
        return chunk


def test_reads_are_block_bounded_never_whole_part():
    # Bounded-memory gate (G2), asserted structurally in CI: the filter must stream
    # the part in blocks and never read (hence never materialize) the whole part.
    payload = _multi_ticker_part(["1301", "7203", "8001", "9984", "9999"], rows=200)
    assert len(payload) > 50_000  # comfortably many blocks at block_bytes=4096
    block = 4096
    reader = _CountingReader(payload)
    out = _read_individual_stock_matches(reader, {"7203", "9984"}, block_bytes=block)
    # No single read returned more than one block, and it took many reads to consume
    # the part -> the whole decompressed part is never held at once.
    assert reader.max_read <= block
    assert reader.n_reads >= len(payload) // block
    # ...and correctness is unaffected by the small block size.
    ref = _norm(_byteloop_matches(io.BytesIO(payload), {"7203", "9984"}))
    assert _vec_lines(out) == ref


def test_last_line_without_trailing_newline():
    a = _line(["1", "2", "3", "4", "5", "1301", "x"])
    b = _line(["1", "2", "3", "4", "5", "7203", "x"])
    payload = (a + "\n" + b).encode("latin-1")  # no trailing newline; match is last
    flt = {"7203"}
    ref = _byteloop_matches(io.BytesIO(payload), flt)
    vec = _read_individual_stock_matches(io.BytesIO(payload), flt)
    assert _vec_lines(vec) == _norm(ref)
    # and the parsed frames are identical (a benign trailing \n never changes the parse)
    schema = {f"column_{i+1}": pl.String for i in range(95)}
    ref_df = pl.read_csv(io.BytesIO(b"".join(ref)), has_header=False,
                         schema_overrides=schema, truncate_ragged_lines=True)
    vec_df = pl.read_csv(io.BytesIO(vec), has_header=False,
                         schema_overrides=schema, truncate_ragged_lines=True)
    assert ref_df.equals(vec_df)


def test_get_1y_dataframe_vectorized_equals_reference(tmp_path):
    payload = _multi_ticker_part(["1301", "7203", "8001", "9984", "9999"], rows=12)
    z = write_zip(tmp_path / "HTICST120.20240104.1.zip", "HTICST120.20240104.1.csv", payload)
    flt = {"7203", "9984"}
    got = get_1y_dataframe(str(z), 2024, "individual_stock", ticker_filter=flt)
    ref_bytes = b"".join(_byteloop_matches(io.BytesIO(payload), flt))
    schema = {f"column_{i+1}": pl.String for i in range(95)}
    ref = pl.read_csv(io.BytesIO(ref_bytes), has_header=False,
                      schema_overrides=schema, truncate_ragged_lines=True)
    assert got.equals(ref)


# ---------------------------------------------------------------------------
# Real-data gate: byte-identity on real multi-part TICST120 (issue #38 requirement)
# ---------------------------------------------------------------------------
def _find_multipart_day() -> list[Path] | None:
    """Return the parts of one real multi-part TICST120 trading day, or None."""
    roots = [
        os.environ.get("TSE_TICK_DATA_ROOT"),
        r"G:\needs",
        r"G:\flash_crash",
    ]
    for root in roots:
        if not root or not os.path.isdir(root):
            continue
        hits = glob.glob(os.path.join(root, "**", "HTICST120.*.zip"), recursive=True)
        by_day: dict[str, list[str]] = {}
        for h in hits:
            toks = Path(h).name.split(".")
            if len(toks) >= 3 and toks[1].isdigit() and len(toks[1]) == 8:
                by_day.setdefault(toks[1], []).append(h)
        multi = {d: v for d, v in by_day.items() if len(v) >= 3}
        if multi:
            day = sorted(multi)[0]
            return sorted(
                (Path(p) for p in multi[day]),
                key=lambda p: int(p.name.split(".")[2]),
            )
    return None


_PARTS = _find_multipart_day()


@pytest.mark.skipif(_PARTS is None, reason="No real multi-part TICST120 data available")
def test_real_part_byte_identity_and_speed():
    parts = _PARTS
    mid = parts[len(parts) // 2]
    # a code guaranteed present in `mid` (its first record's field-5), plus the last
    # part which carries the day's off-auction appendix for many tickers.
    code = part_start_code(mid)
    assert code is not None
    flt = {str(code)}
    checked_positive = False
    for part in (mid, parts[-1]):
        with zipfile.ZipFile(part) as zf:
            with zf.open(zf.namelist()[0]) as f:
                t0 = time.time()
                ref = _byteloop_matches(f, flt)
                ref_secs = time.time() - t0
        with zipfile.ZipFile(part) as zf:
            with zf.open(zf.namelist()[0]) as f:
                t0 = time.time()
                vec = _read_individual_stock_matches(f, flt)
                vec_secs = time.time() - t0
        assert _vec_lines(vec) == _norm(ref), f"kept-line set differs on {part.name}"
        if ref:
            checked_positive = True
        # perf gate: vectorized must not be slower than the byte-loop it replaces.
        assert vec_secs <= ref_secs * 1.10 + 0.5, (
            f"{part.name}: vector {vec_secs:.1f}s slower than byteloop {ref_secs:.1f}s"
        )
    assert checked_positive, "expected at least one part with matching rows"
