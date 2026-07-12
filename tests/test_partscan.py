"""Unit tests for tse_tick.partscan — field-5 parse, first-record probe, and the
contiguous-run part selection (handles the code-overflow the Phase 0 spike found).
"""
from pathlib import Path

from tse_tick.partscan import (
    extract_stock_code,
    part_start_code,
    select_parts_for_day,
)
from tests.synthetic_data import individual_stock_csv, write_zip


def _line(fields):
    return (",".join('"' + f + '"' for f in fields)).encode("ascii")


# --- extract_stock_code ---
def test_extract_stock_code_field5():
    line = _line(["A", "B", "C", "D", "E", "7203", "rest", "..."])  # field index 5
    assert extract_stock_code(line) == "7203"


def test_extract_stock_code_truncates_to_4():
    assert extract_stock_code(_line(["0", "1", "2", "3", "4", "72030", "x"])) == "7203"


def test_extract_stock_code_malformed_returns_none():
    assert extract_stock_code(b'"only","three","fields"') is None


# --- part_start_code ---
def test_part_start_code_reads_first_record(tmp_path: Path):
    z = write_zip(
        tmp_path / "HTICST120.20240104.1.zip", "HTICST120.20240104.1.csv",
        individual_stock_csv("20240104", ["1301"], rows_per_ticker=4),
    )
    assert part_start_code(z) == 1301


def test_part_start_code_bad_zip_returns_none(tmp_path: Path):
    bad = tmp_path / "HTICST120.20240104.2.zip"
    bad.write_bytes(b"not a zip")
    assert part_start_code(bad) is None


# --- select_parts_for_day ---
def _day_parts(tmp_path, code_by_part):
    """code_by_part: list of ticker-lists, one per part (ascending)."""
    paths = []
    for n, codes in enumerate(code_by_part, 1):
        paths.append(write_zip(
            tmp_path / f"HTICST120.20240104.{n}.zip", f"HTICST120.20240104.{n}.csv",
            individual_stock_csv("20240104", codes, rows_per_ticker=4)))
    return paths


def test_select_holding_run_and_last_part(tmp_path):
    # 7203 in part 2; part 3 (8001) is a non-holding middle part; the LAST part is
    # always kept for the trailing appendix (real-data structure).
    parts = _day_parts(tmp_path, [["1301"], ["7203"], ["8001"], ["9999"]])
    names = [p.name for p in select_parts_for_day(parts, {"7203"})]
    assert "HTICST120.20240104.2.zip" in names        # the code-run part
    assert "HTICST120.20240104.4.zip" in names        # last part (appendix)
    assert "HTICST120.20240104.3.zip" not in names    # middle non-holding part skipped


def test_select_captures_overflow_run(tmp_path):
    # 7203 straddles parts 2 AND 3 (overflow); part 4 (last) kept for the appendix.
    # Part 1 is also kept: part 2 STARTS exactly at 7203, so part 1's range
    # [1301, 7203] could hold the code's head rows — the arithmetic selection
    # keeps such a boundary part rather than decompressing it to prove absence.
    parts = _day_parts(tmp_path, [["1301"], ["7203"], ["7203"], ["9999"]])
    chosen = select_parts_for_day(parts, {"7203"})
    assert [p.name for p in chosen] == [
        "HTICST120.20240104.1.zip", "HTICST120.20240104.2.zip",
        "HTICST120.20240104.3.zip", "HTICST120.20240104.4.zip"]


def test_select_multi_ticker_union(tmp_path):
    # 1301 -> part 1; 9999 -> part 3, plus part 2 (its range [7203, 9999] could
    # hold 9999's head — boundary equality); part 3 is also the appendix part.
    parts = _day_parts(tmp_path, [["1301"], ["7203"], ["9999"]])
    chosen = select_parts_for_day(parts, {"1301", "9999"})
    assert sorted(p.name for p in chosen) == [
        "HTICST120.20240104.1.zip", "HTICST120.20240104.2.zip",
        "HTICST120.20240104.3.zip"]


def test_select_bounded_no_boundary_equality(tmp_path):
    # No start equals the code, so the arithmetic never over-selects: the run is
    # exactly the one part whose range contains 7203, plus the appendix part.
    parts = _day_parts(tmp_path, [["1301"], ["7000"], ["8000"], ["9999"]])
    names = [p.name for p in select_parts_for_day(parts, {"7203"})]
    assert names == ["HTICST120.20240104.2.zip", "HTICST120.20240104.4.zip"]


def test_select_below_min_still_checks_last_part(tmp_path):
    # a ticker below the day's minimum has no code-range run, but the last part may
    # hold appendix rows, so it is still selected (never wrongly excluded).
    parts = _day_parts(tmp_path, [["1301"], ["7203"]])
    names = [p.name for p in select_parts_for_day(parts, {"1000"})]
    assert names == ["HTICST120.20240104.2.zip"]                 # the last part


def test_select_falls_back_on_bad_probe(tmp_path):
    parts = _day_parts(tmp_path, [["1301"], ["7203"]])
    (tmp_path / "HTICST120.20240104.2.zip").write_bytes(b"corrupt")  # probe -> None
    assert select_parts_for_day(parts, {"7203"}) is None            # signal: open all
