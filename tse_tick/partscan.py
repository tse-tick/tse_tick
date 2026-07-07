"""Cheap part-pruning for ticker-filtered individual_stock reads.

NEEDS numbers each day's TICST120 parts in ascending stock-code order, code-sorted
within a part, but cuts parts at a fixed ~55 MB size — so a high-volume code spans
a CONTIGUOUS run of consecutive parts (Phase 0 finding; see
``benchmark_extraction_7203/SPIKE_FINDINGS.md``). To read one ticker we probe each
part's first record to bound the search, then walk backward from the upper-bound
part until a part with no match, opening only that run. Degrades to "open all
parts" when the ascending-code layout can't be confirmed, so it is never less
correct than a full scan.
"""
from __future__ import annotations

import bisect
import zipfile
from pathlib import Path
from typing import Iterable, List, Optional, Set


def extract_stock_code(raw_line: bytes) -> Optional[str]:
    """The 4-char stock code (field index 5) of a raw TICST120 line, or ``None``.

    Records are quoted CSV (``"f0","f1",...``); the stock code is field index 5.
    Skip five ``","`` delimiters, then read to the next ``"``. This is the single
    source of truth for the field-5 parse shared by the read fast path and the
    part probes.
    """
    pos = 0
    for _ in range(5):
        idx = raw_line.find(b'","', pos)
        if idx == -1:
            return None
        pos = idx + 3
    end = raw_line.find(b'"', pos)
    if end == -1:
        return None
    code = raw_line[pos:end].strip()[:4]
    if not code:
        return None
    try:
        return code.decode("ascii")
    except UnicodeDecodeError:
        return None


def part_start_code(zip_path: Path) -> Optional[int]:
    """Integer code of a part's FIRST record (its range start), or ``None``.

    Reads only the first line of the part's single member — streaming, so it
    decompresses ~one record, not the whole file. ``None`` if unreadable, empty, or
    non-numeric.
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            if not names:
                return None
            with zf.open(names[0]) as f:
                first = f.readline()
    except (zipfile.BadZipFile, EOFError, OSError):
        return None
    code = extract_stock_code(first)
    return int(code) if code is not None and code.isdigit() else None


def _part_contains(zip_path: Path, tickers: Set[str]) -> bool:
    """True if any record's field-5 code is in ``tickers``.

    Early-exits on the first match — cheap for a part that HAS the ticker (a
    code-sorted part holding a high code reaches it quickly); a full scan only for a
    part that does NOT (the run-terminating boundary part).
    """
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            names = zf.namelist()
            if not names:
                return False
            with zf.open(names[0]) as f:
                for raw in f:
                    code = extract_stock_code(raw)
                    if code is not None and code in tickers:
                        return True
    except (zipfile.BadZipFile, EOFError, OSError):
        return False
    return False


def select_parts_for_day(
    part_paths: List[Path], tickers: Iterable[str]
) -> Optional[List[Path]]:
    """Contiguous run(s) of parts (ONE day, ascending) that hold ``tickers``.

    A high-volume code straddles a contiguous run of consecutive parts (parts are
    size-split, not code-split). Method: probe start codes (cheap); per ticker take
    ``b = last part with start <= code`` (upper bound — parts after ``b`` start
    above the code, so cannot hold it), then scan BACKWARD from ``b`` keeping every
    part that actually contains the code, stopping at the first that does not. Union
    the runs across tickers.

    Returns ``None`` ("open all parts") if any probe fails or the start codes are
    not non-decreasing (ascending-code layout unconfirmed). Returns an empty list if
    every ticker is below the day's minimum code (absent).
    """
    # Nothing to prune with 0 or 1 part — read it in full. This also means a lone,
    # possibly non-code-sorted part can never be wrongly excluded by the probe.
    if len(part_paths) <= 1:
        return None

    starts: List[int] = []
    for p in part_paths:
        s = part_start_code(p)
        if s is None:
            return None
        starts.append(s)
    if any(starts[i] > starts[i + 1] for i in range(len(starts) - 1)):
        return None

    chosen: Set[int] = set()
    for t in tickers:
        t4 = str(t).strip()[:4]
        if not t4.isdigit():
            return None
        i = bisect.bisect_right(starts, int(t4)) - 1
        while i >= 0 and _part_contains(part_paths[i], {t4}):
            chosen.add(i)
            i -= 1

    # The LAST part also carries the day's trailing appendix — off-auction /
    # special records appended after the main ascending-code block, holding
    # out-of-code-order rows for many tickers (e.g. 7203's ~89-300 tail rows on a
    # typical day). A ticker's rows are therefore NOT confined to its code range, so
    # always include the last part. (If the appendix ever spilled into its OWN
    # trailing parts, their first codes would break the ascending order and the
    # monotonic check above would already have fallen back to all parts.)
    chosen.add(len(part_paths) - 1)
    return [part_paths[i] for i in sorted(chosen)]
