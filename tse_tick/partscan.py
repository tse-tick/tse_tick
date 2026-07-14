"""Cheap part-pruning for ticker-filtered individual_stock reads.

NEEDS numbers each day's TICST120 parts in ascending stock-code order, code-sorted
within a part, but cuts parts at a fixed ~55 MB size — so a high-volume code spans
a CONTIGUOUS run of consecutive parts (Phase 0 finding; see
``benchmark_extraction_7203/SPIKE_FINDINGS.md``). To read one ticker we probe each
part's FIRST record only: with non-decreasing start codes, part ``j`` can hold code
``t`` iff ``starts[j] <= t <= starts[j+1]`` (the last part unbounded above), so the
run is selected arithmetically — no part is ever decompressed beyond its first
line here. At most one boundary part that provably-could-but-doesn't hold the code
is over-selected (only when a start code equals ``t`` exactly); the vectorized
filtered read absorbs it. Degrades to "open all parts" when the ascending-code
layout can't be confirmed, so it is never less correct than a full scan.

Codes are compared as the fixed-width 4-char tokens NEEDS writes, not as ints:
TSE issues **alphanumeric** codes from 2024 (e.g. ``"162A"``), which ``int()``
could not parse — one such part disabled pruning for its whole day (~half of all
2024/2025 days; a measured 5.14 GB / 204 s per worker-day instead of 0.18 GB /
13 s). Token order equals NEEDS' ordering because the width is fixed, and for the
all-digit codes that predate 2024 it is identical to their numeric order.
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


def part_start_code(zip_path: Path) -> Optional[str]:
    """The 4-char code TOKEN of a part's FIRST record (its range start), or ``None``.

    Reads only the first line of the part's single member — streaming, so it
    decompresses ~one record, not the whole file. ``None`` if unreadable or empty.

    Returned as the raw token rather than an ``int`` because TSE issues
    **alphanumeric** codes from 2024 (e.g. ``"162A"``). Parsing those with ``int()``
    yielded ``None``, and one ``None`` made :func:`select_parts_for_day` abandon
    pruning for the whole day — on real data ~half of all 2024/2025 days, costing a
    measured 5.14 GB / 204 s per worker-day instead of 0.18 GB / 13 s. Codes are
    fixed-width 4-char tokens, so ordering them lexicographically is exactly the
    ascending order NEEDS writes them in, and for the all-digit codes that predate
    2024 that order is identical to their numeric order — the comparison space is
    unchanged for them.
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
    return extract_stock_code(first)


def select_parts_for_day(
    part_paths: List[Path], tickers: Iterable[str]
) -> Optional[List[Path]]:
    """Contiguous run(s) of parts (ONE day, ascending) that hold ``tickers``.

    A high-volume code straddles a contiguous run of consecutive parts (parts are
    size-split, not code-split). Method: probe start codes (cheap — first line
    only), then bound each ticker's run arithmetically: part ``j`` can hold code
    ``t`` iff ``starts[j] <= t <= starts[j+1]`` (last part unbounded above). The
    old implementation proved a boundary part's non-containment by decompressing
    and Python-parsing it line-by-line; the probed starts already imply it, except
    when a start equals ``t`` exactly — there the boundary part is kept (it may
    hold the code's head/tail rows) and the filtered read drops it cheaply if not.
    Union the runs across tickers.

    Codes are compared as fixed-width 4-char tokens, so alphanumeric codes (TSE
    issues these from 2024, e.g. ``"162A"``) prune like any other; for all-digit
    codes the token order is identical to the numeric order this used to use, so
    the parts selected for them are unchanged.

    Returns ``None`` ("open all parts") if any probe fails, a code is not a 4-char
    token (the fixed width is what makes the token order match NEEDS' ordering), or
    the start codes are not non-decreasing (ascending-code layout unconfirmed). A
    ticker below the day's minimum code has no code-range run (only the appendix
    part is kept).
    """
    # Nothing to prune with 0 or 1 part — read it in full. This also means a lone,
    # possibly non-code-sorted part can never be wrongly excluded by the probe.
    if len(part_paths) <= 1:
        return None

    starts: List[str] = []
    for p in part_paths:
        s = part_start_code(p)
        # Only fixed-width tokens are comparable lexicographically the way NEEDS
        # orders them ("999" would sort ABOVE "1301"), so anything else falls back.
        if s is None or len(s) != 4:
            return None
        starts.append(s)
    if any(starts[i] > starts[i + 1] for i in range(len(starts) - 1)):
        return None

    chosen: Set[int] = set()
    for t in tickers:
        code = str(t).strip()[:4]
        if len(code) != 4:
            return None
        # hi: last part starting at or below the code — parts after it start
        # above the code and cannot hold it.
        hi = bisect.bisect_right(starts, code) - 1
        if hi < 0:
            continue  # below the day's minimum code: no code-range run
        # lo: first part whose NEXT part starts at or above the code — parts
        # before it end below the code (their next start is below it).
        lo = bisect.bisect_left(starts, code, 1) - 1
        chosen.update(range(lo, hi + 1))

    # The LAST part also carries the day's trailing appendix — off-auction /
    # special records appended after the main ascending-code block, holding
    # out-of-code-order rows for many tickers (e.g. 7203's ~89-300 tail rows on a
    # typical day). A ticker's rows are therefore NOT confined to its code range, so
    # always include the last part. (If the appendix ever spilled into its OWN
    # trailing parts, their first codes would break the ascending order and the
    # monotonic check above would already have fallen back to all parts.)
    chosen.add(len(part_paths) - 1)
    return [part_paths[i] for i in sorted(chosen)]
