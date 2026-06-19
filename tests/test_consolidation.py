# tests/test_consolidation.py
"""Single-source-of-truth invariants (the consolidation pass).

The four-type classification (valid / summary / tick / index) and the get_info
field counts used to be duplicated as literals across many modules and drifted —
the root cause behind several past inconsistencies. These tests lock the package
to one source so a future change can't silently reintroduce drift.
"""
import pytest

import tse_tick
from tse_tick.constants import (
    DataType,
    DATA_TYPES,
    VALID_DATA_TYPES,
    SUMMARY_TYPES,
    TICK_TYPES,
    INDEX_TYPES,
    validate_data_type,
)


def test_groupings_partition_the_four_types():
    assert VALID_DATA_TYPES == frozenset(DataType.values())
    assert set(DATA_TYPES) == VALID_DATA_TYPES and len(DATA_TYPES) == 4
    # summary vs tick is a partition; so is index vs stock
    assert SUMMARY_TYPES | TICK_TYPES == VALID_DATA_TYPES
    assert not (SUMMARY_TYPES & TICK_TYPES)
    assert SUMMARY_TYPES == {"stock_summary", "indices_summary"}
    assert TICK_TYPES == {"individual_stock", "indices"}
    assert INDEX_TYPES == {"indices", "indices_summary"}
    # plain-string membership works (the bare strings the public API accepts)
    assert "stock_summary" in SUMMARY_TYPES and "indices" in TICK_TYPES


def test_validate_data_type():
    for t in DATA_TYPES:
        validate_data_type(t)                       # no raise for any valid type
    with pytest.raises(ValueError, match="Unknown data_type"):
        validate_data_type("not_a_type")


def test_modules_share_the_one_validator():
    # Every module must use the shared gate, not a per-file literal set.
    import tse_tick.enhanced as enh
    import tse_tick.query as q
    import tse_tick.ingest as ing
    import tse_tick.io.parquet as iop
    assert enh.validate_data_type is validate_data_type
    assert q.validate_data_type is validate_data_type
    assert ing.validate_data_type is validate_data_type
    assert iop.validate_data_type is validate_data_type


def test_partition_cols_keys_match_valid_types():
    from tse_tick.io.parquet import _DEFAULT_PARTITION_COLS
    assert set(_DEFAULT_PARTITION_COLS) == VALID_DATA_TYPES


def test_get_info_field_counts_match_schemas():
    """get_info's banner numbers must be derivable from the schemas (no drift)."""
    from tse_tick.schemas import (
        get_schema_individual_stock_95,
        get_schema_summary_83,
        get_schema_indices_23,
        get_schema_indices_15,
    )
    from tse_tick.enhanced import get_final_columns

    info = tse_tick.get_info()
    assert f"individual_stock (TICST120) - {len(get_schema_individual_stock_95())} fields" in info
    assert f"stock_summary (TICSS110) - {len(get_final_columns('stock_summary'))} fields" in info
    assert f"indices (TICIT110) - {len(get_final_columns('indices'))} fields" in info
    assert f"indices_summary (TICIS110) - {len(get_final_columns('indices_summary'))} fields" in info
    # raw counts referenced in parentheses
    assert f"({len(get_schema_summary_83())} raw)" in info
    assert f"({len(get_schema_indices_23())} raw, {len(get_schema_indices_15())} in 2016)" in info
