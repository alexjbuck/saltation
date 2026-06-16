"""Unit tests for reset-mode helpers in ``cambrian.migrate.runner``.

Pure-logic tests only — the catalog-touching paths exercise via
``apply_reset`` live in ``tests/integration/test_reset.py``.
"""

from __future__ import annotations

from cambrian.iceberg.checkpoint import Checkpoint
from cambrian.migrate.runner import (
    CHECKPOINT_TAG_PREFIX,
    CURRENT_EVOLUTION_ID,
    RESET_LAST_RESORT_HINT,
    _checkpoint_tag,
    _ident_str_to_tuple,
    _row_to_checkpoint,
)
from cambrian.sidecar.events import TableStateRow


def test_checkpoint_tag_uses_documented_prefix() -> None:
    assert _checkpoint_tag("current") == "cambrian.cp.current"
    assert _checkpoint_tag(CURRENT_EVOLUTION_ID) == f"{CHECKPOINT_TAG_PREFIX}current"


def test_row_to_checkpoint_full_pre_state() -> None:
    row = TableStateRow(
        table_ident="ns.t",
        pre_snapshot_id=42,
        pre_schema_id=2,
        pre_spec_id=1,
        pre_sort_order_id=0,
        pre_metadata_loc="s3://bucket/path",
        tag_ref="cambrian.cp.current",
    )
    cp = _row_to_checkpoint(row)
    assert cp == Checkpoint(
        snapshot_id=42,
        schema_id=2,
        spec_id=1,
        sort_order_id=0,
        metadata_loc="s3://bucket/path",
        tag_ref="cambrian.cp.current",
    )


def test_row_to_checkpoint_metadata_only_pre_state() -> None:
    """A pre-state with no snapshot (table existed but had no appends) is still valid."""
    row = TableStateRow(
        table_ident="ns.t",
        pre_snapshot_id=None,
        pre_schema_id=0,
        pre_spec_id=0,
        pre_sort_order_id=0,
        pre_metadata_loc="s3://bucket/v0.metadata.json",
    )
    cp = _row_to_checkpoint(row)
    assert cp is not None
    assert cp.snapshot_id is None
    assert cp.schema_id == 0


def test_row_to_checkpoint_returns_none_when_pre_state_empty() -> None:
    """A row whose table didn't exist pre-apply returns None — no rollback target."""
    row = TableStateRow(table_ident="ns.brand_new")
    assert _row_to_checkpoint(row) is None


def test_ident_str_to_tuple_two_part() -> None:
    assert _ident_str_to_tuple("ns.t") == ("ns", "t")


def test_ident_str_to_tuple_three_part() -> None:
    assert _ident_str_to_tuple("cat.ns.t") == ("cat", "ns", "t")


def test_ident_str_to_tuple_unqualified() -> None:
    assert _ident_str_to_tuple("t") == ("t",)


def test_reset_hint_frames_reset_as_last_resort() -> None:
    """The user-facing hint must NOT recommend reset; it must frame it as the relief valve."""
    text = RESET_LAST_RESORT_HINT
    assert "last resort" in text or "relief valve" in text
    assert "idempotent" in text.lower()
    # Anti-pattern: the hint must not present reset as the primary or
    # recommended fix. We check for some words that would indicate that.
    lowered = text.lower()
    assert "recommended" not in lowered or "not" in lowered or "prefer" in lowered
