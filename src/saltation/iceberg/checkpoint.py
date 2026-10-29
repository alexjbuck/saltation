"""Capture and pin Iceberg table state.

A ``Checkpoint`` is a frozen tuple of the four pointers that uniquely identify a table's
logical state plus its metadata location at capture time. ``capture`` reads them off a
live ``Table``; ``pin`` creates an Iceberg tag at the captured snapshot so PyIceberg's
expire-snapshots maintenance can't reclaim it before a rollback ever runs.

``Checkpoint`` is intentionally a plain dataclass with public fields so the sidecar
``table_states`` table (M3 / M5) can serialise it row-for-row.
"""

from __future__ import annotations

from dataclasses import dataclass

from pyiceberg.table import Table


@dataclass(frozen=True, slots=True)
class Checkpoint:
    """The four pointers (+ metadata location and optional tag) we capture before mutating.

    ``snapshot_id`` is nullable because a freshly created table with no appends has no
    current snapshot — schema-only / spec-only rollbacks are still meaningful in that
    regime. ``tag_ref`` is None unless ``pin`` was called and actually created a tag
    (which it skips for the no-snapshot case).
    """

    snapshot_id: int | None
    schema_id: int
    spec_id: int
    sort_order_id: int
    metadata_loc: str
    tag_ref: str | None = None


def capture(table: Table) -> Checkpoint:
    """Snapshot the table's current four pointers + metadata location.

    The caller is responsible for re-loading the table from the catalog if there's any
    chance the in-memory view is stale (PyIceberg caches aggressively; ``catalog.load_table``
    refreshes). ``capture`` itself does no IO and trusts the ``Table`` it's handed.
    """
    snap = table.current_snapshot()
    return Checkpoint(
        snapshot_id=snap.snapshot_id if snap is not None else None,
        schema_id=table.schema().schema_id,
        spec_id=table.spec().spec_id,
        sort_order_id=table.sort_order().order_id,
        metadata_loc=table.metadata_location,
    )


def pin(table: Table, *, tag_name: str, snapshot_id: int | None) -> None:
    """Create an Iceberg tag at ``snapshot_id`` so it survives expiration.

    Uses the public ``manage_snapshots().create_tag(...)`` builder rather than the
    ``Transaction._apply`` private API: tag creation isn't part of the rollback's
    atomicity contract, so there's no reason to reach for the private path here.

    Returns silently if ``snapshot_id`` is None — there's literally nothing to pin.
    Metadata-only checkpoints (captured before any append) take this branch.
    """
    if snapshot_id is None:
        return
    with table.manage_snapshots() as ms:
        ms.create_tag(snapshot_id=snapshot_id, tag_name=tag_name)
