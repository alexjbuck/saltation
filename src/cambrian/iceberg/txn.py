"""Rollback primitive: ``restore_pointers`` — the one place ``Transaction._apply`` is used.

PyIceberg 0.11 ships ``Transaction._apply`` as a leading-underscore "private" method.
In practice it's been stable across the 0.x line and is the only API that can atomically
stage multiple ``TableUpdate``s with attached ``TableRequirement``s for a single REST
commit, which is exactly the property cambrian's rollback design depends on:

    Restore main, schema_id, spec_id, sort_order_id in ONE atomic catalog commit, with
    an AssertRefSnapshotId guard so a concurrent writer's commit between checkpoint and
    rollback can't silently clobber.

The public ``Transaction`` context-manager wraps the commit; only the *staging* of the
four-update payload requires ``_apply``. We isolate that call here so the rest of the
codebase has zero direct dependency on PyIceberg internals.

TODO(upstream): file a PyIceberg issue for a public ``Transaction.apply()`` (or accept
batch updates on the existing builder surface) so cambrian can drop the underscore.

INVARIANT: This module contains the ONLY call to ``Transaction._apply`` in cambrian.
``ripgrep '_apply\\b' src/`` should return exactly one match (the call below). New
multi-update-atomic operations must route through ``restore_pointers`` or a future
sibling in this module, not add their own private-API leak.
"""

from __future__ import annotations

from pyiceberg.exceptions import CommitFailedException, RESTError
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.update import (
    AssertRefSnapshotId,
    SetCurrentSchemaUpdate,
    SetDefaultSortOrderUpdate,
    SetDefaultSpecUpdate,
    SetSnapshotRefUpdate,
    TableRequirement,
    TableUpdate,
)

from cambrian.errors import ExternalWriteDetectedError, IllegalStateError


def restore_pointers(
    table: Table,
    *,
    target_snapshot_id: int | None,
    target_schema_id: int,
    target_spec_id: int,
    target_sort_order_id: int,
    expected_current_snapshot_id: int | None,
) -> None:
    """Atomically restore the four pointers (snapshot ref, schema, spec, sort order).

    ``expected_current_snapshot_id`` is the snapshot id the caller observed on the
    ``main`` ref at the moment they decided to roll back. If the catalog disagrees at
    commit time (because another writer advanced ``main``), the
    ``AssertRefSnapshotId`` requirement aborts the commit and this function raises
    ``ExternalWriteDetectedError``.

    The metadata-only case (table never had a snapshot, ``target_snapshot_id`` and
    ``expected_current_snapshot_id`` both ``None``) skips the snapshot-ref update and
    assertion — there's no ``main`` ref to restore — and only rolls back the three
    metadata pointers.

    The asymmetric case (``target_snapshot_id is None`` but the current table has a
    snapshot) is degenerate: the checkpoint predates any append, but somehow appends
    have happened that we now want to undo by reverting to "before main existed". We
    refuse with ``IllegalStateError``: there's no safe semantics for "delete main" as
    part of a rollback and the caller almost certainly has a bug.
    """
    if target_snapshot_id is None and expected_current_snapshot_id is not None:
        msg = (
            "checkpoint predates any append (target snapshot is None) but the table now "
            f"has snapshot {expected_current_snapshot_id} on main; refusing to roll back. "
            "Either the checkpoint is from before this table existed in its current form, "
            "or the caller passed mismatched checkpoint/observation values."
        )
        raise IllegalStateError(msg)

    # ty doesn't understand Pydantic's snake_case-vs-kebab-case alias behaviour: it
    # sees only the `alias="schema-id"` etc. on these Field() declarations and flags the
    # snake_case kwargs as unknown. PyIceberg's own tests and the m4 prototype confirm
    # the snake_case form is the supported call shape. Constructed via this helper so
    # the ignore lives on exactly one line per call site rather than smeared across the
    # body of each multi-line constructor.
    updates: tuple[TableUpdate, ...] = (
        _set_schema(target_schema_id),
        _set_spec(target_spec_id),
        _set_sort_order(target_sort_order_id),
    )
    requirements: tuple[TableRequirement, ...] = ()

    if target_snapshot_id is not None:
        updates = (_set_main_ref(target_snapshot_id), *updates)
        requirements = (_assert_main(expected_current_snapshot_id),)

    try:
        with table.transaction() as txn:
            # The sole _apply call site in cambrian; see module docstring.
            txn._apply(updates=updates, requirements=requirements)
    except (CommitFailedException, RESTError) as err:
        # Client-side: AssertRefSnapshotId raises CommitFailedException before
        # the REST round-trip. Server-side: Lakekeeper rejects the staged
        # commit because the table state at commit time disagrees with the
        # requirement, surfacing as a RESTError subclass (BadRequestError,
        # ServerError, …). Both paths map to "another writer moved main
        # under us" — surface a single user-facing error type.
        raise ExternalWriteDetectedError(
            ref="main",
            expected_snapshot_id=expected_current_snapshot_id,
            observed_snapshot_id=_observed_main_snapshot_id(table),
        ) from err


def _observed_main_snapshot_id(table: Table) -> int | None:
    """Best-effort read of the snapshot id currently on ``main``, for error context.

    Pulls from the in-memory metadata (which is what AssertRefSnapshotId validated
    against). The catalog may have moved on again since; this is for the user-facing
    message only, not for control flow.
    """
    ref = table.refs().get("main")
    return ref.snapshot_id if ref is not None else None


# Pydantic-model constructors below. See note in ``restore_pointers``: ty can't see
# through the kebab-case aliases on the Field() definitions, so each call needs an
# ``unknown-argument`` ignore. SetSnapshotRefUpdate additionally has three optional
# fields whose absence ty mis-reads as ``missing-argument`` even though they default
# to None at runtime.


def _set_schema(schema_id: int) -> SetCurrentSchemaUpdate:
    return SetCurrentSchemaUpdate(schema_id=schema_id)  # ty: ignore[unknown-argument]


def _set_spec(spec_id: int) -> SetDefaultSpecUpdate:
    return SetDefaultSpecUpdate(spec_id=spec_id)  # ty: ignore[unknown-argument]


def _set_sort_order(sort_order_id: int) -> SetDefaultSortOrderUpdate:
    return SetDefaultSortOrderUpdate(sort_order_id=sort_order_id)  # ty: ignore[unknown-argument]


def _set_main_ref(snapshot_id: int) -> SetSnapshotRefUpdate:
    # fmt: off
    return SetSnapshotRefUpdate(ref_name="main", type=SnapshotRefType.BRANCH, snapshot_id=snapshot_id)  # noqa: E501  # ty: ignore[unknown-argument, missing-argument]
    # fmt: on


def _assert_main(snapshot_id: int | None) -> AssertRefSnapshotId:
    return AssertRefSnapshotId(ref="main", snapshot_id=snapshot_id)  # ty: ignore[unknown-argument]
