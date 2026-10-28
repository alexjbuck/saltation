"""Raw table-update commits — the one place ``Transaction._apply`` is used.

Callers: ``restore_pointers`` (the rollback primitive), ``set_location``,
``upgrade_format_version`` and ``replace_schema``. The others exist because
PyIceberg's public ``Transaction.update_location`` is unimplemented,
``upgrade_table_version`` refuses format v3, and ``UpdateSchema`` refuses the
v3 date -> timestamp promotion, while the REST catalog accepts all three.

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

TODO(upstream, #26): a public PyIceberg ``Transaction.apply()`` (or accept
batch updates on the existing builder surface) so cambrian can drop the underscore.

INVARIANT: ``_commit`` holds the ONLY call to ``Transaction._apply`` in cambrian.
``ripgrep '_apply\\b' src/`` should return exactly one match. New raw-update
operations must be siblings in this module that route through ``_commit``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from pyiceberg.exceptions import CommitFailedException, RESTError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.update import (
    AddSchemaUpdate,
    AssertRefSnapshotId,
    RemovePartitionSpecsUpdate,
    RemovePropertiesUpdate,
    RemoveSchemasUpdate,
    RemoveSnapshotRefUpdate,
    RemoveSnapshotsUpdate,
    SetCurrentSchemaUpdate,
    SetDefaultSortOrderUpdate,
    SetDefaultSpecUpdate,
    SetLocationUpdate,
    SetPropertiesUpdate,
    SetSnapshotRefUpdate,
    TableRequirement,
    TableUpdate,
    UpgradeFormatVersionUpdate,
)

from cambrian.errors import ExternalWriteDetectedError


@dataclass(frozen=True)
class Prune:
    snapshot_ids: frozenset[int] = frozenset()
    schema_ids: frozenset[int] = frozenset()
    spec_ids: frozenset[int] = frozenset()


def restore_pointers(
    table: Table,
    *,
    target_snapshot_id: int | None,
    target_schema_id: int,
    target_spec_id: int,
    target_sort_order_id: int,
    expected_current_snapshot_id: int | None,
    target_properties: Mapping[str, str] | None = None,
    prune: Prune | None = None,
) -> None:
    """Atomically restore the four pointers (snapshot ref, schema, spec, sort order).

    With ``target_properties``, the table's properties are restored in the same
    commit (keys set to their old values, keys added since then removed). With
    ``prune``, the snapshots, schemas and specs added since the target are
    removed too, so the table carries no trace of the undone changes.

    ``expected_current_snapshot_id`` is the snapshot id the caller observed on the
    ``main`` ref at the moment they decided to roll back. If the catalog disagrees at
    commit time (because another writer advanced ``main``), the
    ``AssertRefSnapshotId`` requirement aborts the commit and this function raises
    ``ExternalWriteDetectedError``.

    The metadata-only case (table never had a snapshot, ``target_snapshot_id`` and
    ``expected_current_snapshot_id`` both ``None``) skips the snapshot-ref update and
    assertion — there's no ``main`` ref to restore — and only rolls back the three
    metadata pointers.

    When the checkpoint predates any append but the table has a snapshot now, the
    ``main`` ref is removed, which leaves the table with no current snapshot as it
    was, still guarded by the assertion on what ``main`` points at.
    """
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
    elif expected_current_snapshot_id is not None:
        updates = (_remove_main(), *updates)
        requirements = (_assert_main(expected_current_snapshot_id),)

    if target_properties is not None:
        current = table.properties
        changed = {k: v for k, v in target_properties.items() if current.get(k) != v}
        removed = sorted(set(current) - set(target_properties))
        if changed:
            updates = (*updates, SetPropertiesUpdate(updates=changed))
        if removed:
            updates = (*updates, RemovePropertiesUpdate(removals=removed))

    if prune is not None:
        updates = (*updates, *_prune_updates(prune))

    try:
        _commit(table, updates, requirements)
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


def replace_schema(table: Table, schema: Schema) -> None:
    """Add *schema* and make it current.

    For evolutions PyIceberg's UpdateSchema refuses but the spec allows (v3
    date -> timestamp promotion).
    """
    _commit(table, (AddSchemaUpdate(schema=schema), _set_schema(-1)), ())


def set_location(table: Table, location: str) -> None:
    # Transaction.update_location is NotImplemented in PyIceberg (#24).
    _commit(table, (SetLocationUpdate(location=location),), ())


def upgrade_format_version(table: Table, format_version: int) -> None:
    """Upgrade to *format_version*; a no-op at or above it (the spec forbids downgrades)."""
    if table.format_version >= format_version:
        return
    update = UpgradeFormatVersionUpdate(format_version=format_version)  # ty: ignore[unknown-argument, missing-argument]
    # Sent straight to the catalog: a Transaction first replays the update on
    # its local metadata copy, and PyIceberg 0.12 rejects v3 there (#23).
    table.catalog.commit_table(table, (), (update,))


def _commit(
    table: Table,
    updates: tuple[TableUpdate, ...],
    requirements: tuple[TableRequirement, ...],
) -> None:
    with table.transaction() as txn:
        # The sole _apply call site in cambrian; see module docstring.
        txn._apply(updates=updates, requirements=requirements)


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


def _prune_updates(prune: Prune) -> list[TableUpdate]:
    # Snapshots first: a spec or schema can't go while a snapshot uses it.
    out: list[TableUpdate] = []
    if prune.snapshot_ids:
        out.append(RemoveSnapshotsUpdate(snapshot_ids=sorted(prune.snapshot_ids)))  # ty: ignore[unknown-argument, missing-argument]
    if prune.spec_ids:
        out.append(RemovePartitionSpecsUpdate(spec_ids=sorted(prune.spec_ids)))  # ty: ignore[unknown-argument, missing-argument]
    if prune.schema_ids:
        out.append(RemoveSchemasUpdate(schema_ids=sorted(prune.schema_ids)))  # ty: ignore[unknown-argument, missing-argument]
    return out


def _remove_main() -> RemoveSnapshotRefUpdate:
    return RemoveSnapshotRefUpdate(ref_name="main")  # ty: ignore[unknown-argument, missing-argument]


def _assert_main(snapshot_id: int | None) -> AssertRefSnapshotId:
    return AssertRefSnapshotId(ref="main", snapshot_id=snapshot_id)  # ty: ignore[unknown-argument]
