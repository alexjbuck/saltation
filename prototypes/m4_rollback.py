"""M4 precondition prototype: verify atomic four-pointer rollback.

Standalone exploratory script (NOT part of the cambrian package and NOT a
pytest target). Run against the live docker-compose rig under
``docker/compose.yml``:

    docker compose -f docker/compose.yml up -d
    uv run python prototypes/m4_rollback.py

The script exercises the design decision recorded in CLAUDE.md:

    Rollback primitive: one atomic commit restoring four pointers
    (SetSnapshotRefUpdate, SetCurrentSchemaUpdate, SetDefaultSpecUpdate,
    SetDefaultSortOrderUpdate) via Transaction._apply(). Private PyIceberg
    API - isolated in src/cambrian/iceberg/txn.py behind one wrapper
    function.

We need to confirm the private API actually works against Lakekeeper before
building M4 on top of it. The script self-reports PASS / FAIL for two
scenarios:

  1. Full mutation roundtrip (schema, spec, sort-order, snapshot all
     advanced, then restored).
  2. Metadata-only mutation (no new snapshots after the checkpoint), to
     confirm SetSnapshotRefUpdate with snapshot_id == cp.snapshot_id is a
     no-op the catalog accepts.

The script cleans up its namespace and table at the end regardless of
outcome.
"""

from __future__ import annotations

import dataclasses
import logging
import sys
import traceback
import uuid
from typing import Any

import pyarrow as pa
from pyiceberg.catalog import Catalog, load_catalog
from pyiceberg.exceptions import NoSuchNamespaceError, NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.table import Table
from pyiceberg.table.refs import SnapshotRefType
from pyiceberg.table.update import (
    AssertRefSnapshotId,
    SetCurrentSchemaUpdate,
    SetDefaultSortOrderUpdate,
    SetDefaultSpecUpdate,
    SetSnapshotRefUpdate,
)
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

LOG = logging.getLogger("m4_rollback")

# Mirrors tests/integration/conftest.py CATALOG_KWARGS. Hard-coded here
# because this script is intentionally standalone - the conftest fixture
# uses pytest plumbing we don't want to drag in.
CATALOG_NAME = "cambrian"
CATALOG_KWARGS: dict[str, Any] = {
    "uri": "http://localhost:8181/catalog",
    "warehouse": "cambrian",
    "s3.endpoint": "http://localhost:9000",
    "s3.access-key-id": "cambrian-access-key",
    "s3.secret-access-key": "cambrian-secret-key",
    "s3.region": "local",
    "s3.path-style-access": "true",
}


@dataclasses.dataclass(frozen=True)
class Checkpoint:
    """The four pointers (+ metadata location) we capture at S1."""

    snapshot_id: int
    schema_id: int
    spec_id: int
    sort_order_id: int
    metadata_location: str | None


def _arrow_schema_v1() -> pa.Schema:
    """Arrow schema matching the initial Iceberg schema (long, string, int)."""
    return pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.int32()),
        ]
    )


def _initial_iceberg_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=IntegerType(), required=False),
    )


def _capture_checkpoint(table: Table) -> Checkpoint:
    snap = table.current_snapshot()
    if snap is None:
        msg = "cannot checkpoint an empty table (no current snapshot)"
        raise RuntimeError(msg)
    return Checkpoint(
        snapshot_id=snap.snapshot_id,
        schema_id=table.schema().schema_id,
        spec_id=table.spec().spec_id,
        sort_order_id=table.sort_order().order_id,
        metadata_location=table.metadata_location,
    )


def _summary(table: Table) -> str:
    snap = table.current_snapshot()
    snap_id = snap.snapshot_id if snap else None
    return (
        f"snapshot_id={snap_id} "
        f"schema_id={table.schema().schema_id} "
        f"spec_id={table.spec().spec_id} "
        f"sort_order_id={table.sort_order().order_id}"
    )


def _rollback(table: Table, cp: Checkpoint, current_main_snapshot_id: int) -> None:
    """Single atomic four-pointer restore via the private Transaction._apply API."""
    LOG.info("rolling back: cp=%s expected-main-before=%s", cp, current_main_snapshot_id)
    updates = (
        SetSnapshotRefUpdate(
            ref_name="main",
            type=SnapshotRefType.BRANCH,
            snapshot_id=cp.snapshot_id,
        ),
        SetCurrentSchemaUpdate(schema_id=cp.schema_id),
        SetDefaultSpecUpdate(spec_id=cp.spec_id),
        SetDefaultSortOrderUpdate(sort_order_id=cp.sort_order_id),
    )
    requirements = (AssertRefSnapshotId(ref="main", snapshot_id=current_main_snapshot_id),)
    with table.transaction() as txn:
        txn._apply(updates=updates, requirements=requirements)


def _drop_table(catalog: Catalog, ident: str) -> None:
    try:
        catalog.drop_table(ident)
    except NoSuchTableError:
        pass


def _drop_namespace(catalog: Catalog, namespace: str) -> None:
    try:
        for ident in catalog.list_tables(namespace):
            _drop_table(catalog, ".".join(ident))
        catalog.drop_namespace(namespace)
    except NoSuchNamespaceError:
        pass


def scenario_full(catalog: Catalog) -> bool:
    """Scenario 1: advance schema, spec, sort-order, AND snapshot, then rollback.

    Returns True iff all four pointers post-rollback match the captured S1
    checkpoint.
    """
    LOG.info("=" * 72)
    LOG.info("Scenario 1: full mutation roundtrip")
    LOG.info("=" * 72)
    namespace = f"m4_full_{uuid.uuid4().hex[:10]}"
    table_ident = f"{namespace}.t"
    catalog.create_namespace(namespace)

    try:
        table = catalog.create_table(
            identifier=table_ident,
            schema=_initial_iceberg_schema(),
        )
        LOG.info("created %s at %s", table_ident, table.metadata_location)

        # Batch A -> snapshot S0 (PyIceberg's create-table commit may already
        # produce one, but appends are what guarantee a row-bearing snapshot).
        arrow_v1 = _arrow_schema_v1()
        batch_a = pa.Table.from_pylist(
            [
                {"id": 1, "name": "alpha", "value": 10},
                {"id": 2, "name": "beta", "value": 20},
            ],
            schema=arrow_v1,
        )
        table.append(batch_a)
        LOG.info("appended batch A -> %s", _summary(table))

        # Batch B -> snapshot S1: the checkpoint target.
        batch_b = pa.Table.from_pylist(
            [
                {"id": 3, "name": "gamma", "value": 30},
                {"id": 4, "name": "delta", "value": 40},
            ],
            schema=arrow_v1,
        )
        table.append(batch_b)
        LOG.info("appended batch B -> %s", _summary(table))

        # Re-load so the in-memory view matches the catalog after the appends.
        table = catalog.load_table(table_ident)
        history = table.history()
        LOG.info("history after batch B (%d entries):", len(history))
        for entry in history:
            LOG.info("  %s", entry)
        if len(history) < 2:
            LOG.error("expected at least 2 snapshots in history; got %d", len(history))
            return False

        cp = _capture_checkpoint(table)
        LOG.info("checkpoint captured: %s", cp)

        # --- mutate: schema -------------------------------------------------
        with table.update_schema() as us:
            us.add_column("extra", IntegerType(), required=False)
        table = catalog.load_table(table_ident)
        LOG.info("after add_column -> %s", _summary(table))

        # --- mutate: partition spec ----------------------------------------
        with table.update_spec() as us:
            us.add_field("name", IdentityTransform(), "name_part")
        table = catalog.load_table(table_ident)
        LOG.info("after add partition field -> %s", _summary(table))

        # --- mutate: sort order --------------------------------------------
        # PyIceberg 0.11.1's UpdateSortOrder.asc requires an explicit
        # transform (the spec lets you sort by transformed values, not just
        # raw columns).
        with table.update_sort_order() as uso:
            uso.asc("id", IdentityTransform())
        table = catalog.load_table(table_ident)
        LOG.info("after sort_order asc(id) -> %s", _summary(table))

        # --- mutate: snapshot (append onto the new schema/spec) -------------
        # Schema now has an extra nullable column, so we add it to the arrow
        # data. PyArrow will round-trip the missing column as NULLs but we're
        # explicit here.
        arrow_v2 = pa.schema(
            [
                ("id", pa.int64()),
                ("name", pa.string()),
                ("value", pa.int32()),
                ("extra", pa.int32()),
            ]
        )
        batch_c = pa.Table.from_pylist(
            [
                {"id": 5, "name": "epsilon", "value": 50, "extra": 500},
                {"id": 6, "name": "zeta", "value": 60, "extra": 600},
            ],
            schema=arrow_v2,
        )
        table.append(batch_c)
        table = catalog.load_table(table_ident)
        LOG.info("after append batch C -> %s", _summary(table))

        # Sanity: confirm all four pointers actually advanced past the cp.
        if table.current_snapshot() is None:
            LOG.error("current_snapshot() is None after mutations - bug")
            return False
        advanced = (
            table.current_snapshot().snapshot_id != cp.snapshot_id
            and table.schema().schema_id != cp.schema_id
            and table.spec().spec_id != cp.spec_id
            and table.sort_order().order_id != cp.sort_order_id
        )
        if not advanced:
            LOG.error(
                "expected all 4 pointers to advance; got %s vs cp=%s",
                _summary(table),
                cp,
            )
            return False

        current_main = table.current_snapshot().snapshot_id

        # --- rollback ------------------------------------------------------
        _rollback(table, cp, current_main)

        # --- verify --------------------------------------------------------
        table = catalog.load_table(table_ident)
        post = _capture_checkpoint(table)
        LOG.info("post-rollback: %s", post)
        ok = (
            post.snapshot_id == cp.snapshot_id
            and post.schema_id == cp.schema_id
            and post.spec_id == cp.spec_id
            and post.sort_order_id == cp.sort_order_id
        )
        if ok:
            LOG.info("PASS: all four pointers restored")
        else:
            LOG.error("FAIL: post-rollback state diverges from checkpoint")
            LOG.error("  cp=%s", cp)
            LOG.error("  post=%s", post)
        return ok
    finally:
        _drop_table(catalog, table_ident)
        _drop_namespace(catalog, namespace)


def scenario_metadata_only(catalog: Catalog) -> bool:
    """Scenario 2: rollback with NO new snapshots (metadata-only mutations).

    Confirms that SetSnapshotRefUpdate with snapshot_id == cp.snapshot_id is
    a no-op the REST catalog accepts cleanly.
    """
    LOG.info("=" * 72)
    LOG.info("Scenario 2: metadata-only mutation (no new snapshots)")
    LOG.info("=" * 72)
    namespace = f"m4_meta_{uuid.uuid4().hex[:10]}"
    table_ident = f"{namespace}.t"
    catalog.create_namespace(namespace)

    try:
        table = catalog.create_table(
            identifier=table_ident,
            schema=_initial_iceberg_schema(),
        )
        # One append is enough to give us a current_snapshot to anchor on.
        batch_a = pa.Table.from_pylist(
            [{"id": 1, "name": "alpha", "value": 10}],
            schema=_arrow_schema_v1(),
        )
        table.append(batch_a)
        table = catalog.load_table(table_ident)
        cp = _capture_checkpoint(table)
        LOG.info("checkpoint captured: %s", cp)

        # Metadata-only mutations: schema, spec, sort order. NO append.
        with table.update_schema() as us:
            us.add_column("extra", IntegerType(), required=False)
        table = catalog.load_table(table_ident)
        with table.update_spec() as us:
            us.add_field("name", IdentityTransform(), "name_part")
        table = catalog.load_table(table_ident)
        with table.update_sort_order() as uso:
            uso.asc("id", IdentityTransform())
        table = catalog.load_table(table_ident)
        LOG.info("after metadata mutations -> %s", _summary(table))

        if table.current_snapshot() is None:
            LOG.error("lost current_snapshot during metadata mutations")
            return False
        if table.current_snapshot().snapshot_id != cp.snapshot_id:
            LOG.error(
                "snapshot moved unexpectedly: %s vs cp=%s",
                table.current_snapshot().snapshot_id,
                cp.snapshot_id,
            )
            return False

        # current_main == cp.snapshot_id here; SetSnapshotRefUpdate should
        # accept this as a no-op.
        _rollback(table, cp, current_main_snapshot_id=cp.snapshot_id)

        table = catalog.load_table(table_ident)
        post = _capture_checkpoint(table)
        LOG.info("post-rollback: %s", post)
        ok = (
            post.snapshot_id == cp.snapshot_id
            and post.schema_id == cp.schema_id
            and post.spec_id == cp.spec_id
            and post.sort_order_id == cp.sort_order_id
        )
        if ok:
            LOG.info("PASS: metadata-only rollback succeeded")
        else:
            LOG.error("FAIL: metadata-only rollback diverges from checkpoint")
            LOG.error("  cp=%s", cp)
            LOG.error("  post=%s", post)
        return ok
    finally:
        _drop_table(catalog, table_ident)
        _drop_namespace(catalog, namespace)


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-5s %(name)s :: %(message)s",
    )

    catalog = load_catalog(CATALOG_NAME, **CATALOG_KWARGS)
    LOG.info("connected to catalog %r at %s", CATALOG_NAME, CATALOG_KWARGS["uri"])

    results: dict[str, bool] = {}
    for name, fn in (
        ("full", scenario_full),
        ("metadata_only", scenario_metadata_only),
    ):
        try:
            results[name] = fn(catalog)
        except Exception:
            LOG.exception("scenario %r raised", name)
            traceback.print_exc()
            results[name] = False

    LOG.info("=" * 72)
    LOG.info("Final results:")
    for name, ok in results.items():
        LOG.info("  %s: %s", name, "PASS" if ok else "FAIL")
    LOG.info("=" * 72)

    return 0 if all(results.values()) else 1


if __name__ == "__main__":
    sys.exit(main())
