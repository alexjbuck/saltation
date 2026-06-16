"""Integration tests for the M4 rollback primitive.

Exercises ``cambrian.iceberg.checkpoint.capture`` + ``cambrian.iceberg.txn.restore_pointers``
against a live Lakekeeper REST catalog (via the docker-compose rig). Mirrors the
scenarios validated by ``prototypes/m4_rollback.py`` but driven through the cambrian
public API surface rather than the prototype's inline rollback.
"""

from __future__ import annotations

import pyarrow as pa
import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.transforms import IdentityTransform
from pyiceberg.types import IntegerType, LongType, NestedField, StringType

from cambrian.errors import ExternalWriteDetectedError, IllegalStateError
from cambrian.iceberg import Checkpoint, capture, pin, restore_pointers


def _initial_schema() -> Schema:
    return Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
        NestedField(field_id=3, name="value", field_type=IntegerType(), required=False),
    )


def _arrow_v1() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.int32()),
        ]
    )


def _arrow_v2() -> pa.Schema:
    return pa.schema(
        [
            ("id", pa.int64()),
            ("name", pa.string()),
            ("value", pa.int32()),
            ("extra", pa.int32()),
        ]
    )


def _batch_v1(start: int) -> pa.Table:
    return pa.Table.from_pylist(
        [
            {"id": start, "name": f"row-{start}", "value": start * 10},
            {"id": start + 1, "name": f"row-{start + 1}", "value": (start + 1) * 10},
        ],
        schema=_arrow_v1(),
    )


def test_capture_roundtrip_with_appends(ns: str, rest_catalog: RestCatalog) -> None:
    """Full mutation roundtrip: 3 appends + 3 metadata mutations restored in one go."""
    table_id = (ns, "rt")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())

    table.append(_batch_v1(1))
    table.append(_batch_v1(3))
    table = rest_catalog.load_table(table_id)

    cp = capture(table)
    assert cp.snapshot_id is not None
    assert cp.schema_id == 0
    assert cp.spec_id == 0
    assert cp.sort_order_id == 0

    with table.update_schema() as us:
        us.add_column("extra", IntegerType(), required=False)
    table = rest_catalog.load_table(table_id)
    with table.update_spec() as usp:
        usp.add_field("name", IdentityTransform(), "name_part")
    table = rest_catalog.load_table(table_id)
    with table.update_sort_order() as uso:
        uso.asc("id", IdentityTransform())
    table = rest_catalog.load_table(table_id)
    table.append(
        pa.Table.from_pylist(
            [{"id": 99, "name": "after", "value": 990, "extra": 9000}],
            schema=_arrow_v2(),
        )
    )
    table = rest_catalog.load_table(table_id)

    current_snap = table.current_snapshot()
    assert current_snap is not None
    advanced_snap_id = current_snap.snapshot_id
    assert advanced_snap_id != cp.snapshot_id
    assert table.schema().schema_id != cp.schema_id
    assert table.spec().spec_id != cp.spec_id
    assert table.sort_order().order_id != cp.sort_order_id

    restore_pointers(
        table,
        target_snapshot_id=cp.snapshot_id,
        target_schema_id=cp.schema_id,
        target_spec_id=cp.spec_id,
        target_sort_order_id=cp.sort_order_id,
        expected_current_snapshot_id=advanced_snap_id,
    )

    table = rest_catalog.load_table(table_id)
    post = capture(table)
    assert post.snapshot_id == cp.snapshot_id
    assert post.schema_id == cp.schema_id
    assert post.spec_id == cp.spec_id
    assert post.sort_order_id == cp.sort_order_id

    # Confirm data shape matches the pre-mutation rows (4 rows from two appends of 2).
    scanned = table.scan().to_arrow()
    assert scanned.num_rows == 4
    assert set(scanned.column_names) == {"id", "name", "value"}


def test_capture_no_snapshot_metadata_only_rollback(ns: str, rest_catalog: RestCatalog) -> None:
    """A freshly created table with no appends still has restorable schema/spec/sort_order."""
    table_id = (ns, "meta_only")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())
    assert table.current_snapshot() is None

    cp = capture(table)
    assert cp.snapshot_id is None
    assert cp.schema_id == 0
    assert cp.spec_id == 0
    assert cp.sort_order_id == 0

    with table.update_schema() as us:
        us.add_column("extra", IntegerType(), required=False)
    table = rest_catalog.load_table(table_id)
    with table.update_spec() as usp:
        usp.add_field("name", IdentityTransform(), "name_part")
    table = rest_catalog.load_table(table_id)
    with table.update_sort_order() as uso:
        uso.asc("id", IdentityTransform())
    table = rest_catalog.load_table(table_id)

    assert table.current_snapshot() is None
    assert table.schema().schema_id != cp.schema_id
    assert table.spec().spec_id != cp.spec_id
    assert table.sort_order().order_id != cp.sort_order_id

    restore_pointers(
        table,
        target_snapshot_id=cp.snapshot_id,
        target_schema_id=cp.schema_id,
        target_spec_id=cp.spec_id,
        target_sort_order_id=cp.sort_order_id,
        expected_current_snapshot_id=None,
    )

    table = rest_catalog.load_table(table_id)
    post = capture(table)
    assert post.snapshot_id is None
    assert post.schema_id == cp.schema_id
    assert post.spec_id == cp.spec_id
    assert post.sort_order_id == cp.sort_order_id


def test_restore_refuses_concurrent_external_write(ns: str, rest_catalog: RestCatalog) -> None:
    """A stale ``expected_current_snapshot_id`` must abort the rollback, not clobber."""
    table_id = (ns, "concurrent")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())
    table.append(_batch_v1(1))
    table = rest_catalog.load_table(table_id)

    cp = capture(table)
    stale_observation = cp.snapshot_id
    assert stale_observation is not None

    # Simulate an external writer advancing main between checkpoint and rollback.
    table.append(_batch_v1(3))
    table = rest_catalog.load_table(table_id)
    current_snap = table.current_snapshot()
    assert current_snap is not None
    assert current_snap.snapshot_id != stale_observation

    with pytest.raises(ExternalWriteDetectedError) as exc_info:
        restore_pointers(
            table,
            target_snapshot_id=cp.snapshot_id,
            target_schema_id=cp.schema_id,
            target_spec_id=cp.spec_id,
            target_sort_order_id=cp.sort_order_id,
            expected_current_snapshot_id=stale_observation,
        )

    err = exc_info.value
    assert err.ref == "main"
    assert err.expected_snapshot_id == stale_observation
    assert err.observed_snapshot_id == current_snap.snapshot_id


def test_restore_refuses_when_checkpoint_predates_appends(
    ns: str, rest_catalog: RestCatalog
) -> None:
    """The asymmetric "captured before snapshot, now has snapshot" case raises IllegalStateError."""
    table_id = (ns, "asymmetric")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())
    cp = capture(table)
    assert cp.snapshot_id is None

    table.append(_batch_v1(1))
    table = rest_catalog.load_table(table_id)
    current_snap = table.current_snapshot()
    assert current_snap is not None

    with pytest.raises(IllegalStateError):
        restore_pointers(
            table,
            target_snapshot_id=cp.snapshot_id,
            target_schema_id=cp.schema_id,
            target_spec_id=cp.spec_id,
            target_sort_order_id=cp.sort_order_id,
            expected_current_snapshot_id=current_snap.snapshot_id,
        )


def test_pin_creates_tag(ns: str, rest_catalog: RestCatalog) -> None:
    """``pin`` must create an Iceberg tag pointing at the captured snapshot."""
    table_id = (ns, "pinned")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())
    table.append(_batch_v1(1))
    table = rest_catalog.load_table(table_id)

    cp = capture(table)
    assert cp.snapshot_id is not None
    tag_name = "cambrian.cp.test"

    pin(table, tag_name=tag_name, snapshot_id=cp.snapshot_id)

    table = rest_catalog.load_table(table_id)
    refs = table.refs()
    assert tag_name in refs, f"tag {tag_name!r} not in refs: {list(refs)}"
    assert refs[tag_name].snapshot_id == cp.snapshot_id
    assert refs[tag_name].snapshot_ref_type == "tag"


def test_pin_is_noop_for_metadata_only_checkpoint(ns: str, rest_catalog: RestCatalog) -> None:
    """``pin`` returns cleanly without creating anything when the checkpoint has no snapshot."""
    table_id = (ns, "no_snap_pin")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())
    cp = capture(table)
    assert cp.snapshot_id is None

    pin(table, tag_name="cambrian.cp.noop", snapshot_id=cp.snapshot_id)

    table = rest_catalog.load_table(table_id)
    assert "cambrian.cp.noop" not in table.refs()


def test_checkpoint_carries_metadata_location(ns: str, rest_catalog: RestCatalog) -> None:
    """The audit-trail field on Checkpoint must round-trip the table's current metadata.json."""
    table_id = (ns, "meta_loc")
    table = rest_catalog.create_table(identifier=table_id, schema=_initial_schema())
    table.append(_batch_v1(1))
    table = rest_catalog.load_table(table_id)

    cp = capture(table)
    assert isinstance(cp, Checkpoint)
    assert cp.metadata_loc
    assert cp.metadata_loc == table.metadata_location
    assert cp.tag_ref is None
