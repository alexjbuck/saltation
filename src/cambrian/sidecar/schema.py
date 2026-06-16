"""Iceberg schemas + version pin for the ``_cambrian`` sidecar.

Three append-only tables track every action cambrian takes against a catalog:

- ``events`` — one row per logical action (apply, rollback, commit, …).
- ``table_states`` — pre/post pointer-tuple snapshots for each affected table,
  attached to the originating event by ``event_id``.
- ``version`` — single-row table holding the sidecar schema version.

These schemas are the *physical* on-disk shape; bumping
``CAMBRIAN_SIDECAR_VERSION`` requires adding a new function to
``SELF_MIGRATIONS`` (and never editing an existing one — they are pinned by
index and are forward-only).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType, TimestamptzType

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

__all__ = [
    "CAMBRIAN_SIDECAR_VERSION",
    "EVENTS_SCHEMA",
    "EVENTS_TABLE",
    "SELF_MIGRATIONS",
    "TABLE_STATES_SCHEMA",
    "TABLE_STATES_TABLE",
    "VERSION_SCHEMA",
    "VERSION_TABLE",
]

# Fixed internal table names — not user-configurable.
EVENTS_TABLE = "events"
TABLE_STATES_TABLE = "table_states"
VERSION_TABLE = "version"


# ---------------------------------------------------------------------------
# events
# ---------------------------------------------------------------------------

EVENTS_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="event_ts", field_type=TimestamptzType(), required=True),
    # apply | rollback | commit | uncommit | checkpoint
    NestedField(field_id=3, name="event_type", field_type=StringType(), required=True),
    NestedField(field_id=4, name="evolution_id", field_type=StringType(), required=True),
    NestedField(field_id=5, name="evolution_hash", field_type=StringType(), required=True),
    NestedField(field_id=6, name="evolution_sql", field_type=StringType(), required=True),
    NestedField(field_id=7, name="actor", field_type=StringType(), required=True),
    NestedField(field_id=8, name="notes", field_type=StringType(), required=False),
)


# ---------------------------------------------------------------------------
# table_states
# ---------------------------------------------------------------------------

TABLE_STATES_SCHEMA = Schema(
    NestedField(field_id=1, name="event_id", field_type=StringType(), required=True),
    NestedField(field_id=2, name="event_ts", field_type=TimestamptzType(), required=True),
    NestedField(field_id=3, name="table_ident", field_type=StringType(), required=True),
    # Pre-event pointer tuple. snapshot_id is nullable because a freshly-
    # created table has no current snapshot until the first append.
    NestedField(field_id=4, name="pre_snapshot_id", field_type=LongType(), required=False),
    NestedField(field_id=5, name="pre_schema_id", field_type=LongType(), required=False),
    NestedField(field_id=6, name="pre_spec_id", field_type=LongType(), required=False),
    NestedField(field_id=7, name="pre_sort_order_id", field_type=LongType(), required=False),
    NestedField(field_id=8, name="pre_metadata_loc", field_type=StringType(), required=False),
    # Post-event pointer tuple.
    NestedField(field_id=9, name="post_snapshot_id", field_type=LongType(), required=False),
    NestedField(field_id=10, name="post_schema_id", field_type=LongType(), required=False),
    NestedField(field_id=11, name="post_spec_id", field_type=LongType(), required=False),
    NestedField(field_id=12, name="post_sort_order_id", field_type=LongType(), required=False),
    NestedField(field_id=13, name="tag_ref", field_type=StringType(), required=False),
)


# ---------------------------------------------------------------------------
# version
# ---------------------------------------------------------------------------

VERSION_SCHEMA = Schema(
    NestedField(field_id=1, name="version", field_type=LongType(), required=True),
)


# ---------------------------------------------------------------------------
# Self-migration framework
# ---------------------------------------------------------------------------
#
# Each self-migration is forward-only and pinned by its index in this list.
# Never edit a self-migration once it has shipped — append a new one. The
# version persisted in ``_cambrian.version`` equals ``len(SELF_MIGRATIONS)``
# after all migrations have been applied.

# Populated below to keep the schema literals near the top of the file.
SELF_MIGRATIONS: list[Callable[[Catalog, str], None]]


def _v0_to_v1_initial(catalog: Catalog, namespace: str) -> None:
    """Initial bootstrap: create namespace, three tables, insert ``version=1``."""
    # Imported lazily to avoid a circular import between schema and selfmigrate.
    from cambrian.sidecar.selfmigrate import _create_initial_sidecar

    _create_initial_sidecar(catalog, namespace)


SELF_MIGRATIONS = [_v0_to_v1_initial]

# Invariant: CAMBRIAN_SIDECAR_VERSION == len(SELF_MIGRATIONS).
CAMBRIAN_SIDECAR_VERSION: int = len(SELF_MIGRATIONS)
