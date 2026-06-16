"""Unit tests for the sidecar schemas and self-migration framework wiring."""

from __future__ import annotations

from pyiceberg.schema import Schema

from cambrian.sidecar.schema import (
    CAMBRIAN_SIDECAR_VERSION,
    EVENTS_SCHEMA,
    EVENTS_TABLE,
    SELF_MIGRATIONS,
    TABLE_STATES_SCHEMA,
    TABLE_STATES_TABLE,
    VERSION_SCHEMA,
    VERSION_TABLE,
)


def _ids(schema: Schema) -> list[int]:
    return [f.field_id for f in schema.fields]


def test_schemas_are_iceberg_schemas() -> None:
    assert isinstance(EVENTS_SCHEMA, Schema)
    assert isinstance(TABLE_STATES_SCHEMA, Schema)
    assert isinstance(VERSION_SCHEMA, Schema)


def test_field_ids_unique_and_contiguous() -> None:
    for schema in (EVENTS_SCHEMA, TABLE_STATES_SCHEMA, VERSION_SCHEMA):
        ids = _ids(schema)
        assert len(set(ids)) == len(ids), f"duplicate field ids in {schema}"
        assert ids == list(range(1, len(ids) + 1)), f"field ids must be 1..N contiguous, got {ids}"


def test_events_schema_required_columns_present() -> None:
    names = [f.name for f in EVENTS_SCHEMA.fields]
    assert names == [
        "event_id",
        "event_ts",
        "event_type",
        "evolution_id",
        "evolution_hash",
        "evolution_sql",
        "actor",
        "notes",
    ]
    # notes is the only optional field.
    optional = [f.name for f in EVENTS_SCHEMA.fields if not f.required]
    assert optional == ["notes"]


def test_table_states_schema_pre_and_post_pointer_columns() -> None:
    names = [f.name for f in TABLE_STATES_SCHEMA.fields]
    # event_id and event_ts pair every row back to events.
    assert names[:3] == ["event_id", "event_ts", "table_ident"]
    for prefix in ("pre_", "post_"):
        for suffix in ("snapshot_id", "schema_id", "spec_id", "sort_order_id"):
            assert f"{prefix}{suffix}" in names
    assert "pre_metadata_loc" in names
    assert "tag_ref" in names


def test_version_schema_single_column() -> None:
    names = [f.name for f in VERSION_SCHEMA.fields]
    assert names == ["version"]


def test_table_name_constants() -> None:
    assert EVENTS_TABLE == "events"
    assert TABLE_STATES_TABLE == "table_states"
    assert VERSION_TABLE == "version"


def test_version_matches_self_migration_count() -> None:
    """The on-disk version is defined as ``len(SELF_MIGRATIONS)``."""
    assert CAMBRIAN_SIDECAR_VERSION == len(SELF_MIGRATIONS)
    assert CAMBRIAN_SIDECAR_VERSION >= 1


def test_self_migrations_are_callable() -> None:
    for migration in SELF_MIGRATIONS:
        assert callable(migration)
