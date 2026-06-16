"""Integration coverage for transaction-sensitive corpus operations.

The unit oracle (``tests/unit/test_corpus_coverage.py``) proves parse +
dispatch-translation against a mock catalog. Mocks can't validate real
schema/spec/sort-order mutations or multi-commit sequences, and the project's
locked rule requires those to run against Lakekeeper, not ``SqlCatalog`` (which
doesn't replicate REST atomic-multi-update semantics).

This file applies the corpus SQL through the real parse+dispatch path against
the live ``rest_catalog`` and asserts the loaded post-state. It focuses on the
operations the mock can't cover: nested types, partition transforms, the WRITE
distribution family (two commits on one handle), identifier fields, DELETE by
filter, RENAME TABLE, ALTER COLUMN, and namespace properties — plus a couple of
negatives end-to-end.
"""

from __future__ import annotations

import pytest
import sqlglot
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
)

from cambrian.errors import UnsupportedStatementError
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.dispatch import dispatch


def _apply(catalog: RestCatalog, sql: str) -> None:
    """Parse *sql* with the cambrian dialect and dispatch each statement."""
    for stmt in sqlglot.parse(sql, dialect=CambrianSpark):
        if stmt is not None:
            dispatch(catalog, stmt)


def _field(schema: Schema, name: str) -> NestedField:
    return next(f for f in schema.fields if f.name == name)


# ---------------------------------------------------------------------------
# Nested types
# ---------------------------------------------------------------------------


def test_nested_types_build_real_schema(rest_catalog: RestCatalog, ns: str) -> None:
    """CREATE TABLE with struct/array/map/nested combo → real nested schema."""
    _apply(
        rest_catalog,
        f"CREATE TABLE {ns}.t ("
        "point STRUCT<x: DOUBLE, y: DOUBLE>, "
        "tags ARRAY<STRING>, "
        "attrs MAP<STRING, INT>, "
        "points ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>, "
        "lookup MAP<STRING, ARRAY<INT>>"
        ") USING iceberg",
    )
    schema = rest_catalog.load_table((ns, "t")).schema()

    point = _field(schema, "point").field_type
    assert isinstance(point, StructType)
    assert {f.name for f in point.fields} == {"x", "y"}

    tags = _field(schema, "tags").field_type
    assert isinstance(tags, ListType)
    assert isinstance(tags.element_type, StringType)

    attrs = _field(schema, "attrs").field_type
    assert isinstance(attrs, MapType)
    assert isinstance(attrs.key_type, StringType)

    points = _field(schema, "points").field_type
    assert isinstance(points, ListType)
    assert isinstance(points.element_type, StructType)

    lookup = _field(schema, "lookup").field_type
    assert isinstance(lookup, MapType)
    assert isinstance(lookup.value_type, ListType)


# ---------------------------------------------------------------------------
# Partition transforms (truncate add / drop / replace)
# ---------------------------------------------------------------------------


def _spec_transforms(catalog: RestCatalog, ns: str, table: str) -> dict[str, str]:
    """Map partition-field name → its transform's string repr."""
    tbl = catalog.load_table((ns, table))
    return {f.name: str(f.transform) for f in tbl.spec().fields}


def test_add_truncate_partition_field(rest_catalog: RestCatalog, ns: str) -> None:
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (data STRING) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ADD PARTITION FIELD truncate(4, data)")
    transforms = _spec_transforms(rest_catalog, ns, "t")
    assert any("truncate[4]" == v for v in transforms.values()), transforms


def test_drop_truncate_partition_field(rest_catalog: RestCatalog, ns: str) -> None:
    # Add with an explicit alias so DROP can target the partition-field name
    # unambiguously (the default name PyIceberg synthesises is data_trunc_4).
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (data STRING) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ADD PARTITION FIELD truncate(4, data) AS d4")
    assert "d4" in _spec_transforms(rest_catalog, ns, "t")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t DROP PARTITION FIELD d4")
    # After dropping, the truncate field should be voided/absent from the live
    # spec fields (a dropped field becomes a void transform or disappears).
    transforms = _spec_transforms(rest_catalog, ns, "t")
    assert not any("truncate" in v for v in transforms.values()), transforms


def test_drop_partition_field_by_transform_expr(rest_catalog: RestCatalog, ns: str) -> None:
    # Corpus `dpf_transform`: drop by the transform expression with NO alias.
    # The field name Iceberg synthesises (id_bucket) differs from the source
    # column (id), so the handler must resolve it from the live spec.
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ADD PARTITION FIELD bucket(16, id)")
    assert any("bucket[16]" == v for v in _spec_transforms(rest_catalog, ns, "t").values())
    _apply(rest_catalog, f"ALTER TABLE {ns}.t DROP PARTITION FIELD bucket(16, id)")
    transforms = _spec_transforms(rest_catalog, ns, "t")
    assert not any("bucket" in v for v in transforms.values()), transforms
    # Re-applying the drop is an idempotent no-op (nothing left to match).
    _apply(rest_catalog, f"ALTER TABLE {ns}.t DROP PARTITION FIELD bucket(16, id)")


def test_replace_truncate_partition_field(rest_catalog: RestCatalog, ns: str) -> None:
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (data STRING) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ADD PARTITION FIELD truncate(4, data) AS d4")
    _apply(
        rest_catalog,
        f"ALTER TABLE {ns}.t REPLACE PARTITION FIELD d4 WITH truncate(8, data) AS d8",
    )
    transforms = _spec_transforms(rest_catalog, ns, "t")
    assert "truncate[8]" in transforms.values(), transforms
    assert "truncate[4]" not in transforms.values(), transforms


# ---------------------------------------------------------------------------
# WRITE family — sort order + write.distribution-mode (two commits per handle)
# ---------------------------------------------------------------------------


def _sort_columns(catalog: RestCatalog, ns: str, table: str) -> list[str]:
    """Source-column names of the live sort order, in order."""
    tbl = catalog.load_table((ns, table))
    schema = tbl.schema()
    return [schema.find_column_name(f.source_id) for f in tbl.sort_order().fields]


def _distribution_mode(catalog: RestCatalog, ns: str, table: str) -> str | None:
    return catalog.load_table((ns, table)).properties.get("write.distribution-mode")


def _make_sortable_table(catalog: RestCatalog, ns: str) -> None:
    _apply(catalog, f"CREATE TABLE {ns}.t (id BIGINT, category STRING) USING iceberg")


def test_write_ordered_by_unparenthesized(rest_catalog: RestCatalog, ns: str) -> None:
    _make_sortable_table(rest_catalog, ns)
    _apply(rest_catalog, f"ALTER TABLE {ns}.t WRITE ORDERED BY category, id")
    assert _sort_columns(rest_catalog, ns, "t") == ["category", "id"]
    assert _distribution_mode(rest_catalog, ns, "t") == "range"


def test_write_ordered_by_parenthesized(rest_catalog: RestCatalog, ns: str) -> None:
    _make_sortable_table(rest_catalog, ns)
    _apply(rest_catalog, f"ALTER TABLE {ns}.t WRITE ORDERED BY (category, id)")
    assert _sort_columns(rest_catalog, ns, "t") == ["category", "id"]
    assert _distribution_mode(rest_catalog, ns, "t") == "range"


def test_write_locally_ordered_by(rest_catalog: RestCatalog, ns: str) -> None:
    _make_sortable_table(rest_catalog, ns)
    _apply(rest_catalog, f"ALTER TABLE {ns}.t WRITE LOCALLY ORDERED BY category, id")
    assert _sort_columns(rest_catalog, ns, "t") == ["category", "id"]
    assert _distribution_mode(rest_catalog, ns, "t") == "none"


def test_write_distributed_by_partition(rest_catalog: RestCatalog, ns: str) -> None:
    _make_sortable_table(rest_catalog, ns)
    _apply(rest_catalog, f"ALTER TABLE {ns}.t WRITE DISTRIBUTED BY PARTITION")
    assert _distribution_mode(rest_catalog, ns, "t") == "hash"


def test_write_unordered_clears_sort(rest_catalog: RestCatalog, ns: str) -> None:
    _make_sortable_table(rest_catalog, ns)
    _apply(rest_catalog, f"ALTER TABLE {ns}.t WRITE ORDERED BY category, id")
    assert _sort_columns(rest_catalog, ns, "t") == ["category", "id"]
    _apply(rest_catalog, f"ALTER TABLE {ns}.t WRITE UNORDERED")
    assert _sort_columns(rest_catalog, ns, "t") == []


# ---------------------------------------------------------------------------
# Identifier fields (needs required columns / V2)
# ---------------------------------------------------------------------------


def _v2_table_with_required(catalog: RestCatalog, ns: str) -> None:
    """Create a V2 table whose ``id``/``data`` are required (identifier-eligible)."""
    schema = Schema(
        NestedField(1, "id", LongType(), required=True),
        NestedField(2, "data", StringType(), required=True),
    )
    catalog.create_table((ns, "t"), schema=schema, properties={"format-version": "2"})


def test_set_and_drop_identifier_fields(rest_catalog: RestCatalog, ns: str) -> None:
    _v2_table_with_required(rest_catalog, ns)
    _apply(rest_catalog, f"ALTER TABLE {ns}.t SET IDENTIFIER FIELDS id, data")
    schema = rest_catalog.load_table((ns, "t")).schema()
    id_field = _field(schema, "id").field_id
    data_field = _field(schema, "data").field_id
    assert set(schema.identifier_field_ids) == {id_field, data_field}

    _apply(rest_catalog, f"ALTER TABLE {ns}.t DROP IDENTIFIER FIELDS data")
    schema = rest_catalog.load_table((ns, "t")).schema()
    assert set(schema.identifier_field_ids) == {id_field}


# ---------------------------------------------------------------------------
# DELETE by filter — seed, delete, re-apply is a no-op
# ---------------------------------------------------------------------------


def test_delete_by_filter_idempotent(rest_catalog: RestCatalog, ns: str) -> None:
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (id BIGINT, name STRING) USING iceberg")
    _apply(
        rest_catalog,
        f"INSERT INTO {ns}.t VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')",
    )
    _apply(rest_catalog, f"DELETE FROM {ns}.t WHERE id = 1")
    arrow = rest_catalog.load_table((ns, "t")).scan().to_arrow()
    assert sorted(arrow.column("id").to_pylist()) == [2, 3]

    # Re-applying the same DELETE matches no rows → no change.
    _apply(rest_catalog, f"DELETE FROM {ns}.t WHERE id = 1")
    arrow = rest_catalog.load_table((ns, "t")).scan().to_arrow()
    assert sorted(arrow.column("id").to_pylist()) == [2, 3]


# ---------------------------------------------------------------------------
# RENAME TABLE
# ---------------------------------------------------------------------------


def test_rename_table_and_idempotent(rest_catalog: RestCatalog, ns: str) -> None:
    from pyiceberg.exceptions import NoSuchTableError

    _apply(rest_catalog, f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t RENAME TO {ns}.t2")
    assert rest_catalog.table_exists((ns, "t2"))
    with pytest.raises(NoSuchTableError):
        rest_catalog.load_table((ns, "t"))

    # Re-apply: source is already gone → idempotent no-op, t2 still present.
    _apply(rest_catalog, f"ALTER TABLE {ns}.t RENAME TO {ns}.t2")
    assert rest_catalog.table_exists((ns, "t2"))


# ---------------------------------------------------------------------------
# ALTER COLUMN — comment / SET NOT NULL / DROP NOT NULL / reorder
# ---------------------------------------------------------------------------


def test_alter_column_comment(rest_catalog: RestCatalog, ns: str) -> None:
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (measurement DOUBLE) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ALTER COLUMN measurement COMMENT 'unit kb/s'")
    schema = rest_catalog.load_table((ns, "t")).schema()
    assert _field(schema, "measurement").doc == "unit kb/s"


def test_alter_column_set_and_drop_not_null(rest_catalog: RestCatalog, ns: str) -> None:
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (id BIGINT) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ALTER COLUMN id SET NOT NULL")
    assert _field(rest_catalog.load_table((ns, "t")).schema(), "id").required is True
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ALTER COLUMN id DROP NOT NULL")
    assert _field(rest_catalog.load_table((ns, "t")).schema(), "id").required is False


def test_alter_column_reorder(rest_catalog: RestCatalog, ns: str) -> None:
    _apply(rest_catalog, f"CREATE TABLE {ns}.t (a INT, b INT, c INT) USING iceberg")
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ALTER COLUMN c FIRST")
    names = [f.name for f in rest_catalog.load_table((ns, "t")).schema().fields]
    assert names[0] == "c"
    _apply(rest_catalog, f"ALTER TABLE {ns}.t ALTER COLUMN a AFTER b")
    names = [f.name for f in rest_catalog.load_table((ns, "t")).schema().fields]
    assert names.index("a") == names.index("b") + 1


# ---------------------------------------------------------------------------
# Namespace properties
# ---------------------------------------------------------------------------


def test_create_namespace_with_properties(rest_catalog: RestCatalog) -> None:
    import uuid

    from pydantic import ValidationError

    name = f"test_props_{uuid.uuid4().hex[:10]}"
    try:
        _apply(rest_catalog, f"CREATE NAMESPACE {name} WITH PROPERTIES ('owner' = 'eng')")
        props = rest_catalog.load_namespace_properties(name)
        assert props.get("owner") == "eng"

        # ALTER NAMESPACE SET PROPERTIES: this Lakekeeper version returns an
        # UpdateNamespacePropertiesResponse body that the pinned PyIceberg
        # rejects (missing ``updated``/``removed``/``missing`` shape), so the
        # client raises on parsing the *response* even though the server
        # applied the change. Tolerate that upstream incompatibility and verify
        # the mutation landed server-side with a fresh read.
        try:
            _apply(rest_catalog, f"ALTER NAMESPACE {name} SET PROPERTIES ('owner' = 'data')")
        except ValidationError:
            pass
        props = rest_catalog.load_namespace_properties(name)
        assert props.get("owner") == "data"
    finally:
        from pyiceberg.exceptions import NoSuchNamespaceError

        try:
            rest_catalog.drop_namespace(name)
        except NoSuchNamespaceError:
            pass


# ---------------------------------------------------------------------------
# Negatives end-to-end through the real apply path
# ---------------------------------------------------------------------------


def test_negative_create_or_replace_table(rest_catalog: RestCatalog, ns: str) -> None:
    with pytest.raises(UnsupportedStatementError):
        _apply(rest_catalog, f"CREATE OR REPLACE TABLE {ns}.t (id INT) USING iceberg")


def test_negative_call_procedure(rest_catalog: RestCatalog, ns: str) -> None:
    del ns  # CALL has no table precondition
    with pytest.raises(UnsupportedStatementError):
        _apply(rest_catalog, "CALL cat.system.rewrite_data_files('db.t')")
