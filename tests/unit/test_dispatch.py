"""Tests for ``cambrian.sql.dispatch`` — AST → PyIceberg API call translation.

We mock the catalog and the table; each test parses one SQL statement,
runs :func:`dispatch`, and asserts the expected method calls on the mocks.
Idempotent semantics (tolerating "already exists" / "already absent") are
tested with mocks that raise the relevant PyIceberg exception types.
"""

from __future__ import annotations

import warnings
from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import sqlglot
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import NullOrder
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    IdentityTransform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampType,
    TimestamptzType,
)

from cambrian.errors import DispatchError, UnsupportedStatementError
from cambrian.iceberg.affected import TableIdent
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.dispatch import dispatch


def _parse(sql: str):
    tree = sqlglot.parse(sql, dialect=CambrianSpark)
    return [s for s in tree if s is not None]


def _single(sql: str):
    [stmt] = _parse(sql)
    return stmt


def _make_catalog() -> MagicMock:
    return MagicMock(
        spec_set=[
            "create_namespace",
            "drop_namespace",
            "update_namespace_properties",
            "create_table",
            "create_table_transaction",
            "drop_table",
            "rename_table",
            "load_table",
        ]
    )


def _make_table() -> MagicMock:
    table = MagicMock()
    # Default schema and transaction context managers
    schema = MagicMock(spec=Schema)
    schema.as_arrow.return_value = pa.schema(
        [pa.field("id", pa.int64(), nullable=False), pa.field("name", pa.string(), nullable=True)]
    )
    schema.fields = [
        MagicMock(name="id", required=True),
        MagicMock(name="name", required=False),
    ]
    table.schema.return_value = schema
    table.format_version = 2
    table.spec.return_value.fields = []
    return table


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


def test_create_namespace_calls_catalog() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE NAMESPACE foo"))
    catalog.create_namespace.assert_called_once_with("foo")


def test_create_namespace_with_dots() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE NAMESPACE foo.bar"))
    catalog.create_namespace.assert_called_once_with("foo.bar")


def test_create_namespace_idempotent_already_exists() -> None:
    catalog = _make_catalog()
    catalog.create_namespace.side_effect = NamespaceAlreadyExistsError("dup")
    result = dispatch(catalog, _single("CREATE NAMESPACE foo"))
    assert "already exists" in result.notes


def test_drop_namespace_calls_catalog() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("DROP NAMESPACE foo"))
    catalog.drop_namespace.assert_called_once_with("foo")


def test_drop_namespace_if_exists_swallows_missing() -> None:
    catalog = _make_catalog()
    catalog.drop_namespace.side_effect = NoSuchNamespaceError("nope")
    result = dispatch(catalog, _single("DROP NAMESPACE IF EXISTS foo"))
    assert "already absent" in result.notes


def test_drop_namespace_without_if_exists_tolerates_missing() -> None:
    catalog = _make_catalog()
    catalog.drop_namespace.side_effect = NoSuchNamespaceError("nope")
    result = dispatch(catalog, _single("DROP NAMESPACE foo"))
    assert "already absent" in result.notes


# ---------------------------------------------------------------------------
# CREATE TABLE
# ---------------------------------------------------------------------------


def test_create_table_simple() -> None:
    catalog = _make_catalog()
    dispatch(
        catalog,
        _single("CREATE TABLE foo.t (id BIGINT, name STRING) USING iceberg"),
    )
    catalog.create_table.assert_called_once()
    args = catalog.create_table.call_args.kwargs
    assert args["identifier"] == ("foo", "t")
    schema = args["schema"]
    assert isinstance(schema, Schema)
    assert [f.name for f in schema.fields] == ["id", "name"]
    assert isinstance(schema.fields[0].field_type, LongType)
    assert isinstance(schema.fields[1].field_type, StringType)


def test_create_table_type_mapping() -> None:
    catalog = _make_catalog()
    sql = (
        "CREATE TABLE foo.t ("
        "a INT, b BIGINT, c FLOAT, d DOUBLE, e STRING, "
        "f BOOLEAN, g DATE, h TIMESTAMP, i TIMESTAMPTZ, j DECIMAL(10, 2)"
        ") USING iceberg"
    )
    dispatch(catalog, _single(sql))
    schema = catalog.create_table.call_args.kwargs["schema"]
    types = [type(f.field_type) for f in schema.fields]
    assert types[0] is IntegerType
    assert types[1] is LongType
    assert types[3] is DoubleType
    assert types[4] is StringType
    assert types[5] is BooleanType
    assert types[6] is DateType
    assert types[8] is TimestamptzType
    decimal = schema.fields[9].field_type
    assert isinstance(decimal, DecimalType)
    assert decimal.precision == 10
    assert decimal.scale == 2


def test_create_table_already_exists_is_idempotent() -> None:
    """Without IF NOT EXISTS, re-applying CREATE TABLE is still safe."""
    catalog = _make_catalog()
    catalog.create_table.side_effect = TableAlreadyExistsError("dup")
    result = dispatch(catalog, _single("CREATE TABLE foo.t (id INT) USING iceberg"))
    assert "already exists" in result.notes
    assert result.affected_tables == [TableIdent(namespace="foo", name="t")]


def test_create_table_unsupported_type() -> None:
    catalog = _make_catalog()
    # STRUCT/ARRAY/MAP are now in scope; INTERVAL is a primitive cambrian
    # doesn't map, so it stays unsupported.
    with pytest.raises(UnsupportedStatementError):
        dispatch(
            catalog,
            _single("CREATE TABLE foo.t (a INTERVAL) USING iceberg"),
        )


# ---------------------------------------------------------------------------
# DROP TABLE
# ---------------------------------------------------------------------------


def test_drop_table_calls_catalog() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("DROP TABLE foo.t"))
    catalog.drop_table.assert_called_once_with(("foo", "t"))


def test_drop_table_if_exists_swallows_missing() -> None:
    catalog = _make_catalog()
    catalog.drop_table.side_effect = NoSuchTableError("nope")
    result = dispatch(catalog, _single("DROP TABLE IF EXISTS foo.t"))
    assert "already absent" in result.notes


def test_drop_table_idempotent_without_if_exists() -> None:
    catalog = _make_catalog()
    catalog.drop_table.side_effect = NoSuchTableError("nope")
    result = dispatch(catalog, _single("DROP TABLE foo.t"))
    assert "already absent" in result.notes


# ---------------------------------------------------------------------------
# ALTER TABLE - column ops
# ---------------------------------------------------------------------------


def test_alter_add_column() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.side_effect = ValueError("Could not find field")
    dispatch(catalog, _single("ALTER TABLE foo.t ADD COLUMN c INT"))
    table.update_schema.assert_called()
    us = table.update_schema.return_value.__enter__.return_value
    us.add_column.assert_called_once()
    args = us.add_column.call_args
    assert args.args[0] == ("c",)
    assert isinstance(args.args[1], IntegerType)


def test_alter_add_columns_plural_splits_into_n_commits() -> None:
    """ADD COLUMNS (a, b, c) → 3 sequential update_schema commits."""
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.side_effect = ValueError("Could not find field")
    dispatch(catalog, _single("ALTER TABLE foo.t ADD COLUMNS (a INT, b STRING, c BIGINT)"))
    # update_schema invoked once per column → 3 enters of the cm.
    assert table.update_schema.call_count == 3
    # The catalog is re-loaded between additions (once before the alter and
    # once after each add) — 4 loads total for the 3-column case.
    assert catalog.load_table.call_count >= 4


def test_alter_drop_column_singular() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t DROP COLUMN c"))
    us = table.update_schema.return_value.__enter__.return_value
    us.delete_column.assert_called_once_with("c")


def test_alter_drop_column_idempotent_when_absent() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.side_effect = ValueError(
        "Could not find field with name c, case_sensitive=True"
    )
    result = dispatch(catalog, _single("ALTER TABLE foo.t DROP COLUMN c"))
    assert "already absent" in result.notes
    table.update_schema.assert_not_called()


def test_alter_rename_column_idempotent_when_already_renamed() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table

    def find_field(name: str) -> object:
        if name == "a":
            raise ValueError("Could not find field with name a, case_sensitive=True")
        return MagicMock()

    table.schema.return_value.find_field.side_effect = find_field
    result = dispatch(catalog, _single("ALTER TABLE foo.t RENAME COLUMN a TO b"))
    assert "already absent" in result.notes
    table.update_schema.assert_not_called()


def test_alter_rename_column() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t RENAME COLUMN a TO b"))
    us = table.update_schema.return_value.__enter__.return_value
    us.rename_column.assert_called_once_with("a", "b")


def test_alter_column_type() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN a TYPE BIGINT"))
    us = table.update_schema.return_value.__enter__.return_value
    us.update_column.assert_called_once()
    kwargs = us.update_column.call_args.kwargs
    assert kwargs["field_type"].__class__ is LongType
    assert us.update_column.call_args.args[0] == "a"


# ---------------------------------------------------------------------------
# TBLPROPERTIES
# ---------------------------------------------------------------------------


def test_set_tblproperties() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single('ALTER TABLE foo.t SET TBLPROPERTIES ("k" = "v")'))
    txn = table.transaction.return_value.__enter__.return_value
    txn.set_properties.assert_called_once()
    kwargs = txn.set_properties.call_args.kwargs
    assert kwargs["properties"] == {"k": "v"}


def test_unset_tblproperties() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.properties = {"k1": "a", "k2": "b"}
    dispatch(catalog, _single('ALTER TABLE foo.t UNSET TBLPROPERTIES ("k1", "k2")'))
    txn = table.transaction.return_value.__enter__.return_value
    txn.remove_properties.assert_called_once_with("k1", "k2")


def test_unset_tblproperties_skips_absent_keys() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.properties = {"k2": "b"}
    dispatch(catalog, _single('ALTER TABLE foo.t UNSET TBLPROPERTIES ("k1", "k2")'))
    txn = table.transaction.return_value.__enter__.return_value
    txn.remove_properties.assert_called_once_with("k2")


def test_unset_tblproperties_all_absent_is_noop() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.properties = {}
    result = dispatch(catalog, _single('ALTER TABLE foo.t UNSET TBLPROPERTIES ("k1")'))
    assert "already absent" in result.notes
    table.transaction.assert_not_called()


# ---------------------------------------------------------------------------
# Partition fields
# ---------------------------------------------------------------------------


def test_add_partition_field_bare_column() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ADD PARTITION FIELD x"))
    us = table.update_spec.return_value.__enter__.return_value
    us.add_field.assert_called_once()
    args = us.add_field.call_args.args
    assert args[0] == "x"
    assert isinstance(args[1], IdentityTransform)
    assert args[2] is None  # no alias


def test_add_partition_field_bucket() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(
        catalog,
        _single("ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, x)"),
    )
    us = table.update_spec.return_value.__enter__.return_value
    us.add_field.assert_called_once()
    args = us.add_field.call_args.args
    assert args[0] == "x"
    assert isinstance(args[1], BucketTransform)
    assert args[1].num_buckets == 16


def test_add_partition_field_with_alias() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(
        catalog,
        _single("ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, x) AS xb"),
    )
    us = table.update_spec.return_value.__enter__.return_value
    args = us.add_field.call_args.args
    assert args[2] == "xb"


def test_add_partition_field_year_transform() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ADD PARTITION FIELD year(d)"))
    us = table.update_spec.return_value.__enter__.return_value
    args = us.add_field.call_args.args
    assert isinstance(args[1], YearTransform)
    assert args[0] == "d"


def test_add_partition_field_idempotent_duplicate() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.update_spec.return_value.__enter__.return_value.add_field.side_effect = ValueError(
        "Duplicate partition field for: x"
    )
    result = dispatch(catalog, _single("ALTER TABLE foo.t ADD PARTITION FIELD x"))
    assert "already present" in result.notes


def test_drop_partition_field() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t DROP PARTITION FIELD x"))
    us = table.update_spec.return_value.__enter__.return_value
    us.remove_field.assert_called_once_with("x")


def test_replace_partition_field() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(
        catalog,
        _single("ALTER TABLE foo.t REPLACE PARTITION FIELD x WITH bucket(8, y)"),
    )
    us = table.update_spec.return_value.__enter__.return_value
    # Drop the old, add the new.
    us.remove_field.assert_called_with("x")
    us.add_field.assert_called()
    args = us.add_field.call_args.args
    assert args[0] == "y"
    assert isinstance(args[1], BucketTransform)
    assert args[1].num_buckets == 8


# ---------------------------------------------------------------------------
# WRITE ORDERED BY
# ---------------------------------------------------------------------------


def test_write_ordered_by_asc_and_desc() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t WRITE ORDERED BY (a ASC, b DESC)"))
    txn = table.transaction.return_value.__enter__.return_value
    uso = txn.update_sort_order.return_value.__enter__.return_value
    # Spark's default null order: ASC -> NULLS FIRST, DESC -> NULLS LAST.
    assert uso.asc.call_args.args == ("a", IdentityTransform(), NullOrder.NULLS_FIRST)
    assert uso.desc.call_args.args == ("b", IdentityTransform(), NullOrder.NULLS_LAST)
    assert txn.set_properties.call_args.kwargs["properties"] == {"write.distribution-mode": "range"}


def test_write_ordered_by_default_asc() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t WRITE ORDERED BY (a)"))
    txn = table.transaction.return_value.__enter__.return_value
    uso = txn.update_sort_order.return_value.__enter__.return_value
    uso.asc.assert_called_once()


# ---------------------------------------------------------------------------
# INSERT VALUES
# ---------------------------------------------------------------------------


def test_insert_values_calls_append() -> None:
    catalog = _make_catalog()
    table = _make_table()
    # Build a real Schema with two non-required Iceberg fields so the
    # dispatch helper can mirror nullability correctly into PyArrow.
    from pyiceberg.types import IntegerType, NestedField

    real_schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    table.schema.return_value = real_schema
    catalog.load_table.return_value = table
    dispatch(catalog, _single("INSERT INTO foo.t VALUES (1, 'alice'), (2, 'bob')"))
    table.append.assert_called_once()
    appended = table.append.call_args.args[0]
    assert isinstance(appended, pa.Table)
    assert appended.num_rows == 2
    assert appended.column_names == ["id", "name"]


def test_insert_select_is_unsupported() -> None:
    catalog = _make_catalog()
    with pytest.raises(UnsupportedStatementError):
        dispatch(catalog, _single("INSERT INTO foo.t SELECT * FROM bar"))


def test_insert_values_arity_mismatch() -> None:
    catalog = _make_catalog()
    table = _make_table()
    from pyiceberg.types import IntegerType, NestedField

    real_schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    table.schema.return_value = real_schema
    catalog.load_table.return_value = table
    with pytest.raises(DispatchError):
        dispatch(catalog, _single("INSERT INTO foo.t VALUES (1)"))


def test_insert_values_with_null() -> None:
    catalog = _make_catalog()
    table = _make_table()
    from pyiceberg.types import IntegerType, NestedField

    real_schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    table.schema.return_value = real_schema
    catalog.load_table.return_value = table
    dispatch(catalog, _single("INSERT INTO foo.t VALUES (1, NULL)"))
    appended = table.append.call_args.args[0]
    assert appended.column("name").to_pylist() == [None]


# ---------------------------------------------------------------------------
# Misc
# ---------------------------------------------------------------------------


def test_unknown_statement_raises_unsupported() -> None:
    catalog = _make_catalog()
    # MERGE INTO isn't in the v1 list; sqlglot parses it as a Merge.
    merge_sql = (
        "MERGE INTO foo.t USING bar.s ON foo.t.id = bar.s.id WHEN MATCHED THEN UPDATE SET k = 1"
    )
    with pytest.raises(UnsupportedStatementError):
        dispatch(catalog, _single(merge_sql))


def test_transform_day_parses() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ADD PARTITION FIELD day(ts)"))
    us = table.update_spec.return_value.__enter__.return_value
    args = us.add_field.call_args.args
    assert isinstance(args[1], DayTransform)


# ---------------------------------------------------------------------------
# truncate partition transform (sqlglot Trunc node shape)
# ---------------------------------------------------------------------------


def test_add_partition_field_truncate() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ADD PARTITION FIELD truncate(4, data)"))
    us = table.update_spec.return_value.__enter__.return_value
    args = us.add_field.call_args.args
    assert args[0] == "data"
    assert isinstance(args[1], TruncateTransform)
    assert args[1].width == 4


def test_replace_partition_field_with_truncate() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(
        catalog,
        _single("ALTER TABLE foo.t REPLACE PARTITION FIELD x WITH truncate(8, data)"),
    )
    us = table.update_spec.return_value.__enter__.return_value
    args = us.add_field.call_args.args
    assert args[0] == "data"
    assert isinstance(args[1], TruncateTransform)
    assert args[1].width == 8


# ---------------------------------------------------------------------------
# Nested CREATE TABLE types
# ---------------------------------------------------------------------------


def test_create_table_struct() -> None:
    catalog = _make_catalog()
    dispatch(
        catalog,
        _single("CREATE TABLE foo.t (point STRUCT<x: DOUBLE, y: DOUBLE>) USING iceberg"),
    )
    schema = catalog.create_table.call_args.kwargs["schema"]
    assert isinstance(schema.fields[0].field_type, StructType)
    assert [f.name for f in schema.fields[0].field_type.fields] == ["x", "y"]


def test_create_table_array() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE TABLE foo.t (tags ARRAY<STRING>) USING iceberg"))
    schema = catalog.create_table.call_args.kwargs["schema"]
    assert isinstance(schema.fields[0].field_type, ListType)


def test_create_table_map() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE TABLE foo.t (attrs MAP<STRING, INT>) USING iceberg"))
    schema = catalog.create_table.call_args.kwargs["schema"]
    assert isinstance(schema.fields[0].field_type, MapType)


def test_create_table_nested_combo_unique_ids() -> None:
    catalog = _make_catalog()
    dispatch(
        catalog,
        _single(
            "CREATE TABLE foo.t ("
            "points ARRAY<STRUCT<x: DOUBLE, y: DOUBLE>>, lookup MAP<STRING, ARRAY<INT>>"
            ") USING iceberg"
        ),
    )
    schema = catalog.create_table.call_args.kwargs["schema"]
    # Every field id across the whole (nested) schema must be unique.
    ids = [f.field_id for f in schema._lazy_id_to_field.values()]
    assert len(ids) == len(set(ids))


def test_create_table_timestamp_ntz_maps_to_no_tz() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE TABLE foo.t (i TIMESTAMP_NTZ) USING iceberg"))
    schema = catalog.create_table.call_args.kwargs["schema"]
    assert type(schema.fields[0].field_type) is TimestampType


def test_create_or_replace_table_rejected() -> None:
    catalog = _make_catalog()
    with pytest.raises(UnsupportedStatementError):
        dispatch(catalog, _single("CREATE OR REPLACE TABLE foo.t (id INT) USING iceberg"))


def test_ctas_rejected() -> None:
    catalog = _make_catalog()
    with pytest.raises(UnsupportedStatementError):
        dispatch(catalog, _single("CREATE TABLE foo.t USING iceberg AS SELECT * FROM bar"))


# ---------------------------------------------------------------------------
# RENAME TABLE
# ---------------------------------------------------------------------------


def test_rename_table_calls_catalog() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t RENAME TO foo.t2"))
    catalog.rename_table.assert_called_once_with(("foo", "t"), ("foo", "t2"))


def test_rename_table_idempotent_when_source_absent() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    catalog.rename_table.side_effect = NoSuchTableError("gone")
    result = dispatch(catalog, _single("ALTER TABLE foo.t RENAME TO foo.t2"))
    assert "idempotent" in result.notes


# ---------------------------------------------------------------------------
# ALTER COLUMN comment / not null / reposition
# ---------------------------------------------------------------------------


def test_alter_column_comment_only() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN c COMMENT 'docs'"))
    us = table.update_schema.return_value.__enter__.return_value
    assert us.update_column.call_args.kwargs["doc"] == "docs"


def test_alter_column_set_not_null_on_optional_is_refused() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.return_value = MagicMock(required=False)
    with pytest.raises(DispatchError, match="NOT NULL"):
        dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN name SET NOT NULL"))
    table.update_schema.assert_not_called()


def test_alter_column_set_not_null_on_required_is_noop() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.return_value = MagicMock(required=True)
    result = dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN id SET NOT NULL"))
    assert "already required" in result.notes
    table.update_schema.assert_not_called()


def test_alter_column_drop_not_null() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN id DROP NOT NULL"))
    us = table.update_schema.return_value.__enter__.return_value
    assert us.update_column.call_args.kwargs["required"] is False


def test_alter_column_first() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN c FIRST"))
    us = table.update_schema.return_value.__enter__.return_value
    us.move_first.assert_called_once_with("c")


def test_alter_column_after() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN c AFTER name"))
    us = table.update_schema.return_value.__enter__.return_value
    us.move_after.assert_called_once_with("c", "name")


def test_add_column_dotted_path_uses_tuple() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.side_effect = ValueError("Could not find field")
    dispatch(catalog, _single("ALTER TABLE foo.t ADD COLUMN point.z DOUBLE"))
    us = table.update_schema.return_value.__enter__.return_value
    assert us.add_column.call_args.args[0] == ("point", "z")


# ---------------------------------------------------------------------------
# Identifier fields
# ---------------------------------------------------------------------------


def test_set_identifier_fields() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t SET IDENTIFIER FIELDS id, name"))
    us = table.update_schema.return_value.__enter__.return_value
    us.set_identifier_fields.assert_called_once_with("id", "name")


def test_drop_identifier_fields_resets_remaining() -> None:
    catalog = _make_catalog()
    table = _make_table()
    real_schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
        identifier_field_ids=[1, 2],
    )
    table.schema.return_value = real_schema
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t DROP IDENTIFIER FIELDS name"))
    us = table.update_schema.return_value.__enter__.return_value
    us.set_identifier_fields.assert_called_once_with("id")


# ---------------------------------------------------------------------------
# WRITE distribution family
# ---------------------------------------------------------------------------


def test_write_ordered_by_sets_range_mode() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t WRITE ORDERED BY category, id"))
    txn = table.transaction.return_value.__enter__.return_value
    assert txn.set_properties.call_args.kwargs["properties"] == {"write.distribution-mode": "range"}


def test_write_locally_ordered_leaves_distribution_mode() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t WRITE LOCALLY ORDERED BY category, id"))
    txn = table.transaction.return_value.__enter__.return_value
    txn.set_properties.assert_not_called()
    uso = txn.update_sort_order.return_value.__enter__.return_value
    assert uso.asc.call_count == 2


def test_write_distributed_sets_hash_mode() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t WRITE DISTRIBUTED BY PARTITION"))
    txn = table.transaction.return_value.__enter__.return_value
    assert txn.set_properties.call_args.kwargs["properties"] == {"write.distribution-mode": "hash"}


def test_write_unordered_clears_sort_order() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("ALTER TABLE foo.t WRITE UNORDERED"))
    txn = table.transaction.return_value.__enter__.return_value
    txn.update_sort_order.assert_called_once()
    assert txn.set_properties.call_args.kwargs["properties"] == {"write.distribution-mode": "none"}


# ---------------------------------------------------------------------------
# DELETE FROM
# ---------------------------------------------------------------------------


def test_delete_where_calls_delete_with_filter() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("DELETE FROM foo.t WHERE id = 1"))
    table.delete.assert_called_once()
    assert "id = 1" in table.delete.call_args.kwargs["delete_filter"]


def test_delete_no_where_deletes_all() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    dispatch(catalog, _single("DELETE FROM foo.t"))
    table.delete.assert_called_once_with()


def test_delete_no_match_is_idempotent_note_and_swallows_warning() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.delete.side_effect = lambda *a, **k: warnings.warn(
        "Delete operation did not match any records", UserWarning, stacklevel=2
    )
    # Promote any leaked warning to an error: the handler must swallow this one.
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        result = dispatch(catalog, _single("DELETE FROM foo.t WHERE id = 1"))
    assert "matched no rows (idempotent)" in result.notes


def test_delete_reemits_unrelated_warning() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.delete.side_effect = lambda *a, **k: warnings.warn(
        "something else entirely", UserWarning, stacklevel=2
    )
    with pytest.warns(UserWarning, match="something else entirely"):
        result = dispatch(catalog, _single("DELETE FROM foo.t WHERE id = 1"))
    assert "matched no rows" not in result.notes


# ---------------------------------------------------------------------------
# Namespace properties
# ---------------------------------------------------------------------------


def test_create_namespace_with_properties() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE NAMESPACE foo WITH PROPERTIES ('owner' = 'eng')"))
    catalog.create_namespace.assert_called_once_with("foo", properties={"owner": "eng"})


def test_alter_namespace_set_properties() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("ALTER NAMESPACE foo SET PROPERTIES ('owner' = 'data')"))
    catalog.update_namespace_properties.assert_called_once_with("foo", updates={"owner": "data"})


def test_alter_column_type_already_wider_is_noop() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.return_value = MagicMock(
        field_type=DecimalType(precision=14, scale=2)
    )
    result = dispatch(catalog, _single("ALTER TABLE foo.t ALTER COLUMN d TYPE DECIMAL(12, 2)"))
    assert "already wider" in result.notes
    table.update_schema.assert_not_called()


def test_alter_add_column_carries_comment() -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table
    table.schema.return_value.find_field.side_effect = ValueError("Could not find field")
    dispatch(catalog, _single("ALTER TABLE foo.t ADD COLUMN c INT COMMENT 'unit: m/s'"))
    us = table.update_schema.return_value.__enter__.return_value
    assert us.add_column.call_args.kwargs["doc"] == "unit: m/s"


def test_create_table_carries_column_comments() -> None:
    catalog = _make_catalog()
    sql = "CREATE TABLE foo.t (a INT COMMENT 'the a', b STRING NOT NULL) USING iceberg"
    dispatch(catalog, _single(sql))
    schema = catalog.create_table.call_args.kwargs["schema"]
    assert [f.doc for f in schema.fields] == ["the a", None]
    assert [f.required for f in schema.fields] == [False, True]


def test_create_table_partition_props_comment_location() -> None:
    catalog = _make_catalog()
    sql = (
        "CREATE TABLE IF NOT EXISTS foo.t (id BIGINT, ts TIMESTAMP, cat STRING) USING iceberg "
        "PARTITIONED BY (bucket(16, id), days(ts), cat, truncate(4, cat)) "
        "COMMENT 'tbl doc' LOCATION 's3://b/p' "
        "TBLPROPERTIES ('write.metadata.previous-versions-max'='1000')"
    )
    dispatch(catalog, _single(sql))
    kwargs = catalog.create_table.call_args.kwargs
    assert kwargs["location"] == "s3://b/p"
    assert kwargs["properties"] == {
        "comment": "tbl doc",
        "write.metadata.previous-versions-max": "1000",
    }
    spec = kwargs["partition_spec"]
    # Spark's CREATE-time names (PartitionSpec.Builder), not ADD PARTITION FIELD's.
    assert [(f.source_id, str(f.transform), f.name) for f in spec.fields] == [
        (1, "bucket[16]", "id_bucket"),
        (2, "day", "ts_day"),
        (3, "identity", "cat"),
        (3, "truncate[4]", "cat_trunc"),
    ]


def test_create_table_unpartitioned_has_empty_spec() -> None:
    catalog = _make_catalog()
    dispatch(catalog, _single("CREATE TABLE foo.t (id BIGINT) USING iceberg"))
    assert catalog.create_table.call_args.kwargs["partition_spec"].is_unpartitioned()


def test_create_table_unknown_clause_is_refused() -> None:
    catalog = _make_catalog()
    sql = "CREATE TABLE foo.t (id BIGINT) USING iceberg CLUSTERED BY (id) INTO 4 BUCKETS"
    with pytest.raises(UnsupportedStatementError):
        dispatch(catalog, _single(sql))
    catalog.create_table.assert_not_called()


def test_create_table_duplicate_spark_partition_names_refused() -> None:
    catalog = _make_catalog()
    sql = "CREATE TABLE foo.t (c INT) USING iceberg PARTITIONED BY (bucket(4, c), bucket(16, c))"
    with pytest.raises(DispatchError, match="c_bucket"):
        dispatch(catalog, _single(sql))
    catalog.create_table.assert_not_called()
