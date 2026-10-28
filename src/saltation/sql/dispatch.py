"""Translate parsed SQL statements into PyIceberg API calls.

Dispatch is a single ``match``/``isinstance`` over the parsed AST node and
delegates each construct to a private helper. Each helper takes the live
:class:`Catalog` and the statement node, performs the right PyIceberg call,
and returns a :class:`DispatchResult` carrying the touched tables and any
notes.

**Idempotent contract.** Every handler must tolerate the "already at the
desired state" condition for ADD-likes (``NamespaceAlreadyExistsError``,
``TableAlreadyExistsError``, ``Duplicate partition field`` from
``UpdateSpec.add_field``) and the "already absent" condition for DROP-likes
when ``IF EXISTS`` is given. **No rollback on failure.** When a handler
raises, the runner emits a partial-apply event and propagates — restoring
state is M6's job.

**Multi-column ALTERs.** ``ALTER TABLE t ADD COLUMNS (a, b, c)`` is split
into N sequential ``update_schema`` commits because the REST catalog spec
requires one schema change per commit. Each commit is a fresh transaction.
"""

from __future__ import annotations

import warnings
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pydantic
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    ResolveError,
    TableAlreadyExistsError,
)
from pyiceberg.expressions.literals import literal
from pyiceberg.partitioning import PARTITION_FIELD_ID_START, PartitionField, PartitionSpec
from pyiceberg.schema import Schema, promote
from pyiceberg.table.sorting import NullOrder
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    VoidTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    GeometryType,
    IcebergType,
    IntegerType,
    ListType,
    LongType,
    MapType,
    NestedField,
    StringType,
    StructType,
    TimestampNanoType,
    TimestampType,
    TimestamptzType,
    TimeType,
)
from sqlglot import expressions as exp

from saltation.errors import DispatchError, UnsupportedStatementError
from saltation.iceberg.affected import TableIdent, affected_tables
from saltation.iceberg.txn import replace_schema, set_location, upgrade_format_version
from saltation.sql.ast import (
    AddPartitionField,
    AlterColumnPosition,
    AlterNamespaceProperties,
    DropIdentifierFields,
    DropPartitionField,
    ReplacePartitionField,
    SetIdentifierFields,
    UnsetTblProperties,
    WriteDistribution,
    WriteOrderedBy,
)

# Iceberg table property that controls write fan-out; the WRITE distribution
# family sets it. https://iceberg.apache.org/docs/latest/spark-ddl/ (Writing
# Distribution Modes) — ORDERED BY → range, LOCALLY ORDERED BY → none,
# DISTRIBUTED BY PARTITION → hash.
_DISTRIBUTION_MODE_KEY = "write.distribution-mode"
_FORMAT_VERSION_KEY = "format-version"

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table
    from pyiceberg.table.update.schema import UpdateSchema

__all__ = ["DispatchResult", "dispatch"]


@dataclass
class DispatchResult:
    """The outcome of dispatching a single statement.

    ``affected_tables`` is the (possibly-empty) list of tables the statement
    targeted. ``notes`` is free-form text the runner copies into the event
    log for human consumption.
    """

    affected_tables: list[TableIdent] = field(default_factory=list)
    notes: str = ""


def dispatch(
    catalog: Catalog, statement: exp.Expression, *, view_dialects: Sequence[str] = ("spark",)
) -> DispatchResult:
    """Translate *statement* into PyIceberg calls and execute them.

    See module docstring for the idempotent contract.

    Raises:
        UnsupportedStatementError: The construct isn't in saltation's v1
            supported list (e.g. ``INSERT ... SELECT``, ``MERGE``, ``DELETE``).
        DispatchError: The construct is recognised but saltation can't run
            it (schema coercion failure, etc.).
    """
    # CREATE NAMESPACE / DROP NAMESPACE — kind=NAMESPACE on the Create/Drop.
    if isinstance(statement, exp.Create) and _kind(statement) in {
        "NAMESPACE",
        "SCHEMA",
        "DATABASE",
    }:
        return _dispatch_create_namespace(catalog, statement)
    if isinstance(statement, exp.Drop) and _kind(statement) in {
        "NAMESPACE",
        "SCHEMA",
        "DATABASE",
    }:
        return _dispatch_drop_namespace(catalog, statement)

    if isinstance(statement, exp.Create | exp.Drop | exp.Alter) and _kind(statement) == "VIEW":
        # Imported here: the views module reuses this module's helpers.
        from saltation.sql.views import dispatch_view

        return dispatch_view(catalog, statement, view_dialects)

    if isinstance(statement, exp.Create):
        return _dispatch_create_table(catalog, statement)
    if isinstance(statement, exp.Drop):
        return _dispatch_drop_table(catalog, statement)

    if isinstance(statement, AlterNamespaceProperties):
        return _dispatch_alter_namespace(catalog, statement)

    if isinstance(statement, exp.Alter):
        return _dispatch_alter(catalog, statement)

    if isinstance(statement, exp.Insert):
        return _dispatch_insert(catalog, statement)

    if isinstance(statement, exp.Delete):
        return _dispatch_delete(catalog, statement)

    # The parser sometimes leaves a bare ``Semicolon`` at the end of a script
    # whose final statement is just a trailing comment. Treat as no-op so the
    # runner doesn't have to special-case it.
    if isinstance(statement, exp.Semicolon):
        return DispatchResult(notes="(no-op: bare semicolon)")

    raise UnsupportedStatementError(
        statement_sql=statement.sql(),
        reason=(
            f"unrecognised statement type {type(statement).__name__}; "
            "see plan §2.2 for the v1 supported list"
        ),
    )


def _kind(node: exp.Expression) -> str:
    return (node.args.get("kind") or "").upper()


# ---------------------------------------------------------------------------
# Namespace
# ---------------------------------------------------------------------------


def _namespace_from_table_node(node: exp.Expression) -> str:
    """Render a namespace identifier from an ``exp.Table`` node.

    ``CREATE NAMESPACE a.b`` parses to ``Table(this=b, db=a)`` so we render
    the multi-part dotted form by walking ``catalog`` → ``db`` → ``this``.
    """
    if not isinstance(node, exp.Table):
        return str(node)
    parts: list[str] = []
    cat = node.args.get("catalog")
    db = node.args.get("db")
    if cat is not None:
        parts.append(cat.name)
    if db is not None:
        parts.append(db.name)
    parts.append(node.name)
    return ".".join(parts)


def _dispatch_create_namespace(catalog: Catalog, stmt: exp.Create) -> DispatchResult:
    inner = stmt.args.get("this")
    if inner is None:
        raise DispatchError(f"CREATE NAMESPACE missing namespace identifier: {stmt.sql()}")
    namespace = _namespace_from_table_node(inner)
    props = _properties_dict(stmt.args.get("properties"))
    try:
        # Keep the no-properties call positional so it matches the common
        # ``create_namespace(ns)`` shape (and existing tests/back-compat).
        if props:
            catalog.create_namespace(namespace, properties=props)
        else:
            catalog.create_namespace(namespace)
        notes = f"created namespace {namespace}"
    except NamespaceAlreadyExistsError:
        # IF NOT EXISTS is implicit under the idempotent contract — the
        # absence of the explicit clause doesn't change semantics for us.
        notes = f"namespace {namespace} already exists"
    return DispatchResult(notes=notes)


def _dispatch_alter_namespace(catalog: Catalog, stmt: AlterNamespaceProperties) -> DispatchResult:
    inner = stmt.args.get("this")
    namespace = _namespace_from_table_node(inner) if inner is not None else ""
    updates = _properties_dict_from_list(stmt.args.get("expressions"))
    try:
        catalog.update_namespace_properties(namespace, updates=updates)
    except pydantic.ValidationError:
        # PyIceberg requires ``missing`` in the response, but the REST spec
        # makes it optional and Lakekeeper omits it, so a successful update
        # fails to deserialize. Trust the catalog's state instead (#20).
        current = catalog.load_namespace_properties(namespace)
        if any(current.get(k) != v for k, v in updates.items()):
            raise
    return DispatchResult(notes=f"set namespace {namespace} properties {sorted(updates)}")


def _properties_dict(node: exp.Expression | None) -> dict[str, str]:
    """Extract a ``{key: value}`` dict from an ``exp.Properties`` node."""
    if not isinstance(node, exp.Properties):
        return {}
    return _properties_dict_from_list(node.args.get("expressions"))


def _properties_dict_from_list(props: list[exp.Expression] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for prop in props or []:
        if isinstance(prop, exp.Property):
            key = _scalar(prop.args.get("this"))
            value = _scalar(prop.args.get("value"))
            if key is not None:
                out[key] = "" if value is None else value
    return out


def _dispatch_drop_namespace(catalog: Catalog, stmt: exp.Drop) -> DispatchResult:
    inner = stmt.args.get("this")
    if inner is None:
        raise DispatchError(f"DROP NAMESPACE missing namespace identifier: {stmt.sql()}")
    namespace = _namespace_from_table_node(inner)
    if_exists = bool(stmt.args.get("exists"))
    try:
        catalog.drop_namespace(namespace)
        notes = f"dropped namespace {namespace}"
    except NoSuchNamespaceError:
        # Tolerated without IF EXISTS too, like DROP TABLE: a re-applied
        # evolution meets the namespace it already dropped.
        suffix = "IF EXISTS" if if_exists else "idempotent re-apply"
        notes = f"namespace {namespace} already absent ({suffix})"
    return DispatchResult(notes=notes)


# ---------------------------------------------------------------------------
# CREATE/DROP TABLE
# ---------------------------------------------------------------------------


def _table_identifier(table_node: exp.Table) -> tuple[str, ...]:
    """Convert an ``exp.Table`` into a tuple identifier for PyIceberg."""
    parts: list[str] = []
    cat = table_node.args.get("catalog")
    db = table_node.args.get("db")
    if cat is not None:
        parts.append(cat.name)
    if db is not None:
        parts.append(db.name)
    parts.append(table_node.name)
    return tuple(parts)


def _dispatch_create_table(catalog: Catalog, stmt: exp.Create) -> DispatchResult:
    # CREATE OR REPLACE TABLE is a destructive replace — out of scope and not
    # idempotent-safe. Refuse explicitly rather than silently doing a plain
    # create (which drops the REPLACE semantics the user expects).
    if bool(stmt.args.get("replace")):
        raise UnsupportedStatementError(
            statement_sql=stmt.sql(),
            reason=(
                "CREATE OR REPLACE TABLE is out of scope: it implies a destructive replace "
                "that saltation won't perform. Use CREATE TABLE IF NOT EXISTS plus explicit "
                "ALTERs for an idempotent evolution"
            ),
        )
    inner = stmt.args.get("this")
    # CTAS (``CREATE TABLE t AS SELECT ...``) has a Table (not Schema) under
    # ``this`` and a Select under ``expression``. It needs a query engine,
    # which is out of scope — refuse as unsupported (not a generic dispatch
    # error).
    if isinstance(stmt.args.get("expression"), exp.Query) and not isinstance(inner, exp.Schema):
        raise UnsupportedStatementError(
            statement_sql=stmt.sql(),
            reason=(
                "CREATE TABLE ... AS SELECT (CTAS) is out of scope: saltation applies DDL only "
                "and has no query engine to evaluate the SELECT"
            ),
        )
    like = _like_source(stmt)
    if like is not None and isinstance(inner, exp.Table):
        return _create_table_like(catalog, stmt, inner, like)
    if not isinstance(inner, exp.Schema):
        raise DispatchError(
            "CREATE TABLE without a column list isn't supported (saltation needs the schema "
            "to translate to PyIceberg). Use ``CREATE TABLE t (cols...) USING iceberg``."
        )
    table_node = inner.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"CREATE TABLE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    column_defs = inner.args.get("expressions") or []
    schema = _build_iceberg_schema(column_defs)
    if_not_exists = bool(stmt.args.get("exists"))
    options = _create_table_options(stmt)
    version = int(options.properties.get(_FORMAT_VERSION_KEY, "2"))
    if any(f.write_default is not None for f in schema.fields):
        _require_v3(version, "a column DEFAULT")
    if any(_is_v3_type(f.field_type) for f in schema.fields):
        _require_v3(version, "geometry/geography columns")
    names = [_create_partition_name(src, tr) for src, tr in options.partition_fields]
    if dupes := sorted({n for n in names if names.count(n) > 1}):
        raise DispatchError(
            f"PARTITIONED BY yields duplicate partition field name(s) {dupes}: Spark names "
            "CREATE-time fields <col>_bucket / <col>_trunc without the width. Keep one in "
            "CREATE TABLE and add the other with ALTER TABLE ... ADD PARTITION FIELD"
        )

    try:
        # Spec and properties go in the create request itself, so the table
        # never exists without them. (PyIceberg's create *transaction* replays
        # staged updates locally and refuses format v3 there, so it can't be
        # used.)
        table = catalog.create_table(
            identifier=identifier,
            schema=schema,
            location=options.location,
            partition_spec=_create_partition_spec(schema, options.partition_fields),
            properties=options.properties,
        )
        # PyIceberg's create path re-assigns field ids and drops defaults on
        # the way, so write-defaults follow in a second commit (#22).
        if defaults := {
            f.name: f.write_default for f in schema.fields if f.write_default is not None
        }:
            with table.update_schema() as upd:
                for name, value in defaults.items():
                    upd.set_default_value(name, value)
        notes = f"created table {'.'.join(identifier)}"
    except TableAlreadyExistsError:
        if not if_not_exists:
            # Idempotent contract is implicit; even without IF NOT EXISTS we
            # tolerate "already exists" so re-applies of current.sql are
            # safe. The notes string makes the no-op visible in the log.
            notes = f"table {'.'.join(identifier)} already exists (idempotent re-apply)"
        else:
            notes = f"table {'.'.join(identifier)} already exists (IF NOT EXISTS)"
    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes=notes,
    )


@dataclass
class _CreateTableOptions:
    partition_fields: list[tuple[str, Transform]] = field(default_factory=list)
    properties: dict[str, str] = field(default_factory=dict)
    location: str | None = None


def _create_table_options(stmt: exp.Create) -> _CreateTableOptions:
    """Collect PARTITIONED BY / COMMENT / LOCATION / TBLPROPERTIES from a CREATE TABLE.

    Anything else in the property list is refused rather than dropped: a
    silently ignored clause is a table that differs from the SQL.
    """
    options = _CreateTableOptions()
    props_node = stmt.args.get("properties")
    for prop in props_node.expressions if props_node is not None else []:
        if isinstance(prop, exp.FileFormatProperty | exp.LikeProperty):
            continue
        if isinstance(prop, exp.PartitionedByProperty):
            inner = prop.this
            nodes = inner.expressions if isinstance(inner, exp.Schema) else [inner]
            options.partition_fields.extend(_create_partition_field(n) for n in nodes)
        elif isinstance(prop, exp.SchemaCommentProperty):
            # Spark-Iceberg stores the table comment as the ``comment`` property.
            options.properties["comment"] = _scalar(prop.this) or ""
        elif isinstance(prop, exp.LocationProperty):
            options.location = _scalar(prop.this)
        elif type(prop) is exp.Property:
            key = _scalar(prop.args.get("this"))
            if key is not None:
                options.properties[key] = _scalar(prop.args.get("value")) or ""
        else:
            raise UnsupportedStatementError(
                statement_sql=stmt.sql(),
                reason=(
                    f"CREATE TABLE clause {prop.sql()!r} is not supported; v1 handles "
                    "PARTITIONED BY, COMMENT, LOCATION and TBLPROPERTIES"
                ),
            )
    return options


def _create_partition_spec(schema: Schema, fields: list[tuple[str, Transform]]) -> PartitionSpec:
    return PartitionSpec(
        *(
            PartitionField(
                source_id=schema.find_field(source).field_id,
                field_id=PARTITION_FIELD_ID_START + i,
                transform=transform,
                name=_create_partition_name(source, transform),
            )
            for i, (source, transform) in enumerate(fields)
        )
    )


def _create_partition_name(source: str, transform: Transform) -> str:
    # Spark builds CREATE-time specs with Java's PartitionSpec.Builder, whose
    # default names differ from UpdatePartitionSpec's (ADD PARTITION FIELD):
    # <col>_bucket / <col>_trunc carry no width suffix.
    if isinstance(transform, IdentityTransform):
        return source
    suffix = {
        BucketTransform: "bucket",
        TruncateTransform: "trunc",
        YearTransform: "year",
        MonthTransform: "month",
        DayTransform: "day",
        HourTransform: "hour",
    }[type(transform)]
    return f"{source}_{suffix}"


def _like_source(stmt: exp.Create) -> exp.Table | None:
    props = stmt.args.get("properties")
    for prop in props.expressions if props is not None else []:
        if isinstance(prop, exp.LikeProperty) and isinstance(prop.this, exp.Table):
            return prop.this
    return None


def _create_table_like(
    catalog: Catalog, stmt: exp.Create, target: exp.Table, source: exp.Table
) -> DispatchResult:
    # Spark 4.2 CREATE TABLE ... LIKE copies schema, partitioning, sort order
    # and properties; never snapshots or location.
    identifier = _table_identifier(target)
    src = catalog.load_table(_table_identifier(source))
    options = _create_table_options(stmt)
    try:
        catalog.create_table(
            identifier=identifier,
            schema=src.schema(),
            location=options.location,
            partition_spec=src.spec(),
            sort_order=src.sort_order(),
            properties={**src.properties, **options.properties},
        )
        notes = f"created table {'.'.join(identifier)} like {'.'.join(_table_identifier(source))}"
    except TableAlreadyExistsError:
        notes = f"table {'.'.join(identifier)} already exists (idempotent)"
    return DispatchResult(affected_tables=affected_tables(stmt), notes=notes)


def _create_partition_field(node: exp.Expression) -> tuple[str, Transform]:
    # sqlglot's Spark parser gives bucket/truncate dedicated nodes inside
    # PARTITIONED BY (unlike ADD PARTITION FIELD, where they are plain Funcs).
    if isinstance(node, exp.PartitionedByBucket):
        return _column_name(node.this), BucketTransform(num_buckets=_int_literal(node))
    if isinstance(node, exp.PartitionByTruncate):
        return _column_name(node.this), TruncateTransform(width=_int_literal(node))
    if isinstance(node, exp.Func):
        return _column_name(node), _transform_from_func(node)
    return _column_name(node), IdentityTransform()


def _int_literal(node: exp.Expression) -> int:
    lit = node.args.get("expression")
    if not isinstance(lit, exp.Literal) or lit.is_string:
        raise DispatchError(f"expected an integer literal in {node.sql()}")
    return int(lit.this)


def _dispatch_drop_table(catalog: Catalog, stmt: exp.Drop) -> DispatchResult:
    inner = stmt.args.get("this")
    if not isinstance(inner, exp.Table):
        raise DispatchError(f"DROP TABLE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(inner)
    if_exists = bool(stmt.args.get("exists"))
    purge = bool(stmt.args.get("purge"))
    try:
        if purge:
            catalog.purge_table(identifier)
        else:
            catalog.drop_table(identifier)
        notes = f"{'purged' if purge else 'dropped'} table {'.'.join(identifier)}"
    except NoSuchTableError:
        if not if_exists:
            # Same idempotent reasoning as create_table — tolerate the
            # "already absent" case even without IF EXISTS.
            notes = f"table {'.'.join(identifier)} already absent (idempotent re-apply)"
        else:
            notes = f"table {'.'.join(identifier)} already absent (IF EXISTS)"
    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes=notes,
    )


# ---------------------------------------------------------------------------
# ALTER TABLE
# ---------------------------------------------------------------------------


def _dispatch_alter(catalog: Catalog, stmt: exp.Alter) -> DispatchResult:
    table_node = stmt.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"ALTER missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    actions = stmt.args.get("actions") or []
    note_chunks: list[str] = []

    # RENAME TO operates on the catalog + identifier, not a loaded Table. When
    # the source is already gone (idempotent re-apply of a rename), eagerly
    # loading it here would 404 before _rename_table's idempotent handling runs.
    # Skip the load for a pure-rename ALTER.
    rename_only = bool(actions) and all(isinstance(a, exp.AlterRename) for a in actions)
    table = None if rename_only else catalog.load_table(identifier)

    for i, action in enumerate(actions):
        note_chunks.append(_dispatch_alter_action(catalog, table, identifier, action))
        # Reload the table BETWEEN actions so a later action sees the prior
        # one's committed state (PyIceberg caches metadata aggressively, which
        # breaks multi-action ALTERs like ADD then ALTER COLUMN). Skip the
        # reload after the final action: it's a wasted round-trip, and after a
        # RENAME TABLE the source identifier no longer resolves so the reload
        # would 404 on a statement that actually succeeded.
        if i + 1 < len(actions):
            table = catalog.load_table(identifier)

    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes="; ".join(c for c in note_chunks if c),
    )


def _dispatch_alter_action(
    catalog: Catalog, table: Table | None, identifier: tuple[str, ...], action: exp.Expression
) -> str:
    """Route a single ALTER action to its handler. Returns a notes string.

    ``table`` is ``None`` only for a pure-rename ALTER (see ``_dispatch_alter``);
    every other action requires a loaded table and is never reached with None.
    """
    # ALTER TABLE a RENAME TO b — sqlglot emits AlterRename inside actions.
    # Handled first: it works on catalog + identifier and is the only action
    # reachable with ``table is None`` (idempotent rename re-apply).
    if isinstance(action, exp.AlterRename):
        return _rename_table(catalog, identifier, action)

    if table is None:
        raise DispatchError(f"ALTER action {type(action).__name__} requires a loaded table")

    # ADD COLUMN (singular) — sqlglot emits a ColumnDef directly under actions.
    if isinstance(action, exp.ColumnDef):
        return _add_column(table, action)

    # ADD COLUMNS (a, b, c) — wrapped in a Schema; split into N commits.
    if isinstance(action, exp.Schema):
        col_defs = action.args.get("expressions") or []
        notes_parts: list[str] = []
        for col_def in col_defs:
            if not isinstance(col_def, exp.ColumnDef):
                raise DispatchError(
                    f"unexpected element inside ADD COLUMNS list: {type(col_def).__name__}"
                )
            notes_parts.append(_add_column(table, col_def))
            # Reload between additions; REST catalogs commit-per-update.
            table = catalog.load_table(identifier)
        return ", ".join(notes_parts)

    if isinstance(action, exp.Drop):
        return _drop_column(table, action)

    if isinstance(action, exp.RenameColumn):
        return _rename_column(table, action)

    if isinstance(action, exp.AlterColumn):
        return _alter_column(table, action)

    if isinstance(action, AlterColumnPosition):
        return _alter_column_position(table, action)

    if isinstance(action, exp.AlterSet):
        return _set_tblproperties(table, action)

    if isinstance(action, UnsetTblProperties):
        return _unset_tblproperties(table, action)

    if isinstance(action, SetIdentifierFields):
        return _set_identifier_fields(table, action)
    if isinstance(action, DropIdentifierFields):
        return _drop_identifier_fields(table, action)

    if isinstance(action, AddPartitionField):
        return _add_partition_field(table, action)
    if isinstance(action, DropPartitionField):
        return _drop_partition_field(table, action)
    if isinstance(action, ReplacePartitionField):
        return _replace_partition_field(catalog, table, identifier, action)
    if isinstance(action, WriteOrderedBy):
        return _write_ordered_by(table, action)
    if isinstance(action, WriteDistribution):
        return _write_distribution(table, action)

    raise UnsupportedStatementError(
        statement_sql=action.sql(),
        reason=(
            f"unsupported ALTER action {type(action).__name__}; see plan §2.2 "
            "for the v1 supported list"
        ),
    )


def _add_column(table: Table, col_def: exp.ColumnDef) -> str:
    name = col_def.name
    dtype = _iceberg_type_from_sqlglot(col_def.args.get("kind"))
    path = tuple(name.split("."))
    if _has_field(table, name):
        return f"add column {name}: already exists (idempotent)"
    default = _column_default(col_def)
    required = _is_required(col_def)
    if _is_v3_type(dtype):
        _require_v3(table.format_version, f"column {name} of type {dtype}")
    if default is not None:
        _require_v3(table.format_version, f"DEFAULT on column {name}")
    elif required:
        # Spark-Iceberg refuses this: existing rows have no value for the new
        # column, so it can only be required once a v3 default supplies one.
        raise DispatchError(
            f"cannot add required column {name}: existing rows would have no value. "
            "Add it as nullable, or (format v3) give it a DEFAULT"
        )
    with table.update_schema() as us:
        us.add_column(
            path, dtype, doc=_column_comment(col_def), required=required, default_value=default
        )
        _apply_position(us, name, col_def.args.get("position"))
    return f"add column {name}"


def _apply_position(us: UpdateSchema, name: str, position: exp.Expression | None) -> None:
    """Apply a Spark ``FIRST`` / ``AFTER <sibling>`` clause to *name* within *us*."""
    if position is None:
        return
    where = (position.args.get("position") or "").upper()
    if where == "FIRST":
        us.move_first(name)
    elif where == "AFTER":
        us.move_after(
            name,
            _sibling_path(
                name, _column_name(position.args.get("this") or position.args.get("after"))
            ),
        )


def _sibling_path(name: str, other: str) -> str:
    # Spark resolves AFTER's anchor within the moved field's own struct.
    parent, _, _ = name.rpartition(".")
    return f"{parent}.{other}" if parent and "." not in other else other


def _drop_column(table: Table, action: exp.Drop) -> str:
    inner = action.args.get("this")
    if inner is None:
        raise DispatchError(f"DROP COLUMN missing target: {action.sql()}")
    # The action's payload is either an exp.Column (singular form) or an
    # exp.Schema (plural with parens).
    kind = (action.args.get("kind") or "").upper()
    if kind == "COLUMNS" and isinstance(inner, exp.Schema):
        names = [_column_name(col) for col in inner.args.get("expressions") or []]
    else:
        names = [_column_name(inner)]
    notes: list[str] = []
    for name in names:
        if not _has_field(table, name):
            notes.append(f"drop column {name}: already absent (idempotent)")
            continue
        with table.update_schema() as us:
            us.delete_column(name)
        notes.append(f"drop column {name}")
    return ", ".join(notes)


def _has_field(table: Table, name: str) -> bool:
    # Decide idempotent no-ops from table state, not PyIceberg error text:
    # the messages aren't API and have already drifted out from under us.
    try:
        table.schema().find_field(name)
    except ValueError:
        return False
    return True


def _rename_column(table: Table, action: exp.RenameColumn) -> str:
    old = _column_name(action.args["this"])
    new = action.args["to"].name
    if not _has_field(table, old) and _has_field(table, _sibling_path(old, new)):
        return f"rename column {old} -> {new}: source already absent (idempotent)"
    with table.update_schema() as us:
        us.rename_column(old, new)
    return f"rename column {old} -> {new}"


def _alter_column(table: Table, action: exp.AlterColumn) -> str:
    name = action.args["this"].name
    dtype_node = action.args.get("dtype")
    comment_node = action.args.get("comment")
    # ``allow_null`` is set False for SET NOT NULL; ``drop`` + ``allow_null``
    # True is DROP NOT NULL. sqlglot leaves ``allow_null`` unset otherwise.
    allow_null = action.args.get("allow_null")

    field_type: IcebergType | None = None
    doc: str | None = None
    required: bool | None = None
    notes: list[str] = []
    if dtype_node is not None:
        field_type = _iceberg_type_from_sqlglot(dtype_node)
        if _already_widened(table, name, field_type):
            # Re-applying an earlier widening step (decimal(10,2) -> (12,2)
            # after a later -> (14,2)) would be a narrowing; the column
            # already satisfies "at least this wide", so leave the type alone.
            notes.append(f"type -> {field_type}: already wider (idempotent)")
            field_type = None
        elif _is_date_to_timestamp(table, name, field_type):
            _promote_date(table, name, field_type)
            notes.append(f"type -> {field_type}")
            field_type = None
        else:
            notes.append(f"type -> {field_type}")
    if comment_node is not None:
        doc = _scalar(comment_node)
        notes.append("comment")
    if allow_null is False:
        # Spark-Iceberg refuses optional -> required (existing rows may hold
        # NULLs) and treats an already-required column as a no-op.
        if not table.schema().find_field(name).required:
            raise DispatchError(
                f"cannot change nullable column {name} to NOT NULL: Iceberg can't know "
                "that existing rows have no NULLs"
            )
        notes.append("set not null: already required (idempotent)")
    elif allow_null is True:
        required = False
        notes.append("drop not null")
    default_node = action.args.get("default")
    drop_default = bool(action.args.get("drop")) and allow_null is None
    if default_node is not None or drop_default:
        _require_v3(table.format_version, f"a DEFAULT on column {name}")
        value = None if drop_default else _literal_value(default_node)
        try:
            with table.update_schema() as us:
                us.set_default_value(tuple(name.split(".")), value)
        except ValueError as err:
            raise DispatchError(f"cannot change the default of {name}: {err}") from err
        notes.append("drop default" if drop_default else f"default -> {value!r}")

    if not notes:
        raise UnsupportedStatementError(
            statement_sql=action.sql(),
            reason=(
                "ALTER COLUMN with no recognised change (TYPE / COMMENT / SET|DROP NOT NULL) "
                "is not supported"
            ),
        )
    if field_type is not None or required is not None or doc is not None:
        with table.update_schema() as us:
            us.update_column(name, field_type=field_type, required=required, doc=doc)
    return f"alter column {name} ({', '.join(notes)})"


def _is_date_to_timestamp(table: Table, name: str, requested: IcebergType) -> bool:
    try:
        current = table.schema().find_field(name).field_type
    except ValueError:
        return False
    return isinstance(current, DateType) and isinstance(
        requested, TimestampType | TimestampNanoType
    )


# Transforms that give the same value for a date and the timestamp it promotes
# to (spec: Schema Evolution); any other transform on the source blocks it.
_DATE_PROMOTION_SAFE = (YearTransform, MonthTransform, DayTransform, VoidTransform)


def _promote_date(table: Table, name: str, target: IcebergType) -> None:
    """Iceberg v3 date -> timestamp/timestamp_ns, which PyIceberg's UpdateSchema refuses (#25)."""
    _require_v3(table.format_version, f"promoting date column {name} to {target}")
    schema = table.schema()
    field_id = schema.find_field(name).field_id
    for spec in table.specs().values():
        for pf in spec.fields:
            if pf.source_id == field_id and not isinstance(pf.transform, _DATE_PROMOTION_SAFE):
                raise DispatchError(
                    f"cannot promote {name} to {target}: partition field {pf.name} uses "
                    f"{pf.transform}, which gives different values for dates and timestamps"
                )
    next_id = max(table.schemas()) + 1
    struct = _replace_field_type(schema.as_struct(), field_id, target)
    assert isinstance(struct, StructType)
    replace_schema(
        table,
        Schema(
            *struct.fields,
            schema_id=next_id,
            identifier_field_ids=list(schema.identifier_field_ids),
        ),
    )


def _replace_field_type(t: IcebergType, field_id: int, new_type: IcebergType) -> IcebergType:
    def swap(f: NestedField) -> NestedField:
        ftype = (
            new_type
            if f.field_id == field_id
            else _replace_field_type(f.field_type, field_id, new_type)
        )
        return NestedField(
            field_id=f.field_id,
            name=f.name,
            field_type=ftype,
            required=f.required,
            doc=f.doc,
            initial_default=f.initial_default,
            write_default=f.write_default,
        )

    if isinstance(t, StructType):
        return StructType(*(swap(f) for f in t.fields))
    if isinstance(t, ListType):
        element = swap(t.element_field)
        return ListType(
            element_id=element.field_id,
            element_type=element.field_type,
            element_required=element.required,
        )
    if isinstance(t, MapType):
        key, value = swap(t.key_field), swap(t.value_field)
        return MapType(
            key_id=key.field_id,
            key_type=key.field_type,
            value_id=value.field_id,
            value_type=value.field_type,
            value_required=value.required,
        )
    return t


def _already_widened(table: Table, name: str, requested: IcebergType) -> bool:
    try:
        current = table.schema().find_field(name).field_type
    except ValueError:
        return False
    if current == requested:
        return False
    try:
        promote(requested, current)
    except ResolveError:
        return False
    return True


def _alter_column_position(table: Table, action: AlterColumnPosition) -> str:
    name = action.args["this"].name
    position = (action.args.get("position") or "").upper()
    with table.update_schema() as us:
        if position == "FIRST":
            us.move_first(name)
        else:
            after = action.args.get("after")
            if after is None:
                raise DispatchError(f"ALTER COLUMN AFTER missing anchor column: {action.sql()}")
            us.move_after(name, _sibling_path(name, _column_name(after)))
    return f"reorder column {name} {position}"


def _rename_table(catalog: Catalog, identifier: tuple[str, ...], action: exp.AlterRename) -> str:
    target = action.args.get("this")
    if not isinstance(target, exp.Table):
        raise DispatchError(f"RENAME TO missing target table identifier: {action.sql()}")
    to_identifier = _table_identifier(target)
    try:
        catalog.rename_table(identifier, to_identifier)
    except NoSuchTableError:
        # Idempotent re-apply: the source is gone, presumably already renamed
        # to the target. Treat as a no-op note rather than a hard error,
        # mirroring the create/drop already-done handling.
        return (
            f"rename table {'.'.join(identifier)} -> {'.'.join(to_identifier)}: "
            "source already renamed (idempotent)"
        )
    except TableAlreadyExistsError:
        return (
            f"rename table {'.'.join(identifier)} -> {'.'.join(to_identifier)}: "
            "target already exists (idempotent)"
        )
    return f"rename table {'.'.join(identifier)} -> {'.'.join(to_identifier)}"


def _set_identifier_fields(table: Table, action: SetIdentifierFields) -> str:
    names = [_column_name(c) for c in action.args.get("expressions") or []]
    with table.update_schema() as us:
        us.set_identifier_fields(*names)
    return f"set identifier fields {names}"


def _drop_identifier_fields(table: Table, action: DropIdentifierFields) -> str:
    drop = {_column_name(c) for c in action.args.get("expressions") or []}
    # PyIceberg models identifier fields as the full set; "drop" is expressed
    # by re-setting the set minus the dropped names. Compute it from the
    # current schema so re-running converges (idempotent).
    current = set(table.schema().identifier_field_names())
    remaining = sorted(current - drop)
    with table.update_schema() as us:
        us.set_identifier_fields(*remaining)
    return f"drop identifier fields {sorted(drop)} (remaining {remaining})"


# ---------------------------------------------------------------------------
# TBLPROPERTIES
# ---------------------------------------------------------------------------


def _set_tblproperties(table: Table, action: exp.AlterSet) -> str:
    location = action.args.get("location")
    if location is not None:
        target = _scalar(location) or ""
        if table.location() == target:
            return f"set location {target}: unchanged (idempotent)"
        set_location(table, target)
        return f"set location {target}"
    # Pull the (key, value) pairs out of the AlterSet's nested Properties.
    props: dict[str, str] = {}
    for expression in action.args.get("expressions") or []:
        if isinstance(expression, exp.Properties):
            for prop in expression.args.get("expressions") or []:
                if isinstance(prop, exp.Property):
                    key = _scalar(prop.args.get("this"))
                    value = _scalar(prop.args.get("value"))
                    if key is None:
                        continue
                    props[key] = "" if value is None else value
    notes: list[str] = []
    if (version := props.pop(_FORMAT_VERSION_KEY, None)) is not None:
        notes.append(_upgrade_format(table, version))
    if not props:
        if notes:
            return "; ".join(notes)
        raise UnsupportedStatementError(
            statement_sql=action.sql(),
            reason="ALTER TABLE ... SET supports TBLPROPERTIES, LOCATION and IDENTIFIER FIELDS",
        )
    with table.transaction() as txn:
        # PyIceberg's set_properties takes a dict via the first positional
        # ``properties`` arg, NOT **kwargs of stringified Properties.
        txn.set_properties(properties=props)
    return "; ".join([*notes, f"set tblproperties {sorted(props)}"])


def _upgrade_format(table: Table, version: str) -> str:
    # format-version is reserved: the catalog rejects it as a property and
    # needs an upgrade-format-version update instead.
    try:
        target = int(version)
    except ValueError as err:
        raise DispatchError(f"format-version must be an integer, got {version!r}") from err
    if target < table.format_version:
        raise DispatchError(
            f"cannot downgrade format-version {table.format_version} to {target}: "
            "the Iceberg spec only allows upgrades"
        )
    if target == table.format_version:
        return f"format-version {target}: unchanged (idempotent)"
    upgrade_format_version(table, target)
    return f"upgraded format-version to {target}"


def _is_v3_type(t: IcebergType) -> bool:
    if isinstance(t, GeometryType | GeographyType | TimestampNanoType):
        return True
    if isinstance(t, StructType):
        return any(_is_v3_type(f.field_type) for f in t.fields)
    if isinstance(t, ListType):
        return _is_v3_type(t.element_type)
    if isinstance(t, MapType):
        return _is_v3_type(t.key_type) or _is_v3_type(t.value_type)
    return False


def _require_v3(table_version: int, what: str) -> None:
    if table_version < 3:
        raise DispatchError(
            f"{what} needs Iceberg format v3 (table is v{table_version}); upgrade first with "
            "ALTER TABLE ... SET TBLPROPERTIES ('format-version' = '3')"
        )


def _unset_tblproperties(table: Table, action: UnsetTblProperties) -> str:
    keys: list[str] = []
    for k in action.args.get("expressions") or []:
        rendered = _scalar(k)
        if rendered is not None:
            keys.append(rendered)
    if not keys:
        return "unset tblproperties: (no keys parsed)"
    present = [k for k in keys if k in table.properties]
    if not present:
        return f"unset tblproperties {sorted(keys)}: already absent (idempotent)"
    with table.transaction() as txn:
        txn.remove_properties(*present)
    return f"unset tblproperties {sorted(present)}"


def _scalar(node: exp.Expression | None) -> str | None:
    """Render a literal-or-identifier node as a plain string.

    Used for property keys and values. Falls back to ``node.sql()`` when the
    node isn't a recognised scalar shape.
    """
    if node is None:
        return None
    if isinstance(node, exp.Literal):
        return node.this
    if isinstance(node, exp.Identifier | exp.Column):
        return node.name
    return node.sql()


# ---------------------------------------------------------------------------
# Partition fields
# ---------------------------------------------------------------------------


def _add_partition_field(table: Table, action: AddPartitionField) -> str:
    source_column, transform, name = _resolve_partition_field(action)
    try:
        with table.update_spec() as us:
            us.add_field(source_column, transform, name)
    except ValueError as err:
        # PyIceberg refuses duplicate (source, transform) pairs even under
        # different partition-field names. Idempotent contract: log + continue.
        msg = str(err).lower()
        if "duplicate" in msg or "already" in msg:
            return (
                f"add partition field {source_column} ({transform}): already present (idempotent)"
            )
        raise
    return f"add partition field {source_column} ({transform})"


def _drop_partition_field(table: Table, action: DropPartitionField) -> str:
    name = _resolve_drop_partition_name(table, action)
    if name is None:
        # No field in the current spec matches the (source, transform) pair —
        # already dropped (or never added). Idempotent no-op.
        ref = action.args.get("transform") or action.args.get("this")
        label = ref.sql() if ref is not None else "?"
        return f"drop partition field {label}: already absent (idempotent)"
    try:
        with table.update_spec() as us:
            us.remove_field(name)
    except ValueError as err:
        if "not found" in str(err).lower() or "no such" in str(err).lower():
            return f"drop partition field {name}: already absent (idempotent)"
        raise
    return f"drop partition field {name}"


def _replace_partition_field(
    catalog: Catalog,
    table: Table,
    identifier: tuple[str, ...],
    action: ReplacePartitionField,
) -> str:
    # PyIceberg doesn't ship an atomic "replace field" call — sequence drop
    # then add. Both halves are idempotent under the contract above.
    old_name = _partition_field_name_from(action.args.get("this"))
    try:
        with table.update_spec() as us:
            us.remove_field(old_name)
    except ValueError as err:
        if not ("not found" in str(err).lower() or "no such" in str(err).lower()):
            raise
    table = catalog.load_table(identifier)
    transform_node = action.args.get("transform")
    new_alias = action.args.get("alias")
    if isinstance(transform_node, exp.Func):
        source = _column_name(transform_node)
        transform = _transform_from_func(transform_node)
    else:
        source = _column_name(transform_node)
        transform = IdentityTransform()
    new_name = new_alias.name if new_alias is not None else None
    try:
        with table.update_spec() as us:
            us.add_field(source, transform, new_name)
    except ValueError as err:
        if "duplicate" in str(err).lower() or "already" in str(err).lower():
            return (
                f"replace partition field {old_name} -> {source} ({transform}): "
                "target already present (idempotent)"
            )
        raise
    return f"replace partition field {old_name} -> {source} ({transform})"


def _resolve_partition_field(action: AddPartitionField) -> tuple[str, Transform, str | None]:
    """Decode ``AddPartitionField`` into ``(source_column, transform, name)``."""
    transform_node = action.args.get("transform")
    alias = action.args.get("alias")
    if isinstance(transform_node, exp.Func):
        source = _column_name(transform_node)
        transform = _transform_from_func(transform_node)
    else:
        # Bare-column ADD PARTITION FIELD x → identity transform.
        this = action.args.get("this")
        source = _column_name(this)
        transform = IdentityTransform()
    name = alias.name if alias is not None else None
    return source, transform, name


def _resolve_drop_partition_name(table: Table, action: DropPartitionField) -> str | None:
    """Resolve the partition-field name a ``DROP PARTITION FIELD`` removes.

    ``DROP PARTITION FIELD <name>`` references the field directly. ``DROP
    PARTITION FIELD <transform>(<col>)`` references it by (source column,
    transform): Iceberg synthesises the field name (e.g. ``id_bucket``), so the
    bare source column is the wrong handle for ``remove_field``. Match against
    the current spec by source-id + transform instead, returning ``None`` when
    nothing matches (the idempotent already-dropped case).
    """
    transform_node = action.args.get("transform")
    if transform_node is None:
        return _partition_field_name_from(action.args.get("this"))
    source = _column_name(transform_node)
    transform = (
        _transform_from_func(transform_node)
        if isinstance(transform_node, exp.Func)
        else IdentityTransform()
    )
    try:
        source_id = table.schema().find_field(source).field_id
    except ValueError:
        return None
    for pf in table.spec().fields:
        if pf.source_id == source_id and str(pf.transform) == str(transform):
            return pf.name
    return None


def _partition_field_name_from(node: exp.Expression | None) -> str:
    if node is None:
        raise DispatchError("partition field reference is empty")
    if isinstance(node, exp.Func):
        return _column_name(node)
    return _column_name(node)


def _column_name(node: exp.Expression | None) -> str:
    """Best-effort: return the column name implied by *node*.

    Handles three shapes:

    * ``Column``/``Identifier`` → return ``.name`` directly.
    * Anonymous transform call like ``bucket(N, col)`` → walk
      ``expressions`` for the first column/identifier argument.
    * Typed-Func transform like ``year(col)`` which sqlglot lowers to
      ``Year(this=Cast(Column))`` → walk the ``this`` payload through any
      wrapping ``Cast`` to find the underlying column.
    """
    if node is None:
        raise DispatchError("column reference is empty")
    if isinstance(node, exp.Column):
        return ".".join(part.name for part in node.parts)
    if isinstance(node, exp.Identifier):
        return node.name
    # ``truncate(N, col)`` is parsed as Trunc(this=Literal(N), decimals=col),
    # so the source column is under ``decimals`` rather than ``expressions``.
    if isinstance(node, exp.Trunc):
        decimals = node.args.get("decimals")
        if decimals is not None:
            return _column_name(_unwrap_cast(decimals))
    if isinstance(node, exp.Func):
        for arg in node.args.get("expressions") or []:
            if isinstance(arg, exp.Column | exp.Identifier):
                return _column_name(arg)
        this = node.args.get("this")
        if this is not None:
            return _column_name(_unwrap_cast(this))
    if isinstance(node, exp.Cast):
        return _column_name(node.args.get("this"))
    raise DispatchError(f"could not extract source column from {node.sql()}")


def _unwrap_cast(node: exp.Expression) -> exp.Expression:
    """Strip ``Cast(...)`` wrappers added by sqlglot for typed transforms."""
    while isinstance(node, exp.Cast):
        inner = node.args.get("this")
        if inner is None:
            return node
        node = inner
    return node


# Iceberg-Spark transform name → PyIceberg Transform mapping is inlined in
# _transform_from_func below. Each branch knows the arity of its target
# constructor; we avoid a generic table because ty can't see through the
# Transform alias to validate the per-constructor kwargs.


def _transform_from_func(node: exp.Func) -> Transform:
    """Translate an Iceberg-Spark transform call to its PyIceberg ``Transform``."""
    # sqlglot lowers typed time functions (Year, Month, Day, Hour) into
    # subclasses of exp.Func, where ``node.name`` is empty and the class
    # identity is what matters. Untyped/anonymous calls (bucket, truncate,
    # identity, years/months/days/hours when sqlglot doesn't recognise them
    # specifically) carry the function name in ``node.name`` directly.
    # Spark's documented aliases: date(ts) (parsed as CAST(ts AS DATE)) is
    # day(ts); date_hour(ts) is hour(ts).
    if isinstance(node, exp.Cast) and node.to.is_type(exp.DataType.Type.DATE):
        return DayTransform()
    if isinstance(node, exp.Year):
        return YearTransform()
    if isinstance(node, exp.Month):
        return MonthTransform()
    if isinstance(node, exp.Day):
        return DayTransform()
    # ``truncate(N, col)`` → Trunc(this=Literal(N), decimals=col): the width
    # is under ``this``, not ``expressions``.
    if isinstance(node, exp.Trunc):
        width = node.args.get("this")
        if not isinstance(width, exp.Literal):
            raise DispatchError(f"truncate() requires a numeric width literal: {node.sql()}")
        return TruncateTransform(width=int(width.this))
    name = (node.name or _typed_func_name(node)).lower()
    if name == "bucket":
        return BucketTransform(num_buckets=_transform_int_arg(node))
    if name == "truncate":
        return TruncateTransform(width=_transform_int_arg(node))
    if name in ("year", "years"):
        return YearTransform()
    if name in ("month", "months"):
        return MonthTransform()
    if name in ("day", "days"):
        return DayTransform()
    if name in ("hour", "hours", "date_hour"):
        return HourTransform()
    if name == "identity":
        return IdentityTransform()
    raise UnsupportedStatementError(
        statement_sql=node.sql(),
        reason=(
            f"unsupported partition transform {name!r}; expected one of bucket, truncate, "
            "year(s), month(s), day(s), hour(s), identity"
        ),
    )


def _typed_func_name(node: exp.Func) -> str:
    """Render the name of a typed sqlglot Func node (uses ``sql_name`` when present)."""
    sql_name = getattr(node, "sql_name", None)
    if callable(sql_name):
        return sql_name()
    return type(node).__name__


def _transform_int_arg(node: exp.Func) -> int:
    """Pull the first numeric-literal argument from a transform function call."""
    first = (node.args.get("expressions") or [None])[0]
    if not isinstance(first, exp.Literal):
        raise DispatchError(
            f"{node.name}() requires a numeric literal as its first argument: {node.sql()}"
        )
    try:
        return int(first.this)
    except (TypeError, ValueError) as err:
        raise DispatchError(
            f"{node.name}() literal must be an integer; got {first.this!r}"
        ) from err


# ---------------------------------------------------------------------------
# WRITE ORDERED BY
# ---------------------------------------------------------------------------


def _write_ordered_by(table: Table, action: WriteOrderedBy) -> str:
    return _replace_write_order(table, action.args.get("expressions") or [], "range")


def _write_distribution(table: Table, action: WriteDistribution) -> str:
    return _replace_write_order(
        table, action.args.get("expressions") or [], action.args.get("mode")
    )


def _replace_write_order(table: Table, items: list[exp.Expression], mode: str | None) -> str:
    """Replace the sort order (empty = unsorted) and optionally the distribution mode.

    One transaction, matching Spark-Iceberg's SetWriteDistributionAndOrderingExec.
    """
    parts: list[str] = []
    with table.transaction() as txn:
        with txn.update_sort_order() as uso:
            for item in items:
                ordered = item if isinstance(item, exp.Ordered) else exp.Ordered(this=item)
                term = ordered.args.get("this")
                transform = (
                    _transform_from_func(term)
                    if isinstance(term, exp.Func)
                    else IdentityTransform()
                )
                column = _column_name(term)
                desc = bool(ordered.args.get("desc"))
                # sqlglot fills nulls_first from Spark's default (ASC -> NULLS
                # FIRST, DESC -> NULLS LAST) when the SQL doesn't say; PyIceberg's
                # own default is NULLS LAST, so always pass it explicitly.
                nulls = (
                    NullOrder.NULLS_FIRST
                    if ordered.args.get("nulls_first")
                    else NullOrder.NULLS_LAST
                )
                (uso.desc if desc else uso.asc)(column, transform, nulls)
                parts.append(f"{column} {transform} {'DESC' if desc else 'ASC'} {nulls}")
        if mode is not None:
            txn.set_properties(properties={_DISTRIBUTION_MODE_KEY: mode})
    order = ", ".join(parts) or "unsorted"
    return f"write order ({order})" + (f"; distribution-mode={mode}" if mode else "")


# ---------------------------------------------------------------------------
# INSERT VALUES
# ---------------------------------------------------------------------------


def _dispatch_insert(catalog: Catalog, stmt: exp.Insert) -> DispatchResult:
    expression = stmt.args.get("expression")
    if not isinstance(expression, exp.Values):
        raise UnsupportedStatementError(
            statement_sql=stmt.sql(),
            reason=(
                "only INSERT INTO ... VALUES (...) is supported in v1 — INSERT SELECT, "
                "MERGE, DELETE, and UPDATE are out of scope (see plan §2.2)"
            ),
        )
    table_node = stmt.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"INSERT missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    table = catalog.load_table(identifier)
    _require_writable(table, stmt)
    arrow = _values_to_arrow(expression, table)
    table.append(arrow)
    n_rows = arrow.num_rows
    return DispatchResult(
        affected_tables=affected_tables(stmt),
        notes=f"insert {n_rows} row(s) into {'.'.join(identifier)}",
    )


def _dispatch_delete(catalog: Catalog, stmt: exp.Delete) -> DispatchResult:
    table_node = stmt.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"DELETE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    table = catalog.load_table(identifier)
    _require_writable(table, stmt)
    where = stmt.args.get("where")
    if where is not None:
        # PyIceberg parses a string row filter itself; render the WHERE
        # predicate to Spark SQL and hand it the string.
        predicate = where.this
        delete_filter = predicate.sql(dialect="spark")
        matched = _delete(table, delete_filter=delete_filter)
        base = f"delete from {'.'.join(identifier)} where {delete_filter}"
        # A re-run of a DELETE matches no rows — the idempotent steady state,
        # not an error. Reflect it as a note instead of leaking PyIceberg's
        # raw "did not match any records" warning to stderr every re-apply.
        notes = base if matched else f"{base}: matched no rows (idempotent)"
    else:
        matched = _delete(table)
        base = f"delete all rows from {'.'.join(identifier)}"
        notes = base if matched else f"{base}: matched no rows (idempotent)"
    return DispatchResult(affected_tables=affected_tables(stmt), notes=notes)


def _require_writable(table: Table, stmt: exp.Expression) -> None:
    if table.format_version >= 3:
        raise UnsupportedStatementError(
            statement_sql=stmt.sql(),
            reason="PyIceberg 0.12 can't write data files to format v3 tables yet",
        )
    sources = [f.source_id for f in table.spec().fields]
    if len(sources) != len(set(sources)):
        raise UnsupportedStatementError(
            statement_sql=stmt.sql(),
            reason=(
                "PyIceberg 0.12 can't write to a table partitioned more than once on the "
                "same column (#32)"
            ),
        )


def _delete(table: Table, *, delete_filter: str | None = None) -> bool:
    """Run ``table.delete``; return whether any rows matched.

    PyIceberg emits a ``UserWarning`` ("Delete operation did not match any
    records") when the filter matches nothing. Under the idempotent contract
    that's expected on re-apply, so swallow only that warning and report the
    no-match back to the caller; any other warning is re-emitted unchanged.
    """
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if delete_filter is not None:
            table.delete(delete_filter=delete_filter)
        else:
            table.delete()
    matched = True
    for w in caught:
        if "did not match any records" in str(w.message):
            matched = False
        else:
            warnings.warn_explicit(w.message, w.category, w.filename, w.lineno)
    return matched


def _values_to_arrow(values: exp.Values, table: Table) -> pa.Table:
    """Build a PyArrow ``Table`` matching *table*'s Iceberg schema from VALUES.

    Per the PyIceberg quirks memory, PyArrow nullability is strict against
    ``required`` Iceberg fields — we must pass an explicit ``pa.schema`` whose
    per-field ``nullable=`` flags mirror the Iceberg ``required`` flags
    exactly. We derive the target schema from ``table.schema().as_arrow()``
    and run a ``cast(..., safe=True)`` against it so literals are coerced
    in-flight.
    """
    target = table.schema().as_arrow()
    iceberg_fields = table.schema().fields
    # Per-field rebuild with the Iceberg required flag as the source of truth.
    fields = [
        pa.field(t_field.name, t_field.type, nullable=not i_field.required)
        for t_field, i_field in zip(target, iceberg_fields, strict=True)
    ]
    target_schema = pa.schema(fields)

    rows = list(_value_rows(values, target_schema))
    arrow = pa.Table.from_pylist(rows, schema=target_schema)
    # `from_pylist` already coerces simple types; cast() with safe=True
    # surfaces any precision-losing conversion as a hard error instead of
    # silent truncation.
    return arrow.cast(target_schema, safe=True)


def _value_rows(values: exp.Values, schema: pa.Schema) -> Iterable[dict[str, object]]:
    """Yield row dicts matching *schema* from a parsed VALUES list.

    Each row is a Tuple of literals positionally aligned with the schema's
    fields. Strings, numbers, booleans, and NULLs are mapped to their
    Python equivalents; non-literal expressions raise.
    """
    names = list(schema.names)
    for tup in values.args.get("expressions") or []:
        if not isinstance(tup, exp.Tuple):
            raise DispatchError(
                f"each VALUES entry must be a tuple of literals, got {type(tup).__name__}"
            )
        literals = tup.args.get("expressions") or []
        if len(literals) != len(names):
            raise DispatchError(
                f"VALUES tuple has {len(literals)} fields but table has {len(names)} columns"
            )
        yield {names[i]: _literal_value(lit) for i, lit in enumerate(literals)}


def _literal_value(node: exp.Expression) -> str | bool | int | float | None:
    """Convert a parsed literal node to its Python value.

    Supports: Literal (string/number), Boolean, Null. Identifiers/Columns
    inside a VALUES tuple are a user error (we'd be inserting a column
    reference, not data); raise.
    """
    if isinstance(node, exp.Null):
        return None
    if isinstance(node, exp.Boolean):
        return bool(node.this)
    if isinstance(node, exp.Literal):
        if node.is_string:
            return node.this
        # Numeric literals: try int, fall back to float.
        text = node.this
        try:
            return int(text)
        except ValueError:
            pass
        try:
            return float(text)
        except ValueError as err:
            raise DispatchError(f"can't coerce literal {text!r} to a number") from err
    if isinstance(node, exp.Neg):
        inner = _literal_value(node.this)
        if isinstance(inner, int | float):
            return -inner
        raise DispatchError(f"can't negate non-numeric value {inner!r}")
    raise DispatchError(
        f"unsupported literal {type(node).__name__} in VALUES; only string/number/bool/NULL"
    )


# ---------------------------------------------------------------------------
# CREATE TABLE: column-def → Iceberg schema
# ---------------------------------------------------------------------------


class _FieldIds:
    """Monotonic field-id allocator for building (possibly nested) schemas.

    Iceberg requires a unique id for every field, including nested struct
    fields and list/map element/key/value slots. We allocate top-level ids
    first, then nested ids, in a single increasing sequence.
    """

    def __init__(self) -> None:
        self._next = 0

    def next(self) -> int:
        self._next += 1
        return self._next


def _build_iceberg_schema(column_defs: list[exp.Expression]) -> Schema:
    """Assemble an Iceberg ``Schema`` from a list of parsed ``ColumnDef`` nodes."""
    ids = _FieldIds()
    # Allocate top-level ids up front so they stay 1..N regardless of how many
    # nested ids each column's type consumes.
    top: list[tuple[int, exp.ColumnDef]] = []
    for node in column_defs:
        if not isinstance(node, exp.ColumnDef):
            raise DispatchError(f"unexpected node in column list: {type(node).__name__}")
        top.append((ids.next(), node))
    fields: list[NestedField] = []
    for field_id, node in top:
        iceberg_type = _iceberg_type_from_sqlglot(node.args.get("kind"), ids)
        # We assume nullable unless the column-def carries an explicit
        # NOT NULL constraint. Iceberg-Spark's ``required`` is rendered as a
        # ``NOT NULL`` constraint via constraints=[]; sqlglot puts that under
        # ``constraints``.
        required = _is_required(node)
        default = _column_default(node)
        fields.append(
            NestedField(
                field_id=field_id,
                name=node.name,
                field_type=iceberg_type,
                required=required,
                doc=_column_comment(node),
                # The spec sets initial-default only for fields added later;
                # a create-time DEFAULT is the write default.
                write_default=_coerce_default(default, iceberg_type),
            )
        )
    return Schema(*fields)


def _column_comment(col_def: exp.ColumnDef) -> str | None:
    for constraint in col_def.args.get("constraints") or []:
        kind = constraint.args.get("kind") if isinstance(constraint, exp.Expression) else None
        if isinstance(kind, exp.CommentColumnConstraint):
            return _scalar(kind.this)
    return None


def _coerce_default(value: str | bool | int | float | None, to: IcebergType) -> Any:
    if value is None:
        return None
    # literal() is typed with a constrained TypeVar, which a union can't satisfy.
    return literal(value).to(to).value  # ty: ignore[invalid-argument-type]


def _column_default(col_def: exp.ColumnDef) -> str | bool | int | float | None:
    for constraint in col_def.args.get("constraints") or []:
        kind = constraint.args.get("kind") if isinstance(constraint, exp.Expression) else None
        if isinstance(kind, exp.DefaultColumnConstraint):
            return _literal_value(kind.this)
    return None


def _is_required(col_def: exp.ColumnDef) -> bool:
    """Detect ``NOT NULL`` on a ColumnDef without scanning the rendered SQL."""
    for constraint in col_def.args.get("constraints") or []:
        # sqlglot stores NOT NULL as ColumnConstraint with kind=NotNullColumnConstraint.
        kind = constraint.args.get("kind") if isinstance(constraint, exp.Expression) else None
        if isinstance(kind, exp.NotNullColumnConstraint):
            return True
    return False


_SQLGLOT_TYPE_TO_ICEBERG: dict[exp.DataType.Type, type[IcebergType]] = {
    exp.DataType.Type.BOOLEAN: BooleanType,
    exp.DataType.Type.TINYINT: IntegerType,
    exp.DataType.Type.SMALLINT: IntegerType,
    exp.DataType.Type.INT: IntegerType,
    exp.DataType.Type.BIGINT: LongType,
    exp.DataType.Type.FLOAT: FloatType,
    exp.DataType.Type.DOUBLE: DoubleType,
    exp.DataType.Type.VARCHAR: StringType,
    exp.DataType.Type.TEXT: StringType,
    exp.DataType.Type.CHAR: StringType,
    exp.DataType.Type.BINARY: BinaryType,
    exp.DataType.Type.VARBINARY: BinaryType,
    exp.DataType.Type.DATE: DateType,
    exp.DataType.Type.TIME: TimeType,
    # saltation keeps its existing TIMESTAMP→timestamptz mapping deliberately:
    # Spark 3.4+ treats bare TIMESTAMP as tz-bearing, and changing this is a
    # separate decision. Only TIMESTAMP_NTZ is added here (no-tz timestamp).
    exp.DataType.Type.TIMESTAMP: TimestampType,
    exp.DataType.Type.TIMESTAMPNTZ: TimestampType,
    exp.DataType.Type.TIMESTAMPTZ: TimestamptzType,
    exp.DataType.Type.TIMESTAMPLTZ: TimestamptzType,
}


def _iceberg_type_from_sqlglot(
    kind: exp.Expression | None, ids: _FieldIds | None = None
) -> IcebergType:
    """Translate a sqlglot DataType into the corresponding PyIceberg type.

    Covers the Iceberg primitive types representable in Spark SQL plus the
    composite types: ``STRUCT`` → :class:`StructType`, ``ARRAY`` →
    :class:`ListType`, ``MAP`` → :class:`MapType`, recursively. ``ids`` is the
    field-id allocator (a fresh one is used for standalone type resolution,
    e.g. ALTER ... ADD COLUMN, where PyIceberg reassigns ids on commit anyway).
    """
    if not isinstance(kind, exp.DataType):
        raise DispatchError(f"missing or unparseable column type: {kind!r}")
    ids = ids or _FieldIds()
    base = kind.this
    if base == exp.DataType.Type.DECIMAL:
        # Pull precision/scale from the DataType's expressions list.
        params = kind.args.get("expressions") or []
        precision = _decimal_param(params, 0, default=38)
        scale = _decimal_param(params, 1, default=0)
        return DecimalType(precision=precision, scale=scale)
    if base == exp.DataType.Type.STRUCT:
        return _struct_type_from_sqlglot(kind, ids)
    if base == exp.DataType.Type.ARRAY:
        return _list_type_from_sqlglot(kind, ids)
    if base == exp.DataType.Type.MAP:
        return _map_type_from_sqlglot(kind, ids)
    if base in (exp.DataType.Type.GEOMETRY, exp.DataType.Type.GEOGRAPHY):
        return _geo_type(kind)
    if base == exp.DataType.Type.VARIANT:
        raise UnsupportedStatementError(
            statement_sql=kind.sql(),
            reason="variant (Iceberg v3) needs a variant type PyIceberg doesn't provide yet",
        )
    iceberg_cls = _SQLGLOT_TYPE_TO_ICEBERG.get(base)
    if iceberg_cls is None:
        raise UnsupportedStatementError(
            statement_sql=kind.sql(),
            reason=(
                f"unsupported column type {base.name}; v1 covers the Iceberg primitive types "
                "(int, bigint, float, double, string, boolean, date, timestamp/tz, decimal, "
                "binary) plus struct/array/map composites"
            ),
        )
    return iceberg_cls()


def _geo_type(kind: exp.DataType) -> IcebergType:
    # GEOMETRY[(crs)] / GEOGRAPHY[(crs[, algorithm])]. A bare integer is an
    # EPSG SRID; a string is taken as the CRS verbatim (e.g. 'OGC:CRS84').
    params = [_scalar(p.this if isinstance(p, exp.DataTypeParam) else p) for p in kind.expressions]
    crs = None
    if params and params[0] is not None:
        crs = params[0] if not params[0].isdigit() else f"EPSG:{params[0]}"
    if kind.this == exp.DataType.Type.GEOMETRY:
        return GeometryType(crs) if crs else GeometryType()
    algorithm = (params[1] or "").lower() if len(params) > 1 else None
    if crs and algorithm:
        return GeographyType(crs, algorithm)
    return GeographyType(crs) if crs else GeographyType()


def _struct_type_from_sqlglot(kind: exp.DataType, ids: _FieldIds) -> StructType:
    # STRUCT<a: T, b: U> → its children are ColumnDef nodes (name + kind).
    members = kind.args.get("expressions") or []
    # Allocate this struct's field ids before recursing so sibling ids stay
    # contiguous regardless of nested depth.
    allocated = [(ids.next(), m) for m in members]
    fields: list[NestedField] = []
    for field_id, member in allocated:
        if not isinstance(member, exp.ColumnDef):
            raise DispatchError(f"unexpected STRUCT member: {type(member).__name__}")
        fields.append(
            NestedField(
                field_id=field_id,
                name=member.name,
                field_type=_iceberg_type_from_sqlglot(member.args.get("kind"), ids),
                required=_is_required(member),
                doc=_column_comment(member),
            )
        )
    return StructType(*fields)


def _list_type_from_sqlglot(kind: exp.DataType, ids: _FieldIds) -> ListType:
    members = kind.args.get("expressions") or []
    if not members:
        raise DispatchError(f"ARRAY type missing element type: {kind.sql()}")
    element_id = ids.next()
    return ListType(
        element_id=element_id,
        element_type=_iceberg_type_from_sqlglot(members[0], ids),
        element_required=False,
    )


def _map_type_from_sqlglot(kind: exp.DataType, ids: _FieldIds) -> MapType:
    members = kind.args.get("expressions") or []
    if len(members) != 2:
        raise DispatchError(f"MAP type needs key and value types: {kind.sql()}")
    key_id = ids.next()
    value_id = ids.next()
    return MapType(
        key_id=key_id,
        key_type=_iceberg_type_from_sqlglot(members[0], ids),
        value_id=value_id,
        value_type=_iceberg_type_from_sqlglot(members[1], ids),
        value_required=False,
    )


def _decimal_param(params: list[exp.Expression], idx: int, *, default: int) -> int:
    """Pull a DECIMAL(precision, scale) parameter; tolerate missing/malformed.

    The parameter is wrapped in a ``DataTypeParam`` whose ``this`` is itself
    a ``Literal`` carrying the numeric value as a string. The ``.name``
    attribute on ``DataTypeParam`` resolves to that string for us.
    """
    if idx >= len(params):
        return default
    node = params[idx]
    try:
        if isinstance(node, exp.DataTypeParam):
            inner = node.this
            if isinstance(inner, exp.Literal):
                return int(inner.this)
            return int(node.name)
        return int(node.name)
    except (TypeError, ValueError, AttributeError):
        return default
