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
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

import pyarrow as pa
from pyiceberg.exceptions import (
    NamespaceAlreadyExistsError,
    NoSuchNamespaceError,
    NoSuchTableError,
    TableAlreadyExistsError,
)
from pyiceberg.schema import Schema
from pyiceberg.transforms import (
    BucketTransform,
    DayTransform,
    HourTransform,
    IdentityTransform,
    MonthTransform,
    Transform,
    TruncateTransform,
    YearTransform,
)
from pyiceberg.types import (
    BinaryType,
    BooleanType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IcebergType,
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
from sqlglot import expressions as exp

from cambrian.errors import DispatchError, UnsupportedStatementError
from cambrian.iceberg.affected import TableIdent, affected_tables
from cambrian.sql.ast import (
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

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

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


def dispatch(catalog: Catalog, statement: exp.Expression) -> DispatchResult:
    """Translate *statement* into PyIceberg calls and execute them.

    See module docstring for the idempotent contract.

    Raises:
        UnsupportedStatementError: The construct isn't in cambrian's v1
            supported list (e.g. ``INSERT ... SELECT``, ``MERGE``, ``DELETE``).
        DispatchError: The construct is recognised but cambrian can't run
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
    catalog.update_namespace_properties(namespace, updates=updates)
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
        if not if_exists:
            raise
        notes = f"namespace {namespace} already absent (IF EXISTS)"
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
                "that cambrian won't perform. Use CREATE TABLE IF NOT EXISTS plus explicit "
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
                "CREATE TABLE ... AS SELECT (CTAS) is out of scope: cambrian applies DDL only "
                "and has no query engine to evaluate the SELECT"
            ),
        )
    if not isinstance(inner, exp.Schema):
        raise DispatchError(
            "CREATE TABLE without a column list isn't supported (cambrian needs the schema "
            "to translate to PyIceberg). Use ``CREATE TABLE t (cols...) USING iceberg``."
        )
    table_node = inner.args.get("this")
    if not isinstance(table_node, exp.Table):
        raise DispatchError(f"CREATE TABLE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(table_node)
    column_defs = inner.args.get("expressions") or []
    schema = _build_iceberg_schema(column_defs)
    if_not_exists = bool(stmt.args.get("exists"))

    try:
        catalog.create_table(identifier=identifier, schema=schema)
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


def _dispatch_drop_table(catalog: Catalog, stmt: exp.Drop) -> DispatchResult:
    inner = stmt.args.get("this")
    if not isinstance(inner, exp.Table):
        raise DispatchError(f"DROP TABLE missing table identifier: {stmt.sql()}")
    identifier = _table_identifier(inner)
    if_exists = bool(stmt.args.get("exists"))
    try:
        catalog.drop_table(identifier)
        notes = f"dropped table {'.'.join(identifier)}"
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
    # A dotted name (``point.z``) is a nested-field add; PyIceberg takes the
    # path as a tuple. A plain name is passed through as the bare string.
    path: str | tuple[str, ...] = tuple(name.split(".")) if "." in name else name
    # ALTER TABLE ... ADD COLUMN is always nullable in Spark/Iceberg unless
    # the user gives a NOT NULL constraint — we err on the safe side
    # (required=False) so re-applies don't fight a NULL existing row.
    try:
        with table.update_schema() as us:
            us.add_column(path, dtype, required=False)
    except ValueError as err:
        # PyIceberg raises a ValueError on duplicate add. Idempotent contract.
        if "already exists" in str(err).lower() or "duplicate" in str(err).lower():
            return f"add column {name}: already exists (idempotent)"
        raise
    return f"add column {name}"


def _drop_column(table: Table, action: exp.Drop) -> str:
    inner = action.args.get("this")
    if inner is None:
        raise DispatchError(f"DROP COLUMN missing target: {action.sql()}")
    # The action's payload is either an exp.Column (singular form) or an
    # exp.Schema (plural with parens).
    kind = (action.args.get("kind") or "").upper()
    if kind == "COLUMNS" and isinstance(inner, exp.Schema):
        notes: list[str] = []
        for col in inner.args.get("expressions") or []:
            name = col.name
            try:
                with table.update_schema() as us:
                    us.delete_column(name)
                notes.append(f"drop column {name}")
            except ValueError as err:
                if "not found" in str(err).lower():
                    notes.append(f"drop column {name}: already absent (idempotent)")
                else:
                    raise
        return ", ".join(notes)
    # Singular form: ``DROP COLUMN c``.
    name = inner.name
    try:
        with table.update_schema() as us:
            us.delete_column(name)
    except ValueError as err:
        if "not found" in str(err).lower() or "does not exist" in str(err).lower():
            return f"drop column {name}: already absent (idempotent)"
        raise
    return f"drop column {name}"


def _rename_column(table: Table, action: exp.RenameColumn) -> str:
    old = action.args["this"].name
    new = action.args["to"].name
    try:
        with table.update_schema() as us:
            us.rename_column(old, new)
    except ValueError as err:
        # If old name is already absent and new exists, consider it idempotent.
        msg = str(err).lower()
        if "not found" in msg or "does not exist" in msg:
            return f"rename column {old} -> {new}: source already absent (idempotent)"
        raise
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
        notes.append(f"type -> {field_type}")
    if comment_node is not None:
        doc = _scalar(comment_node)
        notes.append("comment")
    if allow_null is False:
        required = True
        notes.append("set not null")
    elif allow_null is True:
        required = False
        notes.append("drop not null")

    if not notes:
        raise UnsupportedStatementError(
            statement_sql=action.sql(),
            reason=(
                "ALTER COLUMN with no recognised change (TYPE / COMMENT / SET|DROP NOT NULL) "
                "is not supported"
            ),
        )
    # SET NOT NULL tightens an optional column to required; PyIceberg blocks
    # that by default (existing rows might hold NULLs). Spark-Iceberg allows
    # the change and leaves data correctness to the user, so opt into the
    # incompatible change only when we're tightening (required is True).
    allow_incompatible = required is True
    with table.update_schema(allow_incompatible_changes=allow_incompatible) as us:
        us.update_column(name, field_type=field_type, required=required, doc=doc)
    return f"alter column {name} ({', '.join(notes)})"


def _alter_column_position(table: Table, action: AlterColumnPosition) -> str:
    name = action.args["this"].name
    position = (action.args.get("position") or "").upper()
    with table.update_schema() as us:
        if position == "FIRST":
            us.move_first(name)
        else:
            after = action.args.get("after")
            anchor = after.name if after is not None else None
            if anchor is None:
                raise DispatchError(f"ALTER COLUMN AFTER missing anchor column: {action.sql()}")
            us.move_after(name, anchor)
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
    if not props:
        return "set tblproperties: (no keys parsed)"
    with table.transaction() as txn:
        # PyIceberg's set_properties takes a dict via the first positional
        # ``properties`` arg, NOT **kwargs of stringified Properties.
        txn.set_properties(properties=props)
    return f"set tblproperties {sorted(props)}"


def _unset_tblproperties(table: Table, action: UnsetTblProperties) -> str:
    keys: list[str] = []
    for k in action.args.get("expressions") or []:
        rendered = _scalar(k)
        if rendered is not None:
            keys.append(rendered)
    if not keys:
        return "unset tblproperties: (no keys parsed)"
    try:
        with table.transaction() as txn:
            txn.remove_properties(*keys)
    except (KeyError, ValueError) as err:
        # PyIceberg raises if the key isn't present. Treat as idempotent —
        # the user asked for the absence; we're delivering it.
        msg = str(err).lower()
        if "not found" in msg or "does not exist" in msg or "missing" in msg:
            return f"unset tblproperties {sorted(keys)}: already absent (idempotent)"
        raise
    return f"unset tblproperties {sorted(keys)}"


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
    if isinstance(node, exp.Column | exp.Identifier):
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
                return arg.name
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
    if name in ("hour", "hours"):
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
    # WRITE ORDERED BY → set the sort order AND the range distribution mode.
    parts = _apply_sort_order(table, action.args.get("expressions") or [])
    _set_distribution_mode(table, "range")
    return f"write ordered by ({', '.join(parts)}); distribution-mode=range"


def _write_distribution(table: Table, action: WriteDistribution) -> str:
    mode = action.args.get("mode") or ""
    sort_items = action.args.get("expressions") or []
    if mode == "unordered":
        # WRITE UNORDERED clears the sort order back to unsorted; PyIceberg's
        # empty update_sort_order context commits the unsorted order.
        with table.update_sort_order():
            pass
        return "write unordered (sort order cleared)"
    parts = _apply_sort_order(table, sort_items) if sort_items else []
    # none → WRITE LOCALLY ORDERED BY; hash → WRITE DISTRIBUTED BY PARTITION.
    _set_distribution_mode(table, mode)
    suffix = f" ordered by ({', '.join(parts)})" if parts else ""
    return f"write distribution-mode={mode}{suffix}"


def _apply_sort_order(table: Table, items: list[exp.Expression]) -> list[str]:
    parts: list[str] = []
    with table.update_sort_order() as uso:
        for item in items:
            if isinstance(item, exp.Ordered):
                col_name = _column_name(item.args.get("this"))
                desc = bool(item.args.get("desc"))
            else:
                col_name = _column_name(item)
                desc = False
            # Per PyIceberg quirks memory: ``asc/desc`` require an explicit
            # transform argument in 0.11.1. We pass IdentityTransform() for
            # the simple column-sort case; transform-in-sort would need the
            # transform decoded here.
            if desc:
                uso.desc(col_name, IdentityTransform())
                parts.append(f"{col_name} DESC")
            else:
                uso.asc(col_name, IdentityTransform())
                parts.append(f"{col_name} ASC")
    return parts


def _set_distribution_mode(table: Table, mode: str) -> None:
    with table.transaction() as txn:
        txn.set_properties(properties={_DISTRIBUTION_MODE_KEY: mode})


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


def _literal_value(node: exp.Expression) -> object:
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
        fields.append(
            NestedField(
                field_id=field_id, name=node.name, field_type=iceberg_type, required=required
            )
        )
    return Schema(*fields)


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
    # cambrian keeps its existing TIMESTAMP→timestamptz mapping deliberately:
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
                required=False,
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
