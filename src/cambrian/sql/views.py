"""``CREATE / ALTER / DROP VIEW`` against an Iceberg REST catalog.

A view's body is written once in cambrian's Spark SQL and stored as one SQL
representation per configured dialect (sqlglot transpiles it). Iceberg also
needs the view's output schema; cambrian has no engine, so sqlglot infers it
from the schemas of the Iceberg tables and views the query reads. A column
whose type can't be inferred (anything read from outside the catalog) must
be CAST in the query.

Re-apply: ``CREATE OR REPLACE VIEW`` adds a version only when the SQL, schema
or properties differ from the current version; ``CREATE VIEW`` and ``IF NOT
EXISTS`` leave an existing view alone, as for tables.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any

from pyiceberg.exceptions import NoSuchTableError, NoSuchViewError, ViewAlreadyExistsError
from pyiceberg.schema import Schema
from pyiceberg.types import (
    DecimalType,
    IcebergType,
    ListType,
    MapType,
    NestedField,
    PrimitiveType,
    StructType,
)
from pyiceberg.view.metadata import ViewVersion
from sqlglot import expressions as exp
from sqlglot.optimizer.annotate_types import annotate_types
from sqlglot.optimizer.qualify import qualify
from sqlglot.schema import MappingSchema

from cambrian.errors import DispatchError, UnsupportedStatementError
from cambrian.iceberg.views import (
    VIEW_SUMMARY,
    rename_view,
    replace_view,
    require_rest,
    update_view_properties,
)
from cambrian.sql.ast import UnsetTblProperties
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.dispatch import (
    DispatchResult,
    _column_comment,
    _iceberg_type_from_sqlglot,
    _properties_dict,
    _properties_dict_from_list,
    _scalar,
    _table_identifier,
)
from cambrian.sql.script import SOURCE_DIALECT, SOURCE_SQL

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.typedef import Identifier
    from pyiceberg.view import View

__all__ = ["dispatch_view"]

# Spark spellings for Iceberg types, so sqlglot can type the view's query.
_SPARK_TYPE = {
    "boolean": "BOOLEAN",
    "int": "INT",
    "long": "BIGINT",
    "float": "FLOAT",
    "double": "DOUBLE",
    "date": "DATE",
    "time": "TIME",
    "timestamp": "TIMESTAMP_NTZ",
    "timestamptz": "TIMESTAMP",
    "string": "STRING",
    "uuid": "STRING",
    "binary": "BINARY",
}


def dispatch_view(
    catalog: Catalog, stmt: exp.Expression, dialects: Sequence[str]
) -> DispatchResult:
    if isinstance(stmt, exp.Create):
        return _create_view(catalog, stmt, dialects)
    if isinstance(stmt, exp.Drop):
        return _drop_view(catalog, stmt)
    if isinstance(stmt, exp.Alter):
        return _alter_view(catalog, stmt, dialects)
    raise UnsupportedStatementError(statement_sql=stmt.sql(), reason="unsupported view statement")


def _create_view(catalog: Catalog, stmt: exp.Create, dialects: Sequence[str]) -> DispatchResult:
    rest = require_rest(catalog)
    target = stmt.this
    columns: list[exp.Expression] = []
    if isinstance(target, exp.Schema):
        columns = list(target.expressions)
        target = target.this
    if not isinstance(target, exp.Table):
        raise DispatchError(f"CREATE VIEW missing view identifier: {stmt.sql()}")
    query = stmt.args.get("expression")
    if not isinstance(query, exp.Query):
        raise DispatchError(f"CREATE VIEW needs AS <query>: {stmt.sql()}")
    ident = _table_identifier(target)
    name = ".".join(ident)
    props = _view_properties(stmt.args.get("properties"))
    schema = _infer_schema(catalog, query, ident[:-1], columns)
    sqls = _representations(query, dialects)

    try:
        existing = rest.load_view(ident)
    except NoSuchViewError:
        existing = None
    if existing is not None:
        if not stmt.args.get("replace"):
            return DispatchResult(notes=f"view {name} already exists (idempotent)")
        if _matches(existing, schema, sqls, props):
            return DispatchResult(notes=f"view {name} unchanged (idempotent)")
        replace_view(rest, existing, schema, sqls, ident[:-1], props)
        return DispatchResult(
            notes=f"replaced view {name} (dialects {sorted(sqls)}){_untranslated(query, sqls)}"
        )

    version = ViewVersion.model_validate(
        {
            "version-id": 1,
            "schema-id": schema.schema_id,
            "summary": VIEW_SUMMARY,
            "representations": [
                {"type": "sql", "sql": sql, "dialect": dialect} for dialect, sql in sqls.items()
            ],
            "default-namespace": list(ident[:-1]),
        }
    )
    try:
        rest.create_view(ident, schema=schema, view_version=version, properties=props)
    except ViewAlreadyExistsError:
        return DispatchResult(notes=f"view {name} already exists (concurrent create)")
    return DispatchResult(
        notes=f"created view {name} (dialects {sorted(sqls)}){_untranslated(query, sqls)}"
    )


def _drop_view(catalog: Catalog, stmt: exp.Drop) -> DispatchResult:
    rest = require_rest(catalog)
    if not isinstance(stmt.this, exp.Table):
        raise DispatchError(f"DROP VIEW missing view identifier: {stmt.sql()}")
    ident = _table_identifier(stmt.this)
    try:
        rest.drop_view(ident)
    except NoSuchViewError:
        return DispatchResult(notes=f"view {'.'.join(ident)} already absent")
    return DispatchResult(notes=f"dropped view {'.'.join(ident)}")


def _alter_view(catalog: Catalog, stmt: exp.Alter, dialects: Sequence[str]) -> DispatchResult:
    rest = require_rest(catalog)
    if not isinstance(stmt.this, exp.Table):
        raise DispatchError(f"ALTER VIEW missing view identifier: {stmt.sql()}")
    ident = _table_identifier(stmt.this)
    notes: list[str] = []
    for action in stmt.args.get("actions") or []:
        if isinstance(action, exp.AlterRename) and isinstance(action.this, exp.Table):
            notes.append(_rename(rest, ident, _table_identifier(action.this)))
            continue
        view = rest.load_view(ident)
        if isinstance(action, exp.Query):
            # ALTER VIEW v AS <query>: a new version; schema and SQL change,
            # properties stay.
            schema = _infer_schema(catalog, action, ident[:-1], [])
            sqls = _representations(action, dialects)
            if _matches(view, schema, sqls, {}):
                notes.append("query unchanged (idempotent)")
            else:
                replace_view(rest, view, schema, sqls, ident[:-1], {})
                notes.append("replaced query")
        elif isinstance(action, exp.AlterSet):
            props = {}
            for node in action.expressions:
                props.update(_properties_dict(node))
            changed = {k: v for k, v in props.items() if view.properties.get(k) != v}
            update_view_properties(rest, view, set_=changed, remove=[])
            notes.append(f"set properties {sorted(props)}")
        elif isinstance(action, UnsetTblProperties):
            keys = [k for k in (_scalar(e) for e in action.expressions) if k is not None]
            present = [k for k in keys if k in view.properties]
            update_view_properties(rest, view, set_={}, remove=present)
            notes.append(f"unset properties {sorted(keys)}")
        else:
            raise UnsupportedStatementError(
                statement_sql=action.sql(),
                reason="ALTER VIEW supports AS <query>, SET/UNSET TBLPROPERTIES and RENAME TO",
            )
    return DispatchResult(notes=f"alter view {'.'.join(ident)}: {'; '.join(notes)}")


def _rename(catalog: Catalog, source: Identifier, target: Identifier) -> str:
    rest = require_rest(catalog)
    if not rest.view_exists(source) and rest.view_exists(target):
        return f"rename to {'.'.join(target)}: already renamed (idempotent)"
    rename_view(rest, source, target)
    return f"renamed to {'.'.join(target)}"


def _view_properties(node: exp.Expression | None) -> dict[str, str]:
    props: dict[str, str] = {}
    for prop in node.expressions if isinstance(node, exp.Properties) else []:
        if isinstance(prop, exp.SchemaCommentProperty):
            props["comment"] = _scalar(prop.this) or ""
        elif type(prop) is exp.Property:
            props.update(_properties_dict_from_list([prop]))
        else:
            raise UnsupportedStatementError(
                statement_sql=prop.sql(),
                reason="CREATE VIEW supports COMMENT and TBLPROPERTIES",
            )
    return props


def _untranslated(query: exp.Query, sqls: dict[str, str]) -> str:
    """Warn about functions sqlglot doesn't know: they're copied unchanged into
    every transpiled dialect, which may not have them."""
    targets = sorted(d for d in sqls if d != query.meta.get(SOURCE_DIALECT, "spark"))
    unknown = sorted({f.name for f in query.find_all(exp.Anonymous)})
    if not targets or not unknown:
        return ""
    return (
        f"; WARNING: functions {unknown} copied untranslated into {targets} "
        "(check those engines provide them)"
    )


def _representations(query: exp.Query, dialects: Sequence[str]) -> dict[str, str]:
    # The dialect the body was written in keeps its text verbatim; the others
    # are transpiled from it.
    source = query.meta.get(SOURCE_DIALECT)
    return {d: query.meta[SOURCE_SQL] if d == source else query.sql(dialect=d) for d in dialects}


def _matches(view: View, schema: Schema, sqls: dict[str, str], props: dict[str, str]) -> bool:
    current = view.current_version()
    stored = {r.root.dialect: r.root.sql for r in current.representations}
    return (
        stored == sqls
        and _shape(view.schema()) == _shape(schema)
        and all(view.properties.get(k) == v for k, v in props.items())
    )


def _shape(schema: Schema) -> list[tuple[str, str, bool, str | None]]:
    return [(f.name, str(f.field_type), f.required, f.doc) for f in schema.fields]


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


def _infer_schema(
    catalog: Catalog,
    query: exp.Query,
    default_namespace: Identifier,
    columns: list[exp.Expression],
) -> Schema:
    if columns and len(columns) != len(query.selects):
        raise DispatchError(
            f"view column list has {len(columns)} names but the query returns "
            f"{len(query.selects)} columns"
        )
    declared = [_declared_type(c) for c in columns] or [None] * len(query.selects)
    inferred = _inferred_types(catalog, query, default_namespace) if None in declared else {}
    fields: list[NestedField] = []
    for i, projection in enumerate(query.selects):
        name = columns[i].name if columns else projection.alias_or_name
        field_type = declared[i] or inferred.get(i)
        if field_type is None:
            raise DispatchError(
                f"can't infer the type of view column {name!r} (it isn't read from an Iceberg "
                "table or view in the catalog); CAST it in the query or give it a type in "
                "the view's column list"
            )
        doc = (
            _column_comment(columns[i])
            if columns and isinstance(columns[i], exp.ColumnDef)
            else None
        )
        fields.append(
            NestedField(field_id=i + 1, name=name, field_type=field_type, required=False, doc=doc)
        )
    return Schema(*fields)


def _declared_type(column: exp.Expression) -> IcebergType | None:
    # Spark's view column list carries only names; cambrian also accepts a
    # type, so views over sources outside the catalog keep their body as-is.
    kind = column.args.get("kind") if isinstance(column, exp.ColumnDef) else None
    return _iceberg_type_from_sqlglot(kind) if kind is not None else None


def _inferred_types(
    catalog: Catalog, query: exp.Query, default_namespace: Identifier
) -> dict[int, IcebergType]:
    dialect = query.meta.get(SOURCE_DIALECT) or CambrianSpark
    mapping = _source_schemas(catalog, query, default_namespace)
    try:
        typed = qualify(
            query.copy(),
            schema=mapping,
            dialect=dialect,
            db=".".join(default_namespace) or None,
            validate_qualify_columns=False,
        )
    except Exception as err:
        raise DispatchError(f"can't resolve the view's columns: {err}") from err
    typed = annotate_types(typed, schema=mapping, dialect=dialect)
    out: dict[int, IcebergType] = {}
    for i, projection in enumerate(typed.selects):
        dtype = projection.type
        if dtype is not None and not dtype.is_type(exp.DataType.Type.UNKNOWN):
            out[i] = _iceberg_type_from_sqlglot(dtype)
    return out


def _source_schemas(
    catalog: Catalog, query: exp.Query, default_namespace: Identifier
) -> MappingSchema:
    ctes = {cte.alias_or_name for cte in query.find_all(exp.CTE)}
    nested: dict[str, Any] = {}
    for table in query.find_all(exp.Table):
        if not table.db and table.name in ctes:
            continue
        ident = _table_identifier(table)
        if len(ident) == 1:
            ident = (*default_namespace, ident[0])
        schema = _load_schema(catalog, ident)
        if schema is None:
            continue
        db = ".".join(ident[:-1])
        nested.setdefault(db, {})[ident[-1]] = {
            f.name: _spark_type(f.field_type) for f in schema.fields
        }
    return MappingSchema(nested, dialect=CambrianSpark)


def _load_schema(catalog: Catalog, ident: tuple[str, ...]) -> Schema | None:
    try:
        return catalog.load_table(ident).schema()
    except NoSuchTableError:
        pass
    try:
        return catalog.load_view(ident).schema()
    except (NoSuchViewError, NotImplementedError):
        return None


def _spark_type(t: IcebergType) -> str:
    if isinstance(t, DecimalType):
        return f"DECIMAL({t.precision}, {t.scale})"
    if isinstance(t, ListType):
        return f"ARRAY<{_spark_type(t.element_type)}>"
    if isinstance(t, MapType):
        return f"MAP<{_spark_type(t.key_type)}, {_spark_type(t.value_type)}>"
    if isinstance(t, StructType):
        return (
            "STRUCT<" + ", ".join(f"{f.name}: {_spark_type(f.field_type)}" for f in t.fields) + ">"
        )
    if isinstance(t, PrimitiveType):
        # Unmapped primitives (geometry, timestamp_ns, ...) stay untyped and
        # must be CAST if a view selects them.
        return _SPARK_TYPE.get(str(t), "UNKNOWN")
    return "UNKNOWN"
