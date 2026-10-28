"""Hypothesis strategies that generate valid evolution scripts plus their expected end state.

The generator walks a small in-memory model of each table and only emits
statements that are legal against the model at that point in the script, so a
dispatch error on a *fresh* apply is a cambrian bug, not a generator artefact.
The model's :meth:`TableModel.fingerprint` uses the same shape as
:func:`catalog_fingerprint`, so tests can compare live catalog state against
the model (correctness) as well as against itself after a re-apply
(idempotency).

Scripts are rendered with a ``{ns}`` placeholder so one generated script can be
applied into several isolated namespaces.
"""

from __future__ import annotations

import copy
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from hypothesis import strategies as st

from cambrian.sql.dispatch import dispatch
from cambrian.sql.script import parse_script

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

__all__ = [
    "ColType",
    "EvolutionScript",
    "Features",
    "TableModel",
    "apply_script",
    "catalog_fingerprint",
    "evolution_scripts",
]

PROP_PREFIX = "cambrian.p."
DISTRIBUTION_MODE = "write.distribution-mode"
TABLE_COMMENT = "comment"


@dataclass(frozen=True)
class ColType:
    sql: str
    iceberg: str
    family: str


INT = ColType("INT", "int", "int")
BIGINT = ColType("BIGINT", "long", "long")
FLOAT = ColType("FLOAT", "float", "float")
DOUBLE = ColType("DOUBLE", "double", "double")
STRING = ColType("STRING", "string", "string")
BOOLEAN = ColType("BOOLEAN", "boolean", "boolean")
DATE = ColType("DATE", "date", "date")
TS_NTZ = ColType("TIMESTAMP_NTZ", "timestamp", "ts")
TS_TZ = ColType("TIMESTAMP", "timestamptz", "ts")
BINARY = ColType("BINARY", "binary", "binary")


def _decimal(precision: int, scale: int) -> ColType:
    return ColType(f"DECIMAL({precision}, {scale})", f"decimal({precision}, {scale})", "decimal")


PRIMITIVES: tuple[ColType, ...] = (
    INT,
    BIGINT,
    FLOAT,
    DOUBLE,
    STRING,
    BOOLEAN,
    DATE,
    TS_NTZ,
    TS_TZ,
    BINARY,
    _decimal(10, 2),
    _decimal(18, 4),
)


def _widenings(t: ColType) -> list[ColType]:
    if t == INT:
        return [BIGINT]
    if t == FLOAT:
        return [DOUBLE]
    if t.family == "decimal":
        p, s = (int(x) for x in t.iceberg[len("decimal(") : -1].split(", "))
        return [_decimal(np, s) for np in (p + 2, 38) if np <= 38 and np > p]
    return []


# (sql template, iceberg transform string) per column family. Identity on
# float/double is legal Iceberg but useless and rejected by some engines.
def _transforms(t: ColType) -> list[tuple[str, str]]:
    fam = t.family
    out: list[tuple[str, str]] = []
    if fam in {"int", "long", "string", "date", "boolean", "decimal", "ts"}:
        out.append(("{c}", "identity"))
    if fam in {"int", "long", "string", "date", "ts", "decimal", "binary"}:
        out += [("bucket(4, {c})", "bucket[4]"), ("bucket(16, {c})", "bucket[16]")]
    if fam in {"int", "long", "string", "decimal", "binary"}:
        out.append(("truncate(8, {c})", "truncate[8]"))
    if fam in {"date", "ts"}:
        out += [("year({c})", "year"), ("month({c})", "month"), ("day({c})", "day")]
    if fam == "ts":
        out.append(("hour({c})", "hour"))
    return out


_TIME_TRANSFORMS = {"year", "month", "day", "hour"}


@dataclass
class Column:
    name: str
    type: ColType
    required: bool = False
    doc: str | None = None
    default: int | str | None = None


@dataclass
class PartitionField:
    source: str
    transform: str
    sql: str


@dataclass
class TableModel:
    name: str
    columns: list[Column] = field(default_factory=list)
    partition: list[PartitionField] = field(default_factory=list)
    sort: list[tuple[str, str]] = field(default_factory=list)
    props: dict[str, str] = field(default_factory=dict)
    identifier: set[str] = field(default_factory=set)
    # Columns ever referenced by a partition field or sort order. Iceberg keeps
    # old specs/orders around and refuses to drop their source columns, so the
    # generator never drops these.
    pinned: set[str] = field(default_factory=set)
    # Columns an ALTER has referenced. Dropping one later makes the script
    # non-replayable (the ALTER hits a missing column on re-apply).
    altered: set[str] = field(default_factory=set)
    ever_identifier: set[str] = field(default_factory=set)
    # Columns a SET NOT NULL has asserted; relaxing one later makes that
    # statement fail on replay (Spark refuses optional -> required).
    asserted_required: set[str] = field(default_factory=set)
    reordered: bool = False
    dropped_partitions: set[tuple[str, str]] = field(default_factory=set)
    next_col: int = 0
    fv: int = 2

    def droppable(self, features: Features) -> list[Column]:
        blocked = self.pinned | self.identifier
        if not features.drop_after_alter:
            blocked |= self.altered
        return [c for c in self.columns if c.name not in blocked]

    def col(self, name: str) -> Column:
        return next(c for c in self.columns if c.name == name)

    def fresh_name(self) -> str:
        name = f"c{self.next_col}"
        self.next_col += 1
        return name

    def fingerprint(self) -> dict[str, Any]:
        return {
            "columns": [
                (c.name, c.type.iceberg, c.required, c.doc, c.default) for c in self.columns
            ],
            "partition": [(p.source, p.transform) for p in self.partition],
            "sort": list(self.sort),
            "props": dict(sorted(self.props.items())),
            "identifier": sorted(self.identifier),
        }


@dataclass(frozen=True)
class Features:
    """Toggles for statement kinds. Off-by-default entries are known gaps."""

    column_comments_on_add: bool = True
    column_comments_on_create: bool = True
    create_partitioned_by: bool = True
    create_comment: bool = True
    create_tblproperties: bool = True
    drop_column: bool = True
    # The three flags below generate scripts where a later statement breaks an
    # earlier one's precondition, so the script can't be replayed. Per the
    # core principle that is reset-mode territory, not an idempotency bug.
    drop_after_alter: bool = False
    # Also covers relaxing a column an earlier SET NOT NULL asserted.
    relax_former_identifier: bool = False
    multiple_reorders: bool = False
    # Re-adding a dropped partition field reorders the spec on replay.
    readd_partition_field: bool = False
    alter_type: bool = True
    alter_comment: bool = True
    alter_nullability: bool = True
    reorder: bool = True
    partition: bool = True
    sort: bool = True
    props: bool = True
    unset_props: bool = True
    identifier: bool = True
    max_tables: int = 2
    # Format v3: create/upgrade v3 tables, column DEFAULTs, date -> timestamp.
    v3: bool = True
    views: bool = True


@dataclass
class EvolutionScript:
    statements: list[str]
    expected: dict[str, dict[str, Any]]

    def render(self, ns: str, upto: int | None = None) -> str:
        stmts = self.statements if upto is None else self.statements[:upto]
        return "\n".join(s.format(ns=ns) for s in stmts)

    def __repr__(self) -> str:
        body = "\n  ".join(self.statements)
        return f"EvolutionScript(\n  {body}\n)"


# (Spark SQL literal body, decoded doc). Spark escapes quotes with a backslash;
# the ANSI/Trino doubled quote ('') does not parse in CambrianSpark.
_DOCS = st.sampled_from(
    [
        ("first", "first"),
        ("second doc", "second doc"),
        ("it\\'s quoted", "it's quoted"),
        ("unit: m/s", "unit: m/s"),
    ]
)
_PROP_KEYS = st.sampled_from([f"{PROP_PREFIX}k{i}" for i in range(4)])
_PROP_VALS = st.sampled_from(["a", "b", "1", "true"])


def _q(ns_table: str) -> str:
    return "{ns}." + ns_table


@st.composite
def _create(draw: st.DrawFn, name: str, features: Features) -> tuple[str, TableModel]:
    t = TableModel(name=name)
    if features.v3 and draw(st.booleans()):
        t.fv = 3
    n = draw(st.integers(min_value=1, max_value=5))
    defs: list[str] = []
    for _ in range(n):
        ctype = draw(st.sampled_from(PRIMITIVES))
        col = Column(t.fresh_name(), ctype, required=draw(st.booleans()))
        d = f"{col.name} {ctype.sql}"
        if col.required:
            d += " NOT NULL"
        if t.fv >= 3 and ctype.sql in _DEFAULTABLE and draw(st.booleans()):
            col.default = draw(_DEFAULTABLE[ctype.sql])
            d += f" DEFAULT {_default_sql(col.default)}"
        if features.column_comments_on_create and draw(st.booleans()):
            lit, col.doc = draw(_DOCS)
            d += f" COMMENT '{lit}'"
        t.columns.append(col)
        defs.append(d)
    sql = f"CREATE TABLE IF NOT EXISTS {_q(name)} ({', '.join(defs)}) USING iceberg"
    if features.create_partitioned_by and draw(st.booleans()):
        exprs: list[str] = []
        for _ in range(draw(st.integers(min_value=1, max_value=3))):
            # Spark names CREATE-time fields <col>_bucket / <col>_trunc with no
            # width, so a second bucket or truncate on one column collides.
            taken = {(p.source, p.transform.split("[")[0]) for p in t.partition}
            options = [
                o
                for o in _partition_options(t, features)
                if (o[0].name, o[2].split("[")[0]) not in taken
            ]
            if not options:
                break
            col, expr, tr = draw(st.sampled_from(options))
            t.partition.append(PartitionField(col.name, tr, expr))
            t.pinned.add(col.name)
            exprs.append(expr)
        sql += f" PARTITIONED BY ({', '.join(exprs)})"
    if features.create_comment and draw(st.booleans()):
        lit, t.props[TABLE_COMMENT] = draw(_DOCS)
        sql += f" COMMENT '{lit}'"
    pairs: dict[str, str] = {"format-version": "3"} if t.fv >= 3 else {}
    if features.create_tblproperties and draw(st.booleans()):
        kv = draw(st.dictionaries(_PROP_KEYS, _PROP_VALS, min_size=1, max_size=3))
        t.props.update(kv)
        pairs.update(kv)
    if pairs:
        sql += " TBLPROPERTIES (" + ", ".join(f"'{k}'='{v}'" for k, v in pairs.items()) + ")"
    return sql + ";", t


def _partition_options(t: TableModel, features: Features) -> list[tuple[Column, str, str]]:
    used = {(p.source, p.transform) for p in t.partition}
    if not features.readd_partition_field:
        used |= t.dropped_partitions
    options: list[tuple[Column, str, str]] = []
    for c in t.columns:
        has_time = any(p.source == c.name and p.transform in _TIME_TRANSFORMS for p in t.partition)
        for tmpl, tr in _transforms(c.type):
            if (c.name, tr) in used or (tr in _TIME_TRANSFORMS and has_time):
                continue
            options.append((c, tmpl.format(c=c.name), tr))
    return options


_DEFAULTABLE = {INT.sql: st.integers(-5, 5), STRING.sql: st.sampled_from(["a", "b"])}


def _default_sql(value: int | str) -> str:
    return f"'{value}'" if isinstance(value, str) else str(value)


def _candidates(t: TableModel, features: Features) -> list[str]:
    kinds = ["add_column"]
    if features.drop_column and t.droppable(features) and len(t.columns) > 1:
        kinds.append("drop_column")
    if features.alter_type and any(_widenings(c.type) for c in t.columns):
        kinds.append("alter_type")
    if features.alter_comment:
        kinds.append("alter_comment")
    if features.alter_nullability:
        kinds.append("nullability")
    if features.reorder and len(t.columns) > 1 and (features.multiple_reorders or not t.reordered):
        kinds.append("reorder")
    if features.partition:
        kinds.append("add_partition")
        if t.partition:
            kinds.append("drop_partition")
    if features.sort:
        kinds.append("sort")
    if features.props:
        kinds.append("set_props")
    if features.unset_props:
        kinds.append("unset_props")
    if features.identifier:
        kinds.append("identifier")
    if features.v3:
        if t.fv < 3:
            kinds.append("upgrade_v3")
        else:
            kinds += ["set_default", "promote_date"]
    return kinds


@st.composite
def _step(draw: st.DrawFn, t: TableModel, features: Features) -> str | None:
    """Emit one ALTER against *t* and mutate the model; None means 'skip'."""
    kind = draw(st.sampled_from(_candidates(t, features)))
    tbl = _q(t.name)

    if kind == "add_column":
        ctype = draw(st.sampled_from(PRIMITIVES))
        col = Column(t.fresh_name(), ctype)
        sql = f"ALTER TABLE {tbl} ADD COLUMN {col.name} {ctype.sql}"
        if t.fv >= 3 and ctype.sql in _DEFAULTABLE and draw(st.booleans()):
            col.default = draw(_DEFAULTABLE[ctype.sql])
            col.required = draw(st.booleans())
            if col.required:
                sql += " NOT NULL"
            sql += f" DEFAULT {_default_sql(col.default)}"
        if features.column_comments_on_add and draw(st.booleans()):
            lit, col.doc = draw(_DOCS)
            sql += f" COMMENT '{lit}'"
        t.columns.append(col)
        return sql + ";"

    if kind == "drop_column":
        col = draw(st.sampled_from(t.droppable(features)))
        t.columns.remove(col)
        return f"ALTER TABLE {tbl} DROP COLUMN {col.name};"

    if kind == "alter_type":
        col = draw(st.sampled_from([c for c in t.columns if _widenings(c.type)]))
        new = draw(st.sampled_from(_widenings(col.type)))
        col.type = new
        t.altered.add(col.name)
        return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} TYPE {new.sql};"

    if kind == "alter_comment":
        col = draw(st.sampled_from(t.columns))
        lit, col.doc = draw(_DOCS)
        t.altered.add(col.name)
        return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} COMMENT '{lit}';"

    if kind == "nullability":
        col = draw(st.sampled_from(t.columns))
        t.altered.add(col.name)
        # Spark-Iceberg only relaxes nullability; SET NOT NULL is a no-op on a
        # required column and refused on an optional one.
        if not col.required:
            return None
        if draw(st.booleans()):
            t.asserted_required.add(col.name)
            return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} SET NOT NULL;"
        if col.name in t.identifier:
            return None
        pinned_required = t.ever_identifier | t.asserted_required
        if col.name in pinned_required and not features.relax_former_identifier:
            return None
        col.required = False
        return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} DROP NOT NULL;"

    if kind == "reorder":
        col = draw(st.sampled_from(t.columns))
        others = [c for c in t.columns if c is not col]
        t.columns.remove(col)
        t.altered.add(col.name)
        t.reordered = True
        if draw(st.booleans()):
            t.columns.insert(0, col)
            return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} FIRST;"
        anchor = draw(st.sampled_from(others))
        t.altered.add(anchor.name)
        t.columns.insert(t.columns.index(anchor) + 1, col)
        return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} AFTER {anchor.name};"

    if kind == "add_partition":
        options = _partition_options(t, features)
        if not options:
            return None
        col, expr, tr = draw(st.sampled_from(options))
        t.partition.append(PartitionField(col.name, tr, expr))
        t.pinned.add(col.name)
        return f"ALTER TABLE {tbl} ADD PARTITION FIELD {expr};"

    if kind == "drop_partition":
        pf = draw(st.sampled_from(t.partition))
        t.partition.remove(pf)
        t.dropped_partitions.add((pf.source, pf.transform))
        return f"ALTER TABLE {tbl} DROP PARTITION FIELD {pf.sql};"

    if kind == "sort":
        cols = draw(st.lists(st.sampled_from(t.columns), min_size=1, max_size=3, unique_by=id))
        parts: list[str] = []
        order: list[tuple[str, str]] = []
        for c in cols:
            desc = draw(st.booleans())
            parts.append(f"{c.name} DESC" if desc else c.name)
            order.append((c.name, "desc" if desc else "asc"))
            t.pinned.add(c.name)
        t.sort = order
        t.props[DISTRIBUTION_MODE] = "range"
        return f"ALTER TABLE {tbl} WRITE ORDERED BY {', '.join(parts)};"

    if kind == "set_props":
        kv = draw(st.dictionaries(_PROP_KEYS, _PROP_VALS, min_size=1, max_size=3))
        t.props.update(kv)
        body = ", ".join(f"'{k}'='{v}'" for k, v in kv.items())
        return f"ALTER TABLE {tbl} SET TBLPROPERTIES ({body});"

    if kind == "unset_props":
        keys = draw(st.lists(_PROP_KEYS, min_size=1, max_size=2, unique=True))
        for k in keys:
            t.props.pop(k, None)
        return f"ALTER TABLE {tbl} UNSET TBLPROPERTIES ({', '.join(repr(k) for k in keys)});"

    if kind == "identifier":
        eligible = [c for c in t.columns if c.required and c.type.family not in {"float", "double"}]
        if t.identifier and draw(st.booleans()):
            drop = draw(st.sampled_from(sorted(t.identifier)))
            t.identifier.discard(drop)
            t.altered.add(drop)
            return f"ALTER TABLE {tbl} DROP IDENTIFIER FIELDS {drop};"
        if not eligible:
            return None
        chosen = draw(st.lists(st.sampled_from(eligible), min_size=1, max_size=2, unique_by=id))
        t.identifier = {c.name for c in chosen}
        t.altered |= t.identifier
        t.ever_identifier |= t.identifier
        return f"ALTER TABLE {tbl} SET IDENTIFIER FIELDS {', '.join(c.name for c in chosen)};"

    if kind == "upgrade_v3":
        t.fv = 3
        return f"ALTER TABLE {tbl} SET TBLPROPERTIES ('format-version' = '3');"

    if kind == "set_default":
        options = [c for c in t.columns if c.type.sql in _DEFAULTABLE]
        if not options:
            return None
        col = draw(st.sampled_from(options))
        t.altered.add(col.name)
        # A required column can't lose its default (rows need a value).
        if col.default is not None and not col.required and draw(st.booleans()):
            col.default = None
            return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} DROP DEFAULT;"
        col.default = draw(_DEFAULTABLE[col.type.sql])
        return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} SET DEFAULT {_default_sql(col.default)};"

    if kind == "promote_date":
        # Partition/sort-pinned dates are skipped: bucket/identity on a date
        # gives different values once it's a timestamp, so the spec forbids it.
        options = [c for c in t.columns if c.type == DATE and c.name not in t.pinned]
        if not options:
            return None
        col = draw(st.sampled_from(options))
        col.type = TS_NTZ
        t.altered.add(col.name)
        return f"ALTER TABLE {tbl} ALTER COLUMN {col.name} TYPE TIMESTAMP_NTZ;"

    raise AssertionError(kind)


@st.composite
def evolution_scripts(
    draw: st.DrawFn,
    features: Features = Features(),
    min_steps: int = 1,
    max_steps: int = 15,
) -> EvolutionScript:
    statements = ["CREATE NAMESPACE IF NOT EXISTS {ns};"]
    tables: dict[str, TableModel] = {}
    n_tables = draw(st.integers(min_value=1, max_value=features.max_tables))
    for i in range(n_tables):
        sql, model = draw(_create(f"t{i}", features))
        statements.append(sql)
        tables[model.name] = model

    n_steps = draw(st.integers(min_value=min_steps, max_value=max_steps))
    for _ in range(n_steps):
        t = tables[draw(st.sampled_from(sorted(tables)))]
        snapshot = copy.deepcopy(t)
        stmt = draw(_step(t, features))
        if stmt is None:
            tables[t.name] = snapshot
            continue
        statements.append(stmt)

    expected: dict[str, dict[str, Any]] = {
        name: m.fingerprint() for name, m in sorted(tables.items())
    }
    # Views last: a view's schema is inferred when it's applied, so a later
    # ALTER of its source would (correctly) make a replay infer differently.
    if features.views:
        for name, t in sorted(tables.items()):
            if not draw(st.booleans()):
                continue
            cols = draw(st.lists(st.sampled_from(t.columns), min_size=1, unique_by=id))
            select = ", ".join(c.name for c in cols)
            statements.append(
                f"CREATE OR REPLACE VIEW {_q('v_' + name)} AS SELECT {select} FROM {_q(name)};"
            )
            expected[f"view:v_{name}"] = {
                "columns": [(c.name, c.type.iceberg, False, None, None) for c in cols]
            }

    return EvolutionScript(statements=statements, expected=expected)


def apply_script(
    catalog: Catalog,
    sql: str,
    view_dialects: Sequence[str] = ("spark",),
    view_source_dialect: str = "spark",
) -> None:
    """Parse and dispatch *sql* statement-by-statement, mirroring the runner loop."""
    for stmt in parse_script(sql, view_source_dialect=view_source_dialect):
        dispatch(catalog, stmt, view_dialects=view_dialects)


def catalog_fingerprint(catalog: Catalog, namespace: str) -> dict[str, dict[str, Any]]:
    """Read every table in *namespace* into the same shape as ``TableModel.fingerprint``."""
    out: dict[str, dict[str, Any]] = {}
    for ident in sorted(catalog.list_tables(namespace)):
        table = catalog.load_table(ident)
        schema = table.schema()
        names = {f.field_id: f.name for f in schema.fields}
        props = {
            k: v
            for k, v in table.properties.items()
            if k.startswith(PROP_PREFIX) or k in {DISTRIBUTION_MODE, TABLE_COMMENT}
        }
        out[ident[-1]] = {
            "columns": [
                (f.name, str(f.field_type), f.required, f.doc, f.write_default)
                for f in schema.fields
            ],
            "partition": [
                (names.get(pf.source_id, f"<dropped:{pf.source_id}>"), str(pf.transform))
                for pf in table.spec().fields
            ],
            "sort": [
                (names.get(sf.source_id, f"<dropped:{sf.source_id}>"), str(sf.direction.value))
                for sf in table.sort_order().fields
            ],
            "props": dict(sorted(props.items())),
            "identifier": sorted(schema.identifier_field_names()),
        }
    for ident in sorted(catalog.list_views(namespace)):
        view_schema = catalog.load_view(ident).schema()
        out[f"view:{ident[-1]}"] = {
            "columns": [
                (f.name, str(f.field_type), f.required, f.doc, f.write_default)
                for f in view_schema.fields
            ]
        }
    return out
