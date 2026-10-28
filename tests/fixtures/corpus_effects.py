"""Expected catalog effect of every positive and invalid corpus entry.

Each :class:`Effect` says what to build first (``base`` + ``setup``) and the
exact :func:`tests.fixtures.catalog_state.diff` each target must show after the
corpus SQL runs. Expectations follow Spark-Iceberg semantics (docs, plus the
Iceberg Spark extension source where the docs are silent), not saltation's
current behaviour. ``foo`` is the namespace placeholder; ``s3://bucket/`` is
rewritten to a location inside the test warehouse.

Targets are table identifiers (``foo.t``), ``ns:<namespace>`` or
``view:<identifier>``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal

from tests.fixtures.catalog_state import ANY

__all__ = ["BASES", "EFFECTS", "VIEW_DIALECTS", "Effect"]

Base = Literal["ns", "wide", "pair", "rows"]

WIDE_SQL = (
    "CREATE TABLE foo.t (id BIGINT NOT NULL, name STRING, data STRING NOT NULL, category STRING, "
    "ts TIMESTAMP, d DATE, measurement FLOAT, c INT, other INT, a INT, b INT, "
    "point STRUCT<x: DOUBLE, y: DOUBLE, z: DOUBLE>, "
    "location STRUCT<lat: DOUBLE, lon: DOUBLE, zone: INT>, "
    "pts ARRAY<STRUCT<x: DOUBLE>>, m MAP<STRING, STRUCT<a: INT>>) USING iceberg "
    "TBLPROPERTIES ('read.split.target-size'='1', 'a'='x', 'b'='y', 'c'='z')"
)
WIDE_COLS = [
    "id:long!",
    "name:string",
    "data:string!",
    "category:string",
    "ts:timestamptz",
    "d:date",
    "measurement:float",
    "c:int",
    "other:int",
    "a:int",
    "b:int",
    "point:struct",
    "point.x:double",
    "point.y:double",
    "point.z:double",
    "location:struct",
    "location.lat:double",
    "location.lon:double",
    "location.zone:int",
    "pts:list",
    "pts.element:struct",
    "pts.element.x:double",
    "m:map",
    "m.key:string!",
    "m.value:struct",
    "m.value.a:int",
]
WIDE_PROPS = {"read.split.target-size": "1", "a": "x", "b": "y", "c": "z"}

BASES: dict[str, list[str]] = {
    "ns": ["CREATE NAMESPACE foo"],
    "wide": ["CREATE NAMESPACE foo", WIDE_SQL],
    "pair": ["CREATE NAMESPACE foo", "CREATE TABLE foo.t (id INT, name STRING) USING iceberg"],
    "rows": [
        "CREATE NAMESPACE foo",
        "CREATE TABLE foo.t (id INT, category STRING) USING iceberg",
        "INSERT INTO foo.t VALUES (1, 'x'), (2, 'x'), (11, 'x'), (12, 'y')",
    ],
}


@dataclass(frozen=True)
class Effect:
    effects: dict[str, dict[str, Any]]
    base: Base | None = "wide"
    setup: tuple[str, ...] = ()
    # Data ops append on every run; everything else must be a no-op the second time.
    reapply: bool = True
    # Test catalogs (by rig name) that refuse this, with why; saltation's request
    # is still correct. Run as strict xfails, so a catalog that starts
    # accepting it is noticed.
    catalog_refuses: dict[str, str] = field(default_factory=dict)


def created(
    cols: list[str],
    *,
    spec: list[str] | None = None,
    sort: list[str] | None = None,
    props: dict[str, str] | None = None,
    ident: list[str] | None = None,
    fv: int = 2,
    loc: Any = ANY,
) -> dict[str, Any]:
    return {
        "exists": True,
        "state": {
            "fv": fv,
            "cols": cols,
            "ident": ident or [],
            "spec": spec or [],
            "sort": sort or [],
            "props": props or {},
            "loc": loc,
            "rows": 0,
        },
    }


def ns_created(props: dict[str, str] | None = None) -> dict[str, Any]:
    return {"exists": True, "state": {"props": props or {}}}


V3 = "ALTER TABLE foo.t SET TBLPROPERTIES ('format-version' = '3')"
# Lakekeeper rejects even a bare "geometry" schema type (HTTP 422). PyIceberg
# 0.12 also quotes the CRS (geometry('EPSG:4326')) where the spec has none (#29, #21).
_NO_GEO = "Lakekeeper (v0.14.2) can't deserialize geometry/geography schema types"
# PyIceberg quotes the CRS/algorithm ("geography('OGC:CRS84', 'vincenty')"),
# which Iceberg Java's type parser rejects (#21).
_QUOTED_GEO = "Iceberg Java can't parse PyIceberg's quoted geography params"


# Views store one SQL text per dialect; the effect tests store two so the
# transpiled representation is checked as well.
VIEW_DIALECTS = ("spark", "trino")
VIEW_V = "CREATE VIEW foo.v AS SELECT id FROM foo.t"


def view_created(
    cols: list[str], sql: str, *, trino: str | None = None, props: dict[str, str] | None = None
) -> dict[str, Any]:
    return {
        "exists": True,
        "state": {
            "cols": cols,
            "sql": {"spark": sql, "trino": trino or sql},
            "props": props or {},
            "versions": 1,
        },
    }


def both(old: str, new: str) -> dict[str, list[str]]:
    return {"spark": [old, new], "trino": [old, new]}


def v(effect: dict[str, Any], **kw: Any) -> Effect:
    return Effect({"view:foo.v": effect}, **kw)


def t(effect: dict[str, Any], **kw: Any) -> Effect:
    return Effect({"foo.t": effect}, **kw)


def cols(*, minus: list[str] | None = None, plus: list[str] | None = None) -> dict[str, Any]:
    return {"cols": {"-": minus or [], "+": plus or []}}


def moved(name: str, *, after: str | None) -> list[str]:
    """WIDE_COLS with top-level or nested *name* (and its children) moved."""
    block = [c for c in WIDE_COLS if c.split(":")[0] == name or c.startswith(name + ".")]
    rest = [c for c in WIDE_COLS if c not in block]
    if after is None:
        parent = name.rsplit(".", 1)[0] if "." in name else None
        idx = (
            0
            if parent is None
            else next(i for i, c in enumerate(rest) if c.startswith(parent + ":")) + 1
        )
    else:
        anchor = [
            i for i, c in enumerate(rest) if c.split(":")[0] == after or c.startswith(after + ".")
        ]
        idx = anchor[-1] + 1
    return rest[:idx] + block + rest[idx:]


def sort_set(*fields: str, mode: tuple[str | None, str] | None = ("range",)) -> dict[str, Any]:
    out: dict[str, Any] = {"sort": {"-": [], "+": _positioned(list(fields))}}
    if mode is not None:
        out["props"] = {
            "write.distribution-mode": [None, mode[0]] if len(mode) == 1 else list(mode)
        }
    return out


def _positioned(items: list[str]) -> list[str]:
    return [
        f"{x} first" if i == 0 else f"{x} after {items[i - 1].split(':', 1)[0]}"
        for i, x in enumerate(items)
    ]


EFFECTS: dict[str, Effect] = {
    # --- namespaces -------------------------------------------------------
    "ns_create": Effect({"ns:foo": ns_created()}, base=None),
    "ns_create_dotted": Effect({"ns:foo.bar": ns_created()}, base="ns"),
    "ns_create_if_not_exists": Effect({"ns:foo": ns_created()}, base=None),
    "ns_create_with_properties": Effect({"ns:foo": ns_created({"owner": "eng"})}, base=None),
    "ns_create_comment": Effect(
        {"ns:foo": ns_created({"comment": "docs", "owner": "eng"})}, base=None
    ),
    "ns_drop": Effect({"ns:foo": {"exists": False}}, base="ns"),
    "ns_drop_if_exists": Effect({"ns:foo": {"exists": False}}, base="ns"),
    "ns_alter_set_properties": Effect(
        {"ns:foo": {"props": {"owner": ["eng", "data"]}}},
        base=None,
        setup=("CREATE NAMESPACE foo WITH PROPERTIES ('owner' = 'eng')",),
    ),
    # --- CREATE TABLE -----------------------------------------------------
    "ct_simple": t(created(["id:long", "name:string"]), base="ns"),
    "ct_if_not_exists": t(created(["id:long"]), base="ns"),
    "ct_all_primitives": t(
        created(
            [
                "a:boolean",
                "b:int",
                "c:long",
                "d:float",
                "e:double",
                "f:decimal(10, 2)",
                "g:date",
                "h:timestamptz",
                "i:timestamp",
                "j:string",
                "k:binary",
            ]
        ),
        base="ns",
    ),
    "ct_timestamptz_alias": t(created(["ts:timestamptz"]), base="ns"),
    "ct_decimal_precision_scale": t(created(["amount:decimal(38, 9)"]), base="ns"),
    "ct_not_null": t(created(["id:long!", "name:string"]), base="ns"),
    "ct_column_comment": t(created(["id:long#unique id", "name:string"]), base="ns"),
    "ct_table_comment": t(created(["id:long"], props={"comment": "table docs"}), base="ns"),
    "ct_location": t(created(["id:long"], loc="s3://bucket/path"), base="ns"),
    "ct_tblproperties": t(
        created(["id:long"], props={"write.format.default": "parquet"}), base="ns"
    ),
    "ct_full": t(
        created(
            ["id:long!#pk", "data:string"],
            spec=["id:bucket[16]:id_bucket"],
            props={"comment": "docs", "k": "v"},
            loc="s3://bucket/t",
        ),
        base="ns",
    ),
    "ct_struct": t(created(["point:struct", "point.x:double", "point.y:double"]), base="ns"),
    "ct_array": t(created(["tags:list", "tags.element:string"]), base="ns"),
    "ct_map": t(created(["attrs:map", "attrs.key:string!", "attrs.value:int"]), base="ns"),
    "ct_nested_combo": t(
        created(
            [
                "points:list",
                "points.element:struct",
                "points.element.x:double",
                "points.element.y:double",
                "lookup:map",
                "lookup.key:string!",
                "lookup.value:list",
                "lookup.value.element:int",
            ]
        ),
        base="ns",
    ),
    "ct_struct_field_not_null_comment": t(
        created(["s:struct", "s.x:double!#xx", "s.y:int"]), base="ns"
    ),
    "ct_char_varchar_small_ints": t(created(["a:string", "b:string", "c:int", "d:int"]), base="ns"),
    # Spark builds CREATE-time specs with PartitionSpec.Builder, whose default
    # names are <col>_bucket / <col>_trunc (no width suffix, unlike ALTER).
    "ct_part_identity_col": t(
        created(["id:long", "category:string"], spec=["category:identity:category"]), base="ns"
    ),
    "ct_part_bucket": t(created(["id:long"], spec=["id:bucket[16]:id_bucket"]), base="ns"),
    "ct_part_truncate": t(
        created(["data:string"], spec=["data:truncate[4]:data_trunc"]), base="ns"
    ),
    "ct_part_years": t(created(["ts:timestamptz"], spec=["ts:year:ts_year"]), base="ns"),
    "ct_part_year": t(created(["ts:timestamptz"], spec=["ts:year:ts_year"]), base="ns"),
    "ct_part_months": t(created(["ts:timestamptz"], spec=["ts:month:ts_month"]), base="ns"),
    "ct_part_month": t(created(["ts:timestamptz"], spec=["ts:month:ts_month"]), base="ns"),
    "ct_part_days": t(created(["ts:timestamptz"], spec=["ts:day:ts_day"]), base="ns"),
    "ct_part_day": t(created(["ts:timestamptz"], spec=["ts:day:ts_day"]), base="ns"),
    "ct_part_date": t(created(["ts:timestamptz"], spec=["ts:day:ts_day"]), base="ns"),
    "ct_part_hours": t(created(["ts:timestamptz"], spec=["ts:hour:ts_hour"]), base="ns"),
    "ct_part_hour": t(created(["ts:timestamptz"], spec=["ts:hour:ts_hour"]), base="ns"),
    "ct_part_date_hour": t(created(["ts:timestamptz"], spec=["ts:hour:ts_hour"]), base="ns"),
    "ct_part_multi": t(
        created(
            ["id:long", "ts:timestamptz", "category:string"],
            spec=["id:bucket[16]:id_bucket", "ts:day:ts_day", "category:identity:category"],
        ),
        base="ns",
    ),
    "ct_part_nested": t(
        created(["p:struct", "p.x:int"], spec=["p.x:bucket[4]:p.x_bucket"]), base="ns"
    ),
    "ct_like": Effect(
        {
            "foo.t2": created(
                WIDE_COLS,
                spec=["category:identity:category"],
                sort=["id:identity:asc:nulls-first"],
                props={**WIDE_PROPS, "write.distribution-mode": "range"},
            )
        },
        setup=(
            "ALTER TABLE foo.t ADD PARTITION FIELD category",
            "ALTER TABLE foo.t WRITE ORDERED BY id",
        ),
    ),
    # --- DROP / RENAME ----------------------------------------------------
    "dt_simple": t({"exists": False}),
    "dt_if_exists": t({"exists": False}),
    "dt_purge": t({"exists": False}),
    "dt_if_exists_purge": t({"exists": False}),
    "rename_table": Effect(
        {"foo.t": {"exists": False}, "foo.t2": created(WIDE_COLS, props=WIDE_PROPS)}
    ),
    # --- columns ----------------------------------------------------------
    "add_column_single": t(
        cols(plus=["c:int after m.value.a"]), setup=("ALTER TABLE foo.t DROP COLUMN c",)
    ),
    "add_column_comment": t(
        cols(plus=["c:string#docs after m.value.a"]), setup=("ALTER TABLE foo.t DROP COLUMN c",)
    ),
    "add_column_not_null": t({}, setup=("ALTER TABLE foo.t DROP COLUMN c",)),
    "add_column_first": t(cols(plus=["c:int first"]), setup=("ALTER TABLE foo.t DROP COLUMN c",)),
    "add_column_after": t(
        cols(plus=["c:int after name"]), setup=("ALTER TABLE foo.t DROP COLUMN c",)
    ),
    "add_column_nested": t(
        cols(plus=["point.z:double after point.y"]),
        setup=("ALTER TABLE foo.t DROP COLUMN point.z",),
    ),
    "add_column_nested_first": t(cols(plus=["point.v:double after point"])),
    "add_column_list_element": t(cols(plus=["pts.element.z:double after pts.element.x"])),
    "add_column_map_value": t(cols(plus=["m.value.b:int after m.value.a"])),
    "add_columns_plural": t(
        cols(plus=["a:int after m.value.a", "b:string after a", "c:long after b"]),
        setup=("ALTER TABLE foo.t DROP COLUMNS (a, b, c)",),
    ),
    "add_columns_plural_comment": t(
        cols(plus=["a:int#x after m.value.a", "b:string after a"]),
        setup=("ALTER TABLE foo.t DROP COLUMNS (a, b)",),
    ),
    "drop_column_single": t(cols(minus=["c:int"])),
    "drop_column_nested": t(cols(minus=["point.z:double"])),
    "drop_columns_plural": t(cols(minus=["a:int", "b:int"])),
    "rename_column": t(cols(minus=["data:string!"], plus=["payload:string! after name"])),
    "rename_column_nested": t(
        cols(minus=["location.lat:double"], plus=["location.latitude:double after location"])
    ),
    "alter_column_type": t(cols(minus=["measurement:float"], plus=["measurement:double after d"])),
    "alter_column_comment": t(
        cols(minus=["measurement:float"], plus=["measurement:float#unit kb/s after d"])
    ),
    "alter_column_type_comment": t(
        cols(minus=["measurement:float"], plus=["measurement:double#unit after d"])
    ),
    "alter_column_first": t({"cols": {"order": moved("c", after=None)}}),
    "alter_column_after": t({"cols": {"order": moved("c", after="other")}}),
    "alter_column_set_not_null": t({}),
    "alter_column_drop_not_null": t(cols(minus=["id:long!"], plus=["id:long first"])),
    "alter_column_nested_comment": t(
        cols(minus=["point.x:double"], plus=["point.x:double#xx after point"])
    ),
    "alter_column_nested_type": t(
        cols(minus=["m.value.a:int"], plus=["m.value.a:long after m.value"])
    ),
    "alter_column_nested_after": t({"cols": {"order": moved("point.z", after="point.x")}}),
    # --- partition evolution (UpdatePartitionSpec names: <col>_bucket_<n>) --
    "apf_identity": t({"spec": {"-": [], "+": ["category:identity:category first"]}}),
    "apf_bucket": t({"spec": {"-": [], "+": ["id:bucket[16]:id_bucket_16 first"]}}),
    "apf_truncate": t({"spec": {"-": [], "+": ["data:truncate[4]:data_trunc_4 first"]}}),
    "apf_year": t({"spec": {"-": [], "+": ["ts:year:ts_year first"]}}),
    "apf_day": t({"spec": {"-": [], "+": ["ts:day:ts_day first"]}}),
    "apf_hour": t({"spec": {"-": [], "+": ["ts:hour:ts_hour first"]}}),
    "apf_alias": t({"spec": {"-": [], "+": ["id:bucket[16]:shard first"]}}),
    "apf_nested": t(
        {"spec": {"-": [], "+": ["location.zone:bucket[4]:location.zone_bucket_4 first"]}}
    ),
    "dpf_identity": t(
        {"spec": {"-": ["category:identity:category"], "+": []}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD category",),
    ),
    "dpf_transform": t(
        {"spec": {"-": ["id:bucket[16]:id_bucket_16"], "+": []}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id)",),
    ),
    "dpf_alias_name": t(
        {"spec": {"-": ["id:bucket[16]:shard"], "+": []}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id) AS shard",),
    ),
    "dpf_column_of_aliased_identity": t(
        {"spec": {"-": ["category:identity:cat"], "+": []}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD category AS cat",),
    ),
    "rpf_by_transform": t(
        {"spec": {"-": ["id:bucket[16]:id_bucket_16"], "+": ["id:bucket[8]:id_bucket_8 first"]}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD bucket(16, id)",),
    ),
    "rpf_missing": t({}),
    "rpf_basic": t(
        {"spec": {"-": ["ts:hour:ts_day"], "+": ["ts:day:ts_day first"]}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD hour(ts) AS ts_day",),
    ),
    "rpf_alias": t(
        {"spec": {"-": ["ts:hour:ts_day"], "+": ["ts:day:day_of_ts first"]}},
        setup=("ALTER TABLE foo.t ADD PARTITION FIELD hour(ts) AS ts_day",),
    ),
    # --- write order: Spark replaces the whole sort order every time;
    # distribution mode is range / none(UNORDERED) / hash(DISTRIBUTED) and
    # untouched by LOCALLY ORDERED BY. Default nulls: ASC first, DESC last.
    "wob_bare": t(sort_set("category:identity:asc:nulls-first", "id:identity:asc:nulls-first")),
    "wob_paren": t(sort_set("category:identity:asc:nulls-first", "id:identity:asc:nulls-first")),
    "wob_asc_desc": t(sort_set("category:identity:asc:nulls-first", "id:identity:desc:nulls-last")),
    "wob_nulls": t(sort_set("category:identity:asc:nulls-last", "id:identity:desc:nulls-first")),
    "wob_transform": t(sort_set("id:bucket[16]:asc:nulls-first")),
    "wob_transform_days": t(sort_set("ts:day:desc:nulls-last", "id:bucket[16]:asc:nulls-first")),
    "wob_locally": t(
        sort_set("category:identity:asc:nulls-first", "id:identity:asc:nulls-first", mode=None)
    ),
    "wob_distributed": t(
        {
            "sort": {"-": ["id:identity:asc:nulls-first"], "+": []},
            "props": {"write.distribution-mode": ["range", "hash"]},
        },
        setup=("ALTER TABLE foo.t WRITE ORDERED BY id",),
    ),
    "wob_distributed_locally": t(
        sort_set("category:identity:asc:nulls-first", "id:identity:asc:nulls-first", mode=("hash",))
    ),
    "wob_unordered": t(
        {
            "sort": {"-": ["id:identity:asc:nulls-first"], "+": []},
            "props": {"write.distribution-mode": ["range", "none"]},
        },
        setup=("ALTER TABLE foo.t WRITE ORDERED BY id",),
    ),
    # --- properties / location -------------------------------------------
    "set_tblproperties": t({"props": {"read.split.target-size": ["1", "268435456"]}}),
    "set_tblproperties_multi": t({"props": {"a": ["x", "1"], "b": ["y", "2"]}}),
    "unset_tblproperties": t({"props": {"read.split.target-size": ["1", None]}}),
    "unset_tblproperties_multi": t(
        {"props": {"a": ["x", None], "b": ["y", None], "c": ["z", None]}}
    ),
    "set_comment_property": t({"props": {"comment": [None, "A table comment."]}}),
    "set_location": t(
        {"loc": [ANY, "s3://bucket/moved"]},
        catalog_refuses={"lakekeeper": "Lakekeeper manages table locations: SetLocationNotAllowed"},
    ),
    # --- identifier fields ------------------------------------------------
    "set_identifier_fields_single": t({"ident": {"-": [], "+": ["id first"]}}),
    "set_identifier_fields_multi": t({"ident": {"-": [], "+": ["data first", "id after data"]}}),
    "drop_identifier_fields": t(
        {"ident": {"-": ["id"], "+": []}}, setup=("ALTER TABLE foo.t SET IDENTIFIER FIELDS id",)
    ),
    # --- format v3 ------------------------------------------------------------
    "ct_time": t(created(["t:time"]), base="ns"),
    "ct_v3_geo_types": t(
        catalog_refuses={"lakekeeper": _NO_GEO, "gravitino": _QUOTED_GEO, "polaris": _QUOTED_GEO},
        effect=created(
            [
                "g:geometry",
                "g2:geometry('EPSG:4326')",
                "gg:geography",
                "gg2:geography('OGC:CRS84', 'vincenty')",
            ],
            fv=3,
        ),
        base="ns",
    ),
    "ct_v3_partitioned": t(
        created(
            ["id:long", "ts:timestamptz"],
            spec=["id:bucket[8]:id_bucket", "ts:day:ts_day"],
            fv=3,
        ),
        base="ns",
    ),
    "ct_v3_geo_on_v2": t({}, base="ns"),
    "ct_v3_default": t(created(["id:long", "status:string=new"], fv=3), base="ns"),
    "ct_default_on_v2": t({}, base="ns"),
    "upgrade_format_v3": t({"fv": [2, 3]}),
    "downgrade_format": t({}, setup=(V3,)),
    "add_column_default_v3": t(
        cols(plus=["c:int=5 after m.value.a"]), setup=(V3, "ALTER TABLE foo.t DROP COLUMN c")
    ),
    "add_column_not_null_default_v3": t(
        cols(plus=["c:int!=5 after m.value.a"]), setup=(V3, "ALTER TABLE foo.t DROP COLUMN c")
    ),
    "add_column_geometry_v3": t(
        cols(plus=["g:geometry after m.value.a"]),
        setup=(V3,),
        catalog_refuses={"lakekeeper": _NO_GEO},
    ),
    "alter_column_set_default": t(
        cols(minus=["c:int"], plus=["c:int=7 after measurement"]), setup=(V3,)
    ),
    "alter_column_drop_default": t(
        cols(minus=["c:int=7"], plus=["c:int after measurement"]),
        setup=(V3, "ALTER TABLE foo.t ALTER COLUMN c SET DEFAULT 7"),
    ),
    "promote_date_to_timestamp": t(
        cols(minus=["d:date"], plus=["d:timestamp after ts"]), setup=(V3,)
    ),
    "promote_date_bucketed": t(
        {}, setup=(V3, "ALTER TABLE foo.t ADD PARTITION FIELD bucket(4, d)")
    ),
    # --- views (stored as spark + trino; see VIEW_DIALECTS) ------------------
    "view_create": v(view_created(["id:long", "name:string"], "SELECT id, name FROM foo.t")),
    "view_create_existing": v({}, setup=(VIEW_V,)),
    "view_create_if_not_exists": v({}, setup=(VIEW_V,)),
    "view_create_or_replace": v(
        {
            "cols": {"-": [], "+": ["name:string after id"]},
            "sql": both("SELECT id FROM foo.t", "SELECT id, name FROM foo.t"),
            "versions": [1, 2],
        },
        setup=(VIEW_V,),
    ),
    "view_column_list": v(
        view_created(
            ["ident:long#the id", "nm:string"],
            "SELECT id, name FROM foo.t",
            props={"comment": "docs", "k": "v"},
        )
    ),
    "view_unqualified_source": v(view_created(["id:long"], "SELECT id FROM t")),
    "view_cast_external": v(
        view_created(
            ["k:string"],
            "SELECT CAST(key AS STRING) AS k FROM ext.topic",
            trino="SELECT TRY_CAST(key AS VARCHAR) AS k FROM ext.topic",
        )
    ),
    "view_untyped_external": v({}),
    "view_select_star": v(
        view_created(["id:int", "name:string"], "SELECT * FROM foo.t"), base="pair"
    ),
    "view_qualified_star": v(
        view_created(
            ["id:int", "name:string", "again:int"], "SELECT t.*, id AS again FROM foo.t AS t"
        ),
        base="pair",
    ),
    "view_unnamed_expression": v({}, base="pair"),
    "view_decimal_arithmetic": v(
        {}, base="ns", setup=("CREATE TABLE foo.d (amt DECIMAL(10, 2)) USING iceberg",)
    ),
    "view_on_view": Effect(
        {"view:foo.v2": view_created(["name:string"], "SELECT name FROM foo.v")},
        setup=("CREATE VIEW foo.v AS SELECT id, name FROM foo.t",),
    ),
    "drop_view": v({"exists": False}, setup=(VIEW_V,)),
    "drop_view_if_exists": v({"exists": False}, setup=(VIEW_V,)),
    "alter_view_as": v(
        {
            "cols": {"-": [], "+": ["name:string after id"]},
            "sql": both("SELECT id FROM foo.t", "SELECT id, name FROM foo.t"),
            "versions": [1, 2],
        },
        setup=(VIEW_V,),
    ),
    "alter_view_set_properties": v({"props": {"k": [None, "v"]}}, setup=(VIEW_V,)),
    "alter_view_unset_properties": v(
        {"props": {"k": ["v", None]}},
        setup=("CREATE VIEW foo.v TBLPROPERTIES ('k' = 'v', 'j' = 'w') AS SELECT id FROM foo.t",),
    ),
    "alter_view_rename": Effect(
        {
            "view:foo.v": {"exists": False},
            "view:foo.w": view_created(["id:long"], "SELECT id FROM foo.t"),
        },
        setup=(VIEW_V,),
    ),
    # --- data ops ---------------------------------------------------------
    "insert_values_single": t({"rows": [0, 1]}, base="pair", reapply=False),
    "insert_values_multi": t({"rows": [0, 2]}, base="pair", reapply=False),
    "insert_values_null": t({"rows": [0, 1]}, base="pair", reapply=False),
    "delete_where": t({"rows": [4, 3]}, base="rows"),
    "delete_where_predicate": t({"rows": [4, 3]}, base="rows"),
}
