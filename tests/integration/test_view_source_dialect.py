"""Views authored in another dialect ([views].source_dialect), against Lakekeeper.

The point of the option is migrating existing views without rewriting them:
the source dialect's representation must be the body byte-for-byte, the other
dialects are transpiled, and re-applying must not add versions.
"""

from __future__ import annotations

import pytest
from pyiceberg.catalog.rest import RestCatalog

from saltation.errors import DispatchError
from tests.fixtures.evolution_strategies import apply_script

DIALECTS = ("trino", "spark", "starrocks")

# Trino-only syntax a Spark parse would misread: a double-quoted identifier,
# ARRAY[...] constructor with 1-based subscript, || concatenation.
EXTERNAL_BODY = (
    "SELECT _key AS device_id, ARRAY[a, b][1] AS first_id, _key || ':' || kind AS label\n"
    'FROM redpanda.default."devices.array.btr"'
)


def _reps(catalog: RestCatalog, ident: tuple[str, str]) -> dict[str, str]:
    view = catalog.load_view(ident)
    return {r.root.dialect: r.root.sql for r in view.current_version().representations}


def test_trino_body_with_typed_columns_is_stored_verbatim(
    rest_catalog: RestCatalog, ns: str
) -> None:
    script = (
        f"CREATE OR REPLACE VIEW {ns}.live "
        f"(device_id STRING COMMENT 'kafka key', first_id STRING, label STRING) AS\n"
        f"{EXTERNAL_BODY};"
    )
    apply_script(rest_catalog, script, DIALECTS, view_source_dialect="trino")

    reps = _reps(rest_catalog, (ns, "live"))
    assert reps["trino"] == EXTERNAL_BODY
    assert "`devices.array.btr`" in reps["spark"]
    assert "`devices.array.btr`" in reps["starrocks"]
    view = rest_catalog.load_view((ns, "live"))
    assert [(f.name, str(f.field_type), f.doc) for f in view.schema().fields] == [
        ("device_id", "string", "kafka key"),
        ("first_id", "string", None),
        ("label", "string", None),
    ]

    apply_script(rest_catalog, script, DIALECTS, view_source_dialect="trino")
    assert len(rest_catalog.load_view((ns, "live")).metadata.versions) == 1


def test_trino_body_over_catalog_tables_infers_types(rest_catalog: RestCatalog, ns: str) -> None:
    apply_script(
        rest_catalog,
        f"CREATE TABLE {ns}.t (id BIGINT, name STRING, ts TIMESTAMP) USING iceberg;",
    )
    body = f"SELECT \"id\", upper(name) || '!' AS shout, CAST(ts AS DATE) AS d FROM {ns}.t"
    apply_script(
        rest_catalog,
        f"CREATE VIEW {ns}.v AS {body};",
        DIALECTS,
        view_source_dialect="trino",
    )
    view = rest_catalog.load_view((ns, "v"))
    assert [(f.name, str(f.field_type)) for f in view.schema().fields] == [
        ("id", "long"),
        ("shout", "string"),
        ("d", "date"),
    ]
    assert _reps(rest_catalog, (ns, "v"))["trino"] == body


def test_untyped_external_column_names_both_fixes(rest_catalog: RestCatalog, ns: str) -> None:
    with pytest.raises(DispatchError, match="column list"):
        apply_script(
            rest_catalog,
            f"CREATE VIEW {ns}.bad AS {EXTERNAL_BODY};",
            DIALECTS,
            view_source_dialect="trino",
        )
    assert not rest_catalog.view_exists((ns, "bad"))


SPARK_BODY = (
    "SELECT _key AS device_id, array(a, b)[0] AS first_id, concat(_key, ':', kind) AS label\n"
    "FROM redpanda.default.`devices.array.btr`"
)
MULTI = (
    "CREATE OR REPLACE VIEW {ns}.live (device_id STRING, first_id STRING, label STRING)\n"
    f"  DIALECT trino AS $$\n{EXTERNAL_BODY}\n$$\n"
    f"  DIALECT spark AS $$\n{SPARK_BODY}\n$$;"
)


def test_dialect_bodies_are_stored_verbatim_and_others_transpiled(
    rest_catalog: RestCatalog, ns: str
) -> None:
    apply_script(rest_catalog, MULTI.format(ns=ns), DIALECTS)
    reps = _reps(rest_catalog, (ns, "live"))
    assert reps["trino"] == EXTERNAL_BODY
    assert reps["spark"] == SPARK_BODY
    # No starrocks body: transpiled from the first (trino) body.
    assert "`devices.array.btr`" in reps["starrocks"]

    apply_script(rest_catalog, MULTI.format(ns=ns), DIALECTS)
    assert len(rest_catalog.load_view((ns, "live")).metadata.versions) == 1


def test_replacing_one_dialect_body_keeps_the_others(rest_catalog: RestCatalog, ns: str) -> None:
    apply_script(rest_catalog, MULTI.format(ns=ns), DIALECTS)
    edited = MULTI.replace("FROM redpanda.default.`devices.array.btr`", "FROM redpanda.default.`x`")
    apply_script(rest_catalog, edited.format(ns=ns), DIALECTS)
    view = rest_catalog.load_view((ns, "live"))
    reps = _reps(rest_catalog, (ns, "live"))
    assert len(view.metadata.versions) == 2
    assert reps["trino"] == EXTERNAL_BODY
    assert reps["spark"].endswith("`x`")


def test_dialect_bodies_must_agree_on_inferred_types(rest_catalog: RestCatalog, ns: str) -> None:
    apply_script(rest_catalog, f"CREATE TABLE {ns}.t (id BIGINT, name STRING) USING iceberg;")
    script = (
        f"CREATE VIEW {ns}.v DIALECT trino AS $$SELECT id FROM {ns}.t$$ "
        f"DIALECT spark AS $$SELECT name AS id FROM {ns}.t$$;"
    )
    with pytest.raises(DispatchError, match="disagree on the type of column 'id'"):
        apply_script(rest_catalog, script, DIALECTS)
    assert not rest_catalog.view_exists((ns, "v"))


def test_trino_body_with_a_backslash_string_is_stored_verbatim(
    rest_catalog: RestCatalog, ns: str
) -> None:
    apply_script(rest_catalog, f"CREATE TABLE {ns}.t (path STRING) USING iceberg;")
    body = f"SELECT replace(path, '\\', '/') AS path FROM {ns}.t"
    apply_script(
        rest_catalog,
        f"CREATE VIEW {ns}.v AS {body};\nALTER TABLE {ns}.t ADD COLUMN c INT;",
        DIALECTS,
        view_source_dialect="trino",
    )
    assert _reps(rest_catalog, (ns, "v"))["trino"] == body
    assert [f.name for f in rest_catalog.load_table((ns, "t")).schema().fields] == ["path", "c"]
