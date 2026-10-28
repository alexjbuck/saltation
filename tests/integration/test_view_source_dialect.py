"""Views authored in another dialect ([views].source_dialect), against Lakekeeper.

The point of the option is migrating existing views without rewriting them:
the source dialect's representation must be the body byte-for-byte, the other
dialects are transpiled, and re-applying must not add versions.
"""

from __future__ import annotations

import pytest
from pyiceberg.catalog.rest import RestCatalog

from cambrian.errors import DispatchError
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
