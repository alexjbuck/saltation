"""An engine queries an Iceberg view saltation stored in that engine's dialect."""

from __future__ import annotations

import pytest
from pyiceberg.catalog.rest import RestCatalog

from .conftest import SaltationProject
from .engines import Engine, coerce_rows


def _script(ns: str, engine: Engine) -> str:
    # Fully qualified: Trino and StarRocks resolve a view body without the
    # view's catalog when the Iceberg view metadata carries no default-catalog.
    # The column list types the view, since the body reads a catalog-qualified
    # name saltation can't resolve on its own.
    body = (
        f"SELECT id, concat(upper(name), '!') AS shout FROM {engine.table_ref(ns, 'vt')} "
        "WHERE id > 1"
    )
    return (
        f"CREATE TABLE IF NOT EXISTS {ns}.vt (id BIGINT, name STRING) USING iceberg;\n"
        f"INSERT INTO {ns}.vt VALUES (1, 'a'), (2, 'b'), (3, 'c');\n"
        f"CREATE OR REPLACE VIEW {ns}.v (id BIGINT, shout STRING)\n"
        f"  DIALECT {engine.view_dialect} AS $$\n{body}\n$$;\n"
    )


def test_engine_reads_view_in_its_dialect(
    engine: Engine, saltation: SaltationProject, rest_catalog: RestCatalog, ns: str
) -> None:
    if not engine.capabilities.reads_views:
        pytest.skip(f"{engine.name} does not read Iceberg views from a REST catalog")
    assert saltation.apply(_script(ns, engine)).status == "applied"
    view = rest_catalog.load_view((ns, "v"))
    assert [r.root.dialect for r in view.current_version().representations] == [engine.view_dialect]

    expected = [(2, "B!"), (3, "C!")]
    got = engine.query(f"SELECT id, shout FROM {engine.table_ref(ns, 'v')} ORDER BY id")
    assert coerce_rows(got, expected) == expected
