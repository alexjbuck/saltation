"""An engine reads a table with a saltation-set sort order and identifier fields."""

from __future__ import annotations

from pyiceberg.catalog.rest import RestCatalog

from .conftest import SaltationProject
from .engines import Engine, coerce_rows


def _script(ns: str) -> str:
    t = f"{ns}.sorted"
    return (
        f"CREATE TABLE IF NOT EXISTS {t} (id BIGINT NOT NULL, created_at TIMESTAMP, note STRING)"
        " USING iceberg;\n"
        f"ALTER TABLE {t} SET IDENTIFIER FIELDS id;\n"
        f"ALTER TABLE {t} WRITE ORDERED BY (id ASC NULLS FIRST, note DESC NULLS LAST);\n"
        f"INSERT INTO {t} VALUES (2, NULL, 'b'), (1, NULL, 'a'), (3, NULL, NULL);\n"
    )


def test_engine_reads_sorted_table_with_identifier_fields(
    engine: Engine, saltation: SaltationProject, rest_catalog: RestCatalog, ns: str
) -> None:
    assert saltation.apply(_script(ns)).status == "applied"
    table = rest_catalog.load_table((ns, "sorted"))
    assert [(f.source_id, f.direction.value) for f in table.sort_order().fields] == [
        (1, "asc"),
        (3, "desc"),
    ]
    assert table.schema().identifier_field_ids == [1]

    assert engine.columns(ns, "sorted") == ["id", "created_at", "note"]
    expected = [(1, "a"), (2, "b"), (3, None)]
    got = engine.query(f"SELECT id, note FROM {engine.table_ref(ns, 'sorted')} ORDER BY id")
    assert coerce_rows(got, expected) == expected
