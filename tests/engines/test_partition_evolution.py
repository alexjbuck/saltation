"""An engine reads data written under two partition specs."""

from __future__ import annotations

from pyiceberg.catalog.rest import RestCatalog

from .conftest import SaltationProject
from .engines import Engine, coerce_rows


def _script(ns: str) -> str:
    t = f"{ns}.part"
    return (
        f"CREATE TABLE IF NOT EXISTS {t} (id BIGINT, region STRING, amount DOUBLE)"
        " USING iceberg PARTITIONED BY (bucket(4, id));\n"
        f"INSERT INTO {t} VALUES (1, 'eu', 1.0), (2, 'us', 2.0), (3, 'eu', 3.0);\n"
        f"ALTER TABLE {t} ADD PARTITION FIELD region;\n"
        f"INSERT INTO {t} VALUES (4, 'eu', 4.0), (5, 'us', 5.0), (6, 'apac', 6.0);\n"
    )


def test_engine_reads_across_partition_evolution(
    engine: Engine, saltation: SaltationProject, rest_catalog: RestCatalog, ns: str
) -> None:
    assert saltation.apply(_script(ns)).status == "applied"
    table = rest_catalog.load_table((ns, "part"))
    assert len(table.specs()) == 2
    assert [f.name for f in table.spec().fields] == ["id_bucket", "region"]

    ref = engine.table_ref(ns, "part")
    got = engine.query(f"SELECT id, region, amount FROM {ref} ORDER BY id")
    expected = [
        (1, "eu", 1.0),
        (2, "us", 2.0),
        (3, "eu", 3.0),
        (4, "eu", 4.0),
        (5, "us", 5.0),
        (6, "apac", 6.0),
    ]
    assert coerce_rows(got, expected) == expected

    eu = engine.query(f"SELECT count(*), sum(amount) FROM {ref} WHERE region = 'eu'")
    assert coerce_rows(eu, [(3, 8.0)]) == [(3, 8.0)]
    by_id = engine.query(f"SELECT count(*) FROM {ref} WHERE id IN (2, 5)")
    assert coerce_rows(by_id, [(2,)]) == [(2,)]
