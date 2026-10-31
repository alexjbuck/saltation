"""An engine reads a typed, partitioned table that saltation created and filled."""

from __future__ import annotations

from datetime import UTC, date, datetime
from decimal import Decimal

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog

from .conftest import SaltationProject
from .engines import Engine, coerce_rows

COLUMNS = [
    "id",
    "flag",
    "qty",
    "ratio",
    "score",
    "price",
    "title",
    "born",
    "ts",
    "attrs",
    "tags",
]


def _script(ns: str) -> str:
    # saltation's INSERT ... VALUES takes untyped literals only, so these rows
    # leave the decimal/date/timestamp/struct/array columns NULL; the fully
    # typed rows are appended through PyIceberg below.
    return (
        f"CREATE TABLE IF NOT EXISTS {ns}.typed (\n"
        "  id BIGINT, flag BOOLEAN, qty INT, ratio FLOAT, score DOUBLE,\n"
        "  price DECIMAL(10, 2), title STRING, born DATE, ts TIMESTAMP,\n"
        "  attrs STRUCT<k: STRING, n: INT>, tags ARRAY<STRING>\n"
        ") USING iceberg\n"
        "PARTITIONED BY (days(ts), bucket(4, id))\n"
        "TBLPROPERTIES ('saltation.engines.case' = 'typed');\n"
        f"INSERT INTO {ns}.typed VALUES\n"
        "  (1, true, 10, 1.5, 2.5, NULL, 'alpha', NULL, NULL, NULL, NULL),\n"
        "  (2, false, 20, 3.5, 4.5, NULL, 'beta', NULL, NULL, NULL, NULL);\n"
    )


def _typed_rows() -> list[dict[str, object]]:
    return [
        {
            "id": 3,
            "flag": True,
            "qty": 30,
            "ratio": 5.5,
            "score": 6.5,
            "price": Decimal("12.34"),
            "title": "gamma",
            "born": date(2026, 1, 2),
            "ts": datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC),
            "attrs": {"k": "x", "n": 1},
            "tags": ["a", "b"],
        },
        {
            "id": 4,
            "flag": False,
            "qty": 40,
            "ratio": 7.5,
            "score": 8.5,
            "price": Decimal("-0.50"),
            "title": "delta",
            "born": date(1999, 12, 31),
            "ts": datetime(2026, 3, 4, 5, 6, 7, tzinfo=UTC),
            "attrs": {"k": "y", "n": 2},
            "tags": ["c"],
        },
    ]


def test_engine_reads_typed_partitioned_table(
    engine: Engine, saltation: SaltationProject, rest_catalog: RestCatalog, ns: str
) -> None:
    assert saltation.apply(_script(ns)).status == "applied"
    table = rest_catalog.load_table((ns, "typed"))
    table.append(pa.Table.from_pylist(_typed_rows(), schema=table.schema().as_arrow()))

    ref = engine.table_ref(ns, "typed")
    assert engine.columns(ns, "typed") == COLUMNS
    assert coerce_rows(engine.query(f"SELECT count(*) FROM {ref}"), [(4,)]) == [(4,)]

    scalars = [
        (1, True, 10, 1.5, 2.5, "alpha"),
        (2, False, 20, 3.5, 4.5, "beta"),
        (3, True, 30, 5.5, 6.5, "gamma"),
        (4, False, 40, 7.5, 8.5, "delta"),
    ]
    got = engine.query(f"SELECT id, flag, qty, ratio, score, title FROM {ref} ORDER BY id")
    assert coerce_rows(got, scalars) == scalars

    typed = [
        (3, Decimal("12.34"), date(2026, 1, 2), "x"),
        (4, Decimal("-0.50"), date(1999, 12, 31), "y"),
    ]
    got = engine.query(f"SELECT id, price, born, attrs.k FROM {ref} WHERE id >= 3 ORDER BY id")
    assert coerce_rows(got, typed) == typed

    nulls = engine.query(f"SELECT count(ts), count(tags), count(price) FROM {ref}")
    assert coerce_rows(nulls, [(2, 2, 2)]) == [(2, 2, 2)]
