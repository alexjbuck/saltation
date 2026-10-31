"""An engine reads a table through saltation's schema evolution steps."""

from __future__ import annotations

from .conftest import SaltationProject
from .engines import Engine, coerce_rows


def _script(ns: str) -> str:
    t = f"{ns}.evo"
    return (
        f"CREATE TABLE IF NOT EXISTS {t} (id BIGINT, qty INT, note STRING, legacy STRING)"
        " USING iceberg;\n"
        f"INSERT INTO {t} VALUES (1, 10, 'a', 'old'), (2, 20, 'b', 'old');\n"
        f"ALTER TABLE {t} ADD COLUMN extra STRING;\n"
        f"INSERT INTO {t} VALUES (3, 30, 'c', 'old', 'x');\n"
        f"ALTER TABLE {t} RENAME COLUMN note TO memo;\n"
        f"ALTER TABLE {t} ALTER COLUMN qty TYPE BIGINT;\n"
        f"ALTER TABLE {t} DROP COLUMN legacy;\n"
    )


def test_engine_reads_across_schema_evolution(
    engine: Engine, saltation: SaltationProject, ns: str
) -> None:
    assert saltation.apply(_script(ns)).status == "applied"

    assert engine.columns(ns, "evo") == ["id", "qty", "memo", "extra"]
    expected = [(1, 10, "a", None), (2, 20, "b", None), (3, 30, "c", "x")]
    got = engine.query(
        f"SELECT id, qty, memo, extra FROM {engine.table_ref(ns, 'evo')} ORDER BY id"
    )
    assert coerce_rows(got, expected) == expected
