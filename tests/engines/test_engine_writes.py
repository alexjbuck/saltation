"""An engine writes to a saltation-managed table; saltation stays idempotent on it."""

from __future__ import annotations

import pytest
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.table import Table

from .conftest import SaltationProject
from .engines import Engine, coerce_rows

ROWS = [(10, "x", "eu", "from-engine"), (11, "y", "us", None), (12, "z", "eu", "again")]


def _script(ns: str) -> str:
    t = f"{ns}.w"
    return (
        f"CREATE TABLE IF NOT EXISTS {t} (id BIGINT, name STRING, region STRING) USING iceberg\n"
        "PARTITIONED BY (region)\n"
        "TBLPROPERTIES ('saltation.engines.case' = 'engine-writes');\n"
        f"ALTER TABLE {t} ADD COLUMN note STRING;\n"
        f"ALTER TABLE {t} SET TBLPROPERTIES ('saltation.engines.reapply' = 'stable');\n"
    )


def _shape(table: Table) -> dict[str, object]:
    snapshot = table.current_snapshot()
    return {
        "columns": [(f.name, str(f.field_type)) for f in table.schema().fields],
        "spec": [(f.name, str(f.transform)) for f in table.spec().fields],
        "properties": {k: v for k, v in table.properties.items() if k.startswith("saltation.")},
        "snapshot": snapshot.snapshot_id if snapshot else None,
    }


def _rows(table: Table) -> list[tuple[object, ...]]:
    data = table.scan().to_arrow().sort_by("id").to_pylist()
    return [(r["id"], r["name"], r["region"], r["note"]) for r in data]


def test_engine_writes_are_read_by_pyiceberg_and_reapply_is_idempotent(
    engine: Engine, saltation: SaltationProject, rest_catalog: RestCatalog, ns: str
) -> None:
    if not engine.capabilities.writes:
        pytest.skip(f"{engine.name} does not write to Iceberg REST tables")
    script = _script(ns)
    assert saltation.apply(script).status == "applied"

    engine.insert(ns, "w", ROWS)
    table = rest_catalog.load_table((ns, "w"))
    assert _rows(table) == ROWS
    got = engine.query(
        f"SELECT id, name, region, note FROM {engine.table_ref(ns, 'w')} ORDER BY id"
    )
    assert coerce_rows(got, ROWS) == ROWS
    before = _shape(table)

    rerun = saltation.apply(script, rerun=True)
    assert rerun.status == "applied"
    after = rest_catalog.load_table((ns, "w"))
    assert _shape(after) == before
    assert _rows(after) == ROWS

    assert saltation.apply(script).status == "unchanged"
