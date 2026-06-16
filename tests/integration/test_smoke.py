"""End-to-end smoke test for the docker-compose test rig.

Creates a table via the Lakekeeper REST catalog, appends a row of PyArrow
data to it, and reads the row back. If this test passes, the rig
(Lakekeeper + rustfs + Postgres + bootstrap) is wired up correctly and is
fit for the subsequent integration tests in M3-M8.
"""

from __future__ import annotations

import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


def test_create_table_append_read_back(ns: str, rest_catalog: RestCatalog) -> None:
    schema = Schema(
        NestedField(field_id=1, name="id", field_type=LongType(), required=True),
        NestedField(field_id=2, name="name", field_type=StringType(), required=True),
    )

    table_id = (ns, "smoke")
    table = rest_catalog.create_table(identifier=table_id, schema=schema)

    arrow_table = pa.Table.from_pydict(
        {"id": [42], "name": ["camby"]},
        schema=pa.schema(
            [
                pa.field("id", pa.int64(), nullable=False),
                pa.field("name", pa.string(), nullable=False),
            ]
        ),
    )
    table.append(arrow_table)

    reloaded = rest_catalog.load_table(table_id)
    scanned = reloaded.scan().to_arrow()

    assert scanned.num_rows == 1
    assert scanned.column("id").to_pylist() == [42]
    assert scanned.column("name").to_pylist() == ["camby"]
