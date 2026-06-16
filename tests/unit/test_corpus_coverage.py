"""Oracle coverage test over the Iceberg SQL corpus.

For every :data:`CORPUS` entry this asserts the *contract* end-to-end:

* ``expected == "apply"``: the SQL parses with :class:`CambrianSpark` to a
  real node (NOT a sqlglot ``Command`` fallback) AND :func:`dispatch` runs
  against a mock catalog/table without raising.
* ``expected == "reject"``: parsing-then-dispatching ultimately raises
  :class:`UnsupportedStatementError`. A ``Command`` fallback that dispatch
  turns into ``UnsupportedStatementError`` is a clean reject; a
  ``DispatchError`` or a silent success is NOT.

The mock catalog/table mirror ``tests/unit/test_dispatch.py`` but are widened
so every in-scope construct has a method to call.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pyarrow as pa
import pytest
import sqlglot
from pyiceberg.schema import Schema
from pyiceberg.types import IntegerType, NestedField, StringType
from sqlglot import expressions as exp
from sqlglot.errors import ParseError

from cambrian.errors import UnsupportedStatementError
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.dispatch import dispatch
from tests.fixtures.iceberg_corpus import CORPUS


def _make_catalog() -> MagicMock:
    return MagicMock(
        spec_set=[
            "create_namespace",
            "drop_namespace",
            "update_namespace_properties",
            "create_table",
            "drop_table",
            "rename_table",
            "load_table",
        ]
    )


def _make_table() -> MagicMock:
    table = MagicMock()
    # Two non-required fields so INSERT VALUES nullability mirrors cleanly.
    real_schema = Schema(
        NestedField(field_id=1, name="id", field_type=IntegerType(), required=False),
        NestedField(field_id=2, name="name", field_type=StringType(), required=False),
    )
    table.schema.return_value = real_schema
    # identifier_field_names() is consulted by DROP IDENTIFIER FIELDS.
    real_schema_mock = MagicMock(wraps=real_schema)
    real_schema_mock.identifier_field_names.return_value = ["id"]
    real_schema_mock.as_arrow.return_value = pa.schema(
        [pa.field("id", pa.int32(), nullable=True), pa.field("name", pa.string(), nullable=True)]
    )
    real_schema_mock.fields = real_schema.fields
    table.schema.return_value = real_schema_mock
    return table


def _parse_one(sql: str) -> exp.Expression:
    tree = sqlglot.parse(sql, dialect=CambrianSpark)
    stmts = [s for s in tree if s is not None]
    assert len(stmts) == 1, f"expected one statement, got {len(stmts)}"
    return stmts[0]


@pytest.mark.parametrize("entry", CORPUS, ids=[e.id for e in CORPUS])
def test_corpus_entry(entry) -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table

    if entry.expected == "apply":
        stmt = _parse_one(entry.sql)
        assert not isinstance(stmt, exp.Command), (
            f"{entry.id}: parsed to a Command fallback (parse gap): {entry.sql!r}"
        )
        # Must dispatch without raising.
        dispatch(catalog, stmt)
    else:
        # A reject may surface at the parse layer (a hard sqlglot ParseError,
        # or a Command fallback which dispatch then rejects) or directly from
        # dispatch. A ParseError means the SQL can never execute — a clean
        # reject; otherwise the terminal error must be UnsupportedStatementError.
        try:
            stmt = _parse_one(entry.sql)
        except ParseError:
            return
        with pytest.raises(UnsupportedStatementError):
            dispatch(catalog, stmt)
