"""Parse-level oracle over the Iceberg SQL corpus.

* ``expected`` in ``{"apply", "invalid"}``: the SQL parses with
  :class:`SaltationSpark` to a real node, not a sqlglot ``Command`` fallback.
  Whether it then does the right thing is proven against Lakekeeper by
  ``tests/integration/test_corpus_effects.py``; a mock catalog accepts any
  call, so "dispatch didn't raise" here would prove nothing.
* ``expected == "reject"``: parsing-then-dispatching ultimately raises
  :class:`UnsupportedStatementError`. Rejects never reach the catalog, so the
  mock is sufficient for them.
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

from saltation.errors import UnsupportedStatementError
from saltation.sql.dialect import SaltationSpark
from saltation.sql.dispatch import dispatch
from tests.fixtures.iceberg_corpus import CORPUS


def _make_catalog() -> MagicMock:
    return MagicMock(
        spec_set=[
            "create_namespace",
            "drop_namespace",
            "update_namespace_properties",
            "create_table",
            "create_table_transaction",
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
    tree = sqlglot.parse(sql, dialect=SaltationSpark)
    stmts = [s for s in tree if s is not None]
    assert len(stmts) == 1, f"expected one statement, got {len(stmts)}"
    return stmts[0]


@pytest.mark.parametrize("entry", CORPUS, ids=[e.id for e in CORPUS])
def test_corpus_entry(entry) -> None:
    catalog = _make_catalog()
    table = _make_table()
    catalog.load_table.return_value = table

    if entry.expected in {"apply", "invalid"}:
        stmt = _parse_one(entry.sql)
        assert not isinstance(stmt, exp.Command), (
            f"{entry.id}: parsed to a Command fallback (parse gap): {entry.sql!r}"
        )
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
