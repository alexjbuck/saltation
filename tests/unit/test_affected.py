"""Tests for ``cambrian.iceberg.affected`` — affected-table extraction.

Two layers under test:

1. ``affected_tables(stmt)`` — pure AST inspection.
2. ``affected_tables_with_overrides(text, stmts)`` — line-scan over the
   expanded text to honour ``-- cambrian:tables ...`` header comments.
"""

from __future__ import annotations

import sqlglot

from cambrian.iceberg.affected import (
    TableIdent,
    affected_tables,
    affected_tables_with_overrides,
    parse_override_comment,
)
from cambrian.sql.dialect import CambrianSpark


def _parse(sql: str) -> list:
    return [s for s in sqlglot.parse(sql, dialect=CambrianSpark) if s is not None]


def _affected(sql: str) -> list[TableIdent]:
    [stmt] = _parse(sql)
    return affected_tables(stmt)


# ---------------------------------------------------------------------------
# Pure AST extraction
# ---------------------------------------------------------------------------


def test_create_table_returns_table_ident() -> None:
    assert _affected("CREATE TABLE foo.bar (id INT) USING iceberg") == [
        TableIdent(namespace="foo", name="bar")
    ]


def test_create_table_with_three_part_name() -> None:
    affected = _affected("CREATE TABLE cat.foo.bar (id INT) USING iceberg")
    # Iceberg flattens cat+foo into a single namespace path.
    assert affected == [TableIdent(namespace="cat.foo", name="bar")]


def test_create_table_unqualified() -> None:
    affected = _affected("CREATE TABLE bar (id INT) USING iceberg")
    assert affected == [TableIdent(namespace=None, name="bar")]


def test_drop_table_returns_table_ident() -> None:
    assert _affected("DROP TABLE foo.bar") == [TableIdent(namespace="foo", name="bar")]


def test_alter_table_returns_table_ident() -> None:
    assert _affected("ALTER TABLE foo.bar ADD COLUMN c INT") == [
        TableIdent(namespace="foo", name="bar")
    ]


def test_alter_table_partition_field_returns_table() -> None:
    """Custom AddPartitionField is an action *inside* Alter; the table comes off Alter."""
    assert _affected("ALTER TABLE foo.bar ADD PARTITION FIELD bucket(16, x)") == [
        TableIdent(namespace="foo", name="bar")
    ]


def test_insert_values_returns_table_ident() -> None:
    assert _affected("INSERT INTO foo.bar VALUES (1)") == [TableIdent(namespace="foo", name="bar")]


def test_create_namespace_returns_empty() -> None:
    assert _affected("CREATE NAMESPACE foo") == []


def test_create_namespace_if_not_exists_returns_empty() -> None:
    assert _affected("CREATE NAMESPACE IF NOT EXISTS foo") == []


def test_drop_namespace_returns_empty() -> None:
    assert _affected("DROP NAMESPACE foo") == []


# ---------------------------------------------------------------------------
# Override-comment parsing
# ---------------------------------------------------------------------------


def test_parse_override_comment_basic() -> None:
    assert parse_override_comment("-- cambrian:tables foo.a") == [
        TableIdent(namespace="foo", name="a"),
    ]


def test_parse_override_comment_multiple() -> None:
    assert parse_override_comment("-- cambrian:tables foo.a, foo.b , bar.c") == [
        TableIdent(namespace="foo", name="a"),
        TableIdent(namespace="foo", name="b"),
        TableIdent(namespace="bar", name="c"),
    ]


def test_parse_override_comment_unqualified() -> None:
    assert parse_override_comment("-- cambrian:tables foo") == [
        TableIdent(namespace=None, name="foo"),
    ]


def test_parse_override_comment_non_directive_returns_none() -> None:
    assert parse_override_comment("-- a regular comment") is None
    assert parse_override_comment("CREATE TABLE foo.bar (id INT)") is None
    assert parse_override_comment("--cambrian:tables foo") == [
        TableIdent(namespace=None, name="foo"),
    ]


def test_parse_override_comment_empty_payload() -> None:
    """An override with no tables listed is a deliberate "no tables" signal."""
    assert parse_override_comment("-- cambrian:tables ") == []


# ---------------------------------------------------------------------------
# affected_tables_with_overrides — full pipeline
# ---------------------------------------------------------------------------


def test_overrides_replace_ast_detection() -> None:
    sql = "-- cambrian:tables foo.x, foo.y\nALTER TABLE foo.t ADD COLUMN c INT;\n"
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    assert result == [
        [TableIdent(namespace="foo", name="x"), TableIdent(namespace="foo", name="y")],
    ]


def test_no_override_falls_back_to_ast() -> None:
    sql = "ALTER TABLE foo.t ADD COLUMN c INT;\n"
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    assert result == [[TableIdent(namespace="foo", name="t")]]


def test_override_only_applies_to_immediately_following_statement() -> None:
    """An override binds to the next statement, not statements further down."""
    sql = (
        "-- cambrian:tables foo.x\n"
        "ALTER TABLE foo.t ADD COLUMN c INT;\n"
        "ALTER TABLE foo.u ADD COLUMN d INT;\n"
    )
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    assert result == [
        [TableIdent(namespace="foo", name="x")],
        [TableIdent(namespace="foo", name="u")],
    ]


def test_intervening_statement_breaks_association() -> None:
    """A real SQL statement between an override and a later statement breaks the link."""
    sql = (
        "-- cambrian:tables foo.x\n"
        "ALTER TABLE foo.t ADD COLUMN c INT;\n"
        "ALTER TABLE foo.u ADD COLUMN d INT;\n"
    )
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    # The override applies to foo.t (immediately below it). foo.u keeps its
    # AST-derived value.
    assert result[1] == [TableIdent(namespace="foo", name="u")]


def test_override_through_blank_lines() -> None:
    """Blank lines between an override and its statement do not break the link."""
    sql = "-- cambrian:tables foo.x\n\n\nALTER TABLE foo.t ADD COLUMN c INT;\n"
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    assert result == [[TableIdent(namespace="foo", name="x")]]


def test_override_through_include_markers() -> None:
    """Synthetic ``-- cambrian:include-begin/-end`` markers don't break the link.

    The include-end after the last statement parses as a trailing
    ``Semicolon`` node carrying the comment; the runner is responsible for
    skipping no-op nodes. Here we only care that the ALTER picks up the
    override from the begin/tables comments above it.
    """
    sql = (
        "-- cambrian:tables foo.x\n"
        "-- cambrian:include-begin a.sql\n"
        "ALTER TABLE foo.t ADD COLUMN c INT;\n"
        "-- cambrian:include-end a.sql\n"
        "ALTER TABLE foo.u ADD COLUMN d INT;\n"
    )
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    # Two real statements: the first picks up the override; the second is
    # untouched (the include-end is its preceding comment but isn't a
    # cambrian:tables directive).
    assert result[0] == [TableIdent(namespace="foo", name="x")]
    assert result[1] == [TableIdent(namespace="foo", name="u")]


def test_create_namespace_with_override() -> None:
    """An override applies even when AST detection would return empty."""
    sql = "-- cambrian:tables foo.x\nCREATE NAMESPACE foo;\n"
    stmts = _parse(sql)
    result = affected_tables_with_overrides(sql, stmts)
    assert result == [[TableIdent(namespace="foo", name="x")]]
