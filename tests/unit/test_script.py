"""parse_script: view bodies in a source dialect, everything else as cambrian Spark."""

from __future__ import annotations

import pytest
from sqlglot import expressions as exp

from cambrian.errors import DispatchError
from cambrian.sql.script import SOURCE_DIALECT, SOURCE_SQL, VIEW_BODIES, parse_script

TRINO_BODY = (
    'SELECT _key AS device_id, ids[1] AS id FROM redpanda.default."devices.array.btr" '
    "WHERE name = 'a;b'"
)


def test_spark_source_is_a_plain_parse() -> None:
    stmts = parse_script("CREATE TABLE a.t (id INT) USING iceberg; DROP TABLE a.t;")
    assert [type(s).__name__ for s in stmts] == ["Create", "Drop"]


def test_trino_view_body_is_reparsed_and_kept_verbatim() -> None:
    script = (
        "CREATE TABLE a.t (id INT) USING iceberg;\n"
        f"-- a view\nCREATE OR REPLACE VIEW a.v (device_id STRING, id STRING) AS\n{TRINO_BODY};\n"
        "ALTER TABLE a.t ADD COLUMN c INT;"
    )
    create_table, view, alter = parse_script(script, view_source_dialect="trino")
    assert isinstance(create_table, exp.Create) and isinstance(alter, exp.Alter)
    query = view.expression
    assert query.meta[SOURCE_SQL] == TRINO_BODY
    assert query.meta[SOURCE_DIALECT] == "trino"
    # Trino reads "devices.array.btr" as an identifier; Spark would read a string.
    assert query.find(exp.Table).name == "devices.array.btr"
    assert view.args.get("replace") is True
    assert [c.name for c in view.this.expressions] == ["device_id", "id"]


def test_semicolon_inside_a_string_does_not_split() -> None:
    stmts = parse_script(f"CREATE VIEW a.v AS {TRINO_BODY};", view_source_dialect="trino")
    assert len(stmts) == 1
    assert stmts[0].expression.meta[SOURCE_SQL] == TRINO_BODY


def test_alter_view_as_body_is_reparsed() -> None:
    (stmt,) = parse_script(f"ALTER VIEW a.v AS {TRINO_BODY}", view_source_dialect="trino")
    assert isinstance(stmt, exp.Alter)
    (query,) = stmt.args["actions"]
    assert query.meta[SOURCE_SQL] == TRINO_BODY


def test_view_statements_without_a_body_parse_as_spark() -> None:
    script = "ALTER VIEW a.v SET TBLPROPERTIES ('k' = 'v'); DROP VIEW a.v;"
    stmts = parse_script(script, view_source_dialect="trino")
    assert [type(s).__name__ for s in stmts] == ["Alter", "Drop"]


def test_dialect_clauses_give_one_verbatim_body_each() -> None:
    script = (
        "CREATE OR REPLACE VIEW a.v (id STRING) COMMENT 'docs'\n"
        f"  DIALECT trino AS $$ {TRINO_BODY} $$\n"
        "  DIALECT spark AS $$SELECT `x` AS id FROM a.t WHERE s = 'a;b'$$;\n"
        "DROP VIEW a.w;"
    )
    view, drop = parse_script(script)
    assert isinstance(drop, exp.Drop)
    bodies = view.meta[VIEW_BODIES]
    assert [(b.meta[SOURCE_DIALECT], b.meta[SOURCE_SQL]) for b in bodies] == [
        ("trino", TRINO_BODY),
        ("spark", "SELECT `x` AS id FROM a.t WHERE s = 'a;b'"),
    ]
    assert view.expression is bodies[0]
    assert view.args.get("replace") is True


def test_dialect_clause_on_alter_view() -> None:
    (stmt,) = parse_script(f"ALTER VIEW a.v DIALECT trino AS $${TRINO_BODY}$$")
    assert isinstance(stmt, exp.Alter)
    assert [b.meta[SOURCE_DIALECT] for b in stmt.meta[VIEW_BODIES]] == ["trino"]


def test_duplicate_dialect_is_refused() -> None:
    with pytest.raises(DispatchError, match="more than one body"):
        parse_script("CREATE VIEW a.v DIALECT trino AS $$SELECT 1$$ DIALECT TRINO AS $$SELECT 2$$")


def test_unknown_dialect_is_refused() -> None:
    with pytest.raises(DispatchError, match="unknown view dialect"):
        parse_script("CREATE VIEW a.v DIALECT nope AS $$SELECT 1$$")


def test_dialect_without_heredoc_body_is_refused() -> None:
    with pytest.raises(DispatchError, match=r"DIALECT <name> AS \$\$"):
        parse_script("CREATE VIEW a.v DIALECT trino AS SELECT 1")
