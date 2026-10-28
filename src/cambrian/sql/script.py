"""Parse an evolution script, reading view bodies in their own SQL dialect.

Everything is cambrian's Spark SQL except the query of a ``CREATE VIEW ... AS``
or ``ALTER VIEW ... AS`` when ``[views].source_dialect`` names another
dialect. Such a body can't go through the Spark parser: it would accept many
Trino queries but misread them (``"x"`` is an identifier in Trino and a string
in Spark). So the script is split into statements by token offsets, the view
header is parsed as Spark, and the body is re-parsed from its original text in
the source dialect. The text is kept on the query's ``meta`` so the view can
store it verbatim for that dialect.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp
from sqlglot.tokens import Token, TokenType

from cambrian.errors import DispatchError
from cambrian.sql.dialect import CambrianSpark

__all__ = ["SOURCE_DIALECT", "SOURCE_SQL", "parse_script"]

SOURCE_SQL = "cambrian_source_sql"
SOURCE_DIALECT = "cambrian_source_dialect"


def parse_script(text: str, *, view_source_dialect: str = "spark") -> list[exp.Expression]:
    if view_source_dialect == "spark":
        return [s for s in sqlglot.parse(text, dialect=CambrianSpark) if s is not None]
    statements: list[exp.Expression] = []
    for chunk in _split(text):
        view = _parse_view(chunk, view_source_dialect)
        if view is not None:
            statements.append(view)
        else:
            statements += [s for s in sqlglot.parse(chunk, dialect=CambrianSpark) if s is not None]
    return statements


def _split(text: str) -> list[str]:
    tokens = CambrianSpark().tokenize(text)
    chunks: list[str] = []
    start = 0
    for token in tokens:
        if token.token_type == TokenType.SEMICOLON:
            chunks.append(text[start : token.start])
            start = token.end + 1
    chunks.append(text[start:])
    return [c for c in chunks if c.strip()]


def _parse_view(chunk: str, dialect: str) -> exp.Expression | None:
    tokens = CambrianSpark().tokenize(chunk)
    as_token = _view_body_start(tokens)
    if as_token is None:
        return None
    head = chunk[: as_token.start]
    body = chunk[as_token.end + 1 :].strip()
    # The header is plain cambrian Spark; a stand-in body keeps it parseable.
    statement = sqlglot.parse_one(f"{head} AS SELECT 1", dialect=CambrianSpark)
    try:
        query = sqlglot.parse_one(body, dialect=dialect)
    except sqlglot.ParseError as err:
        raise DispatchError(f"can't parse the view body as {dialect} SQL: {err}") from err
    if not isinstance(query, exp.Query):
        raise DispatchError(f"view body isn't a query in {dialect} SQL: {body[:80]!r}")
    query.meta[SOURCE_SQL] = body
    query.meta[SOURCE_DIALECT] = dialect
    if isinstance(statement, exp.Create):
        statement.set("expression", query)
    elif isinstance(statement, exp.Alter):
        statement.set("actions", [query])
    return statement


def _view_body_start(tokens: list[Token]) -> Token | None:
    """The top-level AS that opens the query of CREATE/ALTER VIEW, if this is one."""
    words = [t.text.upper() for t in tokens[:6]]
    if not words or words[0] not in {"CREATE", "ALTER"}:
        return None
    if "VIEW" not in words[:4]:
        return None
    depth = 0
    for token in tokens:
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.token_type == TokenType.ALIAS:
            return token
    return None
