"""Parse an evolution script, reading view bodies in their own SQL dialect.

Everything is cambrian's Spark SQL except view bodies, which can be written
in another dialect two ways:

* ``CREATE VIEW v ... DIALECT trino AS $$ <body> $$ [DIALECT spark AS $$...$$]``
  gives one verbatim body per dialect.
* A plain ``CREATE/ALTER VIEW ... AS <query>`` body is in
  ``[views].source_dialect``.

A body can't go through the Spark parser: it would accept many Trino queries
but misread them (``"x"`` is an identifier in Trino, a string in Spark). So the
script is split into statements by token offsets, the view header is parsed as
Spark, and each body is parsed from its original text in its own dialect. The
text rides on the query's ``meta`` so the view can store it verbatim; the
statement's ``meta[VIEW_BODIES]`` lists every body in order.
"""

from __future__ import annotations

import sqlglot
from sqlglot import expressions as exp
from sqlglot.dialects.dialect import Dialect
from sqlglot.tokens import Token, TokenType

from cambrian.errors import DispatchError
from cambrian.sql.dialect import CambrianSpark

__all__ = ["SOURCE_DIALECT", "SOURCE_SQL", "VIEW_BODIES", "parse_script"]

SOURCE_SQL = "cambrian_source_sql"
SOURCE_DIALECT = "cambrian_source_dialect"
VIEW_BODIES = "cambrian_view_bodies"


def parse_script(text: str, *, view_source_dialect: str = "spark") -> list[exp.Expression]:
    statements: list[exp.Expression] = []
    for chunk, tokens in _split(text):
        view = _parse_view(chunk, tokens, view_source_dialect)
        if view is not None:
            statements.append(view)
        else:
            statements += [s for s in sqlglot.parse(chunk, dialect=CambrianSpark) if s is not None]
    return statements


def _split(text: str) -> list[tuple[str, list[Token]]]:
    chunks: list[tuple[str, list[Token]]] = []
    start = 0
    current: list[Token] = []
    for token in CambrianSpark().tokenize(text):
        if token.token_type == TokenType.SEMICOLON:
            chunks.append((text[start : token.start], current))
            start, current = token.end + 1, []
        else:
            current.append(_rebase(token, start))
    chunks.append((text[start:], current))
    return [(c, toks) for c, toks in chunks if toks]


def _rebase(token: Token, offset: int) -> Token:
    token.start -= offset
    token.end -= offset
    return token


def _parse_view(chunk: str, tokens: list[Token], source_dialect: str) -> exp.Expression | None:
    if not _is_view_statement(tokens):
        return None
    dialect_clauses = _dialect_clauses(tokens)
    if dialect_clauses:
        head = chunk[: dialect_clauses[0][0].start]
        bodies = [(name, body.text) for _, name, body in dialect_clauses]
    else:
        as_token = _top_level_as(tokens)
        if as_token is None or source_dialect == "spark":
            return None
        head = chunk[: as_token.start]
        bodies = [(source_dialect, chunk[as_token.end + 1 :])]

    # The header is plain cambrian Spark; a stand-in body keeps it parseable.
    statement = sqlglot.parse_one(f"{head} AS SELECT 1", dialect=CambrianSpark)
    queries = [_parse_body(dialect, body) for dialect, body in bodies]
    seen = [q.meta[SOURCE_DIALECT] for q in queries]
    if dupes := sorted({d for d in seen if seen.count(d) > 1}):
        raise DispatchError(f"view gives more than one body for dialect(s) {dupes}")
    if isinstance(statement, exp.Create):
        statement.set("expression", queries[0])
    elif isinstance(statement, exp.Alter):
        statement.set("actions", [queries[0]])
    statement.meta[VIEW_BODIES] = queries
    return statement


def _parse_body(dialect: str, text: str) -> exp.Query:
    try:
        Dialect.get_or_raise(dialect)
    except ValueError as err:
        raise DispatchError(f"unknown view dialect {dialect!r}") from err
    body = text.strip()
    try:
        query = sqlglot.parse_one(body, dialect=dialect)
    except sqlglot.ParseError as err:
        raise DispatchError(f"can't parse the view body as {dialect} SQL: {err}") from err
    if not isinstance(query, exp.Query):
        raise DispatchError(f"view body isn't a query in {dialect} SQL: {body[:80]!r}")
    query.meta[SOURCE_SQL] = body
    query.meta[SOURCE_DIALECT] = dialect
    return query


def _is_view_statement(tokens: list[Token]) -> bool:
    words = [t.text.upper() for t in tokens[:6]]
    return bool(words) and words[0] in {"CREATE", "ALTER"} and "VIEW" in words[:4]


def _dialect_clauses(tokens: list[Token]) -> list[tuple[Token, str, Token]]:
    """Every top-level ``DIALECT <name> AS $$...$$`` as (DIALECT token, name, body token)."""
    out: list[tuple[Token, str, Token]] = []
    depth = 0
    for i, token in enumerate(tokens):
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.text.upper() == "DIALECT" and i + 3 < len(tokens):
            name, as_, body = tokens[i + 1], tokens[i + 2], tokens[i + 3]
            if as_.token_type != TokenType.ALIAS or body.token_type != TokenType.HEREDOC_STRING:
                raise DispatchError(f"expected DIALECT <name> AS $$ ... $$ near {name.text!r}")
            out.append((token, name.text.lower(), body))
    return out


def _top_level_as(tokens: list[Token]) -> Token | None:
    """The top-level AS that opens the query of CREATE/ALTER VIEW."""
    depth = 0
    for token in tokens:
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.token_type == TokenType.ALIAS:
            return token
    return None
