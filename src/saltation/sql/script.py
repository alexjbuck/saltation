"""Parse an evolution script, reading view bodies in their own SQL dialect.

Everything is saltation's Spark SQL except view bodies, which can be written
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
from sqlglot.errors import TokenError
from sqlglot.tokens import Token, TokenType

from saltation.errors import DispatchError
from saltation.sql.dialect import SaltationSpark

__all__ = ["SOURCE_DIALECT", "SOURCE_SQL", "VIEW_BODIES", "parse_script"]

SOURCE_SQL = "saltation_source_sql"
SOURCE_DIALECT = "saltation_source_dialect"
VIEW_BODIES = "saltation_view_bodies"


def parse_script(text: str, *, view_source_dialect: str = "spark") -> list[exp.Expression]:
    statements: list[exp.Expression] = []
    for chunk, tokens in _split(text, view_source_dialect):
        view = _parse_view(chunk, tokens, view_source_dialect)
        if view is not None:
            statements.append(view)
        else:
            statements += [s for s in sqlglot.parse(chunk, dialect=SaltationSpark) if s is not None]
    return statements


def _split(text: str, source_dialect: str) -> list[tuple[str, list[Token]]]:
    """Statements as (text, Spark tokens rebased to it).

    A view body in *source_dialect* is never lexed as Spark: its quoting rules
    differ (Trino's ``'\\'`` is a one-character string; to Spark it's an open
    escape). Spark tokens stop at the body's ``AS`` and the body runs to the
    ``;`` its own dialect's tokenizer finds.
    """
    chunks: list[tuple[str, list[Token]]] = []
    pos = 0
    while pos < len(text):
        tokens, error = _tokenize(SaltationSpark, text[pos:])
        start, depth, current = 0, 0, []
        resume = None
        for token in tokens:
            if token.token_type == TokenType.SEMICOLON:
                if current:
                    chunks.append((text[pos + start : pos + token.start], current))
                start, current = token.end + 1, []
                continue
            current.append(_rebase(token, start))
            if token.token_type == TokenType.L_PAREN:
                depth += 1
            elif token.token_type == TokenType.R_PAREN:
                depth -= 1
            elif depth == 0 and _opens_source_body(current, source_dialect):
                body_start = pos + token.end + 1
                end = _body_end(text, body_start, source_dialect)
                chunks.append((text[pos + start : end], current))
                resume = end + 1
                break
        if resume is not None:
            pos = resume
            continue
        if error is not None:
            raise DispatchError(f"can't read the script as Spark SQL: {error}") from error
        if current:
            chunks.append((text[pos + start :], current))
        break
    return chunks


def _opens_source_body(tokens: list[Token], source_dialect: str) -> bool:
    """Whether the last token is the ``AS`` before a view body in *source_dialect*."""
    return (
        source_dialect != "spark"
        and tokens[-1].token_type == TokenType.ALIAS
        and _is_view_statement(tokens)
        and not any(t.text.upper() == "DIALECT" for t in tokens)
    )


def _body_end(text: str, start: int, dialect: str) -> int:
    tokens, error = _tokenize(dialect, text[start:])
    for token in tokens:
        if token.token_type == TokenType.SEMICOLON:
            return start + token.start
    if error is not None:
        raise DispatchError(f"can't read the view body as {dialect} SQL: {error}") from error
    return len(text)


def _tokenize(dialect: str | type[Dialect], sql: str) -> tuple[list[Token], TokenError | None]:
    """All tokens, or those before the first tokenizer error and the error."""
    d = Dialect.get_or_raise(dialect)
    tokenizer = d.tokenizer_class(dialect=d)
    try:
        return tokenizer.tokenize(sql), None
    except TokenError as err:
        return list(tokenizer.tokens), err


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

    # The header is plain saltation Spark; a stand-in body keeps it parseable.
    statement = sqlglot.parse_one(f"{head} AS SELECT 1", dialect=SaltationSpark)
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
    i = 0
    while i < len(tokens):
        token = tokens[i]
        if token.token_type == TokenType.L_PAREN:
            depth += 1
        elif token.token_type == TokenType.R_PAREN:
            depth -= 1
        elif depth == 0 and token.token_type == TokenType.ALIAS:
            # A plain ``AS`` starts the query; ``dialect`` past it is a column.
            break
        elif depth == 0 and token.text.upper() == "DIALECT":
            clause = tokens[i : i + 4]
            if (
                len(clause) < 4
                or clause[2].token_type != TokenType.ALIAS
                or clause[3].token_type != TokenType.HEREDOC_STRING
            ):
                raise DispatchError(f"expected DIALECT <name> AS $$ ... $$ near {token.text!r}")
            out.append((token, clause[1].text.lower(), clause[3]))
            i += 4
            continue
        i += 1
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
