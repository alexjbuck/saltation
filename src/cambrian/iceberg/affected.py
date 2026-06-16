"""Extract ``(namespace, table)`` references from parsed SQL statements.

For each parsed statement we want to know which Iceberg tables it touches so
the runner can capture pre/post state into the sidecar's ``table_states``
table. Most constructs carry a single ``exp.Table`` reference under ``this``;
the custom partition-field / write-ordered-by nodes live inside ``exp.Alter``
and inherit the alter's table reference.

A statement that touches no table (e.g. ``CREATE NAMESPACE``) returns an
empty list.

**Header-comment override.** A single line of the form

    -- cambrian:tables ns.t1, ns.t2

immediately preceding a statement overrides the auto-detected list for
that statement. sqlglot already attaches the immediately-preceding ``--``
comments to the statement node as ``stmt.comments`` (a list of strings,
each stripped of the leading ``--``). We read that list so we don't have
to do our own line-scan / token-stream walk.

If a statement carries multiple ``cambrian:tables`` comments stacked
above it, the *last* (closest to the statement) wins — sqlglot keeps
the comments in source order in ``stmt.comments``.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass

from sqlglot import expressions as exp

__all__ = [
    "OVERRIDE_RE",
    "TableIdent",
    "affected_tables",
    "affected_tables_with_overrides",
    "parse_override_comment",
]


# A comment whose stripped text matches this pattern is a cambrian:tables
# override. The leading ``--`` is NOT part of the pattern — sqlglot's
# ``stmt.comments`` carries text *without* the ``--`` prefix. The pattern
# also matches when only whitespace follows ``cambrian:tables`` so a
# directive with empty payload is a deliberate "no tables" signal.
OVERRIDE_RE = re.compile(r"^\s*cambrian:tables(?:\s+(.*?))?\s*$")


@dataclass(frozen=True, slots=True)
class TableIdent:
    """A two-part Iceberg table identifier.

    ``namespace`` may be ``None`` for unqualified references — dispatch
    is then expected to resolve against the current catalog default
    (which cambrian doesn't track explicitly; an unqualified table in an
    evolution is almost certainly a bug, so dispatch should fail loud).
    """

    namespace: str | None
    name: str

    def __str__(self) -> str:
        return f"{self.namespace}.{self.name}" if self.namespace else self.name


def _from_table_node(node: exp.Table) -> TableIdent:
    """Convert an ``exp.Table`` into a :class:`TableIdent`.

    sqlglot stores the three identifier parts as ``this`` (leaf name), ``db``
    (middle), ``catalog`` (outermost). For two-part ``ns.tbl`` references the
    ``db`` slot carries the namespace. The fully-three-part case
    (``cat.ns.tbl``) joins ``catalog.db`` into the namespace because that's
    what PyIceberg expects in tuple identifiers — Iceberg catalogs flatten
    everything below the table name into a single namespace path.
    """
    name = node.name
    db = node.args.get("db")
    cat = node.args.get("catalog")
    parts: list[str] = []
    if cat is not None:
        parts.append(cat.name)
    if db is not None:
        parts.append(db.name)
    namespace = ".".join(parts) if parts else None
    return TableIdent(namespace=namespace, name=name)


def affected_tables(statement: exp.Expression) -> list[TableIdent]:
    """Return the tables touched by *statement*, AST-derived, no overrides.

    The semantics:

    * ``CREATE/DROP TABLE`` and ``ALTER TABLE`` → one table.
    * ``INSERT INTO ... VALUES`` → one table.
    * ``CREATE NAMESPACE`` / ``DROP NAMESPACE`` → empty list (namespace
      operations don't touch a single table).
    * Anything else → empty list (dispatch will reject the statement).
    """
    if isinstance(statement, exp.Create) and _kind(statement) in {
        "NAMESPACE",
        "SCHEMA",
        "DATABASE",
    }:
        return []
    if isinstance(statement, exp.Drop) and _kind(statement) in {
        "NAMESPACE",
        "SCHEMA",
        "DATABASE",
    }:
        return []

    # CREATE TABLE — ``this`` is a Schema with ``this`` = Table.
    if isinstance(statement, exp.Create):
        inner = statement.args.get("this")
        if isinstance(inner, exp.Schema):
            table = inner.args.get("this")
            if isinstance(table, exp.Table):
                return [_from_table_node(table)]
        if isinstance(inner, exp.Table):
            return [_from_table_node(inner)]
        return []

    if isinstance(statement, exp.Drop):
        inner = statement.args.get("this")
        if isinstance(inner, exp.Table):
            return [_from_table_node(inner)]
        return []

    if isinstance(statement, exp.Alter):
        table = statement.args.get("this")
        if isinstance(table, exp.Table):
            return [_from_table_node(table)]
        return []

    if isinstance(statement, exp.Insert):
        table = statement.args.get("this")
        if isinstance(table, exp.Table):
            return [_from_table_node(table)]
        return []

    return []


def _kind(node: exp.Expression) -> str:
    return (node.args.get("kind") or "").upper()


def parse_override_comment(line: str) -> list[TableIdent] | None:
    """Parse a ``-- cambrian:tables ns.t, ns.u`` directive from one source line.

    Accepts a line in two forms (both used in different call paths):

    * From the raw source text (with leading ``--`` and any whitespace).
    * From ``sqlglot``'s ``stmt.comments`` list (without leading ``--``).

    Returns ``None`` if *line* is not a ``cambrian:tables`` directive. Returns
    ``[]`` for a directive with empty payload (a deliberate "no tables"
    signal).
    """
    # Normalise: strip a leading ``--`` so the same regex covers both forms.
    candidate = line.lstrip()
    if candidate.startswith("--"):
        candidate = candidate[2:]
    match = OVERRIDE_RE.match(candidate)
    if not match:
        return None
    payload = (match.group(1) or "").strip()
    if not payload:
        return []
    tables: list[TableIdent] = []
    for raw in payload.split(","):
        token = raw.strip()
        if not token:
            continue
        if "." in token:
            ns, name = token.rsplit(".", 1)
            tables.append(TableIdent(namespace=ns, name=name))
        else:
            tables.append(TableIdent(namespace=None, name=token))
    return tables


def _override_from_comments(comments: list[str] | None) -> list[TableIdent] | None:
    """Extract the (last) ``cambrian:tables`` directive from *comments*.

    sqlglot stores immediately-preceding ``--`` comment text (without the
    ``--`` prefix) in ``stmt.comments``. We scan in reverse so the directive
    closest to the statement wins if a user stacked multiple by accident.
    Returns ``None`` if no directive is present.
    """
    if not comments:
        return None
    for raw in reversed(comments):
        parsed = parse_override_comment(raw)
        if parsed is not None:
            return parsed
    return None


def affected_tables_with_overrides(
    expanded_text: str,
    statements: Iterable[exp.Expression],
) -> list[list[TableIdent]]:
    """Map every statement to its affected-tables list, honouring header overrides.

    For each statement, in priority order:

    1. The latest ``-- cambrian:tables ...`` comment attached by sqlglot in
       ``stmt.comments`` (these are the comments on the lines immediately
       preceding the statement).
    2. The result of :func:`affected_tables` on the statement.

    *expanded_text* is currently unused (the comment scan goes through
    ``stmt.comments``). It's kept in the signature so that future revisions
    can fall back to line-scanning if a sqlglot bump regresses the comment
    propagation; that's strictly more conservative than a silent loss of
    override semantics.
    """
    del expanded_text  # see docstring
    result: list[list[TableIdent]] = []
    for stmt in statements:
        override = _override_from_comments(stmt.comments)
        if override is not None:
            result.append(override)
        else:
            result.append(affected_tables(stmt))
    return result
