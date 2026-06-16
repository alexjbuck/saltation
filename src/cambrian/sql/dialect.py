"""``CambrianSpark`` — a sqlglot Spark dialect extended for Iceberg constructs.

This dialect adds parsing for the Iceberg-Spark DDL extensions that stock
``sqlglot.dialects.spark.Spark`` doesn't recognise:

* ``ALTER TABLE t ADD PARTITION FIELD [transform [AS name]]``
* ``ALTER TABLE t DROP PARTITION FIELD <ref>``
* ``ALTER TABLE t REPLACE PARTITION FIELD <old> WITH <new> [AS name]``
* ``ALTER TABLE t WRITE ORDERED BY (<cols...>)``
* ``ALTER TABLE t DROP COLUMN <name>`` (singular — stock Spark only knows
  the ``DROP COLUMNS (a, b)`` plural form)
* ``ALTER TABLE t UNSET TBLPROPERTIES (<keys>)``

Extension pattern (per spike PR #2): override the minimum surface on
:class:`SparkParser` to detect the Iceberg-specific token sequences and
emit cambrian's custom AST nodes (:mod:`cambrian.sql.ast`); fall through to
``super()`` for everything else. The parent's parser is never rewritten or
forked — only intercepted.

``CREATE NAMESPACE`` / ``DROP NAMESPACE`` already parse in stock Spark
(into ``exp.Create``/``exp.Drop`` with ``kind="NAMESPACE"``). No override
needed; dispatch reads the ``kind`` to route.
"""

from __future__ import annotations

import typing as t

from sqlglot import expressions as exp
from sqlglot.dialects.spark import Spark
from sqlglot.parsers.spark import SparkParser
from sqlglot.tokens import TokenType

from cambrian.sql.ast import (
    AddPartitionField,
    AlterColumnPosition,
    AlterNamespaceProperties,
    DropIdentifierFields,
    DropPartitionField,
    ReplacePartitionField,
    SetIdentifierFields,
    UnsetTblProperties,
    WriteDistribution,
    WriteOrderedBy,
)

_NAMESPACE_TOKENS = (TokenType.NAMESPACE, TokenType.SCHEMA)

__all__ = ["CambrianSpark"]


def _column_arg(func: exp.Func) -> exp.Expr | None:
    """Return the first Column/Identifier argument of an Iceberg transform call.

    Handles both argument shapes sqlglot uses:

    * Anonymous funcs like ``bucket(N, col)`` carry args in ``expressions``.
    * Typed funcs like ``year(col)`` end up as ``Year(this=Cast(Column))`` —
      we walk through ``Cast`` wrappers to find the underlying column.

    Returns ``None`` if no column argument is present (shouldn't happen for
    valid Iceberg DDL, but degrades gracefully).
    """
    # sqlglot's Spark dialect lowers ``truncate(N, col)`` into a built-in
    # ``Trunc`` node whose column lives under ``decimals`` (``this`` is the
    # width literal), unlike ``bucket(N, col)`` which keeps both in
    # ``expressions``. Check ``decimals`` first so truncate's column resolves.
    if isinstance(func, exp.Trunc):
        decimals = func.args.get("decimals")
        if isinstance(decimals, exp.Column | exp.Identifier):
            return decimals
    for arg in func.args.get("expressions") or []:
        if isinstance(arg, exp.Column | exp.Identifier):
            return arg
    this = func.args.get("this")
    while isinstance(this, exp.Cast):
        this = this.args.get("this")
    if isinstance(this, exp.Column | exp.Identifier):
        return this
    return None


def _dotted_path(column: exp.Column) -> str:
    """Render an ``exp.Column`` like ``point.z`` back to its dotted string.

    sqlglot stores ``a.b.c`` as ``Column(this=c, table=b, db=a)``; we walk
    those parts in source order so dispatch can split the path for PyIceberg's
    nested ``add_column`` tuple form.
    """
    parts = [p.name for p in (column.args.get("db"), column.args.get("table")) if p is not None]
    parts.append(column.name)
    return ".".join(parts)


def _parse_partition_field_payload(
    parser: SparkParser,
) -> tuple[exp.Expr | None, exp.Func | None, exp.Expr | None]:
    """Parse ``[<transform> | <col>] [AS <name>]`` and return (this, transform, alias).

    Used by both ``ADD PARTITION FIELD`` and ``REPLACE PARTITION FIELD WITH``
    so the shared shape of the payload doesn't drift between them.
    """
    parsed = parser._parse_field(any_token=True)
    if isinstance(parsed, exp.Func):
        column = _column_arg(parsed)
        this: exp.Expr | None = column
        transform: exp.Func | None = parsed
    else:
        this = parsed
        transform = None
    alias: exp.Expr | None = None
    if parser._match_text_seq("AS"):
        alias = parser._parse_field(any_token=True)
    return this, transform, alias


class CambrianSpark(Spark):
    """Spark dialect with Iceberg DDL extensions.

    Use via ``sqlglot.parse(sql, dialect=CambrianSpark)`` or
    ``sqlglot.parse(sql, dialect=CambrianSpark())``.
    """

    class Parser(SparkParser):
        # Two ALTER_PARSERS entries don't exist in stock Spark: WRITE (for
        # ``WRITE ORDERED BY``) and REPLACE (for ``REPLACE PARTITION FIELD``).
        # We extend by copying the parent's dict and adding the new keys
        # so the parent class's mapping isn't mutated in-place.
        ALTER_PARSERS: t.ClassVar[dict[str, t.Callable[..., t.Any]]] = {
            **SparkParser.ALTER_PARSERS,
            "WRITE": lambda self: self._parse_alter_table_write(),
            "REPLACE": lambda self: self._parse_alter_table_replace(),
            # UNSET TBLPROPERTIES gets its own entry because stock Spark
            # only registers SET; without this UNSET falls to Command.
            "UNSET": lambda self: self._parse_alter_table_unset(),
            # Override SET so ``SET IDENTIFIER FIELDS`` is intercepted before
            # the stock SET-TBLPROPERTIES handler (which leaves IDENTIFIER
            # unconsumed and trips the fall-to-Command guard).
            "SET": lambda self: self._parse_alter_table_set_ext(),
        }

        def _parse_alter_table_add(self) -> list[exp.Expr]:
            # Intercept ``ADD PARTITION FIELD <payload>`` before the parent's
            # _parse_add_alteration closure (which only knows ``ADD PARTITION
            # (...)``). On a non-match we leave the parser position untouched
            # via ``_match_text_seq``'s built-in rewind and delegate.
            if self._match_text_seq("PARTITION", "FIELD"):
                this, transform, alias = _parse_partition_field_payload(self)
                node = AddPartitionField(this=this, transform=transform, alias=alias)
                return [self.expression(node)]
            # ``ADD COLUMN a.b.c <type>`` — a dotted nested path. Stock Spark
            # parses ``ADD COLUMN c`` but trips on the dotted form (it falls to
            # Command). Detect a dotted identifier and build a ColumnDef whose
            # name carries the full path; dispatch splits it into the PyIceberg
            # tuple form.
            nested = self._parse_add_column_nested_path()
            if nested is not None:
                return [nested]
            return super()._parse_alter_table_add()

        def _parse_add_column_nested_path(self) -> exp.Expr | None:
            index = self._index
            # Only the singular ``ADD COLUMN`` form can carry a dotted nested
            # path; the plural ``ADD COLUMNS (...)`` is left to the parent.
            if self._curr is None or self._curr.text.upper() != "COLUMN":
                return None
            self._advance()
            path = self._parse_column()
            # A dotted path parses to a Column with a non-empty ``table``/``db``
            # chain; a plain column has just ``this``. Only intercept dotted.
            if isinstance(path, exp.Column) and (path.args.get("table") or path.args.get("db")):
                kind = self._parse_types()
                if kind is not None:
                    return self.expression(
                        exp.ColumnDef(this=exp.to_identifier(_dotted_path(path)), kind=kind)
                    )
            self._retreat(index)
            return None

        def _parse_alter_table_drop(self) -> list[exp.Expr]:
            # Intercept ``DROP PARTITION FIELD <ref>`` before parent's csv
            # split. We pop into the same shape parent uses (a list of action
            # nodes) so the surrounding _parse_alter machinery is none the
            # wiser.
            if self._match_text_seq("PARTITION", "FIELD"):
                parsed = self._parse_field(any_token=True)
                if isinstance(parsed, exp.Func):
                    column = _column_arg(parsed)
                    node: exp.Expression = DropPartitionField(this=column, transform=parsed)
                else:
                    node = DropPartitionField(this=parsed)
                return [self.expression(node)]
            if self._match_text_seq("IDENTIFIER", "FIELDS"):
                cols = self._parse_csv(lambda: self._parse_field(any_token=True))
                clean = [c for c in cols if c is not None]
                return [self.expression(DropIdentifierFields(expressions=clean))]
            return super()._parse_alter_table_drop()

        def _parse_alter_table_set_ext(self) -> exp.Expr | list[exp.Expr]:
            if self._match_text_seq("IDENTIFIER", "FIELDS"):
                cols = self._parse_csv(lambda: self._parse_field(any_token=True))
                clean = [c for c in cols if c is not None]
                return [self.expression(SetIdentifierFields(expressions=clean))]
            return super()._parse_alter_table_set()

        def _parse_alter_table_alter(self) -> exp.Expr | None:
            # ``ALTER COLUMN c FIRST`` / ``ALTER COLUMN c AFTER other`` — a
            # position-only reorder with no TYPE/COMMENT. Stock Spark builds an
            # AlterColumn but leaves FIRST/AFTER unconsumed, so the statement
            # falls to Command; we consume the trailing position clause and
            # attach it via ``position``/``after`` so dispatch can reorder.
            action = super()._parse_alter_table_alter()
            if (
                isinstance(action, exp.AlterColumn)
                and action.args.get("dtype") is None
                and action.args.get("comment") is None
                and self._curr is not None
            ):
                if self._match_text_seq("FIRST"):
                    return self.expression(AlterColumnPosition(this=action.this, position="FIRST"))
                if self._match_text_seq("AFTER"):
                    return self.expression(
                        AlterColumnPosition(
                            this=action.this,
                            position="AFTER",
                            after=self._parse_field(any_token=True),
                        )
                    )
            return action

        def _parse_alter_drop_action(self) -> exp.Expr | None:
            # Stock Spark's drop_column only handles ``DROP COLUMNS (a, b)``
            # (plural with parens) — singular ``DROP COLUMN c`` falls through.
            # Iceberg-Spark supports the singular form, so we handle it here.
            if self._match_text_seq("DROP", "COLUMN"):
                column = self._parse_column()
                if column is not None:
                    return self.expression(exp.Drop(this=column, kind="COLUMN"))
            return super()._parse_alter_drop_action()

        def _parse_alter_table_write(self) -> list[exp.Expr]:
            # The whole ``WRITE`` distribution-mode family per Iceberg-Spark:
            #   WRITE ORDERED BY <sort>                  → range  + sort
            #   WRITE LOCALLY ORDERED BY <sort>          → none   + sort
            #   WRITE DISTRIBUTED BY PARTITION           → hash   (sort unchanged)
            #   WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY <sort> → hash + sort
            #   WRITE UNORDERED                          → clear sort order
            if self._match_text_seq("UNORDERED"):
                return [self.expression(WriteDistribution(mode="unordered"))]

            distributed = bool(self._match_text_seq("DISTRIBUTED", "BY", "PARTITION"))
            # LOCALLY only meaningfully precedes ORDERED BY; consume it so the
            # mode is "none" (no global range shuffle) rather than "range".
            locally = bool(self._match_text_seq("LOCALLY"))
            ordered = bool(self._match_text_seq("ORDERED", "BY"))
            sort_items = self._parse_ordered_sort_list() if ordered else []

            if distributed:
                # DISTRIBUTED [LOCALLY ORDERED BY ...] → hash. The sort order,
                # if present, is still applied on top of the hash distribution.
                return [self.expression(WriteDistribution(mode="hash", expressions=sort_items))]
            if locally:
                return [self.expression(WriteDistribution(mode="none", expressions=sort_items))]
            if ordered:
                # Bare ``WRITE ORDERED BY ...`` → range distribution + sort.
                # Kept as WriteOrderedBy so the existing dispatch path (and
                # unit tests) stay valid; the runner sets distribution-mode.
                return [self.expression(WriteOrderedBy(expressions=sort_items))]
            # Anything else after WRITE is unsupported; returning an empty list
            # leaves the Alter with no actions, which falls to Command and is
            # rejected at dispatch.
            return []

        def _parse_ordered_sort_list(self) -> list[exp.Expr]:
            # Iceberg's canonical form is an unparenthesized comma list
            # (``ORDERED BY a ASC, b DESC``); cambrian's own tests also use the
            # parenthesized form, so accept both. _parse_ordered carries
            # direction (ASC/DESC), NULLS FIRST/LAST, and transform-in-sort.
            wrapped = bool(self._match(TokenType.L_PAREN))
            items = self._parse_csv(self._parse_ordered)
            if wrapped:
                self._match_r_paren()
            return items

        def _parse_alter_table_replace(self) -> list[exp.Expr]:
            # ``REPLACE PARTITION FIELD <old> WITH <new> [AS <name>]``.
            if self._match_text_seq("PARTITION", "FIELD"):
                old = self._parse_field(any_token=True)
                if not self._match_text_seq("WITH"):
                    # ``REPLACE PARTITION FIELD x`` with no WITH clause is
                    # malformed in Iceberg-Spark. Return empty so dispatch
                    # raises a clear error rather than emitting a half-built
                    # node.
                    return []
                this, transform, alias = _parse_partition_field_payload(self)
                # If WITH was followed by a bare column (no transform), keep
                # the column as this; the ReplacePartitionField node still
                # carries transform=None and dispatch handles both shapes.
                node = ReplacePartitionField(
                    this=old,
                    transform=transform if transform is not None else this,
                    alias=alias,
                )
                return [self.expression(node)]
            return []

        def _parse_alter_table_unset(self) -> list[exp.Expr]:
            # ``UNSET TBLPROPERTIES (k1, k2, ...)``. Stock Spark only registers
            # SET in ALTER_PARSERS, so UNSET TBLPROPERTIES falls through to
            # Command. We emit a custom ``UnsetTblProperties`` AST node that
            # dispatch routes to ``Transaction.remove_properties``.
            if self._match_text_seq("TBLPROPERTIES"):
                self._match_l_paren()
                keys = self._parse_csv(lambda: self._parse_field(any_token=True))
                self._match_r_paren()
                # Defensive: parse_csv may include None entries if the input
                # is malformed; filter so the dispatch layer always sees
                # well-formed key nodes.
                clean_keys = [k for k in keys if k is not None]
                return [self.expression(UnsetTblProperties(expressions=clean_keys))]
            return []

        def _parse_create(self) -> exp.Expression:
            # ``CREATE NAMESPACE ns WITH PROPERTIES (...)`` falls to Command in
            # stock Spark (the trailing WITH PROPERTIES is left unconsumed). We
            # parse the namespace + property tail ourselves and emit a normal
            # exp.Create so the existing create-namespace dispatch handles it.
            index = self._index
            if self._match_set(_NAMESPACE_TOKENS):
                if_not_exists = self._parse_exists(not_=True)
                name = self._parse_table_parts()
                props = self._parse_namespace_properties()
                node = exp.Create(this=name, kind="NAMESPACE", exists=bool(if_not_exists))
                if props is not None:
                    node.set("properties", props)
                return self.expression(node)
            self._retreat(index)
            return super()._parse_create()

        def _parse_namespace_properties(self) -> exp.Properties | None:
            # ``WITH PROPERTIES (k = v, ...)`` — also accept the bare
            # ``PROPERTIES`` and ``WITH DBPROPERTIES`` spellings Spark allows.
            self._match_text_seq("WITH")
            if self._match_texts(("PROPERTIES", "DBPROPERTIES")):
                pairs = self._parse_wrapped_csv(self._parse_namespace_property_pair)
                return self.expression(exp.Properties(expressions=pairs))
            return None

        def _parse_namespace_property_pair(self) -> exp.Expression:
            key = self._parse_field(any_token=True)
            self._match(TokenType.EQ)
            value = self._parse_field(any_token=True)
            return self.expression(exp.Property(this=key, value=value))

        def _parse_alter(self) -> exp.Expression:
            # ``ALTER NAMESPACE ns SET PROPERTIES (...)`` — NAMESPACE isn't in
            # stock Spark's ALTERABLES, so the whole statement falls to Command.
            # Intercept it before delegating to the table-oriented parent.
            index = self._index
            if self._match_set(_NAMESPACE_TOKENS):
                name = self._parse_table_parts()
                self._match(TokenType.SET)
                props = self._parse_namespace_properties()
                if props is not None:
                    return self.expression(
                        AlterNamespaceProperties(this=name, expressions=props.expressions)
                    )
            self._retreat(index)
            return super()._parse_alter()
