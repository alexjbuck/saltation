"""Property: rendering a parsed statement gives SQL that parses back to it.

The SQL cambrian records (events, ``apply --json``) is the rendered AST, so a
lossy generator would misreport what ran: ``ALTER COLUMN c TYPE x COMMENT 'd'``
used to render without its TYPE. Rendering must also be a fixed point, and
how statements are laid out in a script (whitespace, comments) must not change
what they parse to.
"""

from __future__ import annotations

import pytest
from hypothesis import given
from hypothesis import strategies as st

from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.script import parse_script
from tests.fixtures.evolution_strategies import EvolutionScript, Features, evolution_scripts
from tests.fixtures.iceberg_corpus import CORPUS

EVERYTHING = Features(
    drop_after_alter=True,
    relax_former_identifier=True,
    multiple_reorders=True,
    readd_partition_field=True,
    insert=True,
)


def _assert_round_trips(sql: str) -> None:
    for statement in parse_script(sql):
        rendered = statement.sql(dialect=CambrianSpark)
        (again,) = parse_script(rendered)
        assert again == statement, f"{sql!r} rendered as {rendered!r}"
        assert again.sql(dialect=CambrianSpark) == rendered


@pytest.mark.parametrize("entry", [e for e in CORPUS if e.expected == "apply"], ids=lambda e: e.id)
def test_corpus_statements_round_trip(entry) -> None:  # type: ignore[no-untyped-def]
    _assert_round_trips(entry.sql)


@given(script=evolution_scripts(EVERYTHING))
def test_generated_statements_round_trip(script: EvolutionScript) -> None:
    _assert_round_trips(script.render("ns"))


_GAPS = st.sampled_from(["\n", "  ", "\n\n", "\n-- note; with a semicolon\n", "/* a; b */ "])


@given(script=evolution_scripts(EVERYTHING), data=st.data())
def test_layout_does_not_change_the_statements(
    script: EvolutionScript, data: st.DataObject
) -> None:
    statements = [s.format(ns="ns") for s in script.statements]
    gaps = data.draw(st.lists(_GAPS, min_size=len(statements), max_size=len(statements)))
    laid_out = "".join(gap + stmt for gap, stmt in zip(gaps, statements, strict=True))
    one_by_one = [parsed for stmt in statements for parsed in parse_script(stmt)]
    assert parse_script(laid_out) == one_by_one
