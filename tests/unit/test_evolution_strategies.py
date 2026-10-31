"""Generator invariants the idempotency property suites rely on.

Replaying a script re-adds every partition field it dropped. If a later
statement added another time transform on that column, the replay commits a
spec holding both, e.g. ``ADD PARTITION FIELD year(c4)`` over a live
``month(c4)``. PyIceberg (like Iceberg Java) builds that spec without
complaint; Lakekeeper refuses it with a 400 (nightly soak, 2026-10). So in the
default, replayable features no time transform follows a dropped one on the
same column; reset-only features (``readd_partition_field``) keep it.
"""

from __future__ import annotations

import re
from collections import Counter

from hypothesis import given

from tests.fixtures.evolution_strategies import (
    DATE,
    Column,
    EvolutionScript,
    Features,
    TableModel,
    _partition_options,
    evolution_scripts,
)

_TIME = re.compile(r"\b(year|month|day|hour)\((\w+)\)")
_TABLE = re.compile(r"\{ns\}\.(\w+)")

# The nightly failure, minus statements that don't touch partitioning.
NIGHTLY_FAILURE = [
    "CREATE TABLE IF NOT EXISTS {ns}.t0 (c0 BIGINT, c4 DATE) USING iceberg;",
    "ALTER TABLE {ns}.t0 ADD PARTITION FIELD year(c4);",
    "ALTER TABLE {ns}.t0 DROP PARTITION FIELD year(c4);",
    "ALTER TABLE {ns}.t0 ADD PARTITION FIELD month(c4);",
]


def _redundant_time_fields(applied: list[str]) -> list[str]:
    """Statements after which a table holds two time transforms on one column."""
    live: dict[str, set[tuple[str, str]]] = {}
    bad: list[str] = []
    for stmt in applied:
        m = _TABLE.search(stmt)
        if m is None:
            continue
        fields = {(col, tr) for tr, col in _TIME.findall(stmt)}
        if stmt.startswith("CREATE TABLE IF NOT EXISTS"):
            if m[1] not in live:
                live[m[1]] = {f for f in fields if "PARTITIONED BY" in stmt}
        elif " ADD PARTITION FIELD " in stmt:
            live[m[1]] |= fields
        elif " DROP PARTITION FIELD " in stmt:
            live[m[1]] -= fields
        if any(n > 1 for n in Counter(col for col, _ in live.get(m[1], ())).values()):
            bad.append(stmt)
    return bad


def test_checker_flags_the_nightly_failure_on_replay() -> None:
    assert _redundant_time_fields(NIGHTLY_FAILURE) == []
    assert _redundant_time_fields(NIGHTLY_FAILURE * 2) == [NIGHTLY_FAILURE[1]]


def _model_with_dropped_year() -> TableModel:
    t = TableModel(name="t0", columns=[Column("c4", DATE)], next_col=5)
    t.pinned.add("c4")
    t.dropped_partitions.add(("c4", "year"))
    return t


def test_no_time_transform_follows_a_dropped_one() -> None:
    offered = {tr for _, _, tr in _partition_options(_model_with_dropped_year(), Features())}
    assert not offered & {"year", "month", "day", "hour"}
    assert "identity" in offered


def test_reset_only_features_keep_readding_time_transforms() -> None:
    features = Features(readd_partition_field=True)
    offered = {tr for _, _, tr in _partition_options(_model_with_dropped_year(), features)}
    assert {"year", "month", "day"} <= offered


@given(script=evolution_scripts())
def test_replay_never_stacks_time_transforms(script: EvolutionScript) -> None:
    stmts = script.statements
    for k in range(1, len(stmts) + 1):
        assert _redundant_time_fields(stmts[:k] + stmts) == [], script
