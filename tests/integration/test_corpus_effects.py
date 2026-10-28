"""Every positive/invalid corpus entry, applied to Lakekeeper and checked for its exact effect.

``test_corpus_coverage`` only proves the SQL parses; this proves it *does the
right thing*. For each entry: build the base + setup, snapshot every target,
apply the corpus SQL, and require the diff to equal the entry's expected
effect exactly (so a dropped clause and an unexpected side effect both fail).
A second apply must then change nothing, except for data ops. ``invalid``
entries must raise a SaltationError and leave every target untouched.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest
from pyiceberg.catalog.rest import RestCatalog

from saltation.errors import SaltationError, UnsupportedStatementError
from tests.fixtures.catalog_state import (
    diff,
    namespace_snapshot,
    table_snapshot,
    view_snapshot,
)
from tests.fixtures.corpus_effects import BASES, EFFECTS, VIEW_DIALECTS, Effect
from tests.fixtures.evolution_strategies import apply_script
from tests.fixtures.iceberg_corpus import CORPUS, CorpusEntry

from .conftest import _drop_namespace_recursively
from .rig import RIG

ENTRIES = [e for e in CORPUS if e.expected in {"apply", "invalid"}]
PARAMS = [
    pytest.param(
        e,
        id=e.id,
        marks=[pytest.mark.xfail(strict=True, reason=reason)]
        if (reason := EFFECTS[e.id].catalog_refuses.get(RIG.name) if e.id in EFFECTS else None)
        else [],
    )
    for e in ENTRIES
]


def test_every_positive_and_invalid_entry_has_an_effect() -> None:
    assert {e.id for e in ENTRIES} == set(EFFECTS)


@pytest.fixture
def ns(rest_catalog: RestCatalog) -> Iterator[str]:
    namespace = f"fx_{uuid.uuid4().hex[:10]}"
    yield namespace
    for child in _safe(lambda: rest_catalog.list_namespaces(namespace)) or []:
        _drop_namespace_recursively(rest_catalog, ".".join(child))
    _drop_namespace_recursively(rest_catalog, namespace)


def _safe(fn: Callable[[], Any]) -> Any:
    try:
        return fn()
    except Exception:
        return None


def _subst(value: Any, ns: str) -> Any:
    if isinstance(value, str):
        value = value.replace("s3://bucket/", f"{RIG.location_root}/{ns}/")
        return re.sub(r"\bfoo\b", ns, value)
    if isinstance(value, dict):
        return {_subst(k, ns): _subst(v, ns) for k, v in value.items()}
    if isinstance(value, list):
        return [_subst(v, ns) for v in value]
    return value


def _snapshot(catalog: RestCatalog, target: str) -> Any:
    kind, _, name = target.rpartition(":")
    if kind == "ns":
        return namespace_snapshot(catalog, name)
    ident = tuple(name.rsplit(".", 1))
    if kind == "view":
        return view_snapshot(catalog, ident)
    return table_snapshot(catalog, ident, RIG.table_defaults)


def _snapshots(catalog: RestCatalog, targets: list[str]) -> dict[str, Any]:
    return {t: _snapshot(catalog, t) for t in targets}


@pytest.mark.parametrize("entry", PARAMS)
def test_corpus_entry_effect(rest_catalog: RestCatalog, ns: str, entry: CorpusEntry) -> None:
    effect: Effect = EFFECTS[entry.id]
    for sql in [*(BASES[effect.base] if effect.base else []), *effect.setup]:
        apply_script(rest_catalog, _subst(sql, ns), VIEW_DIALECTS)
    targets = list(_subst(list(effect.effects), ns))
    sql = _subst(entry.sql, ns)
    before = _snapshots(rest_catalog, targets)

    if entry.expected == "invalid":
        with pytest.raises(SaltationError) as info:
            apply_script(rest_catalog, sql, VIEW_DIALECTS)
        assert not isinstance(info.value, UnsupportedStatementError), info.value
        assert _snapshots(rest_catalog, targets) == before
        return

    apply_script(rest_catalog, sql, VIEW_DIALECTS)
    after = _snapshots(rest_catalog, targets)
    actual = {t: diff(before[t], after[t]) for t in targets}
    assert actual == _subst(effect.effects, ns)

    if effect.reapply:
        apply_script(rest_catalog, sql, VIEW_DIALECTS)
        again = _snapshots(rest_catalog, targets)
        assert {t: diff(after[t], again[t]) for t in targets} == {t: {} for t in targets}
