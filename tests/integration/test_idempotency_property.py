"""Property: applying an evolution is idempotent and lands on the modelled state.

The runner short-circuits an unchanged ``current.sql`` by hash, so a literal
second ``saltation apply`` proves nothing. What really re-runs is the
parse→dispatch loop: every edit of ``current.sql`` re-applies the whole file
over whatever the previous version left behind, and a retry after a partial
failure re-applies over a prefix. These tests drive that loop directly.

Runs against Lakekeeper: SqlCatalog doesn't share REST commit semantics.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyiceberg.catalog.rest import RestCatalog

from tests.fixtures.evolution_strategies import (
    EvolutionScript,
    apply_script,
    catalog_fingerprint,
    evolution_scripts,
)

from .conftest import _drop_namespace_recursively


@pytest.fixture
def fresh_ns(rest_catalog: RestCatalog) -> Iterator[list[str]]:
    # Hypothesis re-enters the test body many times per fixture instance, so
    # namespaces are minted inside the body and torn down here in bulk.
    created: list[str] = []
    yield created
    for ns in created:
        _drop_namespace_recursively(rest_catalog, ns)


def _mint(created: list[str]) -> str:
    ns = f"prop_{uuid.uuid4().hex[:12]}"
    created.append(ns)
    return ns


@given(script=evolution_scripts())
def test_apply_matches_model_and_reapply_is_noop(
    rest_catalog: RestCatalog, fresh_ns: list[str], script: EvolutionScript
) -> None:
    ns = _mint(fresh_ns)
    sql = script.render(ns)

    apply_script(rest_catalog, sql)
    assert catalog_fingerprint(rest_catalog, ns) == script.expected

    apply_script(rest_catalog, sql)
    assert catalog_fingerprint(rest_catalog, ns) == script.expected


@given(script=evolution_scripts(), data=st.data())
def test_retry_after_partial_apply_converges(
    rest_catalog: RestCatalog, fresh_ns: list[str], script: EvolutionScript, data: st.DataObject
) -> None:
    # A failure at statement k leaves statements[:k] committed; the retry then
    # replays the full script over that state.
    k = data.draw(st.integers(min_value=1, max_value=len(script.statements)), label="prefix")
    ns = _mint(fresh_ns)

    apply_script(rest_catalog, script.render(ns, upto=k))
    apply_script(rest_catalog, script.render(ns))
    assert catalog_fingerprint(rest_catalog, ns) == script.expected


def test_unset_tblproperties_removes_key(rest_catalog: RestCatalog, ns: str) -> None:
    # Lakekeeper < 0.14 kept the key when removing the last remaining property.
    apply_script(
        rest_catalog,
        f"CREATE TABLE {ns}.t (id INT) USING iceberg;\n"
        f"ALTER TABLE {ns}.t SET TBLPROPERTIES ('saltation.p.k'='v');\n"
        f"ALTER TABLE {ns}.t UNSET TBLPROPERTIES ('saltation.p.k');\n",
    )
    assert "saltation.p.k" not in rest_catalog.load_table((ns, "t")).properties
