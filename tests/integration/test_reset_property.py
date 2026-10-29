"""Property: reset mode converges, however many times ``current.sql`` is edited.

Reset is the relief valve for SQL that can't be re-applied: each reset rolls
the affected objects back to where they stood before ``current.sql`` was first
applied, then runs the new text. So after any sequence of edits, the catalog
must look exactly like the committed base plus the *latest* edit applied once.
Edits are generated with the non-replayable statement classes switched on,
since those are what reset exists for.

Runs against the REST rig: rollback is a multi-update atomic commit.
"""

from __future__ import annotations

import uuid
from collections.abc import Iterator

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pyiceberg.catalog.rest import RestCatalog

from saltation.config import EvolutionsConfig, SaltationConfig
from saltation.migrate.runner import apply_reset
from tests.fixtures.evolution_strategies import (
    EvolutionScript,
    Features,
    apply_script,
    catalog_fingerprint,
    edited_scripts,
    evolution_scripts,
)

from .conftest import _drop_namespace_recursively
from .rig import catalog_config

RESET_ONLY = Features(
    drop_after_alter=True,
    relax_former_identifier=True,
    multiple_reorders=True,
    readd_partition_field=True,
    insert=True,
    upgrade_v3=False,
)


@pytest.fixture
def namespaces(rest_catalog: RestCatalog) -> Iterator[list[str]]:
    created: list[str] = []
    yield created
    for ns in created:
        _drop_namespace_recursively(rest_catalog, ns)


@st.composite
def _base_and_edits(draw: st.DrawFn) -> tuple[EvolutionScript, list[EvolutionScript]]:
    base = draw(evolution_scripts(Features(insert=True), max_steps=6), label="base")
    n = draw(st.integers(min_value=1, max_value=4), label="edits")
    edits = [draw(edited_scripts(base, RESET_ONLY), label=f"edit{i}") for i in range(n)]
    return base, edits


@given(case=_base_and_edits())
def test_reset_lands_on_base_plus_latest_edit(
    rest_catalog: RestCatalog,
    namespaces: list[str],
    tmp_path_factory: pytest.TempPathFactory,
    case: tuple[EvolutionScript, list[EvolutionScript]],
) -> None:
    base, edits = case
    ns = f"reset_{uuid.uuid4().hex[:12]}"
    sidecar = f"_saltation_{ns}"
    namespaces += [ns, sidecar]
    current = tmp_path_factory.mktemp("evolutions") / "current.sql"
    config = SaltationConfig(
        catalog=catalog_config(),
        evolutions=EvolutionsConfig(dir=str(current.parent), sidecar_namespace=sidecar),
    )

    apply_script(rest_catalog, base.render(ns))
    for edit in edits:
        current.write_text(edit.render(ns), encoding="utf-8")
        result = apply_reset(config)
        assert result.error is None, result.error

    assert catalog_fingerprint(rest_catalog, ns) == edits[-1].expected
