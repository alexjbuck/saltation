"""Stateful property: the dev lifecycle keeps the catalog and the sidecar honest.

A Hypothesis state machine drives edits of ``current.sql`` through apply,
reset, rollback, commit and uncommit, in any order the preconditions allow,
and after every step checks that:

* the catalog matches the model: the committed evolutions plus the applied
  ``current.sql`` (or just the committed ones when nothing is applied);
* the event log only ever grows, and no event already written changes;
* the sidecar is at the current version (it may start at v1);
* ``committed/`` holds exactly the model's committed evolutions.
"""

from __future__ import annotations

import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import (
    RuleBasedStateMachine,
    initialize,
    invariant,
    precondition,
    rule,
    run_state_machine_as_test,
)
from pyiceberg.catalog.rest import RestCatalog

from saltation.config import EvolutionsConfig, SaltationConfig
from saltation.errors import IllegalStateError
from saltation.migrate.commit import discover_committed_files, saltation_commit, saltation_uncommit
from saltation.migrate.runner import apply_idempotent, apply_reset, rollback_to_last_checkpoint
from saltation.sidecar.events import events, table_states_for_event
from saltation.sidecar.schema import (
    SALTATION_SIDECAR_VERSION,
    SELF_MIGRATIONS,
    TABLE_STATES_TABLE,
)
from saltation.sidecar.selfmigrate import _read_version, ensure_current
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
from .rig import rest_catalog as rig_catalog

RESET_ONLY = Features(
    drop_after_alter=True,
    relax_former_identifier=True,
    multiple_reorders=True,
    readd_partition_field=True,
    insert=True,
    upgrade_v3=False,
)
# An idempotent dev loop appends to current.sql; views go last in a script,
# so a replayable extension can't add any.
EXTENDING = Features(views=False, upgrade_v3=False)


def _extend(prev: EvolutionScript, more: EvolutionScript) -> EvolutionScript:
    return EvolutionScript(
        statements=[*prev.statements, *more.statements], expected=more.expected, models=more.models
    )


class Lifecycle(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.catalog: RestCatalog = rig_catalog("lifecycle")
        self.ns = f"life_{uuid.uuid4().hex[:12]}"
        self.sidecar = f"_saltation_{self.ns}"
        self.dir = Path(tempfile.mkdtemp(prefix="saltation-life-"))
        self.config = SaltationConfig(
            catalog=catalog_config(),
            evolutions=EvolutionsConfig(dir=str(self.dir), sidecar_namespace=self.sidecar),
        )
        self.committed: list[EvolutionScript] = []
        self.current: EvolutionScript | None = None
        self.extending = False
        self.applied = False
        # What the catalog should hold; only running something changes it.
        self.live: dict[str, Any] = {}
        self.seen: dict[str, Any] = {}

    @initialize(
        base=evolution_scripts(Features(insert=True, upgrade_v3=False), max_steps=5),
        legacy=st.booleans(),
    )
    def start(self, base: EvolutionScript, legacy: bool) -> None:
        apply_script(self.catalog, base.render(self.ns))
        self.committed_base = base
        self.live = base.expected
        if legacy:
            # A sidecar an older saltation created: v1 tables, version 1.
            SELF_MIGRATIONS[0](self.catalog, self.sidecar)
        (self.dir / "current.sql").write_text("", encoding="utf-8")

    @property
    def top(self) -> EvolutionScript:
        return self.committed[-1] if self.committed else self.committed_base

    def _write(self, script: EvolutionScript) -> None:
        path = self.dir / "current.sql"
        text = script.render(self.ns)
        # Rewriting the same text leaves an applied current.sql applied.
        self.applied = self.applied and path.read_text(encoding="utf-8") == text
        path.write_text(text, encoding="utf-8")
        self.current = script

    @rule(data=st.data())
    def edit_for_reset(self, data: st.DataObject) -> None:
        self._write(data.draw(edited_scripts(self.top, RESET_ONLY), label="edit"))
        self.extending = False

    @precondition(lambda self: self.current is None or self.extending)
    @rule(data=st.data())
    def extend(self, data: st.DataObject) -> None:
        prev = self.current or EvolutionScript([], self.top.expected, self.top.models)
        more = data.draw(edited_scripts(prev, EXTENDING, max_steps=3), label="extension")
        self._write(_extend(prev, more))
        self.extending = True

    @precondition(lambda self: self.current is not None and self.extending)
    @rule()
    def apply(self) -> None:
        result = apply_idempotent(self.config)
        assert result.error is None, result.error
        assert self.current is not None
        self.applied = True
        self.live = self.current.expected

    @precondition(lambda self: self.current is not None)
    @rule()
    def reset(self) -> None:
        result = apply_reset(self.config)
        assert result.error is None, result.error
        assert self.current is not None
        self.applied = True
        self.live = self.current.expected

    @rule()
    def rollback(self) -> None:
        rollback_to_last_checkpoint(self.config)
        self.applied = False
        self.live = self.top.expected

    @precondition(lambda self: self.current is not None and self.current.statements)
    @rule()
    def commit(self) -> None:
        if not self.applied:
            with pytest.raises(IllegalStateError):
                saltation_commit(self.config, message="refused")
            return
        saltation_commit(self.config, message=f"step {len(self.committed) + 1}")
        assert self.current is not None
        self.committed.append(self.current)
        self.current = None
        self.extending = False
        self.applied = False

    @precondition(lambda self: self.committed and self.current is None)
    @rule()
    def uncommit(self) -> None:
        saltation_uncommit(self.config)
        self.current = self.committed.pop()
        self.extending = False
        self.applied = False
        self.live = self.top.expected

    @invariant()
    def catalog_matches_model(self) -> None:
        if self.live:
            assert catalog_fingerprint(self.catalog, self.ns) == self.live

    @invariant()
    def event_log_only_grows(self) -> None:
        if not self.catalog.namespace_exists(self.sidecar):
            return
        log = {e.event_id: e for e in events(self.catalog, self.sidecar)}
        for event_id, event in self.seen.items():
            assert log.get(event_id) == event, f"event {event_id} changed or vanished"
        self.seen = log

    @invariant()
    def committed_files_match(self) -> None:
        files = discover_committed_files(self.dir / "committed")
        assert len(files) == len(self.committed)

    @invariant()
    def sidecar_is_current(self) -> None:
        if self.catalog.table_exists((self.sidecar, "events")) and self.seen:
            assert _read_version(self.catalog, self.sidecar) == SALTATION_SIDECAR_VERSION

    def teardown(self) -> None:
        for ns in (self.ns, self.sidecar):
            _drop_namespace_recursively(self.catalog, ns)
        shutil.rmtree(self.dir, ignore_errors=True)


def test_lifecycle(rest_catalog: RestCatalog) -> None:
    del rest_catalog  # brings the rig up
    # Each example is ~10 catalog-mutating steps; a quarter of the profile's
    # examples keeps the run comparable to the other properties.
    budget = settings(
        max_examples=max(4, settings.default.max_examples // 4), stateful_step_count=10
    )
    run_state_machine_as_test(Lifecycle, settings=budget)


def test_v1_sidecar_rows_survive_the_v2_migration(rest_catalog: RestCatalog) -> None:
    sidecar = f"_saltation_mig_{uuid.uuid4().hex[:8]}"
    try:
        SELF_MIGRATIONS[0](rest_catalog, sidecar)
        table = rest_catalog.load_table((sidecar, TABLE_STATES_TABLE))
        v1_row = pa.Table.from_pylist(
            [
                {
                    "event_id": "e1",
                    "event_ts": pa.scalar(0, pa.timestamp("us", tz="UTC")).as_py(),
                    "table_ident": "ns.t",
                    "pre_schema_id": 3,
                }
            ],
            schema=table.schema().as_arrow(),
        )
        table.append(v1_row)

        ensure_current(rest_catalog, sidecar, allow_read_only=False)

        assert _read_version(rest_catalog, sidecar) == SALTATION_SIDECAR_VERSION
        (row,) = table_states_for_event(rest_catalog, sidecar, event_id="e1")
        assert (row.table_ident, row.pre_schema_id) == ("ns.t", 3)
        assert (row.object_kind, row.pre_properties, row.pre_view_version_id) == (
            "table",
            None,
            None,
        )
    finally:
        _drop_namespace_recursively(rest_catalog, sidecar)
