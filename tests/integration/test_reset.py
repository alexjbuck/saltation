"""Integration tests for ``apply_reset`` against a live Lakekeeper.

These cover the M6b path end-to-end:

* reset roundtrip: apply → non-idempotent mutation → reset → end state matches new SQL
* external-write detection: a stray append between applies refuses; ``force=True`` overrides
* watch + reset: rapid edits coalesce into a single rollback+apply cycle
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
from anyio import Event
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from saltation.config import DevConfig, EvolutionsConfig, SaltationConfig
from saltation.errors import ExternalWriteDetectedError, IllegalStateError
from saltation.migrate.runner import (
    CURRENT_EVOLUTION_ID,
    apply_idempotent,
    apply_reset,
    rollback_to_last_checkpoint,
)
from saltation.migrate.watch import WatchEvent, watch
from saltation.sidecar.events import latest_event

from .rig import catalog_config
from .rig import rest_catalog as rig_catalog


def _build_config(
    *, evolutions_dir: Path, sidecar_namespace: str, debounce_ms: int = 100
) -> SaltationConfig:
    return SaltationConfig(
        catalog=catalog_config(),
        evolutions=EvolutionsConfig(
            dir=str(evolutions_dir),
            sidecar_namespace=sidecar_namespace,
        ),
        dev=DevConfig(debounce_ms=debounce_ms),
    )


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _verify_catalog() -> RestCatalog:
    return rig_catalog("verify")


@pytest.fixture
def sidecar_ns(rest_catalog: RestCatalog) -> Iterator[str]:
    namespace = f"_saltation_test_{uuid.uuid4().hex[:8]}"
    yield namespace
    try:
        for ident in rest_catalog.list_tables(namespace):
            rest_catalog.drop_table(ident)
        rest_catalog.drop_namespace(namespace)
    except (NoSuchNamespaceError, NamespaceNotEmptyError):
        pass


def test_reset_roundtrip(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Each reset lands on the committed state plus the latest current.sql.

    Nothing here is committed, so the table belongs to current.sql: every
    reset drops it and runs the new text, and rows from earlier versions of
    the file (applied idempotently or by reset) don't survive.
    """
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    def names() -> list[str]:
        rows = _verify_catalog().load_table((ns, "t")).scan().to_arrow().to_pylist()
        return sorted(r["name"] for r in rows)

    def version(name: str) -> str:
        return (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
            f"INSERT INTO {ns}.t VALUES (1, '{name}');\n"
        )

    _write(current, version("seed"))
    apply_idempotent(cfg)
    assert names() == ["seed"]

    _write(current, version("alice"))
    r1 = apply_reset(cfg)
    assert r1.status == "applied"
    assert names() == ["alice"]

    _write(current, version("bob"))
    r2 = apply_reset(cfg)
    assert r2.status == "applied", r2
    assert any(rb.rolled_back for rb in r2.rollbacks), r2.rollbacks
    assert names() == ["bob"]


def test_reset_empties_a_committed_empty_table(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    rest_catalog.create_table((ns, "t"), schema=pa.schema([("id", pa.int64())]))
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    for i in (1, 2):
        _write(current, f"INSERT INTO {ns}.t VALUES ({i});\n")
        assert apply_reset(cfg).status == "applied"
        rows = _verify_catalog().load_table((ns, "t")).scan().to_arrow().column("id")
        assert rows.to_pylist() == [i]


def test_reset_keeps_committed_rows(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """A table that predates current.sql keeps its rows; only current.sql's go."""
    rest_catalog.create_table((ns, "t"), schema=pa.schema([("id", pa.int64())]))
    rest_catalog.load_table((ns, "t")).append(pa.table({"id": [0]}))
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    for i in (1, 2, 3):
        _write(current, f"INSERT INTO {ns}.t VALUES ({i});\n")
        assert apply_reset(cfg).status == "applied"
        rows = _verify_catalog().load_table((ns, "t")).scan().to_arrow().column("id")
        assert sorted(rows.to_pylist()) == [0, i]

    assert rollback_to_last_checkpoint(cfg).status == "applied"
    rows = _verify_catalog().load_table((ns, "t")).scan().to_arrow().column("id")
    assert rows.to_pylist() == [0]

    # Same text after a rollback must run again, not short-circuit.
    assert apply_reset(cfg).status == "applied"
    rows = _verify_catalog().load_table((ns, "t")).scan().to_arrow().column("id")
    assert sorted(rows.to_pylist()) == [0, 3]


def test_reset_external_write_refused_without_force(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Out-of-band write since last apply → reset refuses unless --force."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(
        current,
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    # First reset: captures checkpoint, no actual rollback.
    apply_reset(cfg)

    # Out-of-band append directly via PyIceberg.
    verify = _verify_catalog()
    table = verify.load_table((ns, "t"))
    table.append(
        pa.Table.from_pylist(
            [{"id": 999}],
            schema=pa.schema([("id", pa.int64())]),
        )
    )

    # Edit current.sql so a re-apply is required (hash differs).
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
            f"ALTER TABLE {ns}.t ADD COLUMN extra STRING;\n"
        ),
    )

    # Without --force, the divergence is caught.
    with pytest.raises(ExternalWriteDetectedError):
        apply_reset(cfg)

    # With --force the divergence is overridden and the apply proceeds.
    result = apply_reset(cfg, force=True)
    assert result.status == "applied", result


def test_rollback_command_undoes_last_apply(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """``rollback_to_last_checkpoint`` undoes the prior apply without re-executing."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(
        current,
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    # Apply twice so we have a prior checkpoint to roll back to.
    apply_idempotent(cfg)
    verify = _verify_catalog()
    table = verify.load_table((ns, "t"))
    table.append(
        pa.Table.from_pylist(
            [{"id": 1}, {"id": 2}],
            schema=pa.schema([("id", pa.int64())]),
        )
    )

    # Edit current.sql so the second apply has something to do.
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
            f"ALTER TABLE {ns}.t ADD COLUMN extra STRING;\n"
        ),
    )
    apply_idempotent(cfg)

    # Rollback: drop the ALTER's schema change AND the prior appends.
    rb = rollback_to_last_checkpoint(cfg)
    assert rb.status == "applied"
    assert rb.rollback_event_id is not None
    assert any(r.rolled_back for r in rb.rollbacks), rb.rollbacks

    # The rollback event is now the most recent event for evolution_id="current".
    verify = _verify_catalog()
    latest = latest_event(
        verify,
        sidecar_ns,
        event_type="rollback",
        evolution_id=CURRENT_EVOLUTION_ID,
    )
    assert latest is not None
    assert latest.event_id == rb.rollback_event_id


async def _drive_watch_reset(
    cfg: SaltationConfig,
    *,
    actions: list,
    expected_events: int,
    timeout_s: float = 30.0,
) -> list[WatchEvent]:
    events: list[WatchEvent] = []
    stop = Event()
    saw = asyncio.Event()
    remaining = {"n": expected_events}

    started = asyncio.Event()

    async def _on_event(event: WatchEvent) -> None:
        events.append(event)
        if event.kind == "startup":
            started.set()
        if event.kind in {"applied", "unchanged", "partial", "error"} and event.paths_changed:
            remaining["n"] -= 1
            if remaining["n"] <= 0:
                saw.set()

    watcher_task = asyncio.create_task(
        watch(
            cfg,
            on_event=_on_event,
            stop_event=stop,
            use_reset=True,
            json_output=False,
        )
    )

    try:
        # Edits made before the watcher starts watching (it first runs the
        # initial apply) are never seen; wait for its startup event.
        await asyncio.wait_for(started.wait(), timeout=timeout_s)
        for act in actions:
            await act()
        await asyncio.wait_for(saw.wait(), timeout=timeout_s)
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(watcher_task, timeout=5.0)
    return events


def test_watch_reset_coalesces_rapid_edits(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Two rapid writes under watch+reset → one rollback+apply cycle, not two."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(
        current,
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns, debounce_ms=500)

    async def _rapid_edits() -> None:
        await asyncio.sleep(0.7)
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
                f"ALTER TABLE {ns}.t ADD COLUMN x INT;\n"
            ),
        )
        await asyncio.sleep(0.05)
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
                f"ALTER TABLE {ns}.t ADD COLUMN x INT;\n"
                f"ALTER TABLE {ns}.t ADD COLUMN y INT;\n"
            ),
        )

    events = asyncio.run(
        _drive_watch_reset(cfg, actions=[_rapid_edits], expected_events=1, timeout_s=30.0)
    )

    applies_with_changes = [
        e for e in events if e.kind in {"applied", "unchanged"} and e.paths_changed
    ]
    assert len(applies_with_changes) == 1, [(e.kind, e.paths_changed) for e in events]

    # End state has both x and y columns (the second write won).
    verify = _verify_catalog()
    table = verify.load_table((ns, "t"))
    names = {f.name for f in table.schema().fields}
    assert {"id", "x", "y"}.issubset(names), names


def test_reset_unchanged_when_hash_matches(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Two consecutive resets with the same current.sql: the second is a no-op via hash check.

    Reset still emits a rollback event each time (it always captures fresh
    state for the next reset cycle), but the apply phase short-circuits
    via the M5 hash check.
    """
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(
        current,
        f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n",
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    r1 = apply_reset(cfg)
    assert r1.status == "applied"

    r2 = apply_reset(cfg)
    # Apply phase saw matching hash → "unchanged".
    assert r2.status == "unchanged", r2


def test_reset_with_view_in_script(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Views aren't tables: reset rolls back the table and re-runs the view statement."""
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
            f"INSERT INTO {ns}.t VALUES (0, 'seed');\n"
            f"CREATE OR REPLACE VIEW {ns}.v AS SELECT id FROM {ns}.t;\n"
        ),
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
            f"INSERT INTO {ns}.t VALUES (1, 'alice');\n"
            f"CREATE OR REPLACE VIEW {ns}.v AS SELECT id, name FROM {ns}.t;\n"
        ),
    )
    result = apply_reset(cfg)
    assert result.status == "applied", result
    assert [f.name for f in rest_catalog.load_view((ns, "v")).schema().fields] == ["id", "name"]
    rest_catalog.drop_view((ns, "v"))


def test_reset_refuses_to_undo_a_format_upgrade(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    rest_catalog.create_table((ns, "t"), schema=pa.schema([("id", pa.int64())]))
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    _write(current, f"ALTER TABLE {ns}.t SET TBLPROPERTIES ('format-version' = '3');\n")
    assert apply_reset(cfg).status == "applied"
    _write(current, f"ALTER TABLE {ns}.t ADD COLUMN c INT;\n")
    with pytest.raises(IllegalStateError, match="can't be downgraded"):
        apply_reset(cfg)
    # Refused before touching anything.
    assert [f.name for f in rest_catalog.load_table((ns, "t")).schema().fields] == ["id"]


def test_reset_forgets_partition_fields_of_undone_edits(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Edit 1 partitions by a new column; edit 2 re-adds that column with another type."""
    rest_catalog.create_table((ns, "t"), schema=pa.schema([("id", pa.int64())]))
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)

    _write(
        current,
        f"ALTER TABLE {ns}.t ADD COLUMN region STRING;\n"
        f"ALTER TABLE {ns}.t ADD PARTITION FIELD region;\n"
        f"INSERT INTO {ns}.t VALUES (1, 'eu');\n",
    )
    assert apply_reset(cfg).status == "applied"
    _write(current, f"ALTER TABLE {ns}.t ADD COLUMN region INT;\n")
    assert apply_reset(cfg).status == "applied"

    table = rest_catalog.load_table((ns, "t"))
    assert [(f.name, str(f.field_type)) for f in table.schema().fields] == [
        ("id", "long"),
        ("region", "int"),
    ]
    assert [s.spec_id for s in table.metadata.partition_specs] == [0]
    assert table.current_snapshot() is None
