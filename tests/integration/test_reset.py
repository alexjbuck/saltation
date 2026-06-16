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

from cambrian.config import CambrianConfig, CatalogConfig, DevConfig, EvolutionsConfig
from cambrian.errors import ExternalWriteDetectedError
from cambrian.migrate.runner import (
    CURRENT_EVOLUTION_ID,
    apply_idempotent,
    apply_reset,
    rollback_to_last_checkpoint,
)
from cambrian.migrate.watch import WatchEvent, watch
from cambrian.sidecar.events import latest_event

LAKEKEEPER_URL = "http://localhost:8181"
WAREHOUSE = "cambrian"


def _build_config(
    *, evolutions_dir: Path, sidecar_namespace: str, debounce_ms: int = 100
) -> CambrianConfig:
    return CambrianConfig(
        catalog=CatalogConfig(
            type="rest",
            uri=f"{LAKEKEEPER_URL}/catalog",
            **{
                "warehouse": WAREHOUSE,
                "s3.endpoint": "http://localhost:9000",
                "s3.access-key-id": "cambrian-access-key",
                "s3.secret-access-key": "cambrian-secret-key",
                "s3.region": "local",
                "s3.path-style-access": "true",
            },
        ),
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
    return RestCatalog(
        name="verify",
        **{
            "uri": f"{LAKEKEEPER_URL}/catalog",
            "warehouse": WAREHOUSE,
            "s3.endpoint": "http://localhost:9000",
            "s3.access-key-id": "cambrian-access-key",
            "s3.secret-access-key": "cambrian-secret-key",
            "s3.region": "local",
            "s3.path-style-access": "true",
        },
    )


@pytest.fixture
def sidecar_ns(rest_catalog: RestCatalog) -> Iterator[str]:
    namespace = f"_cambrian_test_{uuid.uuid4().hex[:8]}"
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
    """Reset path: apply idempotently, then mutate current.sql with reset → rolls back + re-applies.

    Reset rolls back tables to the state captured at the start of the
    *previous* reset. For tables that didn't exist at the previous reset
    (i.e. created by the apply itself), the 4-pointer primitive can't
    restore them to non-existence; reset only restores tables that
    existed before the previous reset cycle. That's the documented
    semantic of M4's restore_pointers.
    """
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"

    # Bootstrap the table and insert one row idempotently so the table
    # has a snapshot *before* the first reset captures its pre-state.
    # (The 4-pointer rollback primitive can't restore "no snapshot" if
    # there's now a snapshot — that's M4's IllegalStateError guard. The
    # reset semantic requires the table to be in a fully-formed state
    # at the start of the reset cycle.)
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
            f"INSERT INTO {ns}.t VALUES (0, 'seed');\n"
        ),
    )
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns)
    apply_idempotent(cfg)

    # Switch to reset mode: current.sql now only declares alice (no seed
    # INSERT). The seed remains in the table because it was applied
    # idempotently in the pre-reset bootstrap; reset operates on the
    # table state, not on the SQL text.
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
            f"INSERT INTO {ns}.t VALUES (1, 'alice');\n"
        ),
    )
    r1 = apply_reset(cfg)
    assert r1.status == "applied"

    verify = _verify_catalog()
    rows = verify.load_table((ns, "t")).scan().to_arrow().to_pylist()
    # seed (from idempotent bootstrap) + alice (from reset r1)
    assert sorted(r["name"] for r in rows) == ["alice", "seed"]

    # Second reset: current.sql replaces alice's INSERT with bob's.
    # Reset rolls the table back to pre-r1 (just seed), then re-applies
    # → only bob got appended this cycle. Final state: seed + bob.
    _write(
        current,
        (
            f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT, name STRING) USING iceberg;\n"
            f"INSERT INTO {ns}.t VALUES (2, 'bob');\n"
        ),
    )
    r2 = apply_reset(cfg)
    assert r2.status == "applied", r2
    assert any(rb.rolled_back for rb in r2.rollbacks), r2.rollbacks

    verify = _verify_catalog()
    rows = verify.load_table((ns, "t")).scan().to_arrow().to_pylist()
    assert sorted(r["name"] for r in rows) == ["bob", "seed"]


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
    cfg: CambrianConfig,
    *,
    actions: list,
    expected_events: int,
    timeout_s: float = 30.0,
) -> list[WatchEvent]:
    events: list[WatchEvent] = []
    stop = Event()
    saw = asyncio.Event()
    remaining = {"n": expected_events}

    async def _on_event(event: WatchEvent) -> None:
        events.append(event)
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
