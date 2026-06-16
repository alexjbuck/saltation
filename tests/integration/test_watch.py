"""Integration tests for ``cambrian watch`` against Lakekeeper.

The watch loop runs as an asyncio task in-process; we drive it by
mutating files on disk and asserting against the catalog. ``awatch``'s
real debounce coalesces rapid edits, so two ~10ms-apart writes should
produce one apply, not two.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Iterator
from pathlib import Path

import pytest
from anyio import Event
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.exceptions import NamespaceNotEmptyError, NoSuchNamespaceError

from cambrian.config import CambrianConfig, CatalogConfig, DevConfig, EvolutionsConfig
from cambrian.migrate.watch import WatchEvent, watch

LAKEKEEPER_URL = "http://localhost:8181"
WAREHOUSE = "cambrian"


def _build_config(
    *, evolutions_dir: Path, sidecar_namespace: str, debounce_ms: int
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


async def _drive_watch(
    cfg: CambrianConfig,
    *,
    actions: list,  # list of async callables to run between event observations
    expected_apply_events: int,
    timeout_s: float = 30.0,
) -> list[WatchEvent]:
    """Run the watch loop until *expected_apply_events* apply/unchanged/partial events have arrived.

    ``actions`` is a parallel list of awaitables — between observing each
    apply event we await the next action so the test can mutate files.
    The 0-th action runs *before* the watcher is even given a chance to
    fire.
    """
    events: list[WatchEvent] = []
    stop = Event()
    saw_apply = asyncio.Event()
    applies_remaining = {"n": expected_apply_events}

    async def _on_event(event: WatchEvent) -> None:
        events.append(event)
        # Count any post-startup event tied to a real filesystem change,
        # including errors — the test cases assert on whichever outcomes
        # they expect.
        if event.kind in {"applied", "unchanged", "partial", "error"} and event.paths_changed:
            applies_remaining["n"] -= 1
            if applies_remaining["n"] <= 0:
                saw_apply.set()

    watcher_task = asyncio.create_task(
        watch(
            cfg,
            on_event=_on_event,
            stop_event=stop,
            json_output=False,
            # Use a small debounce so the test runs in a reasonable time;
            # awatch's minimum useful debounce is in the tens of ms.
        )
    )

    # Run actions in order, with a brief settle delay between each so the
    # filesystem-event pipeline can fire.
    try:
        for act in actions:
            await act()
        await asyncio.wait_for(saw_apply.wait(), timeout=timeout_s)
    finally:
        stop.set()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(watcher_task, timeout=5.0)

    return events


def _write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def test_watch_picks_up_edit(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Edit current.sql with the watcher running; the new table shows up in the catalog."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(current, f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns, debounce_ms=100)

    async def _edit_after_warmup() -> None:
        # Give the watcher a moment to settle after the initial apply.
        await asyncio.sleep(0.5)
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.t (id BIGINT) USING iceberg;\n"
                f"CREATE TABLE IF NOT EXISTS {ns}.u (id BIGINT) USING iceberg;\n"
            ),
        )

    events = asyncio.run(_drive_watch(cfg, actions=[_edit_after_warmup], expected_apply_events=1))

    # The expected sequence: error?/applied (initial) -> startup -> applied (post-edit).
    kinds = [e.kind for e in events]
    assert "startup" in kinds, kinds
    assert kinds[-1] in {"applied", "unchanged", "partial"}, kinds

    # The second table now exists.
    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    assert catalog.table_exists((ns, "t"))
    assert catalog.table_exists((ns, "u"))


def test_watch_debounce_coalesces_rapid_edits(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """Two writes within debounce window → one apply, end state matches second."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(current, f"CREATE TABLE IF NOT EXISTS {ns}.t0 (id BIGINT) USING iceberg;\n")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns, debounce_ms=500)

    async def _rapid_edits() -> None:
        await asyncio.sleep(0.5)
        # First edit (will be coalesced with the second).
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.t0 (id BIGINT) USING iceberg;\n"
                f"CREATE TABLE IF NOT EXISTS {ns}.t1 (id BIGINT) USING iceberg;\n"
            ),
        )
        # Within the debounce window; should be coalesced.
        await asyncio.sleep(0.05)
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.t0 (id BIGINT) USING iceberg;\n"
                f"CREATE TABLE IF NOT EXISTS {ns}.t1 (id BIGINT) USING iceberg;\n"
                f"CREATE TABLE IF NOT EXISTS {ns}.t2 (id BIGINT) USING iceberg;\n"
            ),
        )

    events = asyncio.run(
        _drive_watch(cfg, actions=[_rapid_edits], expected_apply_events=1, timeout_s=20.0)
    )

    # Strictly one applied-with-paths event from the debounced batch.
    applies_with_changes = [
        e for e in events if e.kind in {"applied", "unchanged"} and e.paths_changed
    ]
    assert len(applies_with_changes) == 1, [(e.kind, e.paths_changed) for e in events]

    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    assert catalog.table_exists((ns, "t0"))
    assert catalog.table_exists((ns, "t1"))
    assert catalog.table_exists((ns, "t2"))


def test_watch_parse_error_does_not_crash(
    rest_catalog: RestCatalog, ns: str, tmp_path: Path, sidecar_ns: str
) -> None:
    """A SQL parse failure surfaces as an event; the loop keeps watching."""
    del rest_catalog
    evolutions = tmp_path / "evolutions"
    current = evolutions / "current.sql"
    _write(current, f"CREATE TABLE IF NOT EXISTS {ns}.ok (id BIGINT) USING iceberg;\n")
    cfg = _build_config(evolutions_dir=evolutions, sidecar_namespace=sidecar_ns, debounce_ms=100)

    async def _break_then_fix() -> None:
        await asyncio.sleep(0.5)
        # Intentionally unsupported (INSERT ... SELECT).
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.ok (id BIGINT) USING iceberg;\n"
                f"INSERT INTO {ns}.ok SELECT * FROM nowhere;\n"
            ),
        )
        await asyncio.sleep(1.0)
        # Fix it.
        _write(
            current,
            (
                f"CREATE TABLE IF NOT EXISTS {ns}.ok (id BIGINT) USING iceberg;\n"
                f"CREATE TABLE IF NOT EXISTS {ns}.fixed (id BIGINT) USING iceberg;\n"
            ),
        )

    events = asyncio.run(
        _drive_watch(cfg, actions=[_break_then_fix], expected_apply_events=2, timeout_s=30.0)
    )
    kinds = [e.kind for e in events]
    assert "error" in kinds, kinds
    assert "applied" in kinds, kinds

    from pyiceberg.catalog.rest import RestCatalog as _RC

    catalog = _RC(
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
    assert catalog.table_exists((ns, "fixed"))
