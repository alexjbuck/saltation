"""Unit tests for ``cambrian.migrate.watch``.

These don't touch the filesystem (beyond ``tmp_path``) or the catalog —
the runner is monkeypatched, and the watcher generator is a fake async
iterator yielding pre-baked batches.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Sequence
from pathlib import Path

import pytest
from watchfiles import Change

import cambrian.migrate.watch as watch_mod
from cambrian.config import CambrianConfig, CatalogConfig, DevConfig, EvolutionsConfig
from cambrian.errors import CambrianError
from cambrian.migrate.runner import ApplyResult


def _make_config(tmp_path: Path, *, debounce_ms: int = 50) -> CambrianConfig:
    return CambrianConfig(
        catalog=CatalogConfig(type="sql", uri="sqlite:///:memory:"),
        evolutions=EvolutionsConfig(dir=str(tmp_path / "evolutions")),
        dev=DevConfig(debounce_ms=debounce_ms),
    )


class _FakeWatcher:
    """Async iterator that yields a fixed sequence of ``awatch`` batches."""

    def __init__(self, batches: Sequence[set[tuple[Change, str]]]) -> None:
        self._batches = list(batches)

    def __call__(
        self, paths: Sequence[Path], debounce_ms: int, stop_event: object
    ) -> AsyncIterator[set[tuple[Change, str]]]:
        # paths and debounce_ms are recorded for assertion.
        self.last_paths = list(paths)
        self.last_debounce_ms = debounce_ms
        return self._iter()

    async def _iter(self) -> AsyncIterator[set[tuple[Change, str]]]:
        for batch in self._batches:
            # Yield to the event loop so to_thread-scheduled callbacks can fire.
            await asyncio.sleep(0)
            yield batch


def _stub_apply_factory(call_log: list[CambrianConfig]) -> object:
    """Build a stub ``apply_idempotent`` that records calls and returns success."""

    def _stub(
        config: CambrianConfig, *, allow_partial: bool = False, actor: str | None = None
    ) -> ApplyResult:
        del allow_partial, actor
        call_log.append(config)
        return ApplyResult(
            status="applied",
            evolution_hash="deadbeef" + "0" * 56,
            sources=[],
            statements=[],
            event_id="evt-" + str(len(call_log)),
        )

    return _stub


def test_watch_calls_apply_once_per_batch(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two debounced batches → exactly two applies (plus an initial)."""
    cfg = _make_config(tmp_path)
    calls: list[CambrianConfig] = []
    monkeypatch.setattr(watch_mod, "apply_idempotent", _stub_apply_factory(calls))

    fake = _FakeWatcher(
        [
            {(Change.modified, str(tmp_path / "evolutions" / "current.sql"))},
            {(Change.modified, str(tmp_path / "evolutions" / "current.sql"))},
        ]
    )

    events: list[watch_mod.WatchEvent] = []

    async def _run() -> None:
        await watch_mod.watch(
            cfg,
            watcher_factory=fake,
            on_event=events.append,
        )

    asyncio.run(_run())

    # 1 initial + 2 batches.
    assert len(calls) == 3
    # debounce_ms forwarded.
    assert fake.last_debounce_ms == cfg.dev.debounce_ms
    # We observed startup + 3 status events.
    kinds = [e.kind for e in events]
    assert "startup" in kinds
    assert kinds.count("applied") == 3


def test_watch_debounce_override(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicit ``debounce_ms`` overrides config."""
    cfg = _make_config(tmp_path, debounce_ms=500)
    monkeypatch.setattr(watch_mod, "apply_idempotent", _stub_apply_factory([]))

    fake = _FakeWatcher([])

    async def _run() -> None:
        await watch_mod.watch(cfg, debounce_ms=42, watcher_factory=fake, initial_apply=False)

    asyncio.run(_run())
    assert fake.last_debounce_ms == 42


def test_watch_parse_error_keeps_loop_alive(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A CambrianError during apply is reported and the next batch still applies."""
    cfg = _make_config(tmp_path)
    call_count = {"n": 0}

    def _flaky_apply(
        config: CambrianConfig, *, allow_partial: bool = False, actor: str | None = None
    ) -> ApplyResult:
        del config, allow_partial, actor
        call_count["n"] += 1
        if call_count["n"] == 2:
            raise CambrianError("synthetic parse failure")
        return ApplyResult(
            status="applied",
            evolution_hash="cafe" + "0" * 60,
            sources=[],
        )

    monkeypatch.setattr(watch_mod, "apply_idempotent", _flaky_apply)

    fake = _FakeWatcher(
        [
            {(Change.modified, "a.sql")},
            {(Change.modified, "b.sql")},
        ]
    )
    events: list[watch_mod.WatchEvent] = []

    async def _run() -> None:
        await watch_mod.watch(cfg, watcher_factory=fake, on_event=events.append)

    asyncio.run(_run())

    kinds = [e.kind for e in events]
    assert "error" in kinds
    # The third call (second batch) succeeded — loop didn't die on the error.
    assert call_count["n"] == 3
    assert kinds.count("applied") == 2


def test_watch_initial_apply_can_be_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    cfg = _make_config(tmp_path)
    calls: list[CambrianConfig] = []
    monkeypatch.setattr(watch_mod, "apply_idempotent", _stub_apply_factory(calls))

    fake = _FakeWatcher([])

    async def _run() -> None:
        await watch_mod.watch(cfg, watcher_factory=fake, initial_apply=False)

    asyncio.run(_run())
    assert calls == []


def test_watch_use_reset_routes_to_apply_reset(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``use_reset=True`` routes through ``apply_reset``, not ``apply_idempotent``."""
    from cambrian.migrate import runner
    from cambrian.migrate.runner import ResetResult

    cfg = _make_config(tmp_path)
    # Track which entry point fires.
    routed_to: list[str] = []

    def _idem(
        config: CambrianConfig, *, allow_partial: bool = False, actor: str | None = None
    ) -> ApplyResult:
        del config, allow_partial, actor
        routed_to.append("idempotent")
        return ApplyResult(status="applied", evolution_hash="i" * 64)

    def _reset(
        config: CambrianConfig,
        *,
        allow_partial: bool = False,
        force: bool = False,
        actor: str | None = None,
    ) -> ResetResult:
        del config, allow_partial, force, actor
        routed_to.append("reset")
        return ResetResult(status="applied", evolution_hash="r" * 64)

    monkeypatch.setattr(watch_mod, "apply_idempotent", _idem)
    monkeypatch.setattr(runner, "apply_reset", _reset)

    fake = _FakeWatcher([{(Change.modified, "x.sql")}])

    async def _run() -> None:
        await watch_mod.watch(cfg, watcher_factory=fake, use_reset=True)

    asyncio.run(_run())
    # Both the initial apply and the post-edit apply must have gone through reset.
    assert routed_to == ["reset", "reset"]


def test_watch_resolves_targets_from_includes(tmp_path: Path) -> None:
    """``_resolve_watch_targets`` returns the directories of every included file."""
    evolutions = tmp_path / "evolutions"
    nested = evolutions / "current"
    nested.mkdir(parents=True)
    (nested / "a.sql").write_text("CREATE NAMESPACE x;\n", encoding="utf-8")
    (nested / "b.sql").write_text("CREATE NAMESPACE y;\n", encoding="utf-8")
    (evolutions / "current.sql").write_text("--! include current/*.sql\n", encoding="utf-8")

    cfg = _make_config(tmp_path)
    targets = watch_mod._resolve_watch_targets(cfg)  # type: ignore[attr-defined]
    # Both the root evolutions dir (for current.sql) and the current/ dir
    # (for the includes) end up in the set.
    resolved = {p.resolve() for p in targets}
    assert evolutions.resolve() in resolved
    assert nested.resolve() in resolved


def test_watch_resolves_falls_back_when_no_current_sql(tmp_path: Path) -> None:
    cfg = _make_config(tmp_path)
    # No current.sql exists.
    targets = watch_mod._resolve_watch_targets(cfg)  # type: ignore[attr-defined]
    assert targets == [(tmp_path / "evolutions").resolve()]
