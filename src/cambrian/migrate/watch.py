"""``watchfiles``-driven hot-reload loop for ``current.sql`` and resolved includes.

The dev loop: watch the evolutions directory, debounce rapid edits via
``watchfiles.awatch(debounce=...)``, then re-resolve includes (their set
may have changed) and call ``apply_idempotent``. A parse failure in the
SQL prints the error and *keeps watching* — the loop only exits on
explicit stop / interrupt.

The watcher source is parameterised (``watcher_factory``) so unit tests
can inject a fake async iterator that emits rapid synthetic events
without touching the filesystem.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from watchfiles import Change, awatch

from cambrian.errors import CambrianError, EvolutionNotFoundError
from cambrian.migrate.runner import ApplyResult, apply_idempotent

if TYPE_CHECKING:
    from anyio import Event

    from cambrian.config import CambrianConfig

__all__ = ["WatchEvent", "WatcherFactory", "watch"]


# (set of (change, path)) — matches ``awatch``'s yield type.
WatchBatch = set[tuple[Change, str]]
WatcherFactory = Callable[[Sequence[Path], int, "Event | None"], AsyncIterator[WatchBatch]]


@dataclass(frozen=True)
class WatchEvent:
    """One iteration of the watch loop's outcome.

    ``kind`` is one of:

    * ``"applied"`` / ``"unchanged"`` / ``"partial"`` — direct from
      :class:`ApplyResult.status`.
    * ``"error"`` — apply raised a :class:`CambrianError`; the loop kept
      running. ``error`` carries the formatted message.
    * ``"startup"`` — emitted once before the loop begins, after the
      initial apply.
    """

    kind: str
    paths_changed: list[str]
    result: ApplyResult | None = None
    error: str | None = None


def _default_watcher_factory(
    paths: Sequence[Path],
    debounce_ms: int,
    stop_event: Event | None,
) -> AsyncIterator[WatchBatch]:
    """Real ``watchfiles.awatch`` wrapper.

    Watches a *directory* (or set of directories) recursively. We resolve to
    parent directories of the requested paths because awatch is happiest
    when given existing directories rather than individual file paths.
    """
    targets: list[Path] = []
    seen: set[Path] = set()
    for p in paths:
        # Prefer the directory the file lives in; if a directory is passed
        # directly use it as-is. Both are valid awatch inputs.
        d = p if p.is_dir() else p.parent
        d = d.resolve()
        if d not in seen:
            seen.add(d)
            targets.append(d)
    return awatch(*targets, debounce=debounce_ms, stop_event=stop_event)


def _format_human(event: WatchEvent) -> str:
    if event.kind == "startup":
        if event.result is None:
            return "watching..."
        return f"initial apply: {event.result.status}; watching..."
    if event.kind == "error":
        head = ", ".join(sorted(event.paths_changed)[:3])
        location = f" after change(s) in {head}" if head else ""
        return f"error{location}: {event.error}"
    if event.kind == "unchanged":
        return (
            f"unchanged (hash {event.result.evolution_hash[:12]}…)" if event.result else "unchanged"
        )
    if event.result is None:
        return event.kind
    head_changes = ", ".join(sorted(event.paths_changed)[:3]) or "<no fs paths>"
    return (
        f"{event.kind} after change(s) in {head_changes} (hash {event.result.evolution_hash[:12]}…)"
    )


def _format_json(event: WatchEvent) -> str:
    payload: dict[str, object] = {
        "kind": event.kind,
        "paths_changed": event.paths_changed,
        "error": event.error,
    }
    if event.result is not None:
        payload["status"] = event.result.status
        payload["evolution_hash"] = event.result.evolution_hash
        payload["event_id"] = event.result.event_id
        payload["sources"] = [str(p) for p in event.result.sources]
    return json.dumps(payload, default=str)


async def _run_apply(
    config: CambrianConfig,
    *,
    allow_partial: bool,
    use_reset: bool,
    force: bool = False,
) -> ApplyResult:
    """Off-thread apply.

    ``apply_idempotent`` is synchronous and IO-heavy; running it in a thread
    keeps the watch loop responsive. Under ``use_reset=True`` we run
    :func:`apply_reset` and adapt its richer :class:`ResetResult` down to
    an :class:`ApplyResult` shape so the watch loop's event payload stays
    uniform regardless of mode.
    """
    if use_reset:
        from cambrian.migrate.runner import apply_reset

        reset_result = await asyncio.to_thread(
            apply_reset, config, allow_partial=allow_partial, force=force
        )
        return ApplyResult(
            status=reset_result.status,
            evolution_hash=reset_result.evolution_hash,
            sources=reset_result.sources,
            statements=(reset_result.apply_result.statements if reset_result.apply_result else []),
            event_id=reset_result.apply_event_id,
            error=reset_result.error,
        )
    return await asyncio.to_thread(apply_idempotent, config, allow_partial=allow_partial)


async def watch(
    config: CambrianConfig,
    *,
    debounce_ms: int | None = None,
    allow_partial: bool = False,
    use_reset: bool = False,
    force: bool = False,
    json_output: bool = False,
    stop_event: Event | None = None,
    watcher_factory: WatcherFactory | None = None,
    on_event: Callable[[WatchEvent], Awaitable[None] | None] | None = None,
    initial_apply: bool = True,
) -> None:
    """Run the watch loop until *stop_event* is set or the iterator exhausts.

    The loop:

    1. Run an initial apply so ``current.sql`` is in a known state.
    2. Resolve includes to discover the directories we need to watch.
    3. Iterate the watcher; on each debounced batch, re-resolve and re-apply.
    4. Any :class:`CambrianError` (parse failure, dispatch failure) is
       reported and the loop continues; the file might be saved in a
       half-edited state and the next change should fix it.

    Parameters
    ----------
    config:
        Loaded ``CambrianConfig`` — provides ``evolutions.dir`` and
        ``dev.debounce_ms``.
    debounce_ms:
        Override the config's debounce. ``None`` falls through to
        ``config.dev.debounce_ms``.
    use_reset:
        Run each apply in reset mode (rollback + re-apply via
        :func:`apply_reset`). Default is idempotent; reset is the escape
        hatch and the load-bearing principle is "idempotent is the path".
    force:
        Pass through to :func:`apply_reset` — overrides external-write
        detection. No effect when ``use_reset=False``.
    json_output:
        If true, every event prints one JSON line; otherwise human-readable.
    stop_event:
        Optional anyio event for cooperative cancellation (used by tests).
    watcher_factory:
        Test seam — substitute a fake awatch generator. Defaults to
        the real :func:`watchfiles.awatch`.
    on_event:
        Optional async or sync callback invoked for every :class:`WatchEvent`.
        Tests use this to assert apply counts; production uses
        ``json_output`` for the same effect.
    initial_apply:
        If false, skip the warm-up apply. Useful for tests that pre-seed
        their own state.
    """
    debounce = debounce_ms if debounce_ms is not None else config.dev.debounce_ms
    factory = watcher_factory or _default_watcher_factory

    async def _emit(event: WatchEvent) -> None:
        line = _format_json(event) if json_output else _format_human(event)
        print(line, flush=True)
        if on_event is not None:
            result = on_event(event)
            if asyncio.iscoroutine(result):
                await result

    initial: ApplyResult | None = None
    if initial_apply:
        try:
            initial = await _run_apply(
                config, allow_partial=allow_partial, use_reset=use_reset, force=force
            )
        except CambrianError as err:
            await _emit(WatchEvent(kind="error", paths_changed=[], error=str(err)))
        else:
            await _emit(
                WatchEvent(
                    kind=initial.status,
                    paths_changed=[],
                    result=initial,
                )
            )

    watch_targets = _resolve_watch_targets(config)
    await _emit(WatchEvent(kind="startup", paths_changed=[], result=initial))

    async for batch in factory(watch_targets, debounce, stop_event):
        paths_changed = sorted({path for _, path in batch})
        try:
            result = await _run_apply(
                config, allow_partial=allow_partial, use_reset=use_reset, force=force
            )
        except CambrianError as err:
            await _emit(WatchEvent(kind="error", paths_changed=paths_changed, error=str(err)))
            continue
        await _emit(WatchEvent(kind=result.status, paths_changed=paths_changed, result=result))


def _resolve_watch_targets(config: CambrianConfig) -> list[Path]:
    """Pick the directories to feed to ``awatch``.

    Best-effort: try expanding ``current.sql`` to learn every transitively
    included file, then dedupe their parent directories. If ``current.sql``
    doesn't exist or fails to parse, fall back to the evolutions dir alone.
    """
    base = Path(config.evolutions.dir).resolve()
    fallback = [base]
    try:
        # Local import to avoid importing the SQL stack at module load time
        # for callers that never start the watcher.
        from cambrian.sql.include import expand

        current = base / "current.sql"
        if not current.exists():
            return fallback
        expanded = expand(current)
    except (EvolutionNotFoundError, CambrianError, OSError):
        return fallback

    targets: list[Path] = []
    seen: set[Path] = set()
    for source in expanded.sources:
        d = source.parent.resolve()
        if d not in seen:
            seen.add(d)
            targets.append(d)
    if not targets:
        return fallback
    return targets
