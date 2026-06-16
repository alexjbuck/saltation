"""Top-level Typer application.

M0 shipped ``--version`` and ``--help``. M1 adds the ``config`` sub-app for
loading, validating, and inspecting ``cambrian.toml``. M3 adds the lifecycle
commands ``init`` and ``status``. Later milestones register additional
sub-apps on ``app``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any

import typer

from cambrian import __version__
from cambrian.catalog import load_catalog
from cambrian.config import load_config, redacted_dump
from cambrian.errors import (
    CambrianError,
    ExternalWriteDetectedError,
    NotInitializedError,
    SidecarVersionAheadError,
)
from cambrian.migrate import apply_idempotent
from cambrian.migrate.commit import (
    cambrian_commit,
    cambrian_reset_to,
    cambrian_uncommit,
)
from cambrian.migrate.runner import (
    apply_reset,
    rollback_to_last_checkpoint,
)
from cambrian.migrate.sync import SyncResult, cambrian_sync
from cambrian.migrate.watch import watch as _watch_loop
from cambrian.sidecar.events import committed_evolutions, latest_event
from cambrian.sidecar.schema import CAMBRIAN_SIDECAR_VERSION
from cambrian.sidecar.selfmigrate import ensure_current

# Distinct exit codes so callers (CI scripts, wrappers) can distinguish
# "not initialized" from generic errors.
EXIT_NOT_INITIALIZED = 2
EXIT_VERSION_AHEAD = 3
EXIT_EXTERNAL_WRITE = 4

app = typer.Typer(
    name="cambrian",
    help="SQL-driven evolution runner for Apache Iceberg tables.",
    add_completion=False,
    no_args_is_help=True,
)


def _version_callback(value: bool) -> None:
    if value:
        typer.echo(f"cambrian {__version__}")
        raise typer.Exit()


@app.callback()
def _root(
    version: Annotated[
        bool,
        typer.Option(
            "--version",
            help="Show the cambrian version and exit.",
            callback=_version_callback,
            is_eager=True,
        ),
    ] = False,
) -> None:
    """cambrian — SQL-driven evolution runner for Apache Iceberg tables."""


# ---------------------------------------------------------------------------
# `cambrian config` sub-app
# ---------------------------------------------------------------------------

config_app = typer.Typer(
    name="config",
    help="Inspect and validate the cambrian config file.",
    no_args_is_help=True,
)
app.add_typer(config_app, name="config")


def _path_option() -> typer.models.OptionInfo:
    """Build a fresh ``--path`` Option.

    Typer's Option instances aren't safe to share across commands (their
    decls list gets mutated by click during registration), so we mint a new
    one per command via this helper.
    """
    return typer.Option(
        "--path",
        "-p",
        help="Path to the cambrian config TOML.",
    )


def _render_human(data: dict[str, Any], indent: int = 0) -> str:
    """Render a dict as a TOML-ish indented summary for terminal output.

    Not strictly TOML — we keep it simple and readable. JSON output is
    available for machine consumers via ``--json``.
    """
    lines: list[str] = []
    prefix = "  " * indent
    # Render scalar fields first, then nested dicts.
    scalars = {k: v for k, v in data.items() if not isinstance(v, dict)}
    nested = {k: v for k, v in data.items() if isinstance(v, dict)}
    for key, value in scalars.items():
        lines.append(f"{prefix}{key} = {value!r}")
    for key, value in nested.items():
        lines.append(f"{prefix}[{key}]")
        lines.append(_render_human(value, indent=indent + 1))
    return "\n".join(line for line in lines if line)


@config_app.command("show")
def config_show(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON instead of the human view."),
    ] = False,
) -> None:
    """Print the (redacted) effective config.

    Credential-shaped values (token, secret, password, credential, *_key
    patterns) are replaced with ``"***"``. See ``cambrian.config.redacted_dump``
    for the heuristic.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    dumped = redacted_dump(cfg)
    if as_json:
        typer.echo(json.dumps(dumped, indent=2, sort_keys=True, default=str))
    else:
        typer.echo(_render_human(dumped))


@config_app.command("check")
def config_check(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
) -> None:
    """Validate the config file. Exits 0 on success, non-zero with a message on failure."""
    try:
        load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    typer.echo(f"Config valid: {path}")


# ---------------------------------------------------------------------------
# `cambrian init` / `cambrian status`
# ---------------------------------------------------------------------------


def _load(path: Path) -> tuple[Any, Any]:
    """Helper: load config and catalog, mapping config errors to exit-1."""
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc
    catalog = load_catalog(cfg)
    return cfg, catalog


@app.command("init")
def init_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
) -> None:
    """Bootstrap the ``_cambrian`` sidecar in the configured catalog.

    Idempotent: re-running on an already-initialised catalog is a no-op.
    Fails if the sidecar is at a *newer* version than this binary knows.
    """
    cfg, catalog = _load(path)
    namespace = cfg.evolutions.sidecar_namespace

    from cambrian.sidecar.selfmigrate import _version_table_exists

    already = _version_table_exists(catalog, namespace)

    try:
        state = ensure_current(catalog, namespace, allow_read_only=False)
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if already:
        typer.echo(
            f"Already initialized: sidecar at {state.sidecar_namespace} (version={state.version})"
        )
    else:
        typer.echo(f"Initialized sidecar at {state.sidecar_namespace} (version={state.version})")


def _status_payload(
    state: Any,
    committed: list[Any],
    current: Any,
) -> dict[str, Any]:
    return {
        "initialized": True,
        "sidecar_namespace": state.sidecar_namespace,
        "sidecar_version": state.version,
        "is_version_ahead": state.is_version_ahead,
        "committed_count": len(committed),
        "committed_evolutions": [
            {
                "evolution_id": c.evolution_id,
                "event_id": c.event_id,
                "event_ts": c.event_ts.isoformat(),
            }
            for c in committed
        ],
        "current_applied": (
            None
            if current is None
            else {
                "event_id": current.event_id,
                "event_ts": current.event_ts.isoformat(),
                "evolution_hash": current.evolution_hash,
            }
        ),
    }


@app.command("status")
def status_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON instead of the human view."),
    ] = False,
) -> None:
    """Report sidecar version, committed evolutions, and the current in-flight apply.

    Read-only: tolerates a sidecar that is one or more versions ahead of
    this binary (prints a warning). Exits ``2`` if the sidecar has never
    been initialised in this catalog.
    """
    cfg, catalog = _load(path)
    namespace = cfg.evolutions.sidecar_namespace

    try:
        state = ensure_current(catalog, namespace, allow_read_only=True)
    except NotInitializedError as exc:
        if as_json:
            typer.echo(
                json.dumps(
                    {
                        "initialized": False,
                        "sidecar_namespace": namespace,
                        "hint": "run `cambrian init`",
                    },
                    indent=2,
                )
            )
        else:
            typer.echo(
                f"error: sidecar not initialized in namespace '{namespace}'; run `cambrian init`",
                err=True,
            )
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc

    committed = committed_evolutions(catalog, namespace)
    current = latest_event(catalog, namespace, event_type="apply", evolution_id="current")

    if as_json:
        typer.echo(json.dumps(_status_payload(state, committed, current), indent=2))
        return

    if state.is_version_ahead:
        typer.echo(
            f"warning: sidecar is at version {state.version}, this cambrian only "
            f"understands up to version {CAMBRIAN_SIDECAR_VERSION}; running in read-only mode.",
            err=True,
        )

    typer.echo(f"sidecar namespace: {state.sidecar_namespace}")
    typer.echo(f"sidecar version: {state.version}")
    typer.echo(f"committed evolutions: {len(committed)}")
    for c in committed:
        typer.echo(f"  - {c.evolution_id} (event {c.event_id} @ {c.event_ts.isoformat()})")
    if current is None:
        typer.echo("current applied: <none>")
    else:
        typer.echo(
            f"current applied: event {current.event_id} @ {current.event_ts.isoformat()} "
            f"(hash {current.evolution_hash[:12]}…)"
        )


# ---------------------------------------------------------------------------
# `cambrian apply`
# ---------------------------------------------------------------------------


@app.command("apply")
def apply_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    allow_partial: Annotated[
        bool,
        typer.Option(
            "--allow-partial",
            help=(
                "Continue past statement failures and emit a partial-success event. "
                "Without this flag, the first failure surfaces immediately."
            ),
        ),
    ] = False,
    reset: Annotated[
        bool,
        typer.Option(
            "--reset",
            help=(
                "Run in reset mode: roll the affected tables back to their last "
                "checkpoint, then re-apply. Use ONLY for evolutions that genuinely "
                "cannot be expressed idempotently — idempotent is the default and "
                "the safety contract."
            ),
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help=(
                "Override external-write detection when using --reset. Required only "
                "if another writer has advanced one of the affected tables since the "
                "last cambrian apply; using this risks clobbering their work."
            ),
        ),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON instead of the human view."),
    ] = False,
) -> None:
    """Apply ``current.sql`` against the configured catalog.

    Idempotent mode is the default. Reset mode (``--reset`` or
    ``[dev].mode = "reset"``) is the opt-in escape hatch for evolutions
    that genuinely cannot be expressed idempotently: it rolls the affected
    tables back to their last checkpoint, then re-applies. Idempotent is
    the path; reset is the relief valve.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    use_reset = reset or cfg.dev.mode == "reset"

    try:
        if use_reset:
            reset_result = apply_reset(cfg, allow_partial=allow_partial, force=force)
            if as_json:
                typer.echo(json.dumps(_reset_payload(reset_result), indent=2, default=str))
            else:
                _print_reset_human(reset_result)
            if reset_result.status == "partial" or reset_result.error is not None:
                raise typer.Exit(code=1)
            return
        result = apply_idempotent(cfg, allow_partial=allow_partial)
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except ExternalWriteDetectedError as exc:
        typer.echo(f"error: {exc}", err=True)
        typer.echo(
            "  hint: re-run with --force ONLY if you intentionally want to overwrite "
            "the out-of-band writer's commit.",
            err=True,
        )
        raise typer.Exit(code=EXIT_EXTERNAL_WRITE) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(_apply_payload(result), indent=2, default=str))
    else:
        _print_apply_human(result)

    # Partial / errored apply: surface non-zero exit so CI fails loud.
    if result.status == "partial" or result.error is not None:
        raise typer.Exit(code=1)


@app.command("redo")
def redo_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    allow_partial: Annotated[
        bool,
        typer.Option("--allow-partial", help="Continue past statement failures."),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Override external-write detection. See `cambrian apply --reset --help`.",
        ),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON output."),
    ] = False,
) -> None:
    """Alias for ``cambrian apply --reset``.

    Same semantics, shorter to type during a non-idempotent-evolution
    dev loop. Reset is the relief valve — prefer idempotent SQL.
    """
    apply_command(
        path=path,
        allow_partial=allow_partial,
        reset=True,
        force=force,
        as_json=as_json,
    )


@app.command("rollback")
def rollback_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON output."),
    ] = False,
) -> None:
    """Roll the affected tables of the last apply back to their checkpoint, without re-applying.

    Useful when you want to discard a dev iteration's table mutations
    entirely and edit ``current.sql`` from scratch. Does not re-execute
    SQL afterwards — the next ``cambrian apply`` re-applies the (possibly
    edited) ``current.sql``.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        result = rollback_to_last_checkpoint(cfg)
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except ExternalWriteDetectedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_EXTERNAL_WRITE) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(_reset_payload(result), indent=2, default=str))
    else:
        _print_reset_human(result)


# ---------------------------------------------------------------------------
# `cambrian commit` / `cambrian uncommit` / `cambrian reset --to`
# ---------------------------------------------------------------------------


@app.command("commit")
def commit_command(
    message: Annotated[
        str,
        typer.Option(
            "--message",
            "-m",
            help="Short description of the committed evolution (becomes the file slug).",
        ),
    ],
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON output."),
    ] = False,
) -> None:
    """Freeze ``current.sql`` as a committed evolution.

    Preconditions: ``current.sql`` is non-empty and applies cleanly (run
    ``cambrian apply`` first). The current text is moved to
    ``committed/<NNNN>_<slug>.sql``, the affected tables get a checkpoint
    pinned at the ``cambrian.committed.<n>.<slug>`` tag, and ``current.sql``
    is truncated for the next dev iteration.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        result = cambrian_commit(cfg, message=message)
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(_commit_payload(result), default=str))
        return

    typer.echo(f"committed: {result.evolution_id}")
    typer.echo(f"file:      {result.committed_path}")
    typer.echo(f"hash:      {result.evolution_hash[:12]}…")
    typer.echo(f"event:     {result.event_id or '<none>'}")
    if result.affected_tables:
        typer.echo(f"tables:    {', '.join(result.affected_tables)}")


@app.command("uncommit")
def uncommit_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help=(
                "Overwrite a non-empty current.sql with the uncommitted content. "
                "Without --force, uncommit refuses to clobber unsaved work."
            ),
        ),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON output."),
    ] = False,
) -> None:
    """Pop the latest committed evolution back into ``current.sql``.

    Rolls the affected tables back to the checkpoint pinned at commit time
    and deletes the committed file. Refuses if downstream committed files
    exist (a gap in numbering would be a corruption) or if ``current.sql``
    is non-empty without ``--force``.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        result = cambrian_uncommit(cfg, force=force)
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except ExternalWriteDetectedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_EXTERNAL_WRITE) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(_uncommit_payload(result), default=str))
        return

    typer.echo(f"uncommitted: {result.evolution_id}")
    typer.echo(f"restored:    {result.restored_path}")
    typer.echo(f"event:       {result.event_id or '<none>'}")
    if result.rolled_back_tables:
        typer.echo(f"rolled back: {', '.join(result.rolled_back_tables)}")
    if result.skipped_tables:
        typer.echo(f"skipped:     {', '.join(result.skipped_tables)}")


@app.command(
    "reset-to",
    help=(
        "Roll affected tables back to a committed evolution's checkpoint. "
        "INCIDENT RESPONSE ONLY — does not delete committed files, does not "
        "touch downstream commit events. Reset is the relief valve, never "
        "the recommended fix; reach for idempotent SQL first."
    ),
)
def reset_to_command(
    evolution_id: Annotated[
        str,
        typer.Argument(
            help=(
                "The committed evolution id to roll back to, e.g. '0007_add_users'. "
                "See `cambrian status` for the live set."
            ),
        ),
    ],
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit machine-readable JSON output."),
    ] = False,
) -> None:
    """Roll back to a specific committed evolution's checkpoint (escape hatch).

    Use ONLY for incident response. Idempotent SQL is the safety contract;
    if you find yourself reaching for this, file an issue with the evolution
    that forced your hand so we can extend dispatch instead.
    """
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        result = cambrian_reset_to(cfg, evolution_id=evolution_id)
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except ExternalWriteDetectedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_EXTERNAL_WRITE) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(_reset_to_payload(result), default=str))
        return

    typer.echo("mode:        reset --to (escape hatch — incident response only)")
    typer.echo(f"evolution:   {result.evolution_id}")
    typer.echo(f"event:       {result.event_id or '<none>'}")
    if result.rolled_back_tables:
        typer.echo(f"rolled back: {', '.join(result.rolled_back_tables)}")
    if result.skipped_tables:
        typer.echo(f"skipped:     {', '.join(result.skipped_tables)}")


def _commit_payload(result: Any) -> dict[str, Any]:
    return {
        "evolution_id": result.evolution_id,
        "committed_path": str(result.committed_path),
        "evolution_hash": result.evolution_hash,
        "tag_ref": result.tag_ref,
        "event_id": result.event_id,
        "affected_tables": result.affected_tables,
    }


def _uncommit_payload(result: Any) -> dict[str, Any]:
    return {
        "evolution_id": result.evolution_id,
        "restored_path": str(result.restored_path),
        "restored_to_current": True,
        "event_id": result.event_id,
        "rolled_back_tables": result.rolled_back_tables,
        "skipped_tables": result.skipped_tables,
    }


def _reset_to_payload(result: Any) -> dict[str, Any]:
    return {
        "mode": "reset --to",
        "evolution_id": result.evolution_id,
        "event_id": result.event_id,
        "rolled_back_tables": result.rolled_back_tables,
        "skipped_tables": result.skipped_tables,
    }


# ---------------------------------------------------------------------------
# `cambrian watch`
# ---------------------------------------------------------------------------


@app.command("watch")
def watch_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    debounce_ms: Annotated[
        int | None,
        typer.Option(
            "--debounce-ms",
            help=(
                "Override ``[dev].debounce_ms`` for this invocation. "
                "Milliseconds of quiet time required before a batch of edits triggers an apply."
            ),
        ),
    ] = None,
    allow_partial: Annotated[
        bool,
        typer.Option(
            "--allow-partial",
            help="Continue past statement failures and emit a partial-success event.",
        ),
    ] = False,
    reset: Annotated[
        bool,
        typer.Option(
            "--reset",
            help=(
                "Run each re-apply in reset mode. Use ONLY for evolutions that "
                "genuinely cannot be expressed idempotently. Honours "
                '``[dev].mode = "reset"`` from cambrian.toml as well.'
            ),
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Override external-write detection in reset mode. See `cambrian apply --help`.",
        ),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit one JSON line per watch event."),
    ] = False,
) -> None:
    """Watch the evolutions directory and re-apply ``current.sql`` on every change.

    Default mode is idempotent — the safety contract. ``--reset`` (or
    ``[dev].mode = "reset"``) enables the rollback-before-reapply path
    for non-idempotent evolutions. Use it sparingly. A parse or dispatch
    error during a re-apply is reported and the loop keeps watching.
    """
    import asyncio as _asyncio

    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    use_reset = reset or cfg.dev.mode == "reset"

    try:
        _asyncio.run(
            _watch_loop(
                cfg,
                debounce_ms=debounce_ms,
                allow_partial=allow_partial,
                use_reset=use_reset,
                force=force,
                json_output=as_json,
            )
        )
    except KeyboardInterrupt:
        # Typer's default behaviour on Ctrl-C is fine; we just want to make
        # sure we don't dump a traceback at the user.
        raise typer.Exit(code=0) from None
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc


def _apply_payload(result: Any) -> dict[str, Any]:
    return {
        "mode": "idempotent",
        "status": result.status,
        "evolution_id": result.evolution_id,
        "evolution_hash": result.evolution_hash,
        "event_id": result.event_id,
        "sources": [str(p) for p in result.sources],
        "applied_committed": [
            {
                "evolution_id": c.evolution_id,
                "status": c.status,
                "evolution_hash": c.evolution_hash,
                "event_id": c.event_id,
                "error": c.error,
            }
            for c in result.applied_committed
        ],
        "statements": [
            {
                "sql": s.sql,
                "notes": s.notes,
                "affected_tables": [str(t) for t in s.affected_tables],
                "error": s.error,
            }
            for s in result.statements
        ],
        "error": result.error,
    }


def _print_apply_human(result: Any) -> None:
    if result.status == "unchanged":
        typer.echo(f"current.sql unchanged (hash {result.evolution_hash[:12]}…); nothing to apply")
        return
    typer.echo(f"status: {result.status}")
    typer.echo(f"hash:   {result.evolution_hash[:12]}…")
    typer.echo(f"event:  {result.event_id or '<none>'}")
    if result.statements:
        typer.echo(f"statements: {len(result.statements)}")
        for i, s in enumerate(result.statements, start=1):
            head = (s.sql.strip().splitlines()[0] if s.sql.strip() else "<empty>")[:80]
            marker = "ERR" if s.error else " OK"
            typer.echo(f"  [{marker}] #{i}: {head}")
            if s.notes:
                typer.echo(f"        {s.notes}")
            if s.error:
                typer.echo(f"        ! {s.error}", err=True)
    if result.error:
        typer.echo(f"error: {result.error}", err=True)


def _reset_payload(result: Any) -> dict[str, Any]:
    return {
        "mode": "reset",
        "status": result.status,
        "evolution_hash": result.evolution_hash,
        "rollback_event_id": result.rollback_event_id,
        "apply_event_id": result.apply_event_id,
        "sources": [str(p) for p in result.sources],
        "rollbacks": [
            {
                "ident": r.ident,
                "rolled_back": r.rolled_back,
                "from_snapshot_id": r.from_snapshot_id,
                "to_snapshot_id": r.to_snapshot_id,
                "reason": r.reason,
            }
            for r in result.rollbacks
        ],
        "apply_result": (
            _apply_payload(result.apply_result) if result.apply_result is not None else None
        ),
        "error": result.error,
    }


def _print_reset_human(result: Any) -> None:
    typer.echo("mode:   reset (escape hatch — idempotent is the recommended path)")
    typer.echo(f"status: {result.status}")
    if result.evolution_hash:
        typer.echo(f"hash:   {result.evolution_hash[:12]}…")
    typer.echo(f"rollback event: {result.rollback_event_id or '<none>'}")
    typer.echo(f"apply event:    {result.apply_event_id or '<none>'}")
    if result.rollbacks:
        typer.echo(f"rollbacks: {len(result.rollbacks)}")
        for r in result.rollbacks:
            marker = "rb" if r.rolled_back else "--"
            arrow = f" {r.from_snapshot_id} -> {r.to_snapshot_id}" if r.rolled_back else ""
            typer.echo(f"  [{marker}] {r.ident}{arrow} ({r.reason})")
    if result.apply_result is not None:
        typer.echo("apply phase:")
        _print_apply_human(result.apply_result)
    if result.error:
        typer.echo(f"error: {result.error}", err=True)


# ---------------------------------------------------------------------------
# `cambrian sync` (alias: `cambrian download`)
# ---------------------------------------------------------------------------


def _sync_payload(result: SyncResult) -> dict[str, Any]:
    return {
        "dry_run": result.dry_run,
        "written": result.written,
        "overwritten": result.overwritten,
        "skipped": result.skipped,
        "refused": result.refused,
        "discrepancies": result.discrepancies,
        "files": [
            {
                "evolution_id": f.evolution_id,
                "path": str(f.path),
                "status": f.status,
                "catalog_hash": f.catalog_hash,
                "local_hash": f.local_hash,
                "diff": f.diff,
                "note": f.note,
            }
            for f in result.files
        ],
    }


def _print_sync_human(result: SyncResult) -> None:
    prefix = "would " if result.dry_run else ""
    for f in result.files:
        if f.status == "written":
            typer.echo(f"{prefix}write     {f.path} ({f.catalog_hash[:12]}…)")
        elif f.status == "overwritten":
            typer.echo(f"{prefix}overwrite {f.path} ({f.catalog_hash[:12]}…)")
        elif f.status == "skipped":
            typer.echo(f"skip      {f.path} (hash matches)")
        elif f.status == "refused":
            typer.echo(f"refuse    {f.path}", err=True)
            if f.note:
                typer.echo(f"          {f.note}", err=True)
            if f.diff:
                typer.echo(f.diff, err=True)
        else:
            typer.echo(f"discrepancy {f.path}", err=True)
            if f.note:
                typer.echo(f"            {f.note}", err=True)
    typer.echo(
        "summary: "
        f"written={result.written} "
        f"overwritten={result.overwritten} "
        f"skipped={result.skipped} "
        f"refused={result.refused} "
        f"discrepancies={result.discrepancies}"
    )


def _sync_impl(
    path: Path,
    *,
    dry_run: bool,
    diff: bool,
    force: bool,
    as_json: bool,
) -> None:
    """Shared body for ``sync`` and ``download`` (they're aliases)."""
    try:
        cfg = load_config(path)
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    try:
        result = cambrian_sync(cfg, force=force, dry_run=dry_run, diff=diff)
    except NotInitializedError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_NOT_INITIALIZED) from exc
    except SidecarVersionAheadError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=EXIT_VERSION_AHEAD) from exc
    except CambrianError as exc:
        typer.echo(f"error: {exc}", err=True)
        raise typer.Exit(code=1) from exc

    if as_json:
        typer.echo(json.dumps(_sync_payload(result), default=str))
    else:
        _print_sync_human(result)

    if result.has_refusals:
        raise typer.Exit(code=EXIT_EXTERNAL_WRITE)


@app.command("sync")
def sync_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    dry_run: Annotated[
        bool,
        typer.Option(
            "--dry-run",
            help="Plan only; write nothing to disk.",
        ),
    ] = False,
    diff: Annotated[
        bool,
        typer.Option(
            "--diff",
            help=(
                "Print a unified diff of local vs catalog on conflicts. "
                "Implies --dry-run unless --force is also passed."
            ),
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option(
            "--force",
            help="Overwrite local committed files that conflict with the catalog.",
        ),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit a single-line JSON summary."),
    ] = False,
) -> None:
    """Rehydrate the local ``committed/`` directory from the catalog.

    Read-only against the catalog: no events are written. Missing files are
    created, hash-matching files are skipped, and files whose content differs
    from the catalog row are refused unless ``--force`` is passed. Use
    ``--diff`` to inspect conflicts; use ``--dry-run`` to plan without
    touching the filesystem. Exits ``4`` if any conflict was refused so CI
    can fail loud on a drifted checkout.
    """
    _sync_impl(path, dry_run=dry_run, diff=diff, force=force, as_json=as_json)


@app.command("download")
def download_command(
    path: Annotated[Path, _path_option()] = Path("./cambrian.toml"),
    dry_run: Annotated[
        bool,
        typer.Option("--dry-run", help="Plan only; write nothing to disk."),
    ] = False,
    diff: Annotated[
        bool,
        typer.Option(
            "--diff",
            help="Print a unified diff of local vs catalog on conflicts.",
        ),
    ] = False,
    force: Annotated[
        bool,
        typer.Option("--force", help="Overwrite local committed files on conflict."),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Emit a single-line JSON summary."),
    ] = False,
) -> None:
    """Alias for ``cambrian sync``. Same semantics; chosen for muscle memory.

    Postgres-land tools tend to spell this ``download`` (sqitch) or ``sync``
    (graphile-migrate); we accept both. The catalog is the source of truth
    and both commands rehydrate the local ``committed/`` directory from it.
    """
    _sync_impl(path, dry_run=dry_run, diff=diff, force=force, as_json=as_json)
