"""Apply orchestration for ``current.sql`` — idempotent (M5) + reset (M6).

The idempotent runner expands ``current.sql`` (with includes), hashes the
expanded text, short-circuits if the hash matches the most recent ``apply``
event, and otherwise parses and dispatches each statement. One ``apply``
event is emitted regardless of full/partial success, carrying the new hash,
the full expanded SQL, and one ``table_states`` row per affected table.

Reset mode (``apply_reset``) is the opt-in path for non-idempotent
evolutions. It captures (or reuses) checkpoints for every affected table,
detects out-of-band writes via the four-pointer atomic restore's
``AssertRefSnapshotId`` requirement, rolls the tables back, re-executes the
expanded SQL, and emits two events (``rollback`` then ``apply``) so the
audit trail records both halves. Reset is never the default — see CLAUDE.md.
"""

from __future__ import annotations

import getpass
import socket
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Final, Literal

from pyiceberg.exceptions import NoSuchTableError, NoSuchViewError
from pyiceberg.serializers import FromInputFile
from sqlglot import expressions as exp

from saltation.catalog import load_catalog
from saltation.errors import (
    EvolutionNotFoundError,
    ExternalWriteDetectedError,
    IllegalStateError,
    SaltationError,
)
from saltation.iceberg.affected import (
    TableIdent,
    affected_tables_with_overrides,
    affected_views,
)
from saltation.iceberg.checkpoint import Checkpoint, capture, pin
from saltation.iceberg.txn import Prune, restore_pointers
from saltation.iceberg.views import rename_view, require_rest
from saltation.iceberg.views import set_current_version as set_current_view_version
from saltation.sidecar.events import (
    Event,
    TableStateRow,
    applied_committed_ids,
    events,
    latest_event,
    table_states_for_event,
    write_event,
)
from saltation.sidecar.selfmigrate import ensure_current
from saltation.sql.dialect import SaltationSpark
from saltation.sql.dispatch import dispatch
from saltation.sql.include import expand
from saltation.sql.script import parse_script

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table
    from pyiceberg.table.metadata import TableMetadata

    from saltation.config import SaltationConfig

__all__ = [
    "ApplyResult",
    "CommittedApplyResult",
    "ResetResult",
    "StatementResult",
    "apply_idempotent",
    "apply_reset",
    "replay_committed",
    "rollback_to_last_checkpoint",
]

# Evolution id used for the dev-loop ``current.sql`` slot. Reset's checkpoint
# tag is ``saltation.cp.<evolution_id>`` (per CLAUDE.md) → ``saltation.cp.current``.
CURRENT_EVOLUTION_ID = "current"

# A table saltation itself left dropped (or renamed away).
GONE: Final = "gone"
Expected = int | None | Literal["gone"]
CHECKPOINT_TAG_PREFIX = "saltation.cp."


@dataclass
class StatementResult:
    """One statement's outcome inside an apply."""

    sql: str
    notes: str = ""
    affected_tables: list[TableIdent] = field(default_factory=list)
    error: str | None = None


@dataclass
class CommittedApplyResult:
    """Outcome of replaying a single committed evolution."""

    evolution_id: str
    status: str
    evolution_hash: str
    event_id: str | None = None
    error: str | None = None


@dataclass
class ApplyResult:
    """The aggregate outcome of an apply.

    ``status`` is one of ``"unchanged"`` (hash matched), ``"applied"``
    (every statement succeeded), or ``"partial"`` (at least one statement
    failed but the runner continued under ``allow_partial=True``).

    ``applied_committed`` is the list of committed evolutions replayed by
    this invocation (empty when current.sql is the only thing in flight or
    the replay was a no-op).
    """

    status: str
    evolution_hash: str
    sources: list[Path] = field(default_factory=list)
    statements: list[StatementResult] = field(default_factory=list)
    event_id: str | None = None
    error: str | None = None
    applied_committed: list[CommittedApplyResult] = field(default_factory=list)
    evolution_id: str = "current"


def apply_idempotent(
    config: SaltationConfig,
    *,
    allow_partial: bool = False,
    actor: str | None = None,
    rerun: bool = False,
    rerun_committed: frozenset[str] = frozenset(),
) -> ApplyResult:
    """Apply committed evolutions + ``current.sql`` in idempotent mode.

    ``rerun`` skips the unchanged-hash short-circuit (reset has just rolled
    the state back, so the text must run again even if it hasn't changed).

    Steps:

    1. Replay any committed evolutions not yet present in the events log
       (post-hoc edit detection refuses on hash mismatch with a prior
       apply event for that evolution_id).
    2. Expand ``current.sql`` + hash.
    3. Load catalog; ``ensure_current`` on the sidecar.
    4. Check the last ``apply`` event for ``evolution_id="current"``. If its
       ``evolution_hash`` matches the new one: no-op.
    5. Parse all statements via :class:`SaltationSpark`.
    6. For each statement: capture pre-state, dispatch, capture post-state.
    7. Emit one ``apply`` event with the full expanded SQL and N
       ``table_states`` rows.

    Raises:
        EvolutionNotFoundError: ``current.sql`` doesn't exist.
        IllegalStateError: a committed file's content diverges from the
            hash recorded in a prior apply event (post-hoc edit).
        SaltationError: Various — see :mod:`saltation.errors`.
    """
    catalog = load_catalog(config)
    state = ensure_current(catalog, config.evolutions.sidecar_namespace, allow_read_only=False)
    namespace = state.sidecar_namespace

    replay_results = replay_committed(
        config,
        catalog=catalog,
        namespace=namespace,
        allow_partial=allow_partial,
        actor=actor,
        rerun=rerun_committed,
    )

    current_sql_path = _resolve_current_sql(config)
    expanded = expand(current_sql_path)

    if not rerun and _applied_and_standing(catalog, namespace, expanded.hash):
        return ApplyResult(
            status="unchanged",
            evolution_hash=expanded.hash,
            sources=expanded.sources,
            applied_committed=replay_results,
        )

    statements_raw = parse_script(expanded.text, view_source_dialect=config.views.source_dialect)
    statements = [s for s in statements_raw if s is not None]

    per_stmt_tables = affected_tables_with_overrides(expanded.text, statements)
    # Pre-snapshot the table state for every affected table so the post-apply
    # event log has accurate before/after rows. Tables that don't exist yet
    # (e.g. about-to-be-CREATEd) get a row with None pre-fields.
    state_by_ident: dict[str, _TableStateAccumulator] = {}
    for ident in _unique_tables(per_stmt_tables):
        state_by_ident[str(ident)] = _capture_pre(catalog, ident)
    views = _unique_tables(affected_views(s) for s in statements)
    view_rows = [_baseline_view_row(catalog, v) for v in views]

    statement_results: list[StatementResult] = []
    overall_error: str | None = None
    fatal_error: SaltationError | None = None

    for stmt, tables in zip(statements, per_stmt_tables, strict=True):
        try:
            result = dispatch(catalog, stmt, view_dialects=config.views.dialects)
            statement_results.append(
                StatementResult(
                    sql=_safe_sql(stmt),
                    notes=result.notes,
                    affected_tables=tables or result.affected_tables,
                )
            )
        except SaltationError as err:
            statement_results.append(
                StatementResult(
                    sql=_safe_sql(stmt),
                    affected_tables=tables,
                    error=str(err),
                )
            )
            overall_error = str(err)
            if not allow_partial:
                # Per the M5 contract: emit the partial-success event but
                # *also* surface the error to the caller. Reset (rollback on
                # failure) is M6's contract — we never recover here.
                fatal_error = err
                break

    # Post-snapshot every affected table that exists now.
    for acc in state_by_ident.values():
        acc.capture_post(catalog)

    status = "applied"
    if overall_error and not allow_partial:
        status = "partial"
    elif overall_error and allow_partial:
        status = "partial"

    notes = _summarise(statement_results, status)
    event_id = write_event(
        catalog,
        namespace,
        event_type="apply",
        evolution_id="current",
        evolution_hash=expanded.hash,
        evolution_sql=expanded.text,
        actor=actor or _default_actor(),
        notes=notes,
        table_states=[*(acc.to_row() for acc in state_by_ident.values()), *view_rows],
    )

    # If we hit a fatal error under ``allow_partial=False`` the event is
    # written first (so the audit trail captures the partial work), then we
    # surface the exception. Callers (CLI, tests) see the original error
    # type and message — no swallowing.
    if fatal_error is not None:
        raise fatal_error

    return ApplyResult(
        status=status,
        evolution_hash=expanded.hash,
        sources=expanded.sources,
        statements=statement_results,
        event_id=event_id,
        error=overall_error,
        applied_committed=replay_results,
    )


# Load-bearing per CLAUDE.md: idempotent is the path; reset is the relief
# valve. Hints that mention ``--reset`` MUST frame it as a last resort, not
# the recommended fix. Use this constant so the framing stays consistent.
RESET_LAST_RESORT_HINT = (
    "If — and only if — this statement genuinely cannot be expressed "
    "idempotently, ``saltation apply --reset`` will roll the affected tables "
    "back before re-applying. Prefer to make the SQL idempotent first "
    "(e.g. add ``IF EXISTS`` / ``IF NOT EXISTS``); reset is the relief "
    "valve, not the recommended path."
)


def replay_committed(
    config: SaltationConfig,
    *,
    catalog: Catalog,
    namespace: str,
    allow_partial: bool = False,
    actor: str | None = None,
    rerun: frozenset[str] = frozenset(),
) -> list[CommittedApplyResult]:
    """Replay every committed evolution not yet recorded in the events log.

    Evolutions in *rerun* run again even though they're recorded: reset has
    just restored tables to a baseline taken before they were applied.

    Walks ``committed/`` lexicographically. For each ``NNNN_<slug>.sql``:

    * If a prior ``apply`` event exists for that ``evolution_id``:
      - hash matches → skip (already applied);
      - hash mismatch → refuse with :class:`IllegalStateError`
        (post-hoc edit of a committed file).
    * Otherwise dispatch the SQL via the same path used for ``current.sql``
      and emit an ``apply`` event keyed on the evolution_id.

    Returns one :class:`CommittedApplyResult` per file processed.
    """
    from saltation.migrate.commit import discover_committed_files

    evolutions_dir = Path(config.evolutions.dir).resolve()
    committed_dir = evolutions_dir / "committed"
    files = discover_committed_files(committed_dir)
    if not files:
        return []

    applied = applied_committed_ids(catalog, namespace)
    results: list[CommittedApplyResult] = []

    for cf in files:
        text = cf.path.read_text(encoding="utf-8")
        digest = _sha256_hex(text)

        prior_hash = applied.get(cf.evolution_id)
        if prior_hash is not None and (prior_hash != digest or cf.evolution_id not in rerun):
            if prior_hash != digest:
                raise IllegalStateError(
                    f"committed file {cf.path.name} has been edited since it was applied "
                    f"(recorded hash {prior_hash[:12]}…, current hash {digest[:12]}…). "
                    "Committed evolutions are immutable history — restore the file from "
                    "git or run `saltation sync` to reconcile against the catalog."
                )
            results.append(
                CommittedApplyResult(
                    evolution_id=cf.evolution_id,
                    status="unchanged",
                    evolution_hash=digest,
                )
            )
            continue

        result = _apply_one_committed_text(
            catalog,
            namespace,
            evolution_id=cf.evolution_id,
            text=text,
            digest=digest,
            sources=[cf.path],
            allow_partial=allow_partial,
            actor=actor,
            view_dialects=config.views.dialects,
            view_source_dialect=config.views.source_dialect,
        )
        results.append(
            CommittedApplyResult(
                evolution_id=cf.evolution_id,
                status=result.status,
                evolution_hash=result.evolution_hash,
                event_id=result.event_id,
                error=result.error,
            )
        )
        if result.error is not None and not allow_partial:
            raise IllegalStateError(
                f"failed replaying committed evolution {cf.evolution_id}: {result.error}"
            )
    return results


def _sha256_hex(text: str) -> str:
    import hashlib

    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _apply_one_committed_text(
    catalog: Catalog,
    namespace: str,
    *,
    evolution_id: str,
    text: str,
    digest: str,
    sources: list[Path],
    allow_partial: bool,
    actor: str | None,
    view_dialects: Sequence[str],
    view_source_dialect: str,
) -> ApplyResult:
    """Dispatch one committed evolution's SQL and emit an ``apply`` event for it.

    Mirrors the body of :func:`apply_idempotent` but keyed on a stable
    ``evolution_id`` (the ``NNNN_<slug>`` form) rather than ``"current"``.
    """
    statements_raw = parse_script(text, view_source_dialect=view_source_dialect)
    statements = [s for s in statements_raw if s is not None]
    per_stmt_tables = affected_tables_with_overrides(text, statements)

    state_by_ident: dict[str, _TableStateAccumulator] = {}
    for ident in _unique_tables(per_stmt_tables):
        state_by_ident[str(ident)] = _capture_pre(catalog, ident)

    statement_results: list[StatementResult] = []
    overall_error: str | None = None
    fatal_error: SaltationError | None = None

    for stmt, tables in zip(statements, per_stmt_tables, strict=True):
        try:
            result = dispatch(catalog, stmt, view_dialects=view_dialects)
            statement_results.append(
                StatementResult(
                    sql=_safe_sql(stmt),
                    notes=result.notes,
                    affected_tables=tables or result.affected_tables,
                )
            )
        except SaltationError as err:
            statement_results.append(
                StatementResult(sql=_safe_sql(stmt), affected_tables=tables, error=str(err))
            )
            overall_error = str(err)
            if not allow_partial:
                fatal_error = err
                break

    for acc in state_by_ident.values():
        acc.capture_post(catalog)

    status = "applied" if overall_error is None else "partial"

    notes = _summarise(statement_results, status)
    event_id = write_event(
        catalog,
        namespace,
        event_type="apply",
        evolution_id=evolution_id,
        evolution_hash=digest,
        evolution_sql=text,
        actor=actor or _default_actor(),
        notes=notes,
        table_states=[acc.to_row() for acc in state_by_ident.values()],
    )

    if fatal_error is not None:
        raise fatal_error

    return ApplyResult(
        status=status,
        evolution_hash=digest,
        sources=sources,
        statements=statement_results,
        event_id=event_id,
        error=overall_error,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolve_current_sql(config: SaltationConfig) -> Path:
    base = Path(config.evolutions.dir).resolve()
    candidate = base / "current.sql"
    if not candidate.exists():
        raise EvolutionNotFoundError(
            f"current.sql not found at {candidate} "
            f"(config: evolutions.dir = {config.evolutions.dir})"
        )
    return candidate


def _default_actor() -> str:
    """Build a default actor string for the event log.

    Format is ``<user>@<host>``; falls back to ``saltation`` if either part
    can't be determined. The events table stores this verbatim — anything
    that lets a future audit trace "who applied this" works.
    """
    try:
        user = getpass.getuser()
    except OSError:
        user = "saltation"
    try:
        host = socket.gethostname()
    except OSError:
        host = "unknown"
    return f"{user}@{host}"


def _summarise(stmts: list[StatementResult], status: str) -> str:
    n = len(stmts)
    errs = sum(1 for s in stmts if s.error)
    return f"status={status} statements={n} errors={errs}"


def _safe_sql(stmt: exp.Expression) -> str:
    """The statement as SQL for events and ``--json``; never fails the apply over it."""
    try:
        return stmt.sql(dialect=SaltationSpark)
    except Exception:
        return f"<unprintable {type(stmt).__name__}>"


def _unique_tables(per_stmt: Iterable[Iterable[TableIdent]]) -> list[TableIdent]:
    """Flatten and dedupe a per-statement list-of-lists into a single ordered list."""
    seen: dict[str, TableIdent] = {}
    for tables in per_stmt:
        for t in tables:
            seen.setdefault(str(t), t)
    return list(seen.values())


@dataclass
class _TableStateAccumulator:
    """Helper that buffers pre- and post- snapshots of one table for the event log."""

    ident: TableIdent
    pre_snapshot_id: int | None = None
    pre_schema_id: int | None = None
    pre_spec_id: int | None = None
    pre_sort_order_id: int | None = None
    pre_metadata_loc: str | None = None
    pre_properties: dict[str, str] | None = None
    pre_uuid: str | None = None
    post_snapshot_id: int | None = None
    post_schema_id: int | None = None
    post_spec_id: int | None = None
    post_sort_order_id: int | None = None

    def capture_post(self, catalog: Catalog) -> None:
        table = _load_or_none(catalog, self.ident)
        if table is None:
            return
        snap = table.current_snapshot()
        self.post_snapshot_id = snap.snapshot_id if snap is not None else None
        self.post_schema_id = table.schema().schema_id
        self.post_spec_id = table.spec().spec_id
        self.post_sort_order_id = table.sort_order().order_id

    def to_row(self) -> TableStateRow:
        return TableStateRow(
            table_ident=str(self.ident),
            pre_snapshot_id=self.pre_snapshot_id,
            pre_schema_id=self.pre_schema_id,
            pre_spec_id=self.pre_spec_id,
            pre_sort_order_id=self.pre_sort_order_id,
            pre_metadata_loc=self.pre_metadata_loc,
            pre_properties=self.pre_properties,
            pre_uuid=self.pre_uuid,
            post_snapshot_id=self.post_snapshot_id,
            post_schema_id=self.post_schema_id,
            post_spec_id=self.post_spec_id,
            post_sort_order_id=self.post_sort_order_id,
        )


def _capture_pre(catalog: Catalog, ident: TableIdent) -> _TableStateAccumulator:
    acc = _TableStateAccumulator(ident=ident)
    table = _load_or_none(catalog, ident)
    if table is None:
        return acc
    cp = capture(table)
    acc.pre_snapshot_id = cp.snapshot_id
    acc.pre_schema_id = cp.schema_id
    acc.pre_spec_id = cp.spec_id
    acc.pre_sort_order_id = cp.sort_order_id
    acc.pre_metadata_loc = cp.metadata_loc
    acc.pre_properties = dict(table.properties)
    acc.pre_uuid = str(table.metadata.table_uuid)
    return acc


def _load_or_none(catalog: Catalog, ident: TableIdent) -> Table | None:
    """Return ``catalog.load_table(ident)`` or ``None`` if the table doesn't exist."""
    tup: tuple[str, ...]
    if ident.namespace:
        # ``ns.t`` → ``(ns, t)``; ``cat.ns.t`` → ``(cat, ns, t)``.
        tup = (*ident.namespace.split("."), ident.name)
    else:
        tup = (ident.name,)
    try:
        return catalog.load_table(tup)
    except NoSuchTableError:
        return None


# ---------------------------------------------------------------------------
# Reset mode (M6)
# ---------------------------------------------------------------------------


@dataclass
class TableRollback:
    """Summary of one table's rollback within a reset cycle."""

    ident: str
    rolled_back: bool
    from_snapshot_id: int | None = None
    to_snapshot_id: int | None = None
    reason: str = ""


@dataclass
class ResetResult:
    """Outcome of an ``apply_reset`` cycle.

    Captures both the rollback half (per affected table) and the subsequent
    apply half (a regular :class:`ApplyResult`) so callers can see what
    moved without parsing two events out of the log.
    """

    status: str
    evolution_hash: str
    sources: list[Path] = field(default_factory=list)
    rollbacks: list[TableRollback] = field(default_factory=list)
    apply_result: ApplyResult | None = None
    rollback_event_id: str | None = None
    apply_event_id: str | None = None
    error: str | None = None


def _applied_and_standing(catalog: Catalog, namespace: str, evolution_hash: str) -> bool:
    """Whether this exact ``current.sql`` was the last thing applied and no rollback undid it."""
    last = latest_event(catalog, namespace, event_type="apply", evolution_id=CURRENT_EVOLUTION_ID)
    if last is None or last.evolution_hash != evolution_hash:
        return False
    if not _in_cycle(catalog, namespace)(last):
        # Applied before a commit or uncommit that has since moved the tables.
        return False
    undo = latest_event(
        catalog, namespace, event_type="rollback", evolution_id=CURRENT_EVOLUTION_ID
    )
    return undo is None or undo.event_ts < last.event_ts


def _checkpoint_tag(evolution_id: str) -> str:
    return f"{CHECKPOINT_TAG_PREFIX}{evolution_id}"


# The reset baseline: what every table and view a reset has touched looked
# like before ``current.sql`` was first applied, keyed by (kind, ident). An
# object is added the first time a reset sees it in ``current.sql``; nothing
# has changed it since the baseline began, so its state at that moment *is*
# its pre-``current.sql`` state. The baseline is carried forward unchanged
# by every reset (a ``checkpoint`` event) until a commit or uncommit starts
# a new dev cycle.
_Baseline = dict[tuple[str, str], TableStateRow]


def _in_cycle(catalog: Catalog, namespace: str) -> Callable[[Event | None], bool]:
    """Whether an event belongs to the current dev cycle (since the last commit/uncommit)."""
    boundaries = [
        e.event_ts
        for kind in ("commit", "uncommit")
        if (e := latest_event(catalog, namespace, event_type=kind)) is not None
    ]
    start = max(boundaries, default=None)
    return lambda event: event is not None and (start is None or event.event_ts > start)


def _load_baseline(catalog: Catalog, namespace: str) -> _Baseline:
    """The baseline recorded by the last reset, completed from this cycle's applies.

    Idempotent applies record each table's pre-state too, so a table first
    touched by one of those (before any reset, or by ``saltation apply``
    between resets) gets the pre-state of the earliest such apply.
    """
    in_cycle = _in_cycle(catalog, namespace)

    baseline: _Baseline = {}
    checkpoint = latest_event(
        catalog, namespace, event_type="checkpoint", evolution_id=CURRENT_EVOLUTION_ID
    )
    if in_cycle(checkpoint) and checkpoint is not None:
        for r in table_states_for_event(catalog, namespace, event_id=checkpoint.event_id):
            baseline[(r.object_kind, r.table_ident)] = r
    for apply in events(catalog, namespace, event_type="apply", evolution_id=CURRENT_EVOLUTION_ID):
        if not in_cycle(apply):
            continue
        for r in table_states_for_event(catalog, namespace, event_id=apply.event_id):
            baseline.setdefault((r.object_kind, r.table_ident), _pre_only(r))
    return baseline


def _pre_only(row: TableStateRow) -> TableStateRow:
    return TableStateRow(
        table_ident=row.table_ident,
        object_kind=row.object_kind,
        pre_snapshot_id=row.pre_snapshot_id,
        pre_schema_id=row.pre_schema_id,
        pre_spec_id=row.pre_spec_id,
        pre_sort_order_id=row.pre_sort_order_id,
        pre_metadata_loc=row.pre_metadata_loc,
        pre_properties=row.pre_properties,
        pre_view_version_id=row.pre_view_version_id,
        pre_uuid=row.pre_uuid,
    )


def _baseline_table_row(catalog: Catalog, ident: TableIdent) -> TableStateRow:
    table = _load_or_none(catalog, ident)
    if table is None:
        return TableStateRow(table_ident=str(ident))
    cp = capture(table)
    tag = _checkpoint_tag(CURRENT_EVOLUTION_ID)
    try:
        pin(table, tag_name=tag, snapshot_id=cp.snapshot_id)
    except Exception:
        tag = None
    return TableStateRow(
        table_ident=str(ident),
        pre_snapshot_id=cp.snapshot_id,
        pre_schema_id=cp.schema_id,
        pre_spec_id=cp.spec_id,
        pre_sort_order_id=cp.sort_order_id,
        pre_metadata_loc=cp.metadata_loc,
        pre_properties=dict(table.properties),
        pre_uuid=str(table.metadata.table_uuid),
        tag_ref=tag if cp.snapshot_id is not None else None,
    )


def _baseline_view_row(catalog: Catalog, ident: TableIdent) -> TableStateRow:
    try:
        view = catalog.load_view(_ident_to_tuple(ident))
    except NoSuchViewError:
        return TableStateRow(table_ident=str(ident), object_kind="view")
    return TableStateRow(
        table_ident=str(ident),
        object_kind="view",
        pre_view_version_id=view.metadata.current_version_id,
        pre_uuid=str(view.metadata.view_uuid),
    )


def _base_metadata(table: Table, row: TableStateRow) -> TableMetadata | None:
    if row.pre_metadata_loc is None:
        return None
    try:
        return FromInputFile.table_metadata(table.io.new_input(row.pre_metadata_loc))
    except (FileNotFoundError, OSError):
        # Old metadata files can be cleaned up; restore then skips what needs it.
        return None


def _upgraded_since(catalog: Catalog, row: TableStateRow) -> bool:
    table = catalog.load_table(_ident_str_to_tuple(row.table_ident))
    if table.format_version == 1:
        return False
    base = _base_metadata(table, row)
    return base is not None and table.format_version > base.format_version


def _prune_since(table: Table, base: TableMetadata | None) -> Prune | None:
    """What reset removes beyond the pointers: everything added since *base*.

    Specs and schemas the undone edits added would otherwise constrain later
    edits (a new column can't reuse the name of an old identity partition
    field). Skipped when another ref holds one of the newer snapshots.
    """
    if base is None:
        return None
    meta = table.metadata
    snapshots = {s.snapshot_id for s in meta.snapshots} - {s.snapshot_id for s in base.snapshots}
    if any(ref.snapshot_id in snapshots for name, ref in meta.refs.items() if name != "main"):
        return None
    return Prune(
        snapshot_ids=frozenset(snapshots),
        schema_ids=frozenset(
            {s.schema_id for s in meta.schemas} - {s.schema_id for s in base.schemas}
        ),
        spec_ids=frozenset(
            {s.spec_id for s in meta.partition_specs} - {s.spec_id for s in base.partition_specs}
        ),
    )


def _existed(row: TableStateRow) -> bool:
    if row.object_kind == "view":
        return row.pre_view_version_id is not None
    return row.pre_metadata_loc is not None


def _exists(catalog: Catalog, row: TableStateRow) -> bool:
    ident = _ident_str_to_tuple(row.table_ident)
    if row.object_kind == "view":
        return catalog.view_exists(ident)
    return catalog.table_exists(ident)


def _undo_own_moves(catalog: Catalog, baseline: _Baseline) -> list[TableRollback]:
    """Bring back baselined objects that ``current.sql`` itself renamed away or dropped.

    A rename keeps the object's UUID, so a renamed table or view shows up as
    an object that didn't exist at the baseline carrying a lost one's UUID.
    A dropped table is re-registered from its baseline metadata file.
    """
    lost = [r for r in baseline.values() if _existed(r) and not _exists(catalog, r)]
    if not lost:
        return []
    arrived = {
        (r.object_kind, uuid): r
        for r in baseline.values()
        if not _existed(r)
        and _exists(catalog, r)
        and (uuid := _current_uuid(catalog, r)) is not None
    }
    out: list[TableRollback] = []
    for row in lost:
        ident = _ident_str_to_tuple(row.table_ident)
        moved = arrived.get((row.object_kind, row.pre_uuid)) if row.pre_uuid else None
        if moved is not None:
            source = _ident_str_to_tuple(moved.table_ident)
            if row.object_kind == "view":
                rename_view(require_rest(catalog), source, ident)
            else:
                catalog.rename_table(source, ident)
            reason = f"renamed back from {moved.table_ident}"
        elif row.object_kind == "table" and row.pre_metadata_loc is not None:
            try:
                catalog.register_table(ident, row.pre_metadata_loc)
            except Exception:
                continue
            reason = "re-registered after current.sql dropped it"
        else:
            continue
        out.append(TableRollback(ident=row.table_ident, rolled_back=True, reason=reason))
    return out


def _current_uuid(catalog: Catalog, row: TableStateRow) -> str | None:
    ident = _ident_str_to_tuple(row.table_ident)
    if row.object_kind == "view":
        return str(catalog.load_view(ident).metadata.view_uuid)
    return str(catalog.load_table(ident).metadata.table_uuid)


def _restore_baseline(catalog: Catalog, baseline: _Baseline) -> list[TableRollback]:
    """Put every baselined object back the way it was before ``current.sql``."""
    undone = _undo_own_moves(catalog, baseline)
    lost = sorted(
        f"{r.object_kind} {r.table_ident}"
        for r in baseline.values()
        if _existed(r) and not _exists(catalog, r)
    )
    if lost:
        msg = (
            f"reset can't restore {', '.join(lost)}: dropped or renamed since current.sql "
            "was first applied. Recreate it as it was, or commit/uncommit to start a new "
            "baseline."
        )
        raise IllegalStateError(msg)
    upgraded = sorted(
        r.table_ident
        for r in baseline.values()
        if r.object_kind == "table" and _existed(r) and _upgraded_since(catalog, r)
    )
    if upgraded:
        msg = (
            f"reset can't restore {', '.join(upgraded)}: current.sql upgraded its format "
            "version, and Iceberg tables can't be downgraded. Commit the upgrade on its own, "
            "or drop and recreate the table."
        )
        raise IllegalStateError(msg)

    rollbacks: list[TableRollback] = undone
    for row in baseline.values():
        ident = _ident_str_to_tuple(row.table_ident)
        if not _existed(row):
            if not _exists(catalog, row):
                continue
            if row.object_kind == "view":
                catalog.drop_view(ident)
            else:
                catalog.drop_table(ident)
            rollbacks.append(
                TableRollback(
                    ident=row.table_ident,
                    rolled_back=True,
                    reason=f"dropped: {row.object_kind} created by current.sql",
                )
            )
            continue
        if row.object_kind == "view":
            view = catalog.load_view(ident)
            target = row.pre_view_version_id
            if target is not None and view.metadata.current_version_id != target:
                set_current_view_version(require_rest(catalog), view, target)
                rollbacks.append(
                    TableRollback(
                        ident=row.table_ident, rolled_back=True, reason="view version restored"
                    )
                )
            continue
        cp = _row_to_checkpoint(row)
        assert cp is not None
        table = catalog.load_table(ident)
        snap = table.current_snapshot()
        current_snap = snap.snapshot_id if snap is not None else None
        restore_pointers(
            table,
            target_snapshot_id=cp.snapshot_id,
            target_schema_id=cp.schema_id,
            target_spec_id=cp.spec_id,
            target_sort_order_id=cp.sort_order_id,
            expected_current_snapshot_id=current_snap,
            target_properties=row.pre_properties,
            prune=_prune_since(table, _base_metadata(table, row)),
        )
        rollbacks.append(
            TableRollback(
                ident=row.table_ident,
                rolled_back=True,
                from_snapshot_id=current_snap,
                to_snapshot_id=cp.snapshot_id,
                reason="restored to its state before current.sql",
            )
        )
    return rollbacks


def _row_to_checkpoint(row: TableStateRow) -> Checkpoint | None:
    """Reconstruct a :class:`Checkpoint` from a stored ``table_states`` row.

    Returns ``None`` if the row's pre-state is empty (the table didn't
    exist before that apply) — there's nothing to roll back to.
    """
    if (
        row.pre_schema_id is None
        or row.pre_spec_id is None
        or row.pre_sort_order_id is None
        or row.pre_metadata_loc is None
    ):
        return None
    return Checkpoint(
        snapshot_id=row.pre_snapshot_id,
        schema_id=row.pre_schema_id,
        spec_id=row.pre_spec_id,
        sort_order_id=row.pre_sort_order_id,
        metadata_loc=row.pre_metadata_loc,
        tag_ref=row.tag_ref,
    )


def _expected_snapshots(
    catalog: Catalog, namespace: str, baseline: _Baseline
) -> dict[str, Expected]:
    """Where saltation last left each table's ``main`` this dev cycle.

    Replays the cycle's applies (current.sql and committed evolutions) and
    rollbacks in order; tables saltation hasn't touched since the last commit
    or uncommit have no expectation.
    """
    in_cycle = _in_cycle(catalog, namespace)
    cycle = [
        e
        for e in events(catalog, namespace)
        if in_cycle(e)
        and (
            e.event_type == "apply"
            or (e.event_type == "rollback" and e.evolution_id == CURRENT_EVOLUTION_ID)
        )
    ]
    expected: dict[str, Expected] = {}
    for event in cycle:
        if event.event_type == "rollback":
            expected = {
                ident: row.pre_snapshot_id if _existed(row) else GONE
                for (kind, ident), row in baseline.items()
                if kind == "table"
            }
            continue
        for row in table_states_for_event(catalog, namespace, event_id=event.event_id):
            if row.object_kind == "table":
                expected[row.table_ident] = (
                    GONE if row.post_schema_id is None else row.post_snapshot_id
                )
    return expected


def _pending_committed(config: SaltationConfig, catalog: Catalog, namespace: str) -> bool:
    from saltation.migrate.commit import discover_committed_files

    files = discover_committed_files(Path(config.evolutions.dir).resolve() / "committed")
    applied = applied_committed_ids(catalog, namespace)
    return any(cf.evolution_id not in applied for cf in files)


def _committed_in_cycle(catalog: Catalog, namespace: str) -> frozenset[str]:
    """Committed evolutions applied since the last commit/uncommit (e.g. after a pull)."""
    in_cycle = _in_cycle(catalog, namespace)
    return frozenset(
        e.evolution_id
        for e in events(catalog, namespace, event_type="apply")
        if e.evolution_id != CURRENT_EVOLUTION_ID and in_cycle(e)
    )


def _write_baseline(
    catalog: Catalog,
    namespace: str,
    baseline: _Baseline,
    *,
    evolution_hash: str,
    evolution_sql: str,
    actor: str,
    rollbacks: list[TableRollback],
) -> str:
    write_event(
        catalog,
        namespace,
        event_type="checkpoint",
        evolution_id=CURRENT_EVOLUTION_ID,
        evolution_hash=evolution_hash,
        evolution_sql=evolution_sql,
        actor=actor,
        notes=f"reset baseline: {len(baseline)} objects",
        table_states=list(baseline.values()),
    )
    return write_event(
        catalog,
        namespace,
        event_type="rollback",
        evolution_id=CURRENT_EVOLUTION_ID,
        evolution_hash=evolution_hash,
        evolution_sql=evolution_sql,
        actor=actor,
        notes=f"rolled back {len(rollbacks)} of {len(baseline)} objects to the reset baseline",
    )


def apply_reset(
    config: SaltationConfig,
    *,
    allow_partial: bool = False,
    force: bool = False,
    actor: str | None = None,
) -> ResetResult:
    """Apply ``current.sql`` in reset mode.

    Reset is the relief valve for non-idempotent SQL — never the default
    path. Every table and view any version of ``current.sql`` has touched
    goes back to how it stood before ``current.sql`` was first applied
    (objects it created are dropped), then the current text runs once. So
    after any sequence of edits the catalog is the committed state plus the
    latest ``current.sql``.

    1. Short-circuit if the text matches the last apply and nothing moved.
    2. External-write detection against the last apply's post snapshots;
       :class:`ExternalWriteDetectedError` unless ``force=True``.
    3. Add newly touched objects to the baseline, restore the baseline
       (``checkpoint`` and ``rollback`` events), then apply.

    Raises:
        ExternalWriteDetectedError: a table moved out-of-band and ``force``
            is false.
        IllegalStateError: a baselined object was dropped or renamed.
        SaltationError: any of the underlying parse/dispatch/commit errors.
    """
    current_sql_path = _resolve_current_sql(config)
    expanded = expand(current_sql_path)
    catalog = load_catalog(config)
    state = ensure_current(catalog, config.evolutions.sidecar_namespace, allow_read_only=False)
    namespace = state.sidecar_namespace
    actor = actor or _default_actor()

    statements = [
        s
        for s in parse_script(expanded.text, view_source_dialect=config.views.source_dialect)
        if s is not None
    ]
    idents = _unique_tables(affected_tables_with_overrides(expanded.text, statements))
    view_idents = _unique_tables(affected_views(s) for s in statements)

    baseline = _load_baseline(catalog, namespace)
    prior_post_snapshots = _expected_snapshots(catalog, namespace, baseline)
    watched = _unique_tables(
        [idents, [TableIdent(*_split(k)) for (kind, k) in baseline if kind == "table"]]
    )
    diverged = _check_external_writes(catalog, watched, prior_post_snapshots)
    if diverged and not force:
        raise ExternalWriteDetectedError(
            ref=f"main (tables: {', '.join(diverged)})",
            expected_snapshot_id=None,
            observed_snapshot_id=None,
        )

    # Newly committed evolutions (e.g. after a pull) must run on the restored
    # baseline, ahead of current.sql, so they rule out the shortcut.
    pending = _pending_committed(config, catalog, namespace)
    if not diverged and not pending and _applied_and_standing(catalog, namespace, expanded.hash):
        return ResetResult(
            status="unchanged", evolution_hash=expanded.hash, sources=expanded.sources
        )

    for ident in idents:
        if ("table", str(ident)) not in baseline:
            baseline["table", str(ident)] = _baseline_table_row(catalog, ident)
    for ident in view_idents:
        if ("view", str(ident)) not in baseline:
            baseline["view", str(ident)] = _baseline_view_row(catalog, ident)

    rollbacks = _restore_baseline(catalog, baseline)
    rollback_event_id = _write_baseline(
        catalog,
        namespace,
        baseline,
        evolution_hash=expanded.hash,
        evolution_sql=expanded.text,
        actor=actor,
        rollbacks=rollbacks,
    )

    try:
        apply_result = apply_idempotent(
            config,
            allow_partial=allow_partial,
            actor=actor,
            rerun=True,
            rerun_committed=_committed_in_cycle(catalog, namespace),
        )
    except SaltationError as err:
        return ResetResult(
            status="partial",
            evolution_hash=expanded.hash,
            sources=expanded.sources,
            rollbacks=rollbacks,
            rollback_event_id=rollback_event_id,
            error=str(err),
        )

    return ResetResult(
        status=apply_result.status,
        evolution_hash=expanded.hash,
        sources=expanded.sources,
        rollbacks=rollbacks,
        apply_result=apply_result,
        rollback_event_id=rollback_event_id,
        apply_event_id=apply_result.event_id,
        error=apply_result.error,
    )


def rollback_to_last_checkpoint(
    config: SaltationConfig,
    *,
    actor: str | None = None,
) -> ResetResult:
    """Undo ``current.sql``: restore the reset baseline without re-applying.

    Used by ``saltation rollback`` ("undo the dev work and re-edit
    ``current.sql`` from scratch"). Objects touched only by idempotent
    applies since the baseline began are not tracked; the next reset adds
    them. Emits ``checkpoint`` and ``rollback`` events.
    """
    catalog = load_catalog(config)
    state = ensure_current(catalog, config.evolutions.sidecar_namespace, allow_read_only=False)
    namespace = state.sidecar_namespace

    prior = latest_event(catalog, namespace, event_type="apply", evolution_id=CURRENT_EVOLUTION_ID)
    baseline = _load_baseline(catalog, namespace)
    if prior is None or not baseline:
        return ResetResult(status="unchanged", evolution_hash="", sources=[], rollbacks=[])

    rerun = _committed_in_cycle(catalog, namespace)
    rollbacks = _restore_baseline(catalog, baseline)
    event_id = _write_baseline(
        catalog,
        namespace,
        baseline,
        evolution_hash=prior.evolution_hash,
        evolution_sql=prior.evolution_sql,
        actor=actor or _default_actor(),
        rollbacks=rollbacks,
    )
    replay_committed(config, catalog=catalog, namespace=namespace, actor=actor, rerun=rerun)
    return ResetResult(
        status="applied",
        evolution_hash=prior.evolution_hash,
        sources=[],
        rollbacks=rollbacks,
        rollback_event_id=event_id,
    )


def _split(ident: str) -> tuple[str | None, str]:
    ns, _, name = ident.rpartition(".")
    return (ns or None, name)


def _check_external_writes(
    catalog: Catalog,
    idents: list[TableIdent],
    prior_post_snapshots: dict[str, Expected],
) -> list[str]:
    """Return the list of table idents whose current snapshot diverges from prior.

    A table that exists in *prior_post_snapshots* but whose ``main`` snapshot
    has moved since the last apply is reported. New tables (no entry in
    prior) are skipped: there's no prior state to diverge from.
    """
    diverged: list[str] = []
    for ident in idents:
        ident_str = str(ident)
        if ident_str not in prior_post_snapshots:
            continue
        expected = prior_post_snapshots[ident_str]
        table = _load_or_none(catalog, ident)
        if (table is None) != (expected is GONE):
            # Dropped, or recreated, by someone other than saltation.
            diverged.append(ident_str)
            continue
        if table is None:
            continue
        snap = table.current_snapshot()
        observed = snap.snapshot_id if snap is not None else None
        if observed != expected:
            diverged.append(ident_str)
    return diverged


def _ident_to_tuple(ident: TableIdent) -> tuple[str, ...]:
    if ident.namespace:
        return (*ident.namespace.split("."), ident.name)
    return (ident.name,)


def _ident_str_to_tuple(ident: str) -> tuple[str, ...]:
    if "." in ident:
        ns, name = ident.rsplit(".", 1)
        return (*ns.split("."), name)
    return (ident,)
