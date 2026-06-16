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
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot
from pyiceberg.exceptions import NoSuchTableError
from sqlglot import expressions as exp

from cambrian.catalog import load_catalog
from cambrian.errors import (
    CambrianError,
    EvolutionNotFoundError,
    ExternalWriteDetectedError,
    IllegalStateError,
)
from cambrian.iceberg.affected import (
    TableIdent,
    affected_tables_with_overrides,
)
from cambrian.iceberg.checkpoint import Checkpoint, capture, pin
from cambrian.iceberg.txn import restore_pointers
from cambrian.sidecar.events import (
    TableStateRow,
    applied_committed_ids,
    latest_event,
    table_states_for_event,
    write_event,
)
from cambrian.sidecar.selfmigrate import ensure_current
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.dispatch import dispatch
from cambrian.sql.include import expand

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog
    from pyiceberg.table import Table

    from cambrian.config import CambrianConfig

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
# tag is ``cambrian.cp.<evolution_id>`` (per CLAUDE.md) → ``cambrian.cp.current``.
CURRENT_EVOLUTION_ID = "current"
CHECKPOINT_TAG_PREFIX = "cambrian.cp."


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
    config: CambrianConfig,
    *,
    allow_partial: bool = False,
    actor: str | None = None,
) -> ApplyResult:
    """Apply committed evolutions + ``current.sql`` in idempotent mode.

    Steps:

    1. Replay any committed evolutions not yet present in the events log
       (post-hoc edit detection refuses on hash mismatch with a prior
       apply event for that evolution_id).
    2. Expand ``current.sql`` + hash.
    3. Load catalog; ``ensure_current`` on the sidecar.
    4. Check the last ``apply`` event for ``evolution_id="current"``. If its
       ``evolution_hash`` matches the new one: no-op.
    5. Parse all statements via :class:`CambrianSpark`.
    6. For each statement: capture pre-state, dispatch, capture post-state.
    7. Emit one ``apply`` event with the full expanded SQL and N
       ``table_states`` rows.

    Raises:
        EvolutionNotFoundError: ``current.sql`` doesn't exist.
        IllegalStateError: a committed file's content diverges from the
            hash recorded in a prior apply event (post-hoc edit).
        CambrianError: Various — see :mod:`cambrian.errors`.
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
    )

    current_sql_path = _resolve_current_sql(config)
    expanded = expand(current_sql_path)

    last = latest_event(catalog, namespace, event_type="apply", evolution_id="current")
    if last is not None and last.evolution_hash == expanded.hash:
        return ApplyResult(
            status="unchanged",
            evolution_hash=expanded.hash,
            sources=expanded.sources,
            applied_committed=replay_results,
        )

    statements_raw = sqlglot.parse(expanded.text, dialect=CambrianSpark)
    statements = [s for s in statements_raw if s is not None]

    per_stmt_tables = affected_tables_with_overrides(expanded.text, statements)
    # Pre-snapshot the table state for every affected table so the post-apply
    # event log has accurate before/after rows. Tables that don't exist yet
    # (e.g. about-to-be-CREATEd) get a row with None pre-fields.
    state_by_ident: dict[str, _TableStateAccumulator] = {}
    for ident in _unique_tables(per_stmt_tables):
        state_by_ident[str(ident)] = _capture_pre(catalog, ident)

    statement_results: list[StatementResult] = []
    overall_error: str | None = None
    fatal_error: CambrianError | None = None

    for stmt, tables in zip(statements, per_stmt_tables, strict=True):
        try:
            result = dispatch(catalog, stmt)
            statement_results.append(
                StatementResult(
                    sql=_safe_sql(stmt),
                    notes=result.notes,
                    affected_tables=tables or result.affected_tables,
                )
            )
        except CambrianError as err:
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
        table_states=[acc.to_row() for acc in state_by_ident.values()],
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
    "idempotently, ``cambrian apply --reset`` will roll the affected tables "
    "back before re-applying. Prefer to make the SQL idempotent first "
    "(e.g. add ``IF EXISTS`` / ``IF NOT EXISTS``); reset is the relief "
    "valve, not the recommended path."
)


def replay_committed(
    config: CambrianConfig,
    *,
    catalog: Catalog,
    namespace: str,
    allow_partial: bool = False,
    actor: str | None = None,
) -> list[CommittedApplyResult]:
    """Replay every committed evolution not yet recorded in the events log.

    Walks ``committed/`` lexicographically. For each ``NNNN_<slug>.sql``:

    * If a prior ``apply`` event exists for that ``evolution_id``:
      - hash matches → skip (already applied);
      - hash mismatch → refuse with :class:`IllegalStateError`
        (post-hoc edit of a committed file).
    * Otherwise dispatch the SQL via the same path used for ``current.sql``
      and emit an ``apply`` event keyed on the evolution_id.

    Returns one :class:`CommittedApplyResult` per file processed.
    """
    from cambrian.migrate.commit import discover_committed_files

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
        if prior_hash is not None:
            if prior_hash != digest:
                raise IllegalStateError(
                    f"committed file {cf.path.name} has been edited since it was applied "
                    f"(recorded hash {prior_hash[:12]}…, current hash {digest[:12]}…). "
                    "Committed evolutions are immutable history — restore the file from "
                    "git or run `cambrian sync` to reconcile against the catalog."
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
) -> ApplyResult:
    """Dispatch one committed evolution's SQL and emit an ``apply`` event for it.

    Mirrors the body of :func:`apply_idempotent` but keyed on a stable
    ``evolution_id`` (the ``NNNN_<slug>`` form) rather than ``"current"``.
    """
    statements_raw = sqlglot.parse(text, dialect=CambrianSpark)
    statements = [s for s in statements_raw if s is not None]
    per_stmt_tables = affected_tables_with_overrides(text, statements)

    state_by_ident: dict[str, _TableStateAccumulator] = {}
    for ident in _unique_tables(per_stmt_tables):
        state_by_ident[str(ident)] = _capture_pre(catalog, ident)

    statement_results: list[StatementResult] = []
    overall_error: str | None = None
    fatal_error: CambrianError | None = None

    for stmt, tables in zip(statements, per_stmt_tables, strict=True):
        try:
            result = dispatch(catalog, stmt)
            statement_results.append(
                StatementResult(
                    sql=_safe_sql(stmt),
                    notes=result.notes,
                    affected_tables=tables or result.affected_tables,
                )
            )
        except CambrianError as err:
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


def _resolve_current_sql(config: CambrianConfig) -> Path:
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

    Format is ``<user>@<host>``; falls back to ``cambrian`` if either part
    can't be determined. The events table stores this verbatim — anything
    that lets a future audit trace "who applied this" works.
    """
    try:
        user = getpass.getuser()
    except OSError:
        user = "cambrian"
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
    """Best-effort ``.sql()`` for an expression.

    sqlglot's generator raises ``ValueError`` for AST nodes it can't render —
    we extend the Spark dialect with Iceberg-only expressions (``ADD
    PARTITION FIELD``, ``WRITE ORDERED BY``, etc.) without supplying matching
    generators because the runner never has to round-trip them. The JSON
    output, on the other hand, does need *something* to show the user.
    Fall back to the type name so the audit trail keeps something useful.
    """
    try:
        return stmt.sql()
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


def _checkpoint_tag(evolution_id: str) -> str:
    return f"{CHECKPOINT_TAG_PREFIX}{evolution_id}"


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


def _load_prior_checkpoints(
    catalog: Catalog,
    namespace: str,
    *,
    evolution_id: str,
) -> dict[str, Checkpoint]:
    """Read every per-table checkpoint stashed by the most recent ``rollback`` event.

    Reset writes a rollback event each cycle whose ``pre_*`` fields encode
    the captured-fresh state of each affected table at the start of that
    reset cycle — *the state we want to roll back to* on the next reset.
    The latest ``apply`` event's pre-fields would carry idempotent-mode
    state which doesn't include the checkpoint capture.

    Returns an empty dict if no prior rollback event exists.
    """
    prior = latest_event(catalog, namespace, event_type="rollback", evolution_id=evolution_id)
    if prior is None:
        return {}
    rows = table_states_for_event(catalog, namespace, event_id=prior.event_id)
    out: dict[str, Checkpoint] = {}
    for row in rows:
        cp = _row_to_checkpoint(row)
        if cp is not None:
            out[row.table_ident] = cp
    return out


def _load_post_snapshots(
    catalog: Catalog,
    namespace: str,
    *,
    evolution_id: str,
) -> dict[str, int | None]:
    """Return {ident_str: post_snapshot_id} from the most recent ``apply`` for *evolution_id*.

    Used by external-write detection: compare each table's current main
    snapshot against the *post* state recorded by the prior apply. A
    divergence means someone else wrote between then and now.
    """
    prior = latest_event(catalog, namespace, event_type="apply", evolution_id=evolution_id)
    if prior is None:
        return {}
    rows = table_states_for_event(catalog, namespace, event_id=prior.event_id)
    return {row.table_ident: row.post_snapshot_id for row in rows}


def apply_reset(
    config: CambrianConfig,
    *,
    allow_partial: bool = False,
    force: bool = False,
    actor: str | None = None,
) -> ResetResult:
    """Apply ``current.sql`` in reset mode.

    Reset is the relief valve for non-idempotent SQL — never the default
    path. The flow:

    1. Expand + hash ``current.sql``; short-circuit if the hash matches the
       last reset's apply event AND no external writes are detected.
    2. Discover affected tables.
    3. Load prior checkpoints from the last ``apply`` event for ``current``
       (if any). For tables with no prior checkpoint, capture and pin one
       now so a *future* reset can roll back to this state.
    4. External-write detection: each table's current ``main`` snapshot id
       must match the prior apply's ``post_snapshot_id``. Divergence is an
       :class:`ExternalWriteDetectedError` unless ``force=True``.
    5. Roll back each table to its checkpoint via the M4 four-pointer
       restore. Tables without a prior checkpoint are skipped (nothing to
       roll back to).
    6. Emit a ``rollback`` event capturing what moved.
    7. Re-execute the SQL via :func:`apply_idempotent`. Emit its ``apply``
       event with the new post-state.

    Raises:
        ExternalWriteDetectedError: a table moved out-of-band and ``force``
            is false.
        CambrianError: any of the underlying parse/dispatch/commit errors.
    """
    current_sql_path = _resolve_current_sql(config)
    expanded = expand(current_sql_path)
    catalog = load_catalog(config)
    state = ensure_current(catalog, config.evolutions.sidecar_namespace, allow_read_only=False)
    namespace = state.sidecar_namespace

    statements_raw = sqlglot.parse(expanded.text, dialect=CambrianSpark)
    statements = [s for s in statements_raw if s is not None]
    per_stmt_tables = affected_tables_with_overrides(expanded.text, statements)
    idents = _unique_tables(per_stmt_tables)

    prior_checkpoints = _load_prior_checkpoints(
        catalog, namespace, evolution_id=CURRENT_EVOLUTION_ID
    )
    prior_post_snapshots = _load_post_snapshots(
        catalog, namespace, evolution_id=CURRENT_EVOLUTION_ID
    )

    if not force:
        diverged = _check_external_writes(catalog, idents, prior_post_snapshots)
        if diverged:
            details = ", ".join(diverged)
            raise ExternalWriteDetectedError(
                ref=f"main (tables: {details})",
                expected_snapshot_id=None,
                observed_snapshot_id=None,
            )

    rollbacks: list[TableRollback] = []

    # Phase 1: snapshot the pre-reset state of every affected table. This
    # is what the *next* reset will roll back to — captured before phase 2
    # so it survives the upcoming rollback.
    pre_reset_states: dict[str, Checkpoint] = {}
    tag_name = _checkpoint_tag(CURRENT_EVOLUTION_ID)
    for ident in idents:
        table = _load_or_none(catalog, ident)
        if table is None:
            continue
        cp_pre = capture(table)
        pre_reset_states[str(ident)] = cp_pre
        try:
            pin(table, tag_name=tag_name, snapshot_id=cp_pre.snapshot_id)
        except Exception:
            pass

    # Phase 2: roll back to the prior reset's checkpoint, if any.
    for ident in idents:
        ident_str = str(ident)
        table = _load_or_none(catalog, ident)
        if table is None:
            rollbacks.append(
                TableRollback(
                    ident=ident_str,
                    rolled_back=False,
                    reason="table does not exist yet",
                )
            )
            continue

        cp = prior_checkpoints.get(ident_str)
        pre_snap = table.current_snapshot()
        pre_snap_id = pre_snap.snapshot_id if pre_snap is not None else None

        if cp is None:
            rollbacks.append(
                TableRollback(
                    ident=ident_str,
                    rolled_back=False,
                    from_snapshot_id=pre_snap_id,
                    to_snapshot_id=pre_snap_id,
                    reason="no prior checkpoint; captured one for next reset",
                )
            )
            continue

        restore_pointers(
            table,
            target_snapshot_id=cp.snapshot_id,
            target_schema_id=cp.schema_id,
            target_spec_id=cp.spec_id,
            target_sort_order_id=cp.sort_order_id,
            expected_current_snapshot_id=pre_snap_id,
        )
        rolled_back_table = catalog.load_table(_ident_to_tuple(ident))
        post_snap = rolled_back_table.current_snapshot()
        post_snap_id = post_snap.snapshot_id if post_snap is not None else None
        rollbacks.append(
            TableRollback(
                ident=ident_str,
                rolled_back=True,
                from_snapshot_id=pre_snap_id,
                to_snapshot_id=post_snap_id,
                reason="rolled back to prior checkpoint",
            )
        )

    # Phase 3: re-execute the SQL against the rolled-back tables.
    try:
        apply_result = apply_idempotent(config, allow_partial=allow_partial, actor=actor)
    except CambrianError as err:
        return ResetResult(
            status="partial",
            evolution_hash=expanded.hash,
            sources=expanded.sources,
            rollbacks=rollbacks,
            error=str(err),
        )

    # Phase 4: record the pre-reset states captured in phase 1 as the
    # next reset's rollback target. Tables that didn't exist at reset start
    # (i.e. were created by the apply) do NOT get a row — we can't
    # restore to "doesn't exist" with the 4-pointer primitive. The next
    # reset will treat those as fresh (and capture a checkpoint then).
    rollback_states: list[TableStateRow] = []
    for ident in idents:
        ident_str = str(ident)
        cp = pre_reset_states.get(ident_str)
        if cp is None:
            continue
        rollback_states.append(
            TableStateRow(
                table_ident=ident_str,
                pre_snapshot_id=cp.snapshot_id,
                pre_schema_id=cp.schema_id,
                pre_spec_id=cp.spec_id,
                pre_sort_order_id=cp.sort_order_id,
                pre_metadata_loc=cp.metadata_loc,
                post_snapshot_id=cp.snapshot_id,
                post_schema_id=cp.schema_id,
                post_spec_id=cp.spec_id,
                post_sort_order_id=cp.sort_order_id,
                tag_ref=tag_name if cp.snapshot_id is not None else None,
            )
        )

    rollback_event_id = write_event(
        catalog,
        namespace,
        event_type="rollback",
        evolution_id=CURRENT_EVOLUTION_ID,
        evolution_hash=expanded.hash,
        evolution_sql=expanded.text,
        actor=actor or _default_actor(),
        notes=(
            f"rolled back {sum(1 for r in rollbacks if r.rolled_back)} "
            f"of {len(rollbacks)} tables; captured {len(rollback_states)} "
            "pre-reset checkpoints for next reset"
        ),
        table_states=rollback_states,
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
    config: CambrianConfig,
    *,
    actor: str | None = None,
) -> ResetResult:
    """Roll the affected tables of the last ``apply`` for ``current`` back to their checkpoints.

    Does not re-execute ``current.sql`` afterwards. Used by ``cambrian
    rollback``: useful for "I want to undo the dev work I just did and
    re-edit ``current.sql`` from scratch". Emits a ``rollback`` event only.

    The set of "affected tables" is determined by the last apply event's
    ``table_states`` rows, not by re-parsing ``current.sql``. This matches
    user intent: you're rolling back the *state that was last applied*,
    even if ``current.sql`` has since changed in ways that would now
    touch different tables.
    """
    catalog = load_catalog(config)
    state = ensure_current(catalog, config.evolutions.sidecar_namespace, allow_read_only=False)
    namespace = state.sidecar_namespace

    prior = latest_event(catalog, namespace, event_type="apply", evolution_id=CURRENT_EVOLUTION_ID)
    if prior is None:
        # Nothing has been applied; nothing to roll back. Surface a
        # ResetResult with status=unchanged so callers can render a
        # sensible message.
        return ResetResult(
            status="unchanged",
            evolution_hash="",
            sources=[],
            rollbacks=[],
        )

    rows = table_states_for_event(catalog, namespace, event_id=prior.event_id)
    rollbacks: list[TableRollback] = []
    rollback_states: list[TableStateRow] = []

    for row in rows:
        cp = _row_to_checkpoint(row)
        if cp is None:
            rollbacks.append(
                TableRollback(
                    ident=row.table_ident,
                    rolled_back=False,
                    reason="no prior checkpoint",
                )
            )
            continue
        # Parse "ns.t" back into a tuple for catalog.load_table.
        ident_tuple = _ident_str_to_tuple(row.table_ident)
        try:
            table = catalog.load_table(ident_tuple)
        except NoSuchTableError:
            rollbacks.append(
                TableRollback(
                    ident=row.table_ident,
                    rolled_back=False,
                    reason="table no longer exists",
                )
            )
            continue

        current = table.current_snapshot()
        from_snap = current.snapshot_id if current is not None else None
        restore_pointers(
            table,
            target_snapshot_id=cp.snapshot_id,
            target_schema_id=cp.schema_id,
            target_spec_id=cp.spec_id,
            target_sort_order_id=cp.sort_order_id,
            expected_current_snapshot_id=from_snap,
        )
        rolled = catalog.load_table(ident_tuple)
        post = rolled.current_snapshot()
        rollbacks.append(
            TableRollback(
                ident=row.table_ident,
                rolled_back=True,
                from_snapshot_id=from_snap,
                to_snapshot_id=post.snapshot_id if post is not None else None,
                reason="rolled back to prior checkpoint",
            )
        )
        rollback_states.append(
            TableStateRow(
                table_ident=row.table_ident,
                pre_snapshot_id=from_snap,
                pre_schema_id=rolled.schema().schema_id,
                pre_spec_id=rolled.spec().spec_id,
                pre_sort_order_id=rolled.sort_order().order_id,
                pre_metadata_loc=rolled.metadata_location,
                post_snapshot_id=cp.snapshot_id,
                post_schema_id=cp.schema_id,
                post_spec_id=cp.spec_id,
                post_sort_order_id=cp.sort_order_id,
                tag_ref=cp.tag_ref,
            )
        )

    event_id = write_event(
        catalog,
        namespace,
        event_type="rollback",
        evolution_id=CURRENT_EVOLUTION_ID,
        evolution_hash=prior.evolution_hash,
        evolution_sql=prior.evolution_sql,
        actor=actor or _default_actor(),
        notes=f"manual rollback of {sum(1 for r in rollbacks if r.rolled_back)} tables",
        table_states=rollback_states,
    )

    return ResetResult(
        status="applied",
        evolution_hash=prior.evolution_hash,
        sources=[],
        rollbacks=rollbacks,
        rollback_event_id=event_id,
    )


def _check_external_writes(
    catalog: Catalog,
    idents: list[TableIdent],
    prior_post_snapshots: dict[str, int | None],
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
        if table is None:
            # The prior apply touched this table, but it's gone now —
            # that's a strong "external write" signal (someone DROPped it).
            diverged.append(ident_str)
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
