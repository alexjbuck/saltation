"""``cambrian sync`` — rehydrate local ``committed/`` files from the catalog.

The catalog is the source of truth for what was committed and applied to
production. A fresh checkout, or a teammate onboarding to an established
project, runs ``cambrian sync`` to mirror the live committed-evolution set
into their local ``committed/`` directory.

Design notes:

* **Read-only**: ``cambrian sync`` does not append to the events log.
  Sync rehydrates local files from existing catalog state; the catalog
  state itself is unchanged, so the audit trail has nothing new to record.
  An alternative would emit a synthetic ``sync`` event for audit ("who
  pulled which evolutions to disk, when") but that conflates filesystem
  bookkeeping with catalog state. The locked sidecar event types
  (``apply``/``rollback``/``commit``/``uncommit``/``checkpoint``) all
  record *catalog* mutations; ``sync`` doesn't fit.

* **Conflict policy**: idempotent semantics carry over. Missing files
  are written. Files whose content already matches the catalog hash are
  skipped (re-running sync is a no-op). Files whose content differs are
  refused unless ``--force``; ``--diff`` surfaces the unified diff.

* **Internal-consistency guard**: every commit row must satisfy
  ``sha256(evolution_sql) == evolution_hash``. A mismatch means the
  catalog row is malformed; we refuse so the user investigates instead
  of overwriting good local files with bad catalog data.

Locked decisions deferred to v1.x or later: conflict-resolution UI,
three-way merge, partial sync, cross-catalog sync. See CLAUDE.md.
"""

from __future__ import annotations

import difflib
import hashlib
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Literal

from cambrian.catalog import load_catalog
from cambrian.errors import IllegalStateError
from cambrian.sidecar.events import CommittedPayload, committed_payloads
from cambrian.sidecar.selfmigrate import ensure_current

if TYPE_CHECKING:
    from cambrian.config import CambrianConfig

__all__ = [
    "SyncFileResult",
    "SyncResult",
    "SyncStatus",
    "cambrian_sync",
]


SyncStatus = Literal["written", "overwritten", "skipped", "refused", "discrepancy"]


@dataclass
class SyncFileResult:
    """One committed evolution's outcome during a sync."""

    evolution_id: str
    path: Path
    status: SyncStatus
    catalog_hash: str
    local_hash: str | None = None
    diff: str | None = None
    note: str | None = None


@dataclass
class SyncResult:
    """Aggregate outcome of ``cambrian sync``."""

    files: list[SyncFileResult] = field(default_factory=list)
    dry_run: bool = False

    @property
    def written(self) -> int:
        return sum(1 for f in self.files if f.status == "written")

    @property
    def overwritten(self) -> int:
        return sum(1 for f in self.files if f.status == "overwritten")

    @property
    def skipped(self) -> int:
        return sum(1 for f in self.files if f.status == "skipped")

    @property
    def refused(self) -> int:
        return sum(1 for f in self.files if f.status == "refused")

    @property
    def discrepancies(self) -> int:
        return sum(1 for f in self.files if f.status == "discrepancy")

    @property
    def has_refusals(self) -> bool:
        return self.refused > 0 or self.discrepancies > 0


def cambrian_sync(
    config: CambrianConfig,
    *,
    force: bool = False,
    dry_run: bool = False,
    diff: bool = False,
    actor: str | None = None,
) -> SyncResult:
    """Rehydrate ``committed/`` from live ``commit`` events in the catalog.

    Args:
        config: Loaded cambrian config.
        force: Overwrite local files that conflict with catalog content.
        dry_run: Plan only; write nothing to disk. ``--diff`` implies dry-run
            unless ``--force`` is also set.
        diff: Include a unified diff in every conflict result.
        actor: Unused — sync emits no events. Accepted for symmetry with
            other commands so a future audit-event policy change is a no-op
            for callers.

    Returns:
        :class:`SyncResult` with one entry per commit event in the catalog.

    Raises:
        IllegalStateError: a catalog row is internally inconsistent
            (recorded ``evolution_hash`` != sha256(``evolution_sql``)).
    """
    del actor  # explicit no-op; see module docstring

    effective_dry_run = dry_run or (diff and not force)

    catalog = load_catalog(config)
    state = ensure_current(
        catalog,
        config.evolutions.sidecar_namespace,
        allow_read_only=True,
    )
    namespace = state.sidecar_namespace

    payloads = committed_payloads(catalog, namespace)
    evolutions_dir = Path(config.evolutions.dir).resolve()
    committed_dir = evolutions_dir / "committed"

    result = SyncResult(dry_run=effective_dry_run)

    for payload in payloads:
        _verify_catalog_consistency(payload)
        result.files.append(
            _sync_one(
                payload,
                committed_dir=committed_dir,
                force=force,
                dry_run=effective_dry_run,
                include_diff=diff,
            )
        )

    return result


def _verify_catalog_consistency(payload: CommittedPayload) -> None:
    """Fail loudly if ``sha256(evolution_sql) != evolution_hash``.

    Defensive guard against a malformed events row. If this trips the
    catalog itself is the problem — refuse and let the user investigate
    rather than overwriting a good local file with corrupt data.
    """
    computed = hashlib.sha256(payload.evolution_sql.encode("utf-8")).hexdigest()
    if computed != payload.evolution_hash:
        raise IllegalStateError(
            f"catalog row for {payload.evolution_id} is internally inconsistent: "
            f"recorded hash {payload.evolution_hash[:12]}… does not match "
            f"sha256(evolution_sql) = {computed[:12]}…. The events table appears "
            "corrupt; sync refuses to write potentially-bad SQL to disk. "
            "Investigate the sidecar (event_id "
            f"{payload.event_id}) before retrying."
        )


def _sync_one(
    payload: CommittedPayload,
    *,
    committed_dir: Path,
    force: bool,
    dry_run: bool,
    include_diff: bool,
) -> SyncFileResult:
    target = committed_dir / f"{payload.evolution_id}.sql"

    if not target.exists():
        if not dry_run:
            committed_dir.mkdir(parents=True, exist_ok=True)
            target.write_text(payload.evolution_sql, encoding="utf-8")
        return SyncFileResult(
            evolution_id=payload.evolution_id,
            path=target,
            status="written",
            catalog_hash=payload.evolution_hash,
            note="would write" if dry_run else None,
        )

    local_text = target.read_text(encoding="utf-8")
    local_hash = hashlib.sha256(local_text.encode("utf-8")).hexdigest()

    if local_hash == payload.evolution_hash:
        return SyncFileResult(
            evolution_id=payload.evolution_id,
            path=target,
            status="skipped",
            catalog_hash=payload.evolution_hash,
            local_hash=local_hash,
        )

    diff_text = (
        _unified_diff(local_text, payload.evolution_sql, payload.evolution_id)
        if include_diff
        else None
    )

    if force:
        if not dry_run:
            target.write_text(payload.evolution_sql, encoding="utf-8")
        return SyncFileResult(
            evolution_id=payload.evolution_id,
            path=target,
            status="overwritten",
            catalog_hash=payload.evolution_hash,
            local_hash=local_hash,
            diff=diff_text,
            note="would overwrite" if dry_run else None,
        )

    return SyncFileResult(
        evolution_id=payload.evolution_id,
        path=target,
        status="refused",
        catalog_hash=payload.evolution_hash,
        local_hash=local_hash,
        diff=diff_text,
        note=(
            f"local file differs from catalog (local hash {local_hash[:12]}…, "
            f"catalog hash {payload.evolution_hash[:12]}…); re-run with --force "
            "to overwrite or --diff to inspect."
        ),
    )


def _unified_diff(local_text: str, catalog_text: str, evolution_id: str) -> str:
    return "".join(
        difflib.unified_diff(
            local_text.splitlines(keepends=True),
            catalog_text.splitlines(keepends=True),
            fromfile=f"local/{evolution_id}.sql",
            tofile=f"catalog/{evolution_id}.sql",
            lineterm="",
        )
    )
