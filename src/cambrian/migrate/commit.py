"""``commit`` / ``uncommit`` / ``reset --to`` lifecycle commands (M7).

The lifecycle:

* ``commit -m <msg>`` freezes ``current.sql`` as ``committed/<NNNN>_<slug>.sql``,
  pins per-table checkpoints under ``cambrian.committed.<n>.<slug>``, emits a
  ``commit`` event, and truncates ``current.sql``.
* ``uncommit`` pops the latest committed file back to ``current.sql``, rolls
  the affected tables back to the pinned checkpoint, emits an ``uncommit``
  event. Refuses if there are downstream committed files (gap) or if
  ``current.sql`` is non-empty (``--force`` overrides).
* ``reset --to <evolution_id>`` rolls the affected tables of *evolution_id*
  back to that commit's pinned checkpoint. Escape hatch only — does NOT
  delete the committed file, does NOT touch downstream commit events.

The committed-file replay (so ``cambrian apply`` honours committed/ for a
fresh deploy or a ``cambrian sync``) lives in :mod:`cambrian.migrate.runner`
to avoid a circular import on the apply path.
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

import sqlglot

from cambrian.catalog import load_catalog
from cambrian.errors import (
    CambrianError,
    EvolutionNotFoundError,
    IllegalStateError,
)
from cambrian.iceberg.affected import (
    TableIdent,
    affected_tables_with_overrides,
)
from cambrian.iceberg.checkpoint import capture, pin
from cambrian.iceberg.txn import restore_pointers
from cambrian.sidecar.events import (
    TableStateRow,
    latest_event,
    table_states_for_event,
    write_event,
)
from cambrian.sidecar.selfmigrate import ensure_current
from cambrian.sql.dialect import CambrianSpark
from cambrian.sql.include import expand

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

    from cambrian.config import CambrianConfig

__all__ = [
    "COMMITTED_TAG_PREFIX",
    "CommitResult",
    "CommittedFile",
    "ResetToResult",
    "UncommitResult",
    "cambrian_commit",
    "cambrian_reset_to",
    "cambrian_uncommit",
    "compute_evolution_hash",
    "discover_committed_files",
    "next_sequence_number",
    "parse_committed_filename",
    "slugify",
]


COMMITTED_TAG_PREFIX = "cambrian.committed."
SLUG_MAX_LENGTH = 50
_COMMITTED_FILENAME_RE = re.compile(r"^(\d{4})_([a-z0-9-]+)\.sql$")
_SLUG_REPLACE_RE = re.compile(r"[^a-z0-9]+")
_SLUG_TRIM_RE = re.compile(r"^-+|-+$")


@dataclass(frozen=True, slots=True)
class CommittedFile:
    """One file under ``committed/`` decoded into its parts."""

    number: int
    slug: str
    path: Path

    @property
    def evolution_id(self) -> str:
        return f"{self.number:04d}_{self.slug}"

    def tag_ref(self) -> str:
        return f"{COMMITTED_TAG_PREFIX}{self.number}.{self.slug}"


@dataclass
class CommitResult:
    """Outcome of a ``cambrian commit``."""

    evolution_id: str
    committed_path: Path
    evolution_hash: str
    event_id: str | None
    affected_tables: list[str] = field(default_factory=list)
    tag_ref: str = ""


@dataclass
class UncommitResult:
    """Outcome of a ``cambrian uncommit``."""

    evolution_id: str
    restored_path: Path
    event_id: str | None
    rolled_back_tables: list[str] = field(default_factory=list)
    skipped_tables: list[str] = field(default_factory=list)


@dataclass
class ResetToResult:
    """Outcome of ``cambrian reset --to <evolution_id>``."""

    evolution_id: str
    event_id: str | None
    rolled_back_tables: list[str] = field(default_factory=list)
    skipped_tables: list[str] = field(default_factory=list)


def slugify(msg: str) -> str:
    """Lowercase ASCII-safe dash-joined slug; collapses runs, trims, length-capped.

    Empty / all-non-alphanumeric input collapses to ``"evolution"`` rather
    than to the empty string — a missing slug would yield a malformed filename.
    """
    normalized = unicodedata.normalize("NFKD", msg)
    ascii_only = normalized.encode("ascii", "ignore").decode("ascii")
    lowered = ascii_only.lower()
    dashed = _SLUG_REPLACE_RE.sub("-", lowered)
    trimmed = _SLUG_TRIM_RE.sub("", dashed)
    if not trimmed:
        return "evolution"
    if len(trimmed) > SLUG_MAX_LENGTH:
        # Trim from the right then strip a trailing dash that the cut may have left.
        trimmed = _SLUG_TRIM_RE.sub("", trimmed[:SLUG_MAX_LENGTH])
        if not trimmed:
            return "evolution"
    return trimmed


def parse_committed_filename(name: str) -> tuple[int, str] | None:
    """Return ``(number, slug)`` if *name* matches ``NNNN_<slug>.sql``, else ``None``."""
    match = _COMMITTED_FILENAME_RE.match(name)
    if match is None:
        return None
    return int(match.group(1)), match.group(2)


def discover_committed_files(committed_dir: Path) -> list[CommittedFile]:
    """Return every well-formed file under *committed_dir*, ordered by ``number``.

    Files whose names don't match the ``NNNN_<slug>.sql`` shape are ignored
    (silently — they're probably ``.gitkeep`` or editor backups). Returns an
    empty list if the directory doesn't exist.
    """
    if not committed_dir.is_dir():
        return []
    files: list[CommittedFile] = []
    for entry in committed_dir.iterdir():
        if not entry.is_file():
            continue
        parsed = parse_committed_filename(entry.name)
        if parsed is None:
            continue
        number, slug = parsed
        files.append(CommittedFile(number=number, slug=slug, path=entry.resolve()))
    files.sort(key=lambda f: f.number)
    return files


def next_sequence_number(committed_dir: Path) -> int:
    """Return ``max(<n>) + 1`` over existing committed files, or 1 for an empty dir.

    Refuses (raises :class:`IllegalStateError`) if the numbering has gaps —
    a gap signals manual surgery or a corrupted checkout; the safe response
    is to surface it loudly rather than silently allocate around it.
    """
    files = discover_committed_files(committed_dir)
    if not files:
        return 1
    numbers = [f.number for f in files]
    expected = list(range(1, len(numbers) + 1))
    if numbers != expected:
        missing = sorted(set(expected) - set(numbers))
        raise IllegalStateError(
            f"committed/ numbering has gap(s) at {missing}; "
            "refusing to allocate around them. Restore the missing file(s) "
            "from git history or run `cambrian sync` to rehydrate from the catalog."
        )
    return numbers[-1] + 1


def compute_evolution_hash(text: str) -> str:
    """sha256 hex of *text*, encoded as UTF-8. The cross-cambrian canonical hash."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def cambrian_commit(
    config: CambrianConfig,
    *,
    message: str,
    actor: str | None = None,
) -> CommitResult:
    """Freeze the current state into a committed evolution.

    Preconditions:

    * ``current.sql`` exists and is non-empty.
    * Applying ``current.sql`` is a no-op (idempotent state is clean). The
      caller is expected to have run ``cambrian apply`` already — we don't
      re-run it here because that would silently mutate state on commit.

    Side effects (in order):

    1. Compute the next sequence number and slug.
    2. Rename ``current.sql`` → ``committed/<NNNN>_<slug>.sql``; create an
       empty ``current.sql`` in its place.
    3. Capture + pin a checkpoint on every affected table under tag
       ``cambrian.committed.<n>.<slug>``.
    4. Emit a ``commit`` event referencing every checkpoint.

    Raises:
        EvolutionNotFoundError: ``current.sql`` doesn't exist.
        IllegalStateError: ``current.sql`` is empty, or the most recent apply
            event's hash doesn't match the file (commit on dirty state).
    """
    from cambrian.migrate.runner import CURRENT_EVOLUTION_ID

    if not message.strip():
        raise IllegalStateError("commit message must be non-empty")

    evolutions_dir = Path(config.evolutions.dir).resolve()
    current_path = evolutions_dir / "current.sql"
    if not current_path.exists():
        raise EvolutionNotFoundError(
            f"current.sql not found at {current_path} "
            f"(config: evolutions.dir = {config.evolutions.dir})"
        )

    expanded = expand(current_path)
    if not expanded.text.strip():
        raise IllegalStateError(
            "current.sql is empty; nothing to commit. Write the evolution first, "
            "run `cambrian apply` to verify it works, then commit."
        )

    catalog = load_catalog(config)
    state = ensure_current(
        catalog,
        config.evolutions.sidecar_namespace,
        allow_read_only=False,
    )
    namespace = state.sidecar_namespace

    last_apply = latest_event(
        catalog, namespace, event_type="apply", evolution_id=CURRENT_EVOLUTION_ID
    )
    if last_apply is None or last_apply.evolution_hash != expanded.hash:
        raise IllegalStateError(
            "current.sql has not been applied (or has been edited since the last apply). "
            "Run `cambrian apply` first; commit is only safe on a clean idempotent state."
        )

    committed_dir = evolutions_dir / "committed"
    committed_dir.mkdir(parents=True, exist_ok=True)
    number = next_sequence_number(committed_dir)
    slug = slugify(message)
    new_file = CommittedFile(
        number=number, slug=slug, path=committed_dir / f"{number:04d}_{slug}.sql"
    )

    # Compute affected tables BEFORE moving the file so any parse error
    # leaves the working tree untouched.
    statements_raw = sqlglot.parse(expanded.text, dialect=CambrianSpark)
    statements = [s for s in statements_raw if s is not None]
    per_stmt_tables = affected_tables_with_overrides(expanded.text, statements)
    idents = _unique_tables(per_stmt_tables)

    # Pull the pre-apply state from the most recent apply event for current —
    # that's what an `uncommit` rolls back to. The post-apply state (live now)
    # is what we pin under the tag.
    prior_apply_states = {
        row.table_ident: row
        for row in table_states_for_event(catalog, namespace, event_id=last_apply.event_id)
    }

    # 1. Capture + pin checkpoints before moving the file. If pinning fails
    # we want the working tree intact so the user can retry without manually
    # restoring current.sql.
    tag_name = new_file.tag_ref()
    table_states: list[TableStateRow] = []
    for ident in idents:
        ident_tuple = _ident_to_tuple(ident)
        try:
            table = catalog.load_table(ident_tuple)
        except Exception:
            table_states.append(
                TableStateRow(
                    table_ident=str(ident),
                    tag_ref=None,
                )
            )
            continue
        cp = capture(table)
        pin(table, tag_name=tag_name, snapshot_id=cp.snapshot_id)
        prior = prior_apply_states.get(str(ident))
        # pre_* mirrors the apply event's pre_* (rollback target on uncommit);
        # post_* is the freshly captured state (the tag's snapshot).
        table_states.append(
            TableStateRow(
                table_ident=str(ident),
                pre_snapshot_id=prior.pre_snapshot_id if prior is not None else None,
                pre_schema_id=prior.pre_schema_id if prior is not None else None,
                pre_spec_id=prior.pre_spec_id if prior is not None else None,
                pre_sort_order_id=prior.pre_sort_order_id if prior is not None else None,
                pre_metadata_loc=prior.pre_metadata_loc if prior is not None else None,
                post_snapshot_id=cp.snapshot_id,
                post_schema_id=cp.schema_id,
                post_spec_id=cp.spec_id,
                post_sort_order_id=cp.sort_order_id,
                tag_ref=tag_name if cp.snapshot_id is not None else None,
            )
        )

    # 2. Move current.sql → committed/<NNNN>_<slug>.sql and truncate current.
    new_file.path.write_text(expanded.text, encoding="utf-8")
    current_path.write_text("", encoding="utf-8")

    # 3. Emit the commit event.
    from cambrian.migrate.runner import _default_actor as _make_actor

    event_id = write_event(
        catalog,
        namespace,
        event_type="commit",
        evolution_id=new_file.evolution_id,
        evolution_hash=expanded.hash,
        evolution_sql=expanded.text,
        actor=actor or _make_actor(),
        notes=f"committed {len(idents)} table(s); tag={tag_name}",
        table_states=table_states,
    )

    return CommitResult(
        evolution_id=new_file.evolution_id,
        committed_path=new_file.path,
        evolution_hash=expanded.hash,
        event_id=event_id,
        affected_tables=[str(i) for i in idents],
        tag_ref=tag_name,
    )


def cambrian_uncommit(
    config: CambrianConfig,
    *,
    force: bool = False,
    actor: str | None = None,
) -> UncommitResult:
    """Pop the latest committed evolution back to ``current.sql`` and rollback.

    Preconditions:

    * At least one committed file exists.
    * No downstream committed files (i.e. the popped file must be the
      latest — gaps shouldn't exist, but we defend).
    * ``current.sql`` is empty unless *force* is true.

    Side effects:

    1. Read ``committed/<NNNN>_<slug>.sql`` (highest number).
    2. Write its content to ``current.sql`` (clobbering if *force*).
    3. Roll every affected table back to the checkpoint pinned at the
       commit event's tag.
    4. Delete the committed file.
    5. Emit an ``uncommit`` event.

    Raises:
        IllegalStateError: no committed files; downstream files exist;
            ``current.sql`` is non-empty without *force*.
    """
    evolutions_dir = Path(config.evolutions.dir).resolve()
    committed_dir = evolutions_dir / "committed"
    files = discover_committed_files(committed_dir)
    if not files:
        raise IllegalStateError(
            "no committed evolutions to uncommit. The committed/ directory is empty."
        )

    latest_file = files[-1]
    numbers = [f.number for f in files]
    if numbers != list(range(1, len(numbers) + 1)):
        raise IllegalStateError(
            f"committed/ numbering has gap(s); refusing to uncommit. Found numbers {numbers}."
        )

    current_path = evolutions_dir / "current.sql"
    current_existing = current_path.read_text(encoding="utf-8") if current_path.exists() else ""
    if current_existing.strip() and not force:
        raise IllegalStateError(
            "current.sql is non-empty; uncommit would clobber unsaved work. "
            "Re-run with --force to overwrite, or save the current SQL elsewhere first."
        )

    catalog = load_catalog(config)
    state = ensure_current(
        catalog,
        config.evolutions.sidecar_namespace,
        allow_read_only=False,
    )
    namespace = state.sidecar_namespace

    commit_event = latest_event(
        catalog, namespace, event_type="commit", evolution_id=latest_file.evolution_id
    )
    if commit_event is None:
        raise IllegalStateError(
            f"committed file {latest_file.path.name} has no matching commit event in the "
            "catalog. Run `cambrian sync` to reconcile, or remove the file manually if it "
            "was added by hand."
        )

    committed_text = latest_file.path.read_text(encoding="utf-8")
    current_path.write_text(committed_text, encoding="utf-8")

    rows = table_states_for_event(catalog, namespace, event_id=commit_event.event_id)
    rolled_back: list[str] = []
    skipped: list[str] = []
    rollback_states: list[TableStateRow] = []
    for row in rows:
        outcome = _rollback_to_row(catalog, row)
        if outcome is None:
            skipped.append(row.table_ident)
            rollback_states.append(
                TableStateRow(
                    table_ident=row.table_ident,
                    tag_ref=row.tag_ref,
                )
            )
            continue
        rolled_back.append(row.table_ident)
        rollback_states.append(outcome)

    latest_file.path.unlink()

    from cambrian.migrate.runner import _default_actor as _make_actor

    event_id = write_event(
        catalog,
        namespace,
        event_type="uncommit",
        evolution_id=latest_file.evolution_id,
        evolution_hash=commit_event.evolution_hash,
        evolution_sql=committed_text,
        actor=actor or _make_actor(),
        notes=(
            f"uncommitted {latest_file.evolution_id}; "
            f"rolled back {len(rolled_back)} of {len(rows)} tables"
        ),
        table_states=rollback_states,
    )

    return UncommitResult(
        evolution_id=latest_file.evolution_id,
        restored_path=current_path,
        event_id=event_id,
        rolled_back_tables=rolled_back,
        skipped_tables=skipped,
    )


def cambrian_reset_to(
    config: CambrianConfig,
    *,
    evolution_id: str,
    actor: str | None = None,
) -> ResetToResult:
    """Roll affected tables back to the checkpoint pinned at *evolution_id*.

    Escape hatch — for incident response only. Does NOT delete the
    committed file. Does NOT touch downstream commit events. Emits a
    ``rollback`` event so the audit trail records the operation.
    """
    catalog = load_catalog(config)
    state = ensure_current(
        catalog,
        config.evolutions.sidecar_namespace,
        allow_read_only=False,
    )
    namespace = state.sidecar_namespace

    commit_event = latest_event(catalog, namespace, event_type="commit", evolution_id=evolution_id)
    if commit_event is None:
        raise IllegalStateError(
            f"no commit event found for evolution_id={evolution_id!r}. "
            "Check `cambrian status` for the valid set."
        )

    rows = table_states_for_event(catalog, namespace, event_id=commit_event.event_id)
    rolled_back: list[str] = []
    skipped: list[str] = []
    rollback_states: list[TableStateRow] = []
    for row in rows:
        # reset --to restores to the *post-state* of the named commit (the
        # state captured + tag-pinned at commit time), not its pre-state
        # (which is the previous evolution's resting state).
        target = TableStateRow(
            table_ident=row.table_ident,
            pre_snapshot_id=row.post_snapshot_id,
            pre_schema_id=row.post_schema_id,
            pre_spec_id=row.post_spec_id,
            pre_sort_order_id=row.post_sort_order_id,
            pre_metadata_loc=row.tag_ref,
            tag_ref=row.tag_ref,
        )
        outcome = _rollback_to_row(catalog, target)
        if outcome is None:
            skipped.append(row.table_ident)
            rollback_states.append(TableStateRow(table_ident=row.table_ident, tag_ref=row.tag_ref))
            continue
        rolled_back.append(row.table_ident)
        rollback_states.append(outcome)

    from cambrian.migrate.runner import _default_actor as _make_actor

    event_id = write_event(
        catalog,
        namespace,
        event_type="rollback",
        evolution_id=evolution_id,
        evolution_hash=commit_event.evolution_hash,
        evolution_sql=commit_event.evolution_sql,
        actor=actor or _make_actor(),
        notes=(
            f"reset --to {evolution_id}; rolled back {len(rolled_back)} of {len(rows)} tables. "
            "Audit trail preserved; downstream commit events are untouched."
        ),
        table_states=rollback_states,
    )

    return ResetToResult(
        evolution_id=evolution_id,
        event_id=event_id,
        rolled_back_tables=rolled_back,
        skipped_tables=skipped,
    )


def _rollback_to_row(catalog: Catalog, row: TableStateRow) -> TableStateRow | None:
    """Roll table at *row.table_ident* back to *row*'s pre-state. Returns the post-state row.

    Returns ``None`` if the checkpoint can't be restored (no pre-state recorded,
    or the table no longer exists). The caller decides whether that's an error.
    """
    if row.pre_schema_id is None or row.pre_spec_id is None or row.pre_sort_order_id is None:
        return None
    ident_tuple = _ident_str_to_tuple(row.table_ident)
    try:
        table = catalog.load_table(ident_tuple)
    except Exception:
        return None
    current = table.current_snapshot()
    from_snap = current.snapshot_id if current is not None else None
    try:
        restore_pointers(
            table,
            target_snapshot_id=row.pre_snapshot_id,
            target_schema_id=row.pre_schema_id,
            target_spec_id=row.pre_spec_id,
            target_sort_order_id=row.pre_sort_order_id,
            expected_current_snapshot_id=from_snap,
        )
    except CambrianError:
        # Surface the underlying error to the caller via re-raise after
        # writing the rollback event would be ideal, but we don't have the
        # buffer yet. For now: propagate so the user sees the failure.
        raise
    rolled = catalog.load_table(ident_tuple)
    post = rolled.current_snapshot()
    return TableStateRow(
        table_ident=row.table_ident,
        pre_snapshot_id=from_snap,
        pre_schema_id=rolled.schema().schema_id,
        pre_spec_id=rolled.spec().spec_id,
        pre_sort_order_id=rolled.sort_order().order_id,
        pre_metadata_loc=rolled.metadata_location,
        post_snapshot_id=post.snapshot_id if post is not None else row.pre_snapshot_id,
        post_schema_id=row.pre_schema_id,
        post_spec_id=row.pre_spec_id,
        post_sort_order_id=row.pre_sort_order_id,
        tag_ref=row.tag_ref,
    )


def _unique_tables(per_stmt: list[list[TableIdent]]) -> list[TableIdent]:
    seen: dict[str, TableIdent] = {}
    for tables in per_stmt:
        for t in tables:
            seen.setdefault(str(t), t)
    return list(seen.values())


def _ident_to_tuple(ident: TableIdent) -> tuple[str, ...]:
    if ident.namespace:
        return (*ident.namespace.split("."), ident.name)
    return (ident.name,)


def _ident_str_to_tuple(ident: str) -> tuple[str, ...]:
    if "." in ident:
        ns, name = ident.rsplit(".", 1)
        return (*ns.split("."), name)
    return (ident,)
