"""Append-only writers + readers for the sidecar event log.

Atomicity note: PyIceberg's ``Transaction`` API is scoped to a single table.
Cambrian's event-with-table-states pairing therefore spans two commits. We
sequence them deliberately:

1. append the per-table rows to ``<ns>.table_states`` first;
2. append the event row to ``<ns>.events`` second.

If we crash after (1) and before (2), the table_states rows are orphans:
they reference an ``event_id`` that has no matching event. Every reader
joins through ``events.event_id`` (the source of truth), so an orphan is
*invisible* on read and the next ``cambrian`` invocation can append a
fresh event without inheriting the partial write. The inverse ordering
would be unsafe: an event with no recorded table-state would be
unrecoverable for rollback.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Literal

import pyarrow as pa

from cambrian.sidecar.schema import EVENTS_TABLE, TABLE_STATES_TABLE

if TYPE_CHECKING:
    from pyiceberg.catalog import Catalog

__all__ = [
    "CommittedEvolution",
    "CommittedPayload",
    "Event",
    "EventType",
    "TableStateRow",
    "applied_committed_ids",
    "committed_evolutions",
    "committed_payloads",
    "latest_event",
    "table_states_for_event",
    "write_event",
]

EventType = Literal["apply", "rollback", "commit", "uncommit", "checkpoint"]


@dataclass(frozen=True)
class TableStateRow:
    """Per-table pointer-tuple snapshot tied to an event by ``event_id``."""

    table_ident: str
    pre_snapshot_id: int | None = None
    pre_schema_id: int | None = None
    pre_spec_id: int | None = None
    pre_sort_order_id: int | None = None
    pre_metadata_loc: str | None = None
    post_snapshot_id: int | None = None
    post_schema_id: int | None = None
    post_spec_id: int | None = None
    post_sort_order_id: int | None = None
    tag_ref: str | None = None


@dataclass(frozen=True)
class Event:
    """A decoded row from ``<ns>.events``."""

    event_id: str
    event_ts: datetime
    event_type: str
    evolution_id: str
    evolution_hash: str
    evolution_sql: str
    actor: str
    notes: str | None


@dataclass(frozen=True)
class CommittedEvolution:
    """Lightweight view of a ``commit`` event for status / sync."""

    evolution_id: str
    event_id: str
    event_ts: datetime


@dataclass(frozen=True)
class CommittedPayload:
    """A ``commit`` event with its full SQL payload, for ``cambrian sync``.

    Distinct from :class:`CommittedEvolution` because sync needs the
    ``evolution_sql`` and ``evolution_hash`` columns; the lightweight view
    used by ``cambrian status`` doesn't.
    """

    evolution_id: str
    event_id: str
    event_ts: datetime
    evolution_sql: str
    evolution_hash: str


# ---------------------------------------------------------------------------
# PyArrow schemas (derived from the Iceberg schemas in sidecar.schema).
# Pinned here for clarity; PyIceberg accepts either explicit pa.schema or
# inference from the destination table's schema, but explicit is safer
# because nullability mismatches otherwise produce silent type coercions.
# ---------------------------------------------------------------------------

_EVENTS_PA_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("event_type", pa.string(), nullable=False),
        pa.field("evolution_id", pa.string(), nullable=False),
        pa.field("evolution_hash", pa.string(), nullable=False),
        pa.field("evolution_sql", pa.string(), nullable=False),
        pa.field("actor", pa.string(), nullable=False),
        pa.field("notes", pa.string(), nullable=True),
    ]
)

_TABLE_STATES_PA_SCHEMA = pa.schema(
    [
        pa.field("event_id", pa.string(), nullable=False),
        pa.field("event_ts", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("table_ident", pa.string(), nullable=False),
        pa.field("pre_snapshot_id", pa.int64(), nullable=True),
        pa.field("pre_schema_id", pa.int64(), nullable=True),
        pa.field("pre_spec_id", pa.int64(), nullable=True),
        pa.field("pre_sort_order_id", pa.int64(), nullable=True),
        pa.field("pre_metadata_loc", pa.string(), nullable=True),
        pa.field("post_snapshot_id", pa.int64(), nullable=True),
        pa.field("post_schema_id", pa.int64(), nullable=True),
        pa.field("post_spec_id", pa.int64(), nullable=True),
        pa.field("post_sort_order_id", pa.int64(), nullable=True),
        pa.field("tag_ref", pa.string(), nullable=True),
    ]
)


# ---------------------------------------------------------------------------
# Writers
# ---------------------------------------------------------------------------


@dataclass
class _PendingEvent:
    """Buffered event payload assembled before any I/O happens."""

    event_id: str
    event_ts: datetime
    event_type: EventType
    evolution_id: str
    evolution_hash: str
    evolution_sql: str
    actor: str
    notes: str | None
    table_states: Sequence[TableStateRow] = field(default_factory=tuple)


def write_event(
    catalog: Catalog,
    namespace: str,
    *,
    event_type: EventType,
    evolution_id: str,
    evolution_hash: str,
    evolution_sql: str,
    actor: str,
    notes: str | None = None,
    table_states: Sequence[TableStateRow] = (),
    event_ts: datetime | None = None,
) -> str:
    """Append one event (and any attached table-state rows) to the sidecar.

    Returns the freshly-minted ``event_id`` (uuid4) so callers can correlate
    follow-up reads.

    See module docstring for the table_states-then-event ordering rationale.
    """
    event = _PendingEvent(
        event_id=str(uuid.uuid4()),
        event_ts=event_ts or datetime.now(UTC),
        event_type=event_type,
        evolution_id=evolution_id,
        evolution_hash=evolution_hash,
        evolution_sql=evolution_sql,
        actor=actor,
        notes=notes,
        table_states=tuple(table_states),
    )

    if event.table_states:
        ts_arrow = _table_states_to_arrow(event)
        catalog.load_table((namespace, TABLE_STATES_TABLE)).append(ts_arrow)

    catalog.load_table((namespace, EVENTS_TABLE)).append(_event_to_arrow(event))
    return event.event_id


def _event_to_arrow(event: _PendingEvent) -> pa.Table:
    return pa.table(
        {
            "event_id": pa.array([event.event_id], type=pa.string()),
            "event_ts": pa.array([event.event_ts], type=pa.timestamp("us", tz="UTC")),
            "event_type": pa.array([event.event_type], type=pa.string()),
            "evolution_id": pa.array([event.evolution_id], type=pa.string()),
            "evolution_hash": pa.array([event.evolution_hash], type=pa.string()),
            "evolution_sql": pa.array([event.evolution_sql], type=pa.string()),
            "actor": pa.array([event.actor], type=pa.string()),
            "notes": pa.array([event.notes], type=pa.string()),
        },
        schema=_EVENTS_PA_SCHEMA,
    )


def _table_states_to_arrow(event: _PendingEvent) -> pa.Table:
    rows = event.table_states
    n = len(rows)
    return pa.table(
        {
            "event_id": pa.array([event.event_id] * n, type=pa.string()),
            "event_ts": pa.array([event.event_ts] * n, type=pa.timestamp("us", tz="UTC")),
            "table_ident": pa.array([r.table_ident for r in rows], type=pa.string()),
            "pre_snapshot_id": pa.array([r.pre_snapshot_id for r in rows], type=pa.int64()),
            "pre_schema_id": pa.array([r.pre_schema_id for r in rows], type=pa.int64()),
            "pre_spec_id": pa.array([r.pre_spec_id for r in rows], type=pa.int64()),
            "pre_sort_order_id": pa.array([r.pre_sort_order_id for r in rows], type=pa.int64()),
            "pre_metadata_loc": pa.array([r.pre_metadata_loc for r in rows], type=pa.string()),
            "post_snapshot_id": pa.array([r.post_snapshot_id for r in rows], type=pa.int64()),
            "post_schema_id": pa.array([r.post_schema_id for r in rows], type=pa.int64()),
            "post_spec_id": pa.array([r.post_spec_id for r in rows], type=pa.int64()),
            "post_sort_order_id": pa.array([r.post_sort_order_id for r in rows], type=pa.int64()),
            "tag_ref": pa.array([r.tag_ref for r in rows], type=pa.string()),
        },
        schema=_TABLE_STATES_PA_SCHEMA,
    )


# ---------------------------------------------------------------------------
# Readers
# ---------------------------------------------------------------------------


def _scan_events(catalog: Catalog, namespace: str) -> pa.Table:
    return catalog.load_table((namespace, EVENTS_TABLE)).scan().to_arrow()


def latest_event(
    catalog: Catalog,
    namespace: str,
    *,
    event_type: EventType | None = None,
    evolution_id: str | None = None,
) -> Event | None:
    """Return the most recent event matching the optional filters, or ``None``.

    Filters are applied in Python after a full scan — fine for the volumes
    this log accumulates (one row per evolution action). Status / sync code
    needs the most-recent ``apply`` for ``evolution_id="current"`` to know
    what's "in flight" in dev mode.
    """
    arrow = _scan_events(catalog, namespace)
    if arrow.num_rows == 0:
        return None

    rows = arrow.to_pylist()
    if event_type is not None:
        rows = [r for r in rows if r["event_type"] == event_type]
    if evolution_id is not None:
        rows = [r for r in rows if r["evolution_id"] == evolution_id]
    if not rows:
        return None

    latest = max(rows, key=lambda r: r["event_ts"])
    return Event(
        event_id=latest["event_id"],
        event_ts=latest["event_ts"],
        event_type=latest["event_type"],
        evolution_id=latest["evolution_id"],
        evolution_hash=latest["evolution_hash"],
        evolution_sql=latest["evolution_sql"],
        actor=latest["actor"],
        notes=latest["notes"],
    )


def committed_evolutions(catalog: Catalog, namespace: str) -> list[CommittedEvolution]:
    """Return every committed evolution currently live (commit not later uncommitted).

    Ordered oldest-first by the *commit* event's timestamp. An evolution that
    was committed then uncommitted is excluded; the audit trail retains both
    events but the working set is the commits without a later uncommit.
    """
    arrow = _scan_events(catalog, namespace)
    if arrow.num_rows == 0:
        return []

    latest_by_id: dict[str, dict] = {}
    for r in arrow.to_pylist():
        if r["event_type"] not in {"commit", "uncommit"}:
            continue
        existing = latest_by_id.get(r["evolution_id"])
        if existing is None or r["event_ts"] > existing["event_ts"]:
            latest_by_id[r["evolution_id"]] = r

    live = [r for r in latest_by_id.values() if r["event_type"] == "commit"]
    live.sort(key=lambda r: r["event_ts"])
    return [
        CommittedEvolution(
            evolution_id=r["evolution_id"],
            event_id=r["event_id"],
            event_ts=r["event_ts"],
        )
        for r in live
    ]


def committed_payloads(catalog: Catalog, namespace: str) -> list[CommittedPayload]:
    """Return every live commit event with its full SQL + hash, oldest-first.

    Like :func:`committed_evolutions` but carries the ``evolution_sql`` and
    ``evolution_hash`` columns — the payload ``cambrian sync`` writes back to
    disk. Filters out any commit whose ``evolution_id`` has a later
    ``uncommit`` event (uncommitted evolutions shouldn't be re-rehydrated).
    """
    arrow = _scan_events(catalog, namespace)
    if arrow.num_rows == 0:
        return []

    latest_by_id: dict[str, dict] = {}
    for r in arrow.to_pylist():
        if r["event_type"] not in {"commit", "uncommit"}:
            continue
        existing = latest_by_id.get(r["evolution_id"])
        if existing is None or r["event_ts"] > existing["event_ts"]:
            latest_by_id[r["evolution_id"]] = r

    live = [r for r in latest_by_id.values() if r["event_type"] == "commit"]
    live.sort(key=lambda r: r["event_ts"])
    return [
        CommittedPayload(
            evolution_id=r["evolution_id"],
            event_id=r["event_id"],
            event_ts=r["event_ts"],
            evolution_sql=r["evolution_sql"],
            evolution_hash=r["evolution_hash"],
        )
        for r in live
    ]


def applied_committed_ids(catalog: Catalog, namespace: str) -> dict[str, str]:
    """Return ``{evolution_id: evolution_hash}`` for every committed evolution applied.

    An evolution is "applied" if its latest ``apply`` or ``commit`` event is
    newer than the latest ``uncommit`` event (or there is no uncommit). The
    ``commit`` event is the immutable hash anchor written at the moment an
    evolution was promoted from ``current.sql``; ``apply`` events written
    during later cross-checkout replay are also honored.
    """
    arrow = _scan_events(catalog, namespace)
    if arrow.num_rows == 0:
        return {}

    latest_anchor: dict[str, dict] = {}
    latest_uncommit_ts: dict[str, object] = {}
    for r in arrow.to_pylist():
        if r["evolution_id"] == "current":
            continue
        if r["event_type"] in ("apply", "commit"):
            existing = latest_anchor.get(r["evolution_id"])
            if existing is None or r["event_ts"] > existing["event_ts"]:
                latest_anchor[r["evolution_id"]] = r
        elif r["event_type"] == "uncommit":
            existing_ts = latest_uncommit_ts.get(r["evolution_id"])
            if existing_ts is None or r["event_ts"] > existing_ts:
                latest_uncommit_ts[r["evolution_id"]] = r["event_ts"]

    out: dict[str, str] = {}
    for evolution_id, row in latest_anchor.items():
        uncommit_ts = latest_uncommit_ts.get(evolution_id)
        if uncommit_ts is not None and uncommit_ts > row["event_ts"]:
            continue
        out[evolution_id] = row["evolution_hash"]
    return out


def table_states_for_event(
    catalog: Catalog,
    namespace: str,
    *,
    event_id: str,
) -> list[TableStateRow]:
    """Return the per-table state rows attached to *event_id*, in arbitrary order.

    Cambrian's reset path uses this to read the *pre-state* of an earlier
    ``apply`` event so it can roll the affected tables back to that snapshot
    before re-applying.
    """
    arrow = catalog.load_table((namespace, TABLE_STATES_TABLE)).scan().to_arrow()
    if arrow.num_rows == 0:
        return []
    rows = [r for r in arrow.to_pylist() if r["event_id"] == event_id]
    return [
        TableStateRow(
            table_ident=r["table_ident"],
            pre_snapshot_id=r["pre_snapshot_id"],
            pre_schema_id=r["pre_schema_id"],
            pre_spec_id=r["pre_spec_id"],
            pre_sort_order_id=r["pre_sort_order_id"],
            pre_metadata_loc=r["pre_metadata_loc"],
            post_snapshot_id=r["post_snapshot_id"],
            post_schema_id=r["post_schema_id"],
            post_spec_id=r["post_spec_id"],
            post_sort_order_id=r["post_sort_order_id"],
            tag_ref=r["tag_ref"],
        )
        for r in rows
    ]
