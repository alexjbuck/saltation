"""Iceberg primitives: transaction wrapper, checkpoint capture/restore, affected-table detection."""

from saltation.iceberg.affected import (
    TableIdent,
    affected_tables,
    affected_tables_with_overrides,
)
from saltation.iceberg.checkpoint import Checkpoint, capture, pin
from saltation.iceberg.txn import (
    replace_schema,
    restore_pointers,
    set_location,
    upgrade_format_version,
)

__all__ = [
    "Checkpoint",
    "TableIdent",
    "affected_tables",
    "affected_tables_with_overrides",
    "capture",
    "pin",
    "replace_schema",
    "restore_pointers",
    "set_location",
    "upgrade_format_version",
]
