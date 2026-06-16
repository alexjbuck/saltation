"""Typed exceptions raised by cambrian.

Each exception carries a user-facing hint or doc link. Populated as milestones land.
"""

from __future__ import annotations

from pathlib import Path


class CambrianError(Exception):
    """Base class for all cambrian exceptions."""


class IncludeNotFoundError(CambrianError):
    """Raised when an ``--! include`` directive references a path that doesn't exist.

    Carries the directive-relative path and the absolute resolution attempt so the
    error message tells the user both what they typed and what cambrian looked for.
    """

    def __init__(self, *, directive: str, resolved: Path, source: Path) -> None:
        self.directive = directive
        self.resolved = resolved
        self.source = source
        super().__init__(
            f"include directive '--! include {directive}' in {source} does not match any "
            f"file (looked at {resolved})"
        )


class CircularIncludeError(CambrianError):
    """Raised when ``--! include`` resolution forms a cycle.

    ``cycle`` is the path of files visited before the cycle was detected, ending with
    the file that would have re-entered. Reported in include order so the user can
    trace which file pulled which.
    """

    def __init__(self, cycle: list[Path]) -> None:
        self.cycle = list(cycle)
        chain = " -> ".join(str(p) for p in self.cycle)
        super().__init__(f"circular include detected: {chain}")


class UnsupportedStatementError(CambrianError):
    """Raised when dispatch encounters a SQL construct not in the v1 supported list.

    Carries the source-line hint (extracted from the original expanded SQL) and a
    one-line explanation so the CLI layer can format a precise pointer at the offending
    statement. The v1 supported list lives in the plan §2.2 sidecar.
    """

    def __init__(
        self,
        *,
        statement_sql: str,
        reason: str,
        line: int | None = None,
    ) -> None:
        self.statement_sql = statement_sql
        self.reason = reason
        self.line = line
        location = f" at line {line}" if line is not None else ""
        super().__init__(
            f"unsupported SQL statement{location}: {reason}\n"
            f"  statement: {statement_sql.strip()[:200]}"
        )


class DispatchError(CambrianError):
    """Raised when dispatch translation fails for a reason other than "unsupported".

    Examples: an INSERT VALUES whose literal can't be coerced to the table schema,
    or an ALTER COLUMN whose target type isn't representable in Iceberg. Distinct
    from :class:`UnsupportedStatementError` (the SQL is recognised — we just can't
    run it).
    """


class EvolutionNotFoundError(CambrianError):
    """Raised when ``apply`` is asked to run against a missing ``current.sql``."""


class ConfigNotFoundError(CambrianError):
    """Raised when the cambrian config file does not exist at the requested path."""


class InvalidConfigError(CambrianError):
    """Raised when the cambrian config fails schema validation or TOML parsing."""


class MissingEnvVarError(CambrianError):
    """Raised when ``${VAR}`` interpolation in config references unset environment variables."""


class NotInitializedError(CambrianError):
    """Raised when a sidecar-using command runs against a catalog that has never been initialised.

    The fix is always ``cambrian init``; the error message reflects that.
    """

    def __init__(self, message: str | None = None) -> None:
        super().__init__(
            message or "sidecar not initialized in this catalog; run `cambrian init` first"
        )


class SidecarVersionAheadError(CambrianError):
    """Raised when the sidecar's persisted version is newer than this binary understands.

    Carries the on-disk and expected versions so callers can format a useful message.
    Read-only commands may catch this and proceed; mutating commands let it bubble.
    """

    def __init__(self, found_version: int, expected_version: int) -> None:
        self.found_version = found_version
        self.expected_version = expected_version
        super().__init__(
            f"sidecar is at version {found_version} but this cambrian only understands "
            f"up to version {expected_version}; upgrade cambrian to proceed"
        )


class IllegalStateError(CambrianError):
    """Raised when cambrian is asked to perform an operation whose preconditions are violated.

    Distinct from a config or input-validation error: the *callers' wiring* is wrong and
    no amount of retrying will fix it. Carries a hint pointing at the cambrian-level cause
    (not the underlying PyIceberg surface), so the error trail is useful at the CLI layer.
    """


class ExternalWriteDetectedError(CambrianError):
    """Raised when a rollback (or other guarded write) detects that another writer has
    advanced the table's ``main`` ref since the checkpoint was captured.

    The underlying PyIceberg signal is ``CommitFailedException`` from the
    ``AssertRefSnapshotId`` requirement attached to the rollback ``_apply``. We wrap it
    here so callers can distinguish the "someone else wrote" condition from generic
    commit failures, and so the error message reads in cambrian's vocabulary (checkpoint,
    rollback) rather than in PyIceberg's (requirement, base metadata).
    """

    def __init__(
        self,
        *,
        ref: str,
        expected_snapshot_id: int | None,
        observed_snapshot_id: int | None,
    ) -> None:
        self.ref = ref
        self.expected_snapshot_id = expected_snapshot_id
        self.observed_snapshot_id = observed_snapshot_id
        super().__init__(
            f"external write detected on ref {ref!r}: expected snapshot "
            f"{expected_snapshot_id}, found {observed_snapshot_id}. "
            "Another writer advanced the table between checkpoint and rollback; "
            "rollback aborted to avoid clobbering their commit."
        )
