"""Evolution orchestration: apply, watch, commit/uncommit, sync.

The ``watch`` and ``runner`` submodules are deliberately *not* re-exported
at the package level: doing so shadows the submodule attribute on the
package and breaks ``import cambrian.migrate.watch`` for downstream
callers (the package attribute would resolve to the function instead).
Import from the submodule directly: ``from cambrian.migrate.runner
import apply_idempotent``.
"""

from cambrian.migrate.runner import ApplyResult, StatementResult, apply_idempotent
from cambrian.migrate.sync import SyncFileResult, SyncResult, cambrian_sync
from cambrian.migrate.watch import WatchEvent
from cambrian.migrate.watch import watch as watch_loop

__all__ = [
    "ApplyResult",
    "StatementResult",
    "SyncFileResult",
    "SyncResult",
    "WatchEvent",
    "apply_idempotent",
    "cambrian_sync",
    "watch_loop",
]
