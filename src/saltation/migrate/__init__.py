"""Evolution orchestration: apply, watch, commit/uncommit, sync.

The ``watch`` and ``runner`` submodules are deliberately *not* re-exported
at the package level: doing so shadows the submodule attribute on the
package and breaks ``import saltation.migrate.watch`` for downstream
callers (the package attribute would resolve to the function instead).
Import from the submodule directly: ``from saltation.migrate.runner
import apply_idempotent``.
"""

from saltation.migrate.runner import ApplyResult, StatementResult, apply_idempotent
from saltation.migrate.sync import SyncFileResult, SyncResult, saltation_sync
from saltation.migrate.watch import WatchEvent
from saltation.migrate.watch import watch as watch_loop

__all__ = [
    "ApplyResult",
    "StatementResult",
    "SyncFileResult",
    "SyncResult",
    "WatchEvent",
    "apply_idempotent",
    "saltation_sync",
    "watch_loop",
]
