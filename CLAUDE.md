# CLAUDE.md

Project: **cambrian** — a Python 3.12+ CLI that manages SQL **evolutions**
for Apache Iceberg tables via PyIceberg's in-process API. Modeled on
graphile-migrate. `current.sql` + watch-mode hot reload for dev;
`committed/` evolutions applied non-interactively from CI for prod. Same
binary, both contexts.

## Vocabulary — load-bearing

Inside cambrian's code, docs, and JSON the canonical noun is
**evolution** (matching Iceberg's "schema evolution" / "partition
evolution"). The top-level `README.md` deliberately uses "migration"
framing alongside "evolution" for SEO — people search "iceberg
migration tool" and that traffic is load-bearing. Don't touch the
top-level README in routine code changes.

Implementation plan (source of truth):
`/Users/alexjbuck/.claude/plans/the-tool-is-called-purrfect-brook.md`.

## Core principle — load-bearing

**Idempotent is the path; reset is the relief valve.**

- **Idempotent (default)**: every evolution must be re-applicable arbitrarily
  many times with the same end state. `CREATE TABLE IF NOT EXISTS`, `ADD
  COLUMN IF NOT EXISTS`, etc. No rollback machinery; same code path locally
  and in CI.
- **Reset (`--reset` or `[dev].mode = "reset"`)**: explicit opt-in for the
  rare evolution that genuinely cannot be expressed idempotently. Captures a
  checkpoint, rolls back affected tables, re-runs.

This hierarchy shows up in defaults, error messages, docs, examples. Reset
is **never** the recommended fix — only the last resort.

## Locked design decisions — do not revisit

- **PyIceberg in-process** for all catalog/table operations. No JVM, no
  Spark, no Trino.
- **SQL dialect**: Spark subset, parsed by sqlglot via `CambrianSpark`
  (subclass of `sqlglot.dialects.spark.Spark`). Custom `exp.Expression`
  subclasses for Iceberg-specific extensions (`ADD PARTITION FIELD`,
  `WRITE ORDERED BY`, etc.). No regex pre-parser.
- **Auth**: passthrough. `[catalog]` TOML table → `pyiceberg.catalog.load_catalog`
  kwargs verbatim. No first-class OAuth flow.
- **Rollback primitive**: one atomic commit restoring four pointers
  (`SetSnapshotRefUpdate`, `SetCurrentSchemaUpdate`, `SetDefaultSpecUpdate`,
  `SetDefaultSortOrderUpdate`) via `Transaction._apply()`. Private PyIceberg
  API — isolated in `src/cambrian/iceberg/txn.py` behind one wrapper
  function. File upstream for a public `Transaction.apply()`.
- **Sidecar tables** in `_cambrian` namespace: `events`, `table_states`,
  `version`. All append-only. Self-migrated forward by Python functions in
  the binary; never edit a self-migration after release.
- **Snapshot pinning**: Iceberg tag refs `cambrian.cp.<evolution_id>` and
  `cambrian.committed.<n>.<msg>` keep checkpoints alive against expiration.

## Stack and conventions

- Python pinned ≥3.12 (`tomllib`, modern typing).
- `uv` for everything. **Use `uv add <pkg>` — never hand-edit
  `pyproject.toml` versions.** `uv` fetches the latest compatible version
  and writes the lock.
- Set `UV_PROJECT_ENVIRONMENT=.venv` for any `uv` invocation if the venv
  ends up in the wrong place (a parent workspace at `$HOME` can otherwise
  capture it). cambrian's pyproject.toml declares itself a workspace root
  via `[tool.uv.workspace] members = ["."]` to mitigate.
- Format/lint: `ruff check` + `ruff format` (select E, F, I, B, UP, RUF).
  Type-check: `ty check`. Both gates in CI.
- Tests: `pytest`. `tests/unit/` is pure logic plus selective `SqlCatalog`
  use; `tests/integration/` runs the docker-compose stack.
- Default to **no code comments**. Only add a comment for non-obvious
  *why* — hidden constraint, subtle invariant, workaround for a specific
  bug, or surprising behavior. Never write multi-paragraph docstrings.

## Test rig — locked

- **Object storage: rustfs, NOT MinIO.** MinIO's CE licensing changes make
  it a non-option. Use the `rustfs/rustfs` Docker image (S3-API-compatible).
- **Catalog: Lakekeeper** (user runs it in production). REST catalog.
- **Bootstrap**: Lakekeeper requires `POST /management/v1/bootstrap` then
  `POST /management/v1/warehouse`. Non-idempotent — bootstrap container
  tolerates 409s.
- `SqlCatalog` does NOT replicate REST-catalog atomic multi-update
  semantics. Tests that exercise transactions, rollback, or snapshot
  semantics MUST run against Lakekeeper, not SqlCatalog.

## Stacked-PR workflow

- Each milestone is its own branch off the prior PR's tip. Don't gate
  later work on earlier merges. Open PRs incrementally.
- **Graphite (`gt`) is the stack tool here.** Trunk is `main`; every
  feat branch is tracked with its parent. Use `gt log` to see the
  stack, `gt restack` after upstream changes, `gt submit --stack` to
  align GitHub PR bases. Do not hand-rebase across the stack — it
  breaks graphite metadata and you'll spend more time fixing it than
  the manual rebase saved.
- Fan out **independent** PRs to subagents in worktrees (`Agent` with
  `isolation: "worktree"`). Brief each agent self-containedly — they
  don't share context with the main loop.

## Out of scope for v1 — don't build, don't design hooks for

- `INSERT ... SELECT`, MERGE, DELETE
- Declarative-diff mode (desired state → computed evolutions)
- Nessie / branch-based dev workflows
- Cross-catalog evolutions
- Views, RBAC, table maintenance ops as first-class concepts
- Static idempotency lint checks
- Multi-developer `current.sql` collaboration

If a v1 component would clearly benefit from future-proofing for one of
these, leave a code comment and move on. No half-finished implementations.

## File layout

- `cli.py`, `__main__.py`, `__init__.py` — CLI entry, version
- `config.py`, `catalog.py` — TOML config + PyIceberg catalog factory (M1)
- `errors.py` — typed exceptions
- `sql/` — dialect, include resolution, dispatch (M5)
- `iceberg/` — transaction wrapper (rollback primitive), checkpoint,
  affected-table extraction (M4)
- `sidecar/` — schema, bootstrap, events, self-migration (M3). The
  module path keeps the word "migration" because these are
  *sidecar-schema-version* migrations of cambrian's own bookkeeping
  tables, not user-facing evolutions.
- `migrate/` — runner, watch, commit, sync (M5–M8). Module path is
  internal-only; renaming to `evolve/` would be a wider blast radius
  than the semantic win justifies.
- `docker/` — `compose.yml` + `bootstrap.sh` (M2)
- `examples/` — self-contained docker stack + guided walkthrough for
  first-time users. Uses different host ports (9181/9001/5433) than
  the test rig (8181/9000/5432) so both can run simultaneously.

## Known v1.x papercuts (post-V1 backlog)

Surfaced while writing the examples walkthrough; not blockers for V1
but worth knowing about so they don't get re-discovered:

- `evolutions.dir` resolves relative to cwd, not the config file's
  directory. `cambrian apply --path examples/cambrian.toml` from the
  repo root looks for `./evolutions` at the repo root, not under
  `examples/`. Fix: resolve relative to the config file (with an
  opt-out for absolute paths).
- `EvolutionNotFoundError` lands at generic exit code 1. Either add a
  dedicated exit code or include an actionable hint in the error
  ("create current.sql first, or run `cambrian init`").
- `apply --json` payload's rendered SQL strings include sqlglot's
  attached header comments as `/* ... */` prefixes on statement #1.
  Either strip comments from the rendered-SQL field or attach header
  comments to a synthetic head node.
- `status --json` shows a stale `current_applied` field after commit
  (it's the most-recent apply event for `evolution_id="current"`, not
  "what's currently in current.sql"). Either rename or null when
  `current.sql` is empty.
- Custom sqlglot AST nodes (`AddPartitionField`, `WriteOrderedBy`,
  `UnsetTblProperties`, etc.) have no SQL generator — calling
  `.sql()` raises `ValueError`. The runner wraps with `_safe_sql`;
  if you add a new custom node, either register a generator or only
  call it via that helper.
