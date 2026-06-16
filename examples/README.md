# cambrian walkthrough

A guided, end-to-end run through cambrian against a self-contained
Iceberg stack (Lakekeeper + rustfs + postgres in Docker). Skim the
top-level [README](../README.md) for orientation first; this doc is the
hands-on follow-up.

Cambrian calls the units it manages **evolutions** — matching Iceberg's
own "schema evolution" / "partition evolution" vocabulary. (If you've
used Postgres tools like Flyway or graphile-migrate, this is the same
workflow: file history in git, bootstrap new instances by re-applying
the same set.)

## What you'll learn

The cambrian lifecycle has four moving parts: **idempotent apply** of
`current.sql` during dev, **watch mode** for hot-reload, **commit** to
freeze a dev iteration into the canonical `committed/` history, and
**sync** to rehydrate that history on a fresh checkout. By the end of
this walkthrough you'll have done each one against a real catalog.

The load-bearing principle: **idempotent is the path; reset is the
relief valve.** Every evolution here is written so it can be re-run any
number of times with the same end state. We touch reset only as a
postscript.

## Prerequisites

- Docker (Desktop on macOS/Windows; native engine on Linux).
- `uv` (or `pipx`) to run cambrian. The examples below use `uv tool
  install`, but any way that puts `cambrian` on your `$PATH` works.
- An empty shell session and a willingness to copy-paste.

Install cambrian once it's on PyPI:

```bash
uv tool install cambrian
cambrian --version
```

Developing against a local clone of this repo? Substitute `uv run
--project <path-to-cambrian> cambrian` for plain `cambrian` everywhere
below.

All commands assume you've `cd`-ed into the `examples/` directory:

```bash
cd examples
```

## Step 1: Bring up the stack

```bash
docker compose -f docker-compose.yml up -d
```

The stack uses non-default host ports so it can run alongside cambrian's
integration-test rig:

| service    | host port | container port |
|------------|-----------|----------------|
| lakekeeper | 9181      | 8181           |
| rustfs     | 9001      | 9000           |
| postgres   | 5433      | 5432           |

The `bootstrap` container POSTs Lakekeeper's two non-idempotent
provisioning calls (accept terms, create the `cambrian-example`
warehouse) and exits. Wait for it to settle, then verify:

```bash
# Confirm the catalog responds to a config probe.
curl -sS "http://localhost:9181/catalog/v1/config?warehouse=cambrian-example" \
  | head -c 120
```

```text
# expected: a JSON object beginning with {"overrides":{"uri":"http://localhost:9181/catalog",...
```

If the curl fails, check `docker compose -f docker-compose.yml ps` —
both `lakekeeper` and `rustfs` need to be `(healthy)` and the
`bootstrap` container should have exited with code 0.

## Step 2: Initialize the sidecar

cambrian stores its audit trail and checkpoint metadata in **sidecar
tables** that live in the same catalog as your real tables — under a
namespace you pick (`_cambrian` by default).

```bash
cambrian init --path cambrian.toml
```

```text
# expected:
Initialized sidecar at _cambrian (version=1)
```

Three tables now exist under `_cambrian`:

- `events` — append-only audit log of every `apply`, `rollback`,
  `commit`, and `uncommit`.
- `table_states` — per-affected-table snapshot pointers, used by reset
  mode and `reset-to` to reconstruct historical states.
- `version` — schema version of the sidecar itself, so future cambrian
  binaries can self-migrate forward without your intervention.

Re-running `cambrian init` is a no-op:

```bash
cambrian init --path cambrian.toml
```

```text
# expected:
Already initialized: sidecar at _cambrian (version=1)
```

## Step 3: Your first evolution

`current.sql` is your in-flight evolution. The starting copy is:

```sql
CREATE NAMESPACE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.users (
  id    BIGINT,
  email STRING
) USING iceberg;
```

Apply it:

```bash
cambrian apply --json --path cambrian.toml
```

```json
# expected (truncated, your hashes/UUIDs will differ):
{
  "mode": "idempotent",
  "status": "applied",
  "evolution_id": "current",
  "evolution_hash": "2a07e5709c45…",
  "statements": [
    { "notes": "created namespace demo", ... },
    { "notes": "created table demo.users", "affected_tables": ["demo.users"], ... }
  ]
}
```

Now run the exact same command again:

```bash
cambrian apply --json --path cambrian.toml
```

```json
# expected:
{
  "mode": "idempotent",
  "status": "unchanged",
  ...
  "statements": [],
  ...
}
```

`status: "unchanged"` is the idempotent contract in action. cambrian
hashes the expanded `current.sql`, compares it to the most recent
`apply` event, and short-circuits when nothing has changed. You can
safely re-invoke `cambrian apply` on a flaky CI job, after a partial
deploy, or in a tight watch loop, and you'll only ever apply the SQL
once per distinct hash.

## Step 4: Iterate

Add a column to `current.sql`:

```sql
CREATE NAMESPACE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.users (
  id    BIGINT,
  email STRING
) USING iceberg;

ALTER TABLE demo.users ADD COLUMN IF NOT EXISTS name STRING;
```

Apply again:

```bash
cambrian apply --json --path cambrian.toml
```

The hash changes, so cambrian runs the file. The CREATE statements log
`already exists` notes; the ALTER actually does the work:

```json
# expected (truncated):
{
  "status": "applied",
  "statements": [
    { "notes": "namespace demo already exists", ... },
    { "notes": "table demo.users already exists (IF NOT EXISTS)", ... },
    { "notes": "add column name", "affected_tables": ["demo.users"], ... }
  ]
}
```

That's the contract: every statement in `current.sql` is written so it
no-ops on a catalog that already has the change. cambrian doesn't have
a "rollback this column" command for you — you simply edit the file and
re-apply.

While iterating, `cambrian watch --path cambrian.toml` runs the same
apply loop on every file save. Ctrl-C exits.

## Step 5: Commit

When you're happy with the in-flight state, freeze it:

```bash
cambrian commit -m "users table v1" --json --path cambrian.toml
```

```json
# expected:
{
  "evolution_id": "0001_users-table-v1",
  "committed_path": ".../examples/committed/0001_users-table-v1.sql",
  "evolution_hash": "b047b2d8bda5…",
  "tag_ref": "cambrian.committed.1.users-table-v1",
  "event_id": "...",
  "affected_tables": ["demo.users"]
}
```

Three things happened:

1. `current.sql` was renamed to `committed/0001_users-table-v1.sql` and
   replaced with an empty file. `current.sql` is yours again to start
   the next iteration.
2. A `commit` event was recorded in `_cambrian.events`.
3. Iceberg tag refs `cambrian.committed.1.users-table-v1` (and a
   per-table `cambrian.cp.0001_users-table-v1`) pin the post-commit
   snapshots so Iceberg's snapshot expiration won't reclaim them.

`committed/` is the contract between dev and prod: those files are
immutable, applied in order, and shipped via git.

Inspect the audit trail:

```bash
cambrian status --json --path cambrian.toml
```

```json
# expected:
{
  "initialized": true,
  "sidecar_namespace": "_cambrian",
  "sidecar_version": 1,
  "committed_count": 1,
  "committed_evolutions": [
    {
      "evolution_id": "0001_users-table-v1",
      "event_id": "...",
      "event_ts": "..."
    }
  ],
  "current_applied": { ... }
}
```

## Step 6: Production replay

The CI side of the contract: on a fresh checkout (or a fresh node in a
deploy pipeline), `cambrian sync` rehydrates the local `committed/`
directory from the catalog. The catalog is the source of truth.

Simulate a fresh clone:

```bash
mkdir -p fresh-clone
cp cambrian.toml fresh-clone/
cd fresh-clone

cambrian sync --json --path cambrian.toml
```

```json
# expected:
{
  "dry_run": false,
  "written": 1,
  "overwritten": 0,
  "skipped": 0,
  "refused": 0,
  "files": [
    { "evolution_id": "0001_users-table-v1", "status": "written", ... }
  ]
}
```

The committed file is now present locally:

```bash
ls committed/
# expected:
# 0001_users-table-v1.sql
```

A second `sync` is a clean no-op (`written: 0`, `skipped: 1`):

```bash
cambrian sync --json --path cambrian.toml
```

In a real deploy, CI runs `cambrian sync` (to rehydrate), then
`cambrian apply` — which replays any committed evolutions the catalog
hasn't yet recorded, then re-checks `current.sql`. Same binary, same
flags, idempotent end state.

Hop back up before tearing down:

```bash
cd ..
```

## Bonus: reset is the relief valve

Some evolutions genuinely cannot be expressed idempotently — a column
type change with data loss, a rebuild of a partitioned table, etc.
That's what `cambrian apply --reset` is for: it captures (or reuses) a
checkpoint of every affected table, rolls the tables back to it, and
re-runs `current.sql` from scratch.

Reset is **never** the recommended fix for a normal iteration. It
needs explicit opt-in (`--reset` or `[dev].mode = "reset"`), refuses to
run when another writer has touched the affected tables (`--force` to
override), and emits a separate `rollback` event in the audit trail
ahead of the new `apply`. Read the main [README](../README.md) and
[`docs/cli-json.md`](../docs/cli-json.md) before reaching for it.

## Tear down

```bash
docker compose -f docker-compose.yml down -v
```

`-v` drops the postgres volume too, so the next `up -d` starts from a
truly clean state. Your local `committed/` (and the simulated
`fresh-clone/`) is git-ignored under `examples/.gitignore`; delete it
by hand if you want to start the walkthrough over from scratch.
