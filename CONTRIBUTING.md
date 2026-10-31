# Contributing to saltation

Thank you for your interest in saltation. This guide explains what kinds of
contributions are welcome, how to set up a development environment, and what a
pull request needs before it can be merged.

By participating you agree to follow the [Code of Conduct](./CODE_OF_CONDUCT.md).
Security problems are not reported through issues or pull requests; see
[SECURITY.md](./SECURITY.md).

## Contribution policy

### Bug fixes and performance improvements: open a PR

You do not need to ask first. Open a pull request directly.

- A bug fix must include a regression test that fails without the fix and
  passes with it. If you found the bug through a property test, add the
  shrunk example as an explicit `@example` or a dedicated test.
- A performance improvement must include evidence: a benchmark, a timing
  comparison, or a count of catalog or object-store round trips before and
  after. State how you measured it in the PR description.

If you would rather report than fix, use the
[bug report](https://github.com/alexjbuck/saltation/issues/new?template=bug_report.yml)
or [performance](https://github.com/alexjbuck/saltation/issues/new?template=performance.yml)
issue forms.

### Features: open an issue first

New features must be coordinated through an issue before any code is written.
Open a
[feature request](https://github.com/alexjbuck/saltation/issues/new?template=feature_request.yml)
and wait for a maintainer to agree on the design. A feature PR without a
linked, accepted issue will be closed, however good the code is. This keeps
design discussion out of code review and protects the locked design decisions
listed below.

"Feature" includes new SQL statements or clauses, new CLI commands or flags,
new configuration keys, new JSON fields, and new exit codes. Anything that
changes the public contract described in
[docs/compatibility.md](./docs/compatibility.md) is a feature.

### Out of scope for v1

The following are deliberately not part of v1. Please do not open PRs for
them, and expect feature requests for them to be closed as "not planned"
unless the maintainer has reopened the topic:

- `INSERT ... SELECT`, `MERGE`, and `DELETE` beyond the simple in-process
  `DELETE FROM <table> WHERE <predicate>` that is already supported.
- Declarative-diff mode (desired state in, computed evolutions out).
- Nessie or other branch-based dev workflows.
- Cross-catalog evolutions.
- RBAC, and table maintenance operations (compaction, snapshot expiry, and
  similar) as first-class concepts.
- Static idempotency lint checks.
- Multi-developer `current.sql` collaboration.

### Upstream bugs

Some problems are not saltation's to fix: PyIceberg, a REST catalog, or a
query engine misbehaves. These are tracked as issues opened with the
[upstream form](https://github.com/alexjbuck/saltation/issues/new?template=upstream.yml).
The convention is:

- Title: `[upstream: <component>] <summary>`, for example
  `[upstream: pyiceberg] Appends fail on specs with two partition fields on the same source column`.
- Labels: `upstream` plus the component (`pyiceberg`, `lakekeeper`,
  `polaris`, or `gravitino`).
- Body: a summary written so it can be filed upstream as is, exact versions,
  a minimal reproduction, how saltation is affected and what it does about it
  today (an explicit refusal, a workaround, or a `catalog_refuses` entry), and
  a link to the upstream issue once it exists.

saltation works around upstream problems with a clear error or an explicit
refusal, never with a silent behavioural difference. The issue is closed when
the upstream fix is released and saltation's minimum version includes it.

## Development setup

You need:

- [uv](https://docs.astral.sh/uv/). uv installs and manages Python for you;
  saltation supports Python 3.12, 3.13, and 3.14.
- Docker with Compose v2, only for the integration and engine test suites.
  Unit tests need nothing beyond uv.

Clone the repository and install every dependency group from the lockfile:

```bash
git clone https://github.com/alexjbuck/saltation.git
cd saltation
uv sync --frozen --all-groups
```

If uv creates the virtual environment somewhere other than `./.venv` (a
parent workspace can capture it), set `UV_PROJECT_ENVIRONMENT=.venv` for
every `uv` command.

Install the pre-commit hooks (ruff, ruff-format, ty, zizmor, actionlint, and
basic file hygiene checks) so the cheap checks run before you push:

```bash
uvx pre-commit install
```

### Everyday commands

```bash
uv run ruff check src tests          # lint
uv run ruff format src tests         # format
uv run ty check src tests            # type-check
uv run pytest tests/unit -n auto     # unit and property tests
```

Run the integration suite against a live REST catalog. The test session
starts the selected docker-compose stack and tears it down at the end:

```bash
SALTATION_TEST_CATALOG=lakekeeper uv run pytest tests/integration
SALTATION_TEST_CATALOG=polaris uv run pytest tests/integration
SALTATION_TEST_CATALOG=gravitino uv run pytest tests/integration
```

On Linux, add `127.0.0.1 host.docker.internal` to `/etc/hosts` first; the
storage endpoint the rigs vend to clients uses that name. Docker Desktop on
macOS and Windows provides it already.

Run the engine interoperability tests (see [docs/engines.md](./docs/engines.md)
for what each engine checks and what it needs):

```bash
SALTATION_TEST_ENGINE=trino uv run pytest tests/engines
```

Hypothesis budgets are selected with `HYPOTHESIS_PROFILE`: `dev` (the
default), `ci`, `nightly`, and `fuzz`. Use `ci` locally when you change
anything a property test covers:

```bash
HYPOTHESIS_PROFILE=ci uv run pytest tests/unit -n auto
HYPOTHESIS_PROFILE=fuzz uv run pytest tests/unit/test_parser_fuzz.py
```

Never edit `pyproject.toml` dependency versions by hand. Use `uv add <pkg>`
(or `uv add --group dev <pkg>`) so uv resolves the version and updates
`uv.lock`. CI runs `uv lock --check`.

## Project layout

The package lives in `src/saltation/`:

| Path | Contents |
| ---- | -------- |
| `cli.py`, `__main__.py`, `__init__.py` | CLI entry point and version. |
| `config.py`, `catalog.py` | TOML configuration and the PyIceberg catalog factory. |
| `errors.py` | Typed exceptions. Every user-facing failure is a `SaltationError` subclass. |
| `sql/` | The `SaltationSpark` sqlglot dialect, custom AST nodes, `--! include` resolution, and statement dispatch. |
| `iceberg/` | The transaction wrapper (the rollback primitive), checkpoints, affected-table extraction, and views. |
| `sidecar/` | The `_saltation` sidecar tables: schema, bootstrap, events, and self-migration of saltation's own bookkeeping tables. |
| `migrate/` | The runner, watch mode, commit and uncommit, and sync. The module name is internal and stays as is. |

Outside the package:

| Path | Contents |
| ---- | -------- |
| `tests/unit/` | Pure logic and property tests, plus selective `SqlCatalog` use. |
| `tests/integration/` | Tests against the docker-compose catalog rigs; `rig.py` maps `SALTATION_TEST_CATALOG` to a stack and client properties. |
| `tests/engines/` | Engine interoperability tests selected by `SALTATION_TEST_ENGINE`. |
| `tests/fixtures/` | The SQL corpus (`iceberg_corpus.py`), its expected effects (`corpus_effects.py`), and Hypothesis strategies. |
| `docker/` | Compose stacks: Lakekeeper in `docker/`, the others in `docker/<catalog>/`. |
| `examples/` | A self-contained stack and guided walkthrough for first-time users, on different host ports from the test rig. |
| `docs/` | JSON output schemas, compatibility contract, engines, releasing, and maintainer notes. |

## Design principles

These are settled. A PR that works against them will not be merged; if you
think one is wrong, open an issue and make the case there.

### Idempotent is the path; reset is the relief valve

Every evolution must be re-applicable any number of times with the same end
state (`CREATE TABLE IF NOT EXISTS`, `ADD COLUMN IF NOT EXISTS`, and so on).
The same code path runs locally and in CI, with no rollback machinery.

Reset (`--reset` or `[dev].mode = "reset"`) is an explicit opt-in for the rare
evolution that cannot be expressed idempotently. Its invariant is
property-tested: the catalog ends up as the committed state plus the latest
`current.sql`. Reset is never the recommended fix. Defaults, error messages,
docs, and examples must all point at the idempotent form first.

### Locked decisions

- **PyIceberg in-process** for every catalog and table operation. No JVM,
  Spark, or Trino at runtime.
- **SQL is a Spark subset parsed by sqlglot** through the `SaltationSpark`
  dialect. Iceberg-specific syntax (`ADD PARTITION FIELD`, `WRITE ORDERED BY`,
  and so on) is modelled as custom `exp.Expression` subclasses. There is no
  regex pre-parser, and one will not be added. A new custom node also needs an
  entry in `SaltationSpark.Generator.TRANSFORMS`;
  `tests/unit/test_roundtrip_property.py` fails until it renders back to the
  same AST.
- **Auth is passthrough.** The `[catalog]` TOML table is passed verbatim to
  `pyiceberg.catalog.load_catalog`. There is no first-class OAuth flow.
- **The rollback primitive is isolated.** One atomic commit restores the
  snapshot ref, schema, partition spec, sort order, and table properties. It
  uses private PyIceberg API, so it lives in `src/saltation/iceberg/txn.py`
  behind one wrapper function. Do not call `Transaction._apply()` or similar
  private API anywhere else.
- **Sidecar tables are append-only.** `events`, `table_states`, and `version`
  in the `_saltation` namespace are only ever appended to. They are migrated
  forward by Python functions in the binary, and a released self-migration is
  never edited; add a new one instead.
- **Vocabulary.** The canonical noun in code, docs, and JSON is
  **evolution**, matching Iceberg's "schema evolution" and "partition
  evolution". The top-level README deliberately also says "migration" so
  people searching for an Iceberg migration tool find it; leave that framing
  alone.

## Testing expectations

Every behavioural change comes with tests at the right level:

- **Unit tests** (`tests/unit/`) for parsing, dispatch decisions,
  configuration, and anything that does not need a real catalog.
- **Property-based tests** with [Hypothesis](https://hypothesis.readthedocs.io/)
  for anything with an invariant: round-tripping SQL through the dialect,
  idempotent re-application, reset convergence, include resolution, and config
  parsing. Prefer extending an existing strategy in
  `tests/fixtures/evolution_strategies.py` over writing a new one.
- **Integration tests** (`tests/integration/`) for anything that touches
  transactions, rollback, checkpoints, or snapshot semantics. `SqlCatalog`
  does not reproduce REST-catalog atomic multi-update semantics, so these
  must run against the docker rigs, not `SqlCatalog`. CI runs the suite
  against Lakekeeper, Polaris, and Gravitino; your change must pass on all
  three.
- **Engine interoperability tests** (`tests/engines/`) when a change affects
  what other engines see: table metadata, view bodies, partition specs, sort
  orders, or properties. CI reads saltation's output with Trino, Spark,
  StarRocks, and DuckDB.

### New SQL surface

Any new statement or clause needs entries in the corpus:

1. Add a `CorpusEntry` to `tests/fixtures/iceberg_corpus.py` with a stable
   `id`, a `category`, the `sql`, the `expected` verdict (`apply`, `reject`,
   or `invalid`), and a `note` citing the authoritative source (usually the
   Iceberg Spark DDL docs).
2. For `apply` and `invalid` entries, add the expected effect on the catalog
   to `EFFECTS` in `tests/fixtures/corpus_effects.py`.
   `tests/integration/test_corpus_effects.py` fails until every such entry has
   one, and `tests/unit/test_corpus_completeness.py` fails if the dispatcher
   supports a construct that no corpus entry exercises.
3. If a test catalog genuinely refuses the statement while saltation's request
   is correct, record that in the effect's `catalog_refuses`, keyed by rig
   name (`lakekeeper`, `polaris`, `gravitino`) with the exact exception type
   and message pattern. Do not skip the test or loosen the assertion. If the
   refusal is a catalog bug, also open an upstream issue.

Statements that are out of scope must be refused with
`UnsupportedStatementError` and a clear message, and they belong in the corpus
too, as `reject` entries.

## Code style

- `ruff check` with rule sets E, F, I, B, UP, RUF, and S (flake8-bandit), and
  `ruff format`, with a line length of 100. CI fails on any finding or
  formatting diff.
- `ty check` must pass. Every function signature is fully typed.
- No code comments by default. Add one only to explain a non-obvious *why*:
  a hidden constraint, a subtle invariant, a workaround for a specific bug,
  or surprising behaviour. Do not restate what the code does.
- Docstrings are one line or a short paragraph. No multi-paragraph docstrings.
- Raise typed `SaltationError` subclasses from `errors.py` for anything a user
  can trigger, with a one-line message that says what to do next. A raw
  PyIceberg, HTTP, or `KeyError` traceback reaching the user is a bug.
- Use modern Python 3.12 idioms: `X | None`, built-in generics, `tomllib`.

## Change files and versioning

Releases are driven by [knope](https://knope.tech) change files. See
[docs/releasing.md](./docs/releasing.md) for the full release flow.

### Writing a change file

Every PR with a user-facing change adds one file under `.changeset/`. Run
`knope document-change` and answer the prompts, or write it by hand:

```markdown
---
default: patch
---

# Reset no longer rescans the whole sidecar on every save

Optional detail in full sentences. This becomes part of the CHANGELOG entry.
```

The heading is the release-note line, so write it for users: what changed
for them, not how the code changed. The change type is one of:

| Type | Use it for |
| ---- | ---------- |
| `major` | A breaking change to the public contract: removed or renamed commands, flags, config keys, or JSON fields; changed exit codes; changed on-disk formats; removed public Python API. |
| `minor` | A backwards-compatible feature: new SQL surface, a new command or flag, a new JSON field. |
| `patch` | A bug fix or performance improvement with no contract change. |

Before 1.0, knope shifts these down one level: `major` bumps `0.x` to
`0.(x+1)`, and `minor` and `patch` bump the patch number.

PRs that change nothing user-facing (CI, tests, internal refactors, and docs
that do not describe new behaviour) take the `no-changeset` label instead.
Ask a maintainer to apply it if you cannot; the `changeset` check fails
without one or the other.

### Public contract checks

[docs/compatibility.md](./docs/compatibility.md) defines what is public. Two
checks guard it:

- The `semver` workflow runs [griffe](https://mkdocstrings.github.io/griffe/)
  against `main` and fails on a breaking change to the public Python API
  unless the PR adds a `major` change file. Run it locally with
  `uv run griffe check saltation --search src --against origin/main`.
- Snapshot tests in `tests/unit/test_cli_contract.py` (using
  [syrupy](https://github.com/syrupy-project/syrupy)) pin CLI help text, exit
  codes, and JSON output shapes. If you change any of these on purpose,
  regenerate the snapshots and commit them:

  ```bash
  uv run pytest tests/unit/test_cli_contract.py --snapshot-update
  ```

  A snapshot diff is a contract change. Review it as such, and pick the
  change type accordingly: removing or renaming something is `major`, adding
  something is `minor`.

## Pull request checklist

Before you mark a PR ready for review:

- [ ] Bug fix or performance PR, or a feature with a linked issue the
      maintainer has accepted.
- [ ] Tests added: a regression test for a bug, evidence for a performance
      change, corpus and effect entries for new SQL.
- [ ] `uv run ruff check src tests`, `uv run ruff format --check src tests`,
      `uv run ty check src tests`, and `uv run pytest tests/unit -n auto`
      pass locally.
- [ ] Integration tests run against at least one catalog if the change
      touches catalog behaviour.
- [ ] A `.changeset/*.md` file, or a note explaining why `no-changeset`
      applies.
- [ ] Snapshots updated and the change type matches the contract impact.
- [ ] Docs updated (`README.md`, `docs/cli-json.md`, `docs/compatibility.md`)
      if user-visible behaviour changed.

The [pull request template](./.github/PULL_REQUEST_TEMPLATE.md) repeats this
list.

## Commit messages

Use a plain imperative subject line of about 72 characters or fewer, such as
`Refuse ALTER VIEW RENAME onto an existing view`. Do not use conventional
commit prefixes like `feat:` or `fix:`; release notes come from change files,
not commit messages. Add a body when the reason for the change is not obvious
from the subject, explaining why rather than what. Keep messages about the
change itself: no tool or model identifiers.

Because PRs are squash-merged, the PR title becomes the commit subject on
`main`. Write the title the same way.

## Review and merge

- The maintainer reviews every PR. Expect questions about design fit,
  test coverage, and error messages as much as about the code.
- All CI must be green before merge, including the integration jobs for all
  three catalogs and the engine interoperability jobs. A flaky failure is
  still a failure; say so in the PR rather than re-running until it passes.
- PRs are squash-merged into `main`, which keeps a linear history.
- Keep PRs focused. Unrelated cleanups belong in their own PR.

## Licensing

saltation is licensed under the [Apache License 2.0](./LICENSE). There is no
CLA and no DCO sign-off requirement. Contributions are accepted under the
same license (inbound equals outbound), as described in section 5 of the
Apache License: anything you intentionally submit for inclusion is licensed
under Apache-2.0 without additional terms.

## Getting help

GitHub Discussions are not enabled. If you are unsure whether something is a
bug, or want to sound out an idea before writing a feature request, open an
issue with the closest matching form and say so. Read the
[README](./README.md) and the [docs](./docs) first; many questions about
behaviour are answered there. For security issues, follow
[SECURITY.md](./SECURITY.md) and do not open a public issue.
