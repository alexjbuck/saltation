# Compatibility and versioning

What saltation promises to keep working between releases, how a version
number says when that promise is broken, and the checks that keep the two
honest.

## The public contract

Breaking any of these is a **major** change.

**CLI** (the `saltation` command; see [cli-json.md](cli-json.md))

- Command names, including aliases (`redo`, `download`) and the `config`
  sub-commands.
- Options and their short forms (`--path`/`-p`, `--json`, …), whether an
  option is a flag or takes a value, its default, and which options and
  arguments are required.
- Exit codes: `0` success, `1` generic error, `2` sidecar not initialised,
  `3` sidecar newer than this binary, `4` external write detected.
- Every `--json` payload: its keys, their nesting, and constant values such
  as `"mode": "idempotent"`. Adding a key is not breaking; removing or
  renaming one is, and so is changing a key's type.

Human-readable output (the non-`--json` view, help text, error wording) is
**not** contract. Scripts should use `--json` and exit codes.

**On-disk formats**

- `current.sql`, and the `--! include <path-or-glob>` and
  `-- saltation:tables ns.t, …` directives inside it.
- `committed/<NNNN>_<slug>.sql`: the file naming, numbering and content.
- `saltation.toml`: its tables and keys, and `${VAR}` / `$${VAR}`
  substitution.
- The SQL dialect: a statement that applies today keeps applying with the
  same effect.

**Catalog state**

- The sidecar tables in the `_saltation` namespace (`events`,
  `table_states`, `version`). Their schema only moves forward, through the
  self-migrations in `saltation.sidecar.schema`, which never change once
  released. A binary refuses to write to a sidecar newer than itself (exit
  `3`), so older binaries stay safe.
- The snapshot tag refs `saltation.cp.<evolution_id>` and
  `saltation.committed.<n>.<msg>`.

**Python API**

- Importable names in the `saltation` package whose names don't start with
  `_`, with their signatures (parameters, their kinds and defaults) and
  base classes. `saltation.errors` (the exception hierarchy, all rooted at
  `SaltationError`) and `saltation.config` are the parts meant for
  embedding.

## Not contract

- Anything whose name starts with `_`: modules, functions, attributes,
  parameters.
- Where code lives inside `saltation.migrate`, `saltation.iceberg` and
  `saltation.sidecar`. These are the CLI's internals and may be split,
  merged or moved. (The semver check below can't tell internals from the
  rest, so it still flags a breaking change there; declaring it costs a
  pre-1.0 minor bump.)
- The private PyIceberg API wrapped by `saltation.iceberg.txn`.
- Help text, log lines, and the wording of error messages.

## Version numbers

Versions come from knope change files (`.changeset/*.md`, see
[releasing.md](releasing.md)). Each one declares a level:

| Change file      | Use it for                                  | Before 1.0   | From 1.0     |
|------------------|---------------------------------------------|--------------|--------------|
| `default: major` | anything that breaks the contract above     | 0.x → 0.x+1  | x → x+1      |
| `default: minor` | new commands, options, keys, SQL support    | 0.x.y → y+1  | x.y → x.y+1  |
| `default: patch` | fixes that keep the contract                | 0.x.y → y+1  | x.y.z → z+1  |

Before 1.0 knope shifts every level down one place, so a breaking change
bumps the minor number (`0.3.2` → `0.4.0`) and everything else bumps the
patch number. Pin `saltation~=0.3.0` to take fixes but not breaks.

A break that can be avoided should be: keep the old option, key or name
working, mark it deprecated in its help or docstring, and remove it in a
later `major` change.

## Supported Python

CPython 3.12, 3.13 and 3.14; CI runs the unit suite on all three. Dropping a
Python version is a `major` change; adding one is `minor`.

## How it's enforced

- **`semver` workflow** (`.github/workflows/semver.yml`) runs
  `scripts/check_semver.py --base origin/<base branch>` on every PR. It
  compares the PR against its base branch:
  - the Python API, with [griffe](https://mkdocstrings.github.io/griffe/)'s
    breaking-change detection (removed names, changed signatures, changed
    base classes);
  - the CLI, by loading both versions' Typer app: removed commands, options
    or option aliases, newly required options or arguments, flags that now
    take a value, changed defaults, changed exit codes, and removed or
    changed `--json` keys.

  It fails if anything breaks and no change file in the PR says
  `default: major`. No label skips it. Run it locally with
  `uv run python scripts/check_semver.py --base origin/main`, or add
  `--explain` to list the breaks without failing.
- **Contract snapshots** (`tests/unit/test_cli_contract.py`, stored in
  `tests/unit/__snapshots__/`) record every command's `--help`, the options
  and arguments, the exit codes, the `--json` keys, and each module's public
  names. A PR that changes any of them changes a snapshot, which shows up in
  review. Update them with `uv run pytest --snapshot-update`, then add a
  `major` change file if something was removed or changed, or `minor` if
  something was only added.
- The on-disk formats and sidecar tables aren't checked mechanically. The
  integration suite exercises them, and changes to them need a reviewer's
  eye.
