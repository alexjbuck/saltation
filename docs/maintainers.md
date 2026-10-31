# Maintainer guide

This page is for the repository owner. It covers the one-time GitHub and PyPI
setup that cannot be done from files in the repository, and the routines for
release day and issue triage. Contributor-facing rules live in
[CONTRIBUTING.md](../CONTRIBUTING.md); the release mechanics live in
[releasing.md](./releasing.md).

## One-time repository setup

Work through these in order. Each item names where the setting lives. "Settings"
means the repository's **Settings** tab.

### 1. Squash merging only

**Settings → General → Pull Requests**

- [ ] Allow squash merging: on. Set **Default commit message** to
      **Pull request title and description** (or **Pull request title**).
- [ ] Allow merge commits: off.
- [ ] Allow rebase merging: off.
- [ ] Always suggest updating pull request branches: on.
- [ ] Automatically delete head branches: on.

The squash commit must keep the release PR's title as its subject.
`prepare-release.yml` skips commits whose message contains
`chore: prepare release`, so a different default would make the release merge
open a new, empty release PR.

### 2. Branch ruleset on `main`

**Settings → Rules → Rulesets → New ruleset → New branch ruleset**

- [ ] Name: `main`. Enforcement status: **Active**. Target branches:
      **Include default branch**.
- [ ] Bypass list: empty. If you need an escape hatch, add only yourself, with
      "For pull requests only".
- [ ] **Restrict deletions**: on.
- [ ] **Block force pushes**: on.
- [ ] **Require linear history**: on.
- [ ] **Require a pull request before merging**: on, with
      **Dismiss stale pull request approvals when new commits are pushed** on
      and **Require conversation resolution before merging** on. While you are
      the only maintainer, set required approvals to 0 and leave
      **Require review from Code Owners** off: GitHub does not let authors
      approve their own PRs, so either setting would block your own changes.
      Only people with write access can merge, so outside PRs still need you.
      Once there is a second maintainer, require 1 approval and Code Owners
      review (`.github/CODEOWNERS` assigns everything to `@alexjbuck`).
- [ ] **Require status checks to pass**: on, with **Require branches to be up
      to date before merging** on. Add these checks:

| Workflow | Check names |
| -------- | ----------- |
| `ci.yml` | `lint + type + unit (py3.12)`, `lint + type + unit (py3.13)`, `lint + type + unit (py3.14)`, `lock`, `integration (lakekeeper)`, `integration (polaris)`, `integration (gravitino)` |
| `engines.yml` | `engines (trino)`, `engines (spark)`, `engines (starrocks)`, `engines (duckdb)` |
| `changeset.yml` | `changeset` |
| `semver.yml` | the griffe API check and the CLI contract job |
| `ci.yml` (`package` reusable workflow) | the wheel and sdist build, and the install smoke jobs |
| `security.yml` | `zizmor (workflow audit)`, `pip-audit (locked dependencies)` |

The check picker only offers names that have run at least once in the last
week, so open a throwaway PR first, let every workflow run, and then copy the
exact names from its checks tab. Matrix jobs are listed once per matrix value.
Do not require the nightly soak, the weekly fuzz run, Scorecard, or CodeQL as
status checks; they run on schedules or report through code scanning instead.

A job skipped by its `if:` condition counts as passing. That is how the
`changeset` check lets through PRs labelled `no-changeset` and the release PR
from the `release` branch.

### 3. GitHub Actions permissions

**Settings → Actions → General**

- [ ] **Actions permissions**: allow all actions, or restrict to actions from
      GitHub and verified creators plus the ones the workflows use. If the
      option to require actions pinned to a full-length commit SHA is
      available, turn it on; every workflow already pins by SHA.
- [ ] **Approval for running fork pull request workflows from contributors**:
      **Require approval for first-time contributors**.
- [ ] **Workflow permissions**: **Read repository contents and packages
      permissions**. Each workflow asks for the extra permissions it needs.
- [ ] **Allow GitHub Actions to create and approve pull requests**: on, unless
      you configure `RELEASE_TOKEN` (next item).

`prepare-release.yml` currently fails on `main` with "GitHub Actions is not
permitted to create or approve pull requests". Fix it one of two ways:

- turn on **Allow GitHub Actions to create and approve pull requests** above.
  The release PR then opens, but because it was opened with `GITHUB_TOKEN`,
  CI does not run on it automatically; or
- (preferred) add a repository secret `RELEASE_TOKEN` under
  **Settings → Secrets and variables → Actions → Secrets**. Use a
  fine-grained personal access token scoped to this repository only, with
  **Contents: Read and write** and **Pull requests: Read and write**, or a
  GitHub App installation token with the same permissions. PRs opened with it
  trigger CI normally. Set a calendar reminder for its expiry date.

### 4. Environments

**Settings → Environments → New environment**

- [ ] `testpypi`: no protection rules.
- [ ] `pypi`: **Required reviewers** set to yourself (and any co-maintainer).
      Turn on **Prevent self-review** only once there is a second reviewer.

Leave **Deployment branches and tags** at **No restriction** on both. The
publish jobs run from release events and manual dispatches whose refs differ,
and a branch rule that does not match makes the job fail before the approval
gate. The required reviewers on `pypi` are the real control.

### 5. PyPI and TestPyPI trusted publishing

On [PyPI](https://pypi.org/manage/account/publishing/) and on
[TestPyPI](https://test.pypi.org/manage/account/publishing/), add a pending
trusted publisher (it becomes a normal one after the first upload):

| Field | PyPI | TestPyPI |
| ----- | ---- | -------- |
| PyPI project name | `saltation` | `saltation` |
| Owner | `alexjbuck` | `alexjbuck` |
| Repository name | `saltation` | `saltation` |
| Workflow name | `publish-pypi.yml` | `publish-pypi.yml` |
| Environment name | `pypi` | `testpypi` |

The workflow name must be the file that runs the `pypa/gh-action-pypi-publish`
step. If publishing moves to another workflow file, update both publishers.

### 6. Turn on publishing

**Settings → Secrets and variables → Actions → Variables → New repository variable**

- [ ] `PYPI_PUBLISH_ENABLED` = `true`, only after steps 4 and 5 are done.
      Until then the release workflow still tags and creates the GitHub
      release with the wheel, sdist, and binaries, but skips both package
      indexes.

### 7. Labels

**Issues → Labels → New label**, or run the commands below with the `gh` CLI.

| Label | Used for |
| ----- | -------- |
| `no-changeset` | PRs with nothing user-facing; skips the `changeset` check. |
| `bug` | Incorrect behaviour. Applied by the bug report form. |
| `performance` | Slowness. Applied by the performance form. |
| `enhancement` | Feature requests. Applied by the feature form. |
| `upstream` | Problems in PyIceberg, a catalog, or an engine. Applied by the upstream form. |
| `pyiceberg` | Upstream component: PyIceberg. |
| `lakekeeper` | Upstream component or rig: Lakekeeper. |
| `polaris` | Upstream component or rig: Apache Polaris. |
| `gravitino` | Upstream component or rig: Apache Gravitino. |
| `good first issue` | Small, well-specified issues for new contributors. |
| `help wanted` | Accepted work the maintainer would welcome a PR for. |

```bash
gh label create no-changeset --color ededed --description "Nothing user-facing; no change file needed" -R alexjbuck/saltation
gh label create performance --color fbca04 --description "Something is slower than it should be" -R alexjbuck/saltation
gh label create upstream --color 5319e7 --description "Bug in PyIceberg, a catalog, or an engine" -R alexjbuck/saltation
gh label create pyiceberg --color c5def5 --description "Upstream component: PyIceberg" -R alexjbuck/saltation
gh label create lakekeeper --color c5def5 --description "Lakekeeper catalog" -R alexjbuck/saltation
gh label create polaris --color c5def5 --description "Apache Polaris catalog" -R alexjbuck/saltation
gh label create gravitino --color c5def5 --description "Apache Gravitino catalog" -R alexjbuck/saltation
gh label create bug --color d73a4a --description "Something isn't working" -R alexjbuck/saltation --force
gh label create enhancement --color a2eeef --description "New feature or request" -R alexjbuck/saltation --force
gh label create "good first issue" --color 7057ff --description "Good for newcomers" -R alexjbuck/saltation --force
gh label create "help wanted" --color 008672 --description "Extra attention is needed" -R alexjbuck/saltation --force
```

`--force` updates a label that already exists (GitHub creates `bug`,
`enhancement`, `good first issue`, and `help wanted` by default). Run the
others without it; they fail harmlessly if the label exists.

### 8. Code security

**Settings → Advanced Security** (labelled **Code security** on some
accounts)

- [ ] **Dependency graph**: on.
- [ ] **Dependabot alerts**: on.
- [ ] **Dependabot security updates**: on. Version updates come from
      `.github/dependabot.yml` (weekly, for uv dependencies, GitHub Actions,
      and Docker images) and need no toggle.
- [ ] **Secret Protection** (secret scanning): on, with **Push protection**
      on.
- [ ] **Private vulnerability reporting**: on. `SECURITY.md` points
      reporters at the advisory form, which only works once this is enabled.
- [ ] **Code scanning → CodeQL analysis**: leave **Default setup** off (not
      configured). `security.yml` runs CodeQL as an advanced setup for
      `python` and `actions`; with default setup also enabled, GitHub rejects
      the advanced workflow's uploads and the job fails. If default setup was
      turned on by accident, switch it off there before the next run.

### 9. OpenSSF Scorecard

- [ ] Nothing to enable. The Scorecard job in `security.yml` needs the
      repository to be public, which it is, and publishes results with OIDC.
      Results appear under **Security → Code scanning** and at
      `https://scorecard.dev/viewer/?uri=github.com/alexjbuck/saltation`.

### 10. Optional

- [ ] **Settings → General → Features → Discussions**: off for now.
      `.github/ISSUE_TEMPLATE/config.yml` disables blank issues and points
      questions at the README. If you enable Discussions later, add a contact
      link for it there and update "Getting help" in `CONTRIBUTING.md`.
- [ ] **Settings → General → Features → Wikis**: off; documentation lives in
      `docs/`.
- [ ] **Settings → Rules → Rulesets → New tag ruleset** targeting `v*`, with
      **Restrict deletions** and **Block force pushes**, so release tags
      cannot be moved.

## Release day

1. Open the PR titled `chore: prepare release <version>` (from the `release`
   branch). Read the body: it is the release notes. Check that the version
   bump matches the change types (a `major` change file must be there for any
   breaking change the `semver` check flagged).
2. Wait for CI on the release PR to go green, including the integration,
   engine, and package jobs. If CI did not run, `RELEASE_TOKEN` is missing or
   expired (setup step 3).
3. Squash-merge the release PR. Keep the default commit message.
4. Watch `release.yml`. It builds the wheel, sdist, and standalone binaries,
   tags `v<version>`, and creates the GitHub release with all of them
   attached. With `PYPI_PUBLISH_ENABLED=true`, it then publishes to TestPyPI,
   smoke-tests that upload, and waits on the `pypi` environment.
5. Approve the `pypi` deployment from the workflow run page.
6. Check the result:
   - <https://github.com/alexjbuck/saltation/releases> shows the tag with the
     wheel, sdist, and a binary for each platform;
   - <https://pypi.org/project/saltation/> shows the new version;
   - `uvx saltation@<version> --version` prints the new version.
7. If a step after tagging fails, fix the cause and re-run `release.yml`
   manually with the existing tag (see [releasing.md](./releasing.md)).

## Triage

Aim to look at new issues and PRs at least weekly.

### New issues

- Every issue arrives with a label from its form (`bug`, `performance`,
  `enhancement`, or `upstream`). Confirm it is the right one.
- For a bug or performance report, try to reproduce it. If it reproduces and
  is small and well specified, add `good first issue` or `help wanted`.
- Close duplicates as **Duplicate**, linking the original.
- Close anything that should have gone through `SECURITY.md` and ask the
  reporter to resubmit privately; edit out any sensitive details.

### Feature requests

Feature requests are where the design discussion happens, so answer them
with a decision, not just acknowledgement:

- **Accept**: say so explicitly in a comment ("Accepted; a PR is welcome")
  and note the agreed SQL or CLI surface and change type. Add `help wanted`
  if you would welcome an outside contributor.
- **Needs design**: ask the specific questions that block a decision.
- **Decline**: close as **Not planned** with the reason. For anything on the
  CLAUDE.md "Out of scope for v1" list, link the list in `CONTRIBUTING.md`.

A feature PR without an accepted issue is closed with a pointer to the
feature form.

### Pull requests

- Bug fix and performance PRs need no prior issue, but must carry a
  regression test or measurements.
- Apply `no-changeset` only when nothing user-facing changed.
- First-time contributors' workflow runs need your approval (setup step 3).
  Read the diff, especially any change under `.github/`, before approving.

### Upstream problems

When saltation hits a bug in PyIceberg, a catalog, or an engine:

1. Open an issue with the upstream form. Title it
   `[upstream: <component>] <summary>` and label it `upstream` plus the
   component (`pyiceberg`, `lakekeeper`, `polaris`, or `gravitino`).
2. Write the body as a draft for the upstream tracker: summary, exact
   versions, minimal reproduction, and saltation's current handling. Mark it
   "Not filed upstream yet" until it is.
3. Make saltation fail clearly in the meantime: an explicit refusal with an
   actionable message, or a `catalog_refuses` entry in
   `tests/fixtures/corpus_effects.py` so the test notices when the catalog
   starts accepting the statement.
4. File upstream after searching for duplicates and re-checking the latest
   release, then link the upstream issue.
5. Close the saltation issue when the upstream fix is released and
   saltation's minimum supported version, or the pinned rig image, includes
   it. Remove the matching `catalog_refuses` entry or workaround in the same
   PR.
