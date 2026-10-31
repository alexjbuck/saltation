<!--
Thank you for contributing. Bug fixes and performance improvements are welcome without an issue.
Features need an issue the maintainer has accepted before a PR. See CONTRIBUTING.md.
-->

## Type of change

- [ ] Bug fix
- [ ] Performance improvement
- [ ] Feature (accepted issue: #)
- [ ] Documentation
- [ ] CI, tooling, or tests only

## Summary

<!-- What changes for users, and why. Link the issue this fixes, if any (for example "Fixes #123"). -->

## How this was tested

- [ ] Unit tests (`uv run pytest tests/unit -n auto`)
- [ ] Property tests with `HYPOTHESIS_PROFILE=ci`
- [ ] Integration tests against: <!-- lakekeeper, polaris, gravitino -->
- [ ] Engine interoperability tests against: <!-- trino, spark, starrocks, duckdb -->
- [ ] Corpus entries added in `tests/fixtures/iceberg_corpus.py` and `tests/fixtures/corpus_effects.py` (new SQL surface)

<!-- For a bug fix, name the regression test. For a performance change, paste the before and after measurements and how you took them. -->

## Change file

- [ ] Added `.changeset/<name>.md` with change type: <!-- major, minor, or patch -->
- [ ] Not user-facing; requesting the `no-changeset` label because: <!-- reason -->

## Public contract impact

<!-- See docs/compatibility.md. -->

- [ ] None
- [ ] CLI help, exit-code, or JSON snapshots updated (`uv run pytest tests/unit/test_cli_contract.py --snapshot-update`) and the diff is intended
- [ ] Breaking change: the change file is `major` and the PR summary explains the upgrade path for users

## Checklist

- [ ] `uv run ruff check src tests` and `uv run ruff format --check src tests` pass
- [ ] `uv run ty check src tests` passes
- [ ] Evolutions and examples stay idempotent; reset is not presented as the fix
- [ ] No private PyIceberg API used outside `src/saltation/iceberg/txn.py`
- [ ] Code comments explain only non-obvious reasons; docstrings are short
- [ ] User-facing errors are `SaltationError` subclasses with a one-line, actionable message
- [ ] Docs updated (`README.md`, `docs/`) where behaviour changed
