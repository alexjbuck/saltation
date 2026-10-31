# Security policy

## Supported versions

saltation is pre-1.0. Security fixes are released for the latest minor
version only. Upgrade to the newest release before reporting, and confirm the
problem still reproduces there.

| Version | Supported |
| ------- | --------- |
| Latest `0.x` minor release | Yes |
| Older releases | No |

## Reporting a vulnerability

Do not open a public issue, pull request, or discussion for a security
problem.

Report it privately through GitHub's private vulnerability reporting:
<https://github.com/alexjbuck/saltation/security/advisories/new>. This keeps
the report, the discussion, and any fix private until a coordinated release.

If that form is unavailable (the repository owner has to enable private
vulnerability reporting), email the maintainer at
[alexjbuck@gmail.com](mailto:alexjbuck@gmail.com) with "saltation security"
in the subject.

Please include:

- the saltation version (`saltation --version`), Python version, and
  operating system;
- the catalog type and version, and the PyIceberg version, if relevant;
- a description of the issue and its impact;
- steps or a minimal configuration and SQL file that reproduce it, with any
  real credentials removed.

## Response targets

- **Acknowledgement** within 5 business days of the report.
- **Assessment** of severity and affected versions, shared with you as soon
  as it is done.
- **Fix** released within 90 days of the report. Most fixes should take far
  less; if a fix depends on an upstream project, we will tell you and agree
  on a timeline.
- **Coordinated disclosure.** We publish a GitHub security advisory (with a
  CVE where appropriate) when the fixed release is available, and credit the
  reporter unless you ask us not to. Please do not disclose the issue
  publicly before then.

## Scope

saltation is a command-line tool that runs in a developer's shell or a CI
job. Keep these properties in mind when assessing a report:

- **It acts with the credentials it is given.** The `[catalog]` table in
  `saltation.toml` is passed verbatim to `pyiceberg.catalog.load_catalog`.
  saltation can do anything those catalog and storage credentials allow,
  including dropping tables. Restricting what a deploy can touch is the job of
  the catalog's own access control, and the credentials you give saltation
  should be scoped accordingly.
- **Configuration values support `${VAR}` substitution** from the process
  environment, which is the recommended way to supply secrets in CI. Anyone
  who can edit `saltation.toml` can reference any environment variable visible
  to the saltation process and pass it to the catalog client. Treat the
  configuration file, and the `evolutions/` directory it points to, with the
  same trust as other code in the repository.
- **Evolution files are executed.** SQL in `current.sql` and `committed/`
  (including files pulled in by `--! include`) is applied to the catalog. Do
  not run evolutions you have not reviewed.
- **`saltation config show` masks credential-shaped values**, but log output
  from PyIceberg or the catalog client is outside saltation's control.
- **There are no network listeners.** saltation opens outbound connections to
  the configured catalog and object store only. `saltation watch` watches the
  local filesystem; it does not accept connections.

Examples of issues that are in scope:

- a credential or substituted secret leaking into saltation's own output,
  JSON payloads, sidecar tables, or error messages;
- `saltation sync` writing outside the `committed/` directory because of
  data read back from the catalog's sidecar tables;
- a sidecar or checkpoint operation affecting tables outside the ones an
  evolution names;
- a vulnerable dependency pinned in `uv.lock` that saltation actually
  exercises in a way that makes it exploitable.

## Out of scope

Report these to the project concerned rather than to saltation:

- vulnerabilities in REST catalogs (Lakekeeper, Apache Polaris, Apache
  Gravitino, and others), object stores, or query engines (Trino, Spark,
  StarRocks, DuckDB);
- vulnerabilities in PyIceberg, PyArrow, sqlglot, or other dependencies,
  unless saltation's use of them is what makes the issue exploitable;
- the docker-compose stacks under `docker/` and `examples/`, which are local
  test and demo rigs with fixed, non-secret credentials and are not meant to
  be exposed to a network;
- anything that requires an attacker to already control `saltation.toml`,
  the evolution files, or the environment of the saltation process.

If you are unsure whether something is in scope, report it privately anyway.
