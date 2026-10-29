## 0.1.1 (2026-10-29)

### Features

#### Renamed from cambrian to saltation

The package, the `saltation` command, `import saltation`, the config file (`saltation.toml`), the default sidecar namespace (`_saltation`) and the snapshot tag prefixes (`saltation.cp.*`, `saltation.committed.*`) all use the new name; `cambrian` was already taken on PyPI.

#### Reset and rollback restore everything current.sql touched

`saltation apply --reset` and `saltation rollback` now return every table and view that any version of `current.sql` touched to how it stood before `current.sql` was first applied, then (for reset) run the latest text once. After any sequence of edits the catalog is the committed state plus the latest `current.sql`. Previously only the last cycle's tables rolled back, so earlier edits leaked through from the third reset on.

- Tables and views created by `current.sql` are dropped; table properties and view versions are restored; a table that had no snapshot is emptied again.
- Reset refuses, before touching anything, when it can't restore: a table was dropped or renamed, or its format version was upgraded (Iceberg can't downgrade).
- Re-running the same text after a rollback applies it again instead of reporting it unchanged.
- The sidecar moves to version 2 (three new optional `table_states` columns), migrated automatically.

#### Tested against Lakekeeper, Apache Polaris and Apache Gravitino

The full integration suite runs against each catalog's latest stable release on every PR. Catalog-specific refusals are documented in the effect corpus.

#### Uncommit restores the state before the evolution, and commit refuses a rolled-back current.sql

`saltation uncommit` now restores every table and view the committed evolution's dev cycle touched (dropping the ones it created) rather than only the last apply's tables. `saltation commit` refuses when `current.sql` was rolled back after its last apply.

### Fixes

#### INSERT into bucket/truncate-partitioned tables works; unsupported writes fail clearly

saltation now depends on `pyiceberg[pyiceberg-core]`, which PyIceberg needs to write to tables partitioned by `bucket` or `truncate`. INSERT and DELETE on a format v3 table, or on a table partitioned more than once on the same column, now fail with an explicit error instead of a raw PyIceberg exception (both are PyIceberg 0.12 limitations).

#### DROP and REPLACE PARTITION FIELD resolve the field like Spark

A transform call (`bucket(16, id)`) matches the field with that source column and transform, a bare column name means `identity(column)` whatever the field is called, and any other bare name is a partition field name. `REPLACE PARTITION FIELD` now removes and adds in one spec update and fails when the field to replace isn't in the spec, instead of adding the new field and reporting success.

#### Recorded SQL matches the statement that ran

Every saltation extension (partition fields, write order and distribution, identifier fields, UNSET TBLPROPERTIES, ALTER COLUMN position, namespace properties) now renders back to SQL, so events and `--json` output show the real statement. Fixed renderings that lost or changed meaning: `ALTER COLUMN ... TYPE ... COMMENT ...` dropped the type, `TIME` columns rendered as `TIMESTAMP`, `truncate(W, col)` lost its column, and `CREATE NAMESPACE ... WITH PROPERTIES` rendered unparseable SQL.

#### View bodies in another dialect keep their own string rules

With `[views].source_dialect` set (for example to `trino`), saltation split scripts into statements using Spark's string rules, where a backslash escapes the next character. A Trino view body such as `REPLACE(path, '\', '/')` then failed to parse, or could be split in the wrong place. The view body is now read with its own dialect's quoting rules, and a script saltation can't tokenize at all is reported as a dispatch error instead of a raw tokenizer exception.

#### Replacing a view with an earlier body works on Lakekeeper

Replacing a view back to a body it had before (A → B → A) failed on Lakekeeper with a 503. saltation now points the view at the matching earlier version instead of re-adding it, which works on every catalog. Replacing a view without changing its columns also works on Java-based catalogs (Polaris, Gravitino).
