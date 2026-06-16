-- Iceberg SQL corpus: rename_table
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [rename_table] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; ALTER TABLE ... RENAME TO.
ALTER TABLE foo.t RENAME TO foo.t2;
