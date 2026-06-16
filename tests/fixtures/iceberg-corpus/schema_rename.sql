-- Iceberg SQL corpus: schema_rename
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [rename_column] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; RENAME COLUMN.
ALTER TABLE foo.t RENAME COLUMN data TO payload;

-- [rename_column_nested] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; nested RENAME COLUMN.
ALTER TABLE foo.t RENAME COLUMN location.lat TO latitude;
