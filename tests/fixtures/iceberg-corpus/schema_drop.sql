-- Iceberg SQL corpus: schema_drop
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [drop_column_single] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; singular DROP COLUMN (Iceberg extension).
ALTER TABLE foo.t DROP COLUMN c;

-- [drop_column_nested] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; nested DROP COLUMN.
ALTER TABLE foo.t DROP COLUMN point.z;

-- [drop_columns_plural] expected=apply :: Plural DROP COLUMNS.
ALTER TABLE foo.t DROP COLUMNS (a, b);
