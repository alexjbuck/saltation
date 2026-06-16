-- Iceberg SQL corpus: schema_alter
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [alter_column_type] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; type widening.
ALTER TABLE foo.t ALTER COLUMN measurement TYPE DOUBLE;

-- [alter_column_comment] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; comment-only ALTER COLUMN.
ALTER TABLE foo.t ALTER COLUMN measurement COMMENT 'unit kb/s';

-- [alter_column_type_comment] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; TYPE and COMMENT together.
ALTER TABLE foo.t ALTER COLUMN measurement TYPE DOUBLE COMMENT 'unit';

-- [alter_column_first] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; reorder to FIRST.
ALTER TABLE foo.t ALTER COLUMN c FIRST;

-- [alter_column_after] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; reorder AFTER.
ALTER TABLE foo.t ALTER COLUMN c AFTER other;

-- [alter_column_set_not_null] expected=apply :: SET NOT NULL (requirement tightening).
ALTER TABLE foo.t ALTER COLUMN id SET NOT NULL;

-- [alter_column_drop_not_null] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; DROP NOT NULL.
ALTER TABLE foo.t ALTER COLUMN id DROP NOT NULL;
