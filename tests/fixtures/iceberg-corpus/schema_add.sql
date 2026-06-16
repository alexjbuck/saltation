-- Iceberg SQL corpus: schema_add
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [add_column_single] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; singular ADD COLUMN.
ALTER TABLE foo.t ADD COLUMN c INT;

-- [add_column_comment] expected=apply :: ADD COLUMN with per-column COMMENT.
ALTER TABLE foo.t ADD COLUMN c STRING COMMENT 'docs';

-- [add_column_not_null] expected=apply :: ADD COLUMN with NOT NULL.
ALTER TABLE foo.t ADD COLUMN c INT NOT NULL;

-- [add_column_first] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; FIRST positioning.
ALTER TABLE foo.t ADD COLUMN c INT FIRST;

-- [add_column_after] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; AFTER positioning.
ALTER TABLE foo.t ADD COLUMN c INT AFTER name;

-- [add_column_nested] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; nested-field add (dotted path).
ALTER TABLE foo.t ADD COLUMN point.z DOUBLE;

-- [add_columns_plural] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; plural ADD COLUMNS list.
ALTER TABLE foo.t ADD COLUMNS (a INT, b STRING, c BIGINT);

-- [add_columns_plural_comment] expected=apply :: Plural ADD COLUMNS with comment.
ALTER TABLE foo.t ADD COLUMNS (a INT COMMENT 'x', b STRING);
