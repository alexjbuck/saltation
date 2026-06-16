-- Iceberg SQL corpus: neg_create
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [neg_ctas] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; CTAS out of scope (needs query engine).
CREATE TABLE foo.t USING iceberg AS SELECT * FROM bar;

-- [neg_create_or_replace_table] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; CREATE OR REPLACE TABLE out of scope (not idempotent-safe).
CREATE OR REPLACE TABLE foo.t (id INT) USING iceberg;

-- [neg_replace_table_as_select] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; REPLACE TABLE AS SELECT out of scope.
REPLACE TABLE foo.t USING iceberg AS SELECT * FROM bar;
