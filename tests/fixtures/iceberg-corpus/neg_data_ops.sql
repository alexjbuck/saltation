-- Iceberg SQL corpus: neg_data_ops
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [neg_insert_select] expected=reject :: iceberg.apache.org/docs/latest/spark-writes; INSERT ... SELECT out of scope (needs query engine).
INSERT INTO foo.t SELECT * FROM bar;

-- [neg_update] expected=reject :: iceberg.apache.org/docs/latest/spark-writes; UPDATE out of scope.
UPDATE foo.t SET k = 1 WHERE id = 2;

-- [neg_merge] expected=reject :: iceberg.apache.org/docs/latest/spark-writes; MERGE out of scope.
MERGE INTO foo.t USING bar.s ON foo.t.id = bar.s.id WHEN MATCHED THEN UPDATE SET k = 1 WHEN NOT MATCHED THEN INSERT (id, k) VALUES (bar.s.id, 0);
