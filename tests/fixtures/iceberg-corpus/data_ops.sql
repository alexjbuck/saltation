-- Iceberg SQL corpus: data_ops
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [insert_values_single] expected=apply :: iceberg.apache.org/docs/latest/spark-writes; INSERT VALUES single row.
INSERT INTO foo.t VALUES (1, 'alice');

-- [insert_values_multi] expected=apply :: iceberg.apache.org/docs/latest/spark-writes; INSERT VALUES multi-row.
INSERT INTO foo.t VALUES (1, 'alice'), (2, 'bob');

-- [insert_values_null] expected=apply :: INSERT VALUES with NULL.
INSERT INTO foo.t VALUES (1, NULL);

-- [delete_where] expected=apply :: iceberg.apache.org/docs/latest/spark-writes; DELETE FROM ... WHERE — feasible via PyIceberg delete().
DELETE FROM foo.t WHERE id = 1;

-- [delete_where_predicate] expected=apply :: DELETE with compound predicate.
DELETE FROM foo.t WHERE category = 'x' AND id > 10;
