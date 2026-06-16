-- Iceberg SQL corpus: neg_procedures
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [neg_call_rewrite_data_files] expected=reject :: iceberg.apache.org/docs/latest/spark-procedures; table maintenance procedure out of scope.
CALL cat.system.rewrite_data_files('db.t');

-- [neg_call_expire_snapshots] expected=reject :: iceberg.apache.org/docs/latest/spark-procedures; expire_snapshots out of scope.
CALL cat.system.expire_snapshots(table => 'db.t', older_than => TIMESTAMP '2021-06-30 00:00:00.000');

-- [neg_call_rollback_to_snapshot] expected=reject :: iceberg.apache.org/docs/latest/spark-procedures; rollback procedure out of scope.
CALL cat.system.rollback_to_snapshot('db.t', 1);

-- [neg_call_rewrite_manifests] expected=reject :: iceberg.apache.org/docs/latest/spark-procedures; rewrite_manifests out of scope.
CALL cat.system.rewrite_manifests('db.t');
