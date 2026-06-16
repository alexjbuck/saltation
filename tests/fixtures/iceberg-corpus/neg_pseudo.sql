-- Iceberg SQL corpus: neg_pseudo
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [neg_optimize_write] expected=reject :: Not real Iceberg-Spark SQL; from a user's example. Parses to Command.
ALTER TABLE foo.t OPTIMIZE WRITE;

-- [neg_expire_snapshots_pseudo] expected=reject :: Not real Iceberg-Spark SQL; real path is CALL expire_snapshots. Parses to Command.
ALTER TABLE foo.t EXPIRE SNAPSHOTS BEFORE NOW();
