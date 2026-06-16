-- Iceberg SQL corpus: neg_branching
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [neg_create_branch] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; branching out of scope (Nessie-style workflow excluded).
ALTER TABLE foo.t CREATE BRANCH `audit-branch`;

-- [neg_create_tag] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; tagging out of scope.
ALTER TABLE foo.t CREATE TAG `historical-tag`;

-- [neg_replace_branch] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; replace branch out of scope.
ALTER TABLE foo.t REPLACE BRANCH `audit-branch` AS OF VERSION 4567 RETAIN 60 DAYS;

-- [neg_drop_branch] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; drop branch out of scope.
ALTER TABLE foo.t DROP BRANCH `audit-branch`;

-- [neg_drop_tag] expected=reject :: iceberg.apache.org/docs/latest/spark-ddl; drop tag out of scope.
ALTER TABLE foo.t DROP TAG `historical-tag`;
