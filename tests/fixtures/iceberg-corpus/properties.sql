-- Iceberg SQL corpus: properties
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [set_tblproperties] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; SET TBLPROPERTIES.
ALTER TABLE foo.t SET TBLPROPERTIES ('read.split.target-size' = '268435456');

-- [set_tblproperties_multi] expected=apply :: Multiple keys in SET.
ALTER TABLE foo.t SET TBLPROPERTIES ('a' = '1', 'b' = '2');

-- [unset_tblproperties] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; UNSET TBLPROPERTIES.
ALTER TABLE foo.t UNSET TBLPROPERTIES ('read.split.target-size');

-- [unset_tblproperties_multi] expected=apply :: Multiple keys in UNSET.
ALTER TABLE foo.t UNSET TBLPROPERTIES ('a', 'b', 'c');
