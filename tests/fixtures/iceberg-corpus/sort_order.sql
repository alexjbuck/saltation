-- Iceberg SQL corpus: sort_order
-- Generated from tests/fixtures/iceberg_corpus.py (source of truth).
-- Each statement is tagged with its corpus id and expected verdict.

-- [wob_bare] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; canonical WRITE ORDERED BY (no parens, comma list).
ALTER TABLE foo.t WRITE ORDERED BY category, id;

-- [wob_paren] expected=apply :: Parenthesised form (cambrian's existing tests use this).
ALTER TABLE foo.t WRITE ORDERED BY (category, id);

-- [wob_asc_desc] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; explicit directions.
ALTER TABLE foo.t WRITE ORDERED BY category ASC, id DESC;

-- [wob_nulls] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; NULLS FIRST/LAST.
ALTER TABLE foo.t WRITE ORDERED BY category ASC NULLS LAST, id DESC NULLS FIRST;

-- [wob_transform] expected=apply :: Transform-in-sort variant.
ALTER TABLE foo.t WRITE ORDERED BY bucket(16, id);

-- [wob_locally] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; WRITE LOCALLY ORDERED BY.
ALTER TABLE foo.t WRITE LOCALLY ORDERED BY category, id;

-- [wob_distributed] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; WRITE DISTRIBUTED BY PARTITION.
ALTER TABLE foo.t WRITE DISTRIBUTED BY PARTITION;

-- [wob_distributed_locally] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; distributed + locally ordered.
ALTER TABLE foo.t WRITE DISTRIBUTED BY PARTITION LOCALLY ORDERED BY category, id;

-- [wob_unordered] expected=apply :: iceberg.apache.org/docs/latest/spark-ddl; WRITE UNORDERED.
ALTER TABLE foo.t WRITE UNORDERED;
