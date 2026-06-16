-- Your in-flight migration. Edit this file during dev; `cambrian apply`
-- (or `cambrian watch`) re-runs it idempotently. When you're happy with
-- the state, `cambrian commit -m "<message>"` freezes it into
-- `committed/NNNN_<slug>.sql`.
--
-- The cardinal rule: every statement must be re-runnable. Use IF NOT
-- EXISTS, IF EXISTS, and explicit ALTER paths.

CREATE NAMESPACE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.users (
  id    BIGINT,
  email STRING
) USING iceberg;
