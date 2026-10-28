---
default: minor
---

# Reset and rollback restore everything current.sql touched

`cambrian apply --reset` and `cambrian rollback` now return every table and view that any version of `current.sql` touched to how it stood before `current.sql` was first applied, then (for reset) run the latest text once. After any sequence of edits the catalog is the committed state plus the latest `current.sql`. Previously only the last cycle's tables rolled back, so earlier edits leaked through from the third reset on.

- Tables and views created by `current.sql` are dropped; table properties and view versions are restored; a table that had no snapshot is emptied again.
- Reset refuses, before touching anything, when it can't restore: a table was dropped or renamed, or its format version was upgraded (Iceberg can't downgrade).
- Re-running the same text after a rollback applies it again instead of reporting it unchanged.
- The sidecar moves to version 2 (three new optional `table_states` columns), migrated automatically.
