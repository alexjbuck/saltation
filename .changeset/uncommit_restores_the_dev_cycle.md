---
default: minor
---

# Uncommit restores the state before the evolution, and commit refuses a rolled-back current.sql

`cambrian uncommit` now restores every table and view the committed evolution's dev cycle touched (dropping the ones it created) rather than only the last apply's tables. `cambrian commit` refuses when `current.sql` was rolled back after its last apply.
