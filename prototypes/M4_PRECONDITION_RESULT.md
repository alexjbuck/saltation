# M4 precondition result: rollback prototype

## Verdict

**GO.**

Both prototype scenarios pass against a live Lakekeeper v0.12.2 catalog
(over docker-compose, with rustfs as the warehouse). The atomic
four-pointer rollback via `Transaction._apply()` works as the plan
prescribes. We can build M4 on top of this primitive.

## Environment

- PyIceberg `0.11.1` (from `uv.lock`)
- Lakekeeper `v0.12.2` REST catalog
- rustfs `1.0.0-beta.4` warehouse
- Python `3.12`

## What the prototype exercises

`prototypes/m4_rollback.py` runs two scenarios end-to-end against the
docker rig:

1. **Full mutation roundtrip.** Create `t`, append batches A and B
   (yielding two snapshots), checkpoint at S1 (`snapshot_id`, `schema_id`,
   `spec_id`, `sort_order_id`, `metadata_location`). Then mutate the table
   in four orthogonal ways:

   - `update_schema().add_column("extra", IntegerType(), required=False)`
     -> `schema_id` 0 → 1
   - `update_spec().add_field("name", IdentityTransform(), "name_part")`
     -> `spec_id` 0 → 1
   - `update_sort_order().asc("id", IdentityTransform())`
     -> `sort_order_id` 0 → 1
   - `append(batch_c)` on the new schema -> snapshot advances past S1

   Then a single `Transaction._apply(updates=…, requirements=…)` call
   with the four `Set*Update` records and one `AssertRefSnapshotId` guard.
   Re-load and check all four pointers match the checkpoint. PASS.

2. **Metadata-only rollback.** Same setup but no post-checkpoint
   `append` — only schema/spec/sort-order mutations advance. The
   `current_snapshot()` is still the checkpoint snapshot, and
   `_apply` is asked to "restore" `main` to the same snapshot it
   already points at. The Lakekeeper REST commit handles this as a
   no-op for the snapshot ref while still rotating schema/spec/sort
   defaults. PASS.

Both scenarios drop their table and namespace in a `try/finally`, so
the catalog comes out clean.

## Confirmed import paths (PyIceberg 0.11.1)

All five classes referenced by the plan are reachable at the path the
plan assumes:

```python
from pyiceberg.table.update import (
    SetSnapshotRefUpdate,
    SetCurrentSchemaUpdate,
    SetDefaultSpecUpdate,
    SetDefaultSortOrderUpdate,
    AssertRefSnapshotId,
)
```

## `Transaction._apply` signature (observed)

```python
Transaction._apply(
    self,
    updates: tuple[TableUpdate, ...],
    requirements: tuple[TableRequirement, ...] = (),
) -> Transaction
```

Notes:

- The annotation is `tuple[...]`, not `list[...]`. PyIceberg's
  Pydantic models coerce a list to a tuple in practice, but the
  M4 wrapper should accept and forward tuples to match the API
  contract.
- It returns the same `Transaction`. Inside a `with table.transaction()
  as txn:` block, the `__exit__` is what commits — the call itself only
  *stages* the updates and runs the requirement assertions in-memory.
- The docstring is `"Check if the requirements are met, and applies the
  updates to the metadata."`. Read: it calls `_stage(updates,
  requirements)` and only `commit_transaction()` if `self._autocommit` is
  true (which, inside a `with`, it is not). So the four pointer changes
  ride in *one* REST commit at `__exit__` time. That's the property we
  needed.

## Field-level findings on the four update classes

- `SetSnapshotRefUpdate` accepts either `type="branch"` or
  `type=SnapshotRefType.BRANCH` (Pydantic coerces the string).
- `SetCurrentSchemaUpdate(schema_id=int)`,
  `SetDefaultSpecUpdate(spec_id=int)`,
  `SetDefaultSortOrderUpdate(sort_order_id=int)` are trivial single-int
  records.
- `AssertRefSnapshotId(ref="main", snapshot_id=int | None)`. Use `None`
  to assert the ref does *not* exist; in our rollback the snapshot id is
  always non-None because we required an existing branch state at
  checkpoint time.

## API drift relative to the plan's pseudocode

One small API drift in the *setup* path (not in the rollback primitive
itself):

- `UpdateSortOrder.asc(source_column_name, transform, null_order=…)` in
  PyIceberg 0.11.1 requires an explicit `transform` argument. The plan's
  pseudocode wrote `uso.asc("id")`; we pass `uso.asc("id",
  IdentityTransform())`. The right transform to use for an unmodified
  column sort is `IdentityTransform()`.

This is mutation-side, not rollback-side, so it doesn't impact M4 — but
note it for any cambrian code that exercises sort orders in tests.

## Ergonomic notes for the M4 wrapper

A few observations on what the `src/cambrian/iceberg/txn.py` wrapper
should look like, based on building the prototype:

1. **Use a tuple, not list.** Match the signature literally.

2. **Refresh-before-rollback is necessary.** PyIceberg caches table
   metadata aggressively. Between mutating with `update_schema()`,
   `update_spec()`, `update_sort_order()` and committing, you should
   `catalog.load_table(ident)` to refresh the in-memory view. The
   wrapper itself should accept a fresh `Table` from the caller (don't
   try to refresh internally — caller knows when reads should happen).

3. **`AssertRefSnapshotId` is the only requirement we need.** It's
   sufficient as a concurrent-writer guard at rollback time. We do *not*
   need to assert schema_id / spec_id / sort_order_id because we're
   restoring them unconditionally; if a concurrent writer rotated them
   between our checkpoint and our rollback, the `main`-snapshot guard
   will catch the conflict first (it advances on the same commit path).

4. **The transaction wrapper is the right boundary, not `_apply`.**
   The pattern is:

   ```python
   with table.transaction() as txn:
       txn._apply(updates=…, requirements=…)
   ```

   `_apply` is private, but the `with` block is public. The cambrian
   wrapper should expose a `restore_pointers(table, cp,
   current_main_snapshot_id) -> None` that takes a `Checkpoint`
   dataclass and the expected current-main snapshot id, builds the
   four updates and one requirement inside, and runs the `with` block.
   Single function, type-checked at the boundary, easy to file
   upstream for a public `Transaction.apply()` later (see CLAUDE.md).

5. **Empty-table case.** `current_snapshot()` is `None` before any
   append. The plan's checkpoint primitive should refuse to checkpoint
   an empty table — the rollback semantics aren't useful there
   (there's nothing to roll back to). The prototype's
   `_capture_checkpoint` raises in this case; mirror in M4.

6. **Metadata-only commits are cheap and safe.** Scenario 2 confirms
   that asking the REST catalog to set `main` to the snapshot id it
   already has is accepted (Lakekeeper does the right thing). This
   means M4 doesn't need a special "no snapshot changed" code path —
   the same primitive works whether or not the snapshot moved.

## Lakekeeper-side observations

No errors in `docker compose logs lakekeeper` during the test runs.
The `/v1/.../commit` endpoint accepted the four-update payload in a
single round trip in both scenarios. Two metadata.json files are
written per rollback (one for the staged updates, one for the
post-commit state), as expected.

## Files

- `prototypes/m4_rollback.py` — the prototype script. Self-contained,
  not imported by anything in `src/cambrian/`. Kept as a reference for
  future readers and as a re-runnable smoke check for the rollback
  primitive.
- `prototypes/M4_PRECONDITION_RESULT.md` — this document.
