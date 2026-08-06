# BE Storage Module — Review Guide

## Tablet Locks

| Lock | Type | Role |
|------|------|------|
| `_meta_lock` | `shared_mutex` | Version maps and tablet metadata visibility |
| `_rowset_update_lock` | `mutex` | Serializes delete bitmap updates (publish / MoW) |
| `_base_compaction_lock` | `mutex` | Serializes base compaction |
| `_cumulative_compaction_lock` | `mutex` | Serializes cumulative compaction |

### Checkpoints

- [ ] `_rs_version_map` and `_stale_rs_version_map` accessed under `_meta_lock` with correct shared/exclusive mode?
- [ ] Tablet-meta mutations under exclusive `_meta_lock`?
- [ ] Nested locking follows established preconditions? (`update_delete_bitmap_without_lock()` requires both `_rowset_update_lock` and `_meta_lock`)
- [ ] TxnManager lock order: `_txn_lock → _txn_map_lock`?

## Rowset and Version Lifecycle

- [ ] `add_rowset()` / `modify_rowsets()` under exclusive `_meta_lock`?
- [ ] Version continuity preserved, or intentional same-version replacement used correctly?
- [ ] Same-version replacement: old rowset moved to unused-rowset tracking before new becomes authoritative?
- [ ] Reader/rowset code respects split lifetime: `shared_ptr` ownership + reader `acquire()` / `release()`?
- [ ] `StorageEngine::_unused_rowsets` deletable only when `use_count() == 1`?

## Delete Bitmap (MoW)

- [ ] Cloud mode: `TEMP_VERSION_COMMON` and sentinels replaced before bitmap use?
- [ ] Bitmap calculation serialized under `_rowset_update_lock`?
- [ ] Compaction bitmap uses latest compaction counters, not stale snapshots?

## Segment Writing

- [ ] MoW tables: `SegmentWriterOptions::enable_unique_key_merge_on_write` set to `true` on every path?
