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

- [ ] MoW tables: `VerticalSegmentWriterOptions::enable_unique_key_merge_on_write` set to `true` on every path?

## Rowset Segment Position and Physical ID

A segment's position inside a rowset and its physical segment ID are different concepts.

- Segment-list rowset layout is currently supported only in cloud mode. Cloud-mode code must
  handle both explicit `segment_ids` and legacy rowsets whose empty `segment_ids` implies
  contiguous physical IDs in `[0, num_segments())`.
- Local-mode rowsets still use contiguous physical segment IDs. Local-only metadata and workflows,
  such as `RemoteRowsetGcPB`, `BinlogMetaEntryPB`, local binlog files, and local storage migration,
  intentionally do not persist `segment_ids`.
- A local-only path may rely on position equaling physical ID only when that mode restriction is an
  established invariant and is explicit in the surrounding code or comment. Do not carry that
  assumption into shared or cloud-mode code.
- `Rowset::num_segments()` and `RowsetMeta::num_segments()` return a segment count, not an upper
  bound for physical segment IDs.
- Physical segment IDs may be nonzero or non-contiguous when `segment_ids` is present.
- Never assume that a position in `[0, num_segments())` is also the physical segment ID.
- Prefer `for (auto segment : rowset->segments())` or
  `for (auto segment : rowset_meta->segments())` when traversing rowset segments.
- Use `segment.pos()` only for position-aligned metadata arrays, such as `num_segment_rows`,
  `segments_key_bounds`, `segments_file_size`, and `inverted_index_file_info`.
- Use `segment.id()` for physical identity, including segment and index file paths, cache keys,
  delete bitmap keys, `RowLocation`, RPC fields, and persisted segment IDs.
- When only a position is available, obtain the physical ID through `rowset->segment(pos).id()`,
  `rowset_meta->segment(pos).id()`, or `rowset_segment_id(rowset_meta_pb, pos)` for raw protobuf
  metadata.
- A loop over `[0, num_segments())` is allowed only when the loop variable represents a position.
  Name it `pos`, `segment_pos`, or `segment_index`, never `segment_id`.
- Tests for code that consumes physical segment IDs should use nonzero IDs and, where applicable,
  non-contiguous IDs.

### Checkpoints

- [ ] Does cloud-mode code support both explicit `segment_ids` and the legacy empty-list fallback?
- [ ] If code relies on contiguous IDs, is it guaranteed and clearly documented to be local-only?
- [ ] Does any code assume that segment position equals physical segment ID?
- [ ] Are file paths, cache keys, delete bitmap keys, and row locations built from `segment.id()`?
- [ ] Are position-aligned metadata arrays accessed with `segment.pos()`?
- [ ] Does raw protobuf code use `rowset_segment_id()` instead of treating the position as an ID?
