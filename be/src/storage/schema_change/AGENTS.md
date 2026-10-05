# Schema Change — Review Guide

## Strategy Selection

Three strategies: `LinkedSchemaChange` (metadata-only), `VSchemaChangeDirectly` (rewrite, no sort), `VLocalSchemaChangeWithSorting` (full rewrite with sort).

- [ ] Request classification flows through the existing parse-and-dispatch path?

## Lock Order

Required: `base_tablet push_lock → new_tablet push_lock → base_tablet header_lock → new_tablet header_lock`

- [ ] Four-level order preserved?

## MoW Delete Bitmap Flow

Staged flow: (1) dual-write rowsets skip immediate bitmap calc → (2) converted rowsets catch up bitmaps → (3) new publish blocked during incremental catch-up → (4) state switches to `TABLET_RUNNING`.

- [ ] Four-step ordering preserved exactly?
- [ ] Base and cumulative compaction locks held before MoW delete bitmap calculation?

## Dropped-Column Trap

- [ ] `return_columns` derived before dropped columns merged? (Dropped columns needed for delete-predicate evaluation)
- [ ] `ignore_schema_change_check` stays an explicit escape hatch, not normal production behavior?
