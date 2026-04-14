# Unit Test Plan

## Context
- Change source: current branch `ttl-filecache-fix`, replacing the temporary `newest_write_timestamp` anchor with `tablet_meta->creation_time()` for file-cache TTL expiration.
- Intent: keep file-cache TTL expiration stable across write/read/warmup/sync_meta without changing `newest_write_timestamp` semantics.

## Scope summary
- Files/modules touched:
  - `be/src/io/cache/file_cache_expiration.h`
  - `be/src/olap/rowset/rowset_writer_context.h`
  - `be/src/olap/rowset/rowset_reader_context.h`
  - `be/src/olap/rowset/beta_rowset_reader.cpp`
  - `be/src/olap/tablet_reader.cpp`
  - `be/src/cloud/cloud_tablet.cpp`
  - `be/src/cloud/cloud_internal_service.cpp`
  - `be/src/cloud/cloud_warm_up_manager.cpp`
  - `be/src/olap/tablet.cpp`
  - `be/src/cloud/cloud_tablet.cpp`
- Behaviors changed:
  - File-cache TTL expiration is now computed from a stable tablet-level base timestamp.
  - `newest_write_timestamp` remains a rowset freshness field and is no longer repurposed for file-cache TTL.
  - Pending rowset initialization no longer persists `newest_write_timestamp` early for TTL alignment.

## Test cases
1. `calc_file_cache_expiration_time_returns_zero_for_invalid_or_expired_base`
   - Purpose: verify clamp behavior is still correct for invalid base, expired base, and overflow.
   - Setup/fixtures: direct helper test.
   - Inputs: `base_timestamp <= 0`, expired `base_timestamp + ttl`, overflow edge.
   - Expected outcomes: helper returns `0`.
   - Mocks/stubs: none.
2. `calc_file_cache_expiration_time_uses_base_timestamp`
   - Purpose: verify the helper preserves the stable anchor semantics.
   - Setup/fixtures: direct helper test.
   - Inputs: valid `base_timestamp`, positive `ttl_seconds`.
   - Expected outcomes: helper returns `base_timestamp + ttl_seconds`.
   - Mocks/stubs: none.
3. `rowset_writer_context_uses_file_cache_base_timestamp`
   - Purpose: ensure writer-side cache expiration uses `file_cache_base_timestamp` rather than `newest_write_timestamp`.
   - Setup/fixtures: instantiate `RowsetWriterContext`.
   - Inputs: different values for `file_cache_base_timestamp` and `newest_write_timestamp`.
   - Expected outcomes: `get_file_writer_options().file_cache_expiration` follows the base timestamp.
   - Mocks/stubs: none.

## Coverage matrix (optional)
- Helper clamp/overflow -> test cases 1 and 2
- Writer anchor propagation -> test case 3

## Risks & gaps
- Read/warmup/sync_meta integration is hard to unit-test directly without large fixtures; that coverage will be carried by regression tests and targeted code inspection.
- No direct unit coverage for clone/restore semantics in this patch.

## Approvals
- [x] Plan approved by user through the request to proceed end-to-end in this turn
