# Doris Regression Test Plan Template

## 1. Summary of Change
- **Intent**: switch the temporary file-cache TTL anchor from `newest_write_timestamp` to `tablet_meta->creation_time()` so expiration is stable across write/read/warmup/sync_meta without affecting rowset freshness semantics.
- **Affected modules/files**:
  - `be/src/io/cache/file_cache_expiration.h`
  - `be/src/olap/rowset/rowset_writer_context.h`
  - `be/src/olap/rowset/rowset_reader_context.h`
  - `be/src/olap/rowset/beta_rowset_reader.cpp`
  - `be/src/olap/tablet_reader.cpp`
  - `be/src/cloud/cloud_internal_service.cpp`
  - `be/src/cloud/cloud_tablet.cpp`
  - `be/src/cloud/cloud_warm_up_manager.cpp`
  - `regression-test/suites/cloud_p0/cache/ttl/...`
- **User-provided context**: this is a temporary fix for TTL inconsistency on branch-4.0/3.1. Stability and cache correctness are the priority; changing TTL semantics from latest-write based to tablet-creation based is acceptable for the target dynamic-partition-heavy workloads.

## 2. Risk Analysis
- **Primary risk**: an old tablet may stop placing newly written data into the TTL queue because TTL is now anchored to tablet creation time.
- **Secondary risks**:
  - one remaining path may still calculate expiration from rowset time and reintroduce rename/modify drift
  - `sync_meta` may update segment data but miss inverted index cache entries
  - metrics-based regression may be flaky if cache population is too small
- **Backward compatibility concerns**: file-cache TTL semantics change from “relative to latest rowset write” to “relative to tablet creation time”.

## 3. Test Scope
- **Target suites**: `regression-test/suites/cloud_p0/cache/ttl`
- **Excluded areas**: multi-cluster warmup orchestration, restore/clone end-to-end, non-cloud suites
- **Required environment**: cloud docker regression environment with one alive BE in the target cluster

## 4. Test Matrix
| Scenario | Setup | Action | Expected Result | Signal/Metric |
| --- | --- | --- | --- | --- |
| TTL on a fresh tablet still enters TTL queue | Create table with `file_cache_ttl_seconds`, load/query data soon after create | Load or query to populate file cache | TTL cache is populated | `ttl_cache_size > 0` |
| Late write on an old tablet does not refresh TTL | Create table with short TTL, wait past TTL before the first load on that table | Load/query after waiting | New cache entries are treated as non-TTL | `ttl_cache_size == 0`, `normal_queue_cache_size > 0` |

## 5. Data & Setup
- **Tables/schemas**: simple single-bucket table in cloud cache suite
- **Test data**: generated rows via `INSERT INTO ... SELECT FROM numbers(...)` with a sufficiently wide string payload; avoid `BROKER LOAD` / external object-store dependencies
- **Config toggles**:
  - `enable_evict_file_cache_in_advance=false`
  - `file_cache_enter_disk_resource_limit_mode_percent=99`

## 6. Assertions
- **Result correctness**:
  - TTL queue is used for fresh tablets
  - TTL queue is not used for late writes to old tablets
- **Metrics / logs / profiles**:
  - validate `ttl_cache_size`
  - validate `normal_queue_cache_size`
- **Negative cases**:
  - no reliance on `newest_write_timestamp` refresh behavior

## 7. Implementation Notes
- **Candidate test files**:
  - add `regression-test/suites/cloud_p0/cache/ttl/test_ttl_creation_time_anchor.groovy`
- **Reuse existing utilities**:
  - cache clear HTTP helper
  - metrics scraping via `/brpc_metrics`
  - docker `ClusterOptions` cloud-mode wrapper for a self-contained regression environment
- **Cleanup requirements**:
  - clear file cache before the test
  - drop created tables after the test

## 8. Approval Checklist
- [x] Plan reviewed by user through the request to proceed end-to-end in this turn
- [x] User approved implementation
