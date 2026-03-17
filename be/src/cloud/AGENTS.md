# BE Cloud Module — Review Guide

## Meta-Service RPC Pattern

`retry_rpc` in `cloud_meta_mgr.cpp` wraps BE-to-meta-service RPC error handling.

### Checkpoints

- [ ] New meta-service RPCs reuse `retry_rpc` instead of open-coding retry?
- [ ] `INVALID_ARGUMENT` returns immediately without retry?
- [ ] `KV_TXN_CONFLICT` keeps its separate retry budget from generic retries?
- [ ] Retry count, timeout, and backoff stay consistent with existing config pattern?

## CloudTablet Sync

- [ ] Lock order preserved: `_sync_meta_lock → _meta_lock`?
- [ ] `sync_rowsets()` double-checks local max version before remote work?
- [ ] Stale compaction-count detection checked against latest local counters before applying returned rowsets?
- [ ] `NOT_FOUND` and full re-sync paths clear local version state before rebuilding?
- [ ] `sync_if_not_running()` clears `_rs_version_map`, `_stale_rs_version_map`, and `_timestamped_version_tracker` before re-syncing?
