# Cloud Meta-Service — Review Guide

## Transaction Commit Paths

Two paths: `commit_txn_immediately` and `commit_txn_eventually`.

- [ ] Path selection consistent with existing rules: 2PC state, lazy-commit switches, rowset-count threshold, fuzz forcing, `TXN_BYTES_TOO_LARGE` fallback?
- [ ] `commit_txn_immediately` restart on pending lazy-committed txns has clear timeout/termination story?
- [ ] Begin-txn id allocation preserves label-plus-versionstamp pattern?

## RPC Boundary

- [ ] New handlers have `RPC_PREPROCESS` and `RPC_RATE_LIMIT`?
- [ ] `MetaServiceCode` set on every return path?
- [ ] Handlers idempotent under `MetaServiceProxy` retries for `KV_TXN_STORE_*_RETRYABLE`, `KV_TXN_MAYBE_COMMITTED`, `KV_TXN_TOO_OLD`, and `KV_TXN_CONFLICT` (when conflict retry enabled)?

## Cloud MoW

- [ ] Responses tolerate sentinel marks and `TEMP_VERSION_COMMON` that BE must rewrite before use?
- [ ] Commit/publish changes keep version and compaction metadata consistent with BE delete bitmap calculation?
