# Cloud Recycler — Review Guide

- [ ] Mark-before-delete two-phase flow preserved: mark `is_recycled` first, delete physical data in later round?
- [ ] Abort-before-delete aligned with origin: load rowsets abort txns, compaction/schema-change rowsets abort jobs?
- [ ] Packed files: fix metadata → delete object storage → reread KV → remove KV only if still recyclable?
- [ ] Conflict/retry paths idempotent and restart-safe? (Recycler phases often fail fast, not best-effort)
