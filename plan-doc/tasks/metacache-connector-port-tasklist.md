# Task list — port connector hand-rolled caches onto the copied cache framework

Design: [designs/metacache-connector-port-design.md](./designs/metacache-connector-port-design.md)
Scope (user, 2026-07-01): iceberg + paimon together.

- [x] **C1** — `fe-connector-cache` compile against Caffeine **2.9.3** (child-first per-plugin linkage; iceberg
  runs 2.9.3). Verified: build + 20 framework tests green against 2.9.3.
- [ ] **C2** — iceberg latest-snapshot cache → `MetaCacheEntry` adapter (contextual, access-TTL, cap 1000).
- [ ] **C3** — iceberg manifest cache → `MetaCacheEntry` adapter (contextual, no-TTL, cap 100000).
- [ ] **C4** — paimon: add Caffeine 2.9.3 dep + latest-snapshot cache → `MetaCacheEntry` adapter.

Flip-gated (cannot run this session, no cluster): `test_iceberg_table_meta_cache` /
`test_paimon_table_meta_cache` + redeploy classloader smoke check.
