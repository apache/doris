# Task list — port connector hand-rolled caches onto the copied cache framework

Design: [designs/metacache-connector-port-design.md](./designs/metacache-connector-port-design.md)
Scope (user, 2026-07-01): iceberg + paimon together.

- [x] **C1** — `fe-connector-cache` compile against Caffeine **2.9.3** (child-first per-plugin linkage; iceberg
  runs 2.9.3). Verified: build + 20 framework tests green against 2.9.3. Commit `24e4c830aeb`.
- [x] **C2** — iceberg latest-snapshot cache → `MetaCacheEntry` adapter (contextual, access-TTL, cap 1000).
  Commit `0be2679a7ac`. IcebergLatestSnapshotCacheTest 5/5 + IcebergConnectorCacheTest 6/6.
- [x] **C3** — iceberg manifest cache → `MetaCacheEntry` adapter (contextual, no-TTL, cap 100000).
  Commit `bc27505eace`. IcebergManifestCacheTest 4/4 + IcebergScanPlanProviderTest 88/88.
- [x] **C4** — paimon: add Caffeine 2.9.3 dep + latest-snapshot cache → `MetaCacheEntry` adapter.
  Commit `47c4bcc6fd9`. PaimonLatestSnapshotCacheTest 5/5 + PaimonConnectorCacheTest 4/4. Plugin zip verified
  to bundle exactly `caffeine-2.9.3.jar` (no version conflict).

**Verification done:** FULL iceberg module suite green (0 failures), FULL paimon module suite green (0
failures), checkstyle 0 on both modules, connector import gate clean on my files (only the pre-existing HMS
false-positive remains). Clean-room adversarial review run.

Flip-gated (cannot run this session, no cluster): `test_iceberg_table_meta_cache` /
`test_paimon_table_meta_cache` + redeploy classloader smoke check (the ONE thing that proves the plugin-bundled
`MetaCacheEntry` links the plugin's Caffeine correctly end-to-end).
