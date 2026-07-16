# Hive connector-owned cache — clean-room adversarial re-review (2026-07-10)

> Scope: the 6 dormant commits `f742651990d`(C-a) `4fe55d88fab`(C-b) `7b05df6e55e`(C-c)
> `7c0ee1ffb2a`(C-d) `7bf90a7fe3c`(C-e) `12e0c9177c2`(C-f) — the Hive connector scan-side cache.
> Method: 9 independent clean-room reviewers (blind to the design rationale) → adversarial refutation of
> every finding → cross-check vs the signed-off design conclusions. Workflow `wf_124cb0a7-6bb`.
> Result: 7 findings raised, **2 survived** as confirmed, 5 refuted to nit/clean.

## Verdict

**Sound to carry into the cutover, with one recommended fix.** No blocker, no dormancy/byte-neutrality
breakage that affects a live connector, no TCCL or classloader hazard. The two survivors are both
behavior *coarsenings* that were consciously accepted in the design doc — but the review shows one of
them (FileSystem.get) rests on a **mitigation that does not actually hold**, so it is worth re-opening.

## Resolution (2026-07-10, user-approved)

Both confirmed findings were fixed (user chose fix-now for both):
- **Finding #1 (FileSystem.get)** → `fda344e6022` — restored the legacy blast-radius distinction: a
  systemic `FileSystem.get` failure fails loud (plain `DorisConnectorException`, not skipped); a local
  `listStatus` failure stays skip-with-warn (new `HiveDirectoryListingException`). Tests +6.
- **Finding #2 (REFRESH DATABASE)** → `cdc837563a7` — added a generic `Connector.invalidateDb(db)` SPI
  (no-op default) and wired `RefreshManager.refreshDbInternal` to it; `HiveConnector.invalidateDb` drops
  both cache layers (`CachingHmsClient.flushDb` + `HiveFileListingCache.invalidateDb`) for every table in
  the db. Tests +2. **This upgrades finding #2 from "accept-deferred" to fixed.**
- **hudi byte-neutrality nit** → recommended (not yet applied): mark `fe-connector-cache`
  `<optional>true</optional>` in `fe-connector-hms/pom.xml` so the unused, Caffeine-less cache jar stops
  flowing into the hudi plugin zip. Awaiting user go-ahead (benign now; removes a future NoClassDefFound
  trap the day hudi reuses the caching client).

## Confirmed findings (survived adversarial verification)

### 1. FileSystem.get failure now silently skips partitions (was fail-loud) — recommend fix
- **Where:** `HiveFileListingCache.listFromFileSystem` wraps *both* `FileSystem.get` and `listStatus` in
  one `try → DorisConnectorException`; `HiveScanPlanProvider.listAndSplitFiles` catches that and
  skips-the-partition-with-a-warning.
- **Legacy (pre-cache):** `FileSystem.get` ran *outside* the inner try → its `IOException` propagated and
  the caller re-threw it as `DorisConnectorException` = **fail loud**. Only `listStatus` failures were
  skipped-with-warn. So legacy drew a deliberate line: *storage-init failure = loud; one unreadable
  partition dir = tolerate.*
- **Now:** both are skipped. A `FileSystem.get` failure is **systemic** (all partitions of a table share
  one scheme+authority → it fails for all or none), so a broken storage config makes the scan **silently
  return empty/partial results with no error**, where legacy failed the query loudly.
- **The documented mitigation does not hold.** The design doc accepts this because "a broken storage
  config still fails loud via the row-count estimate." Verified false: `estimateDataSize`
  (`HiveConnectorMetadata.java:771`) **catches `RuntimeException` and returns -1** — it degrades silently,
  never throws to the user; and for a table that already has HMS stats the estimate path is not even
  invoked. So nothing surfaces the error.
- **Verify verdict:** CONFIRMED, regression-vs-legacy = true. Severity medium (silent wrong *results*, not
  a crash; narrow trigger = storage misconfig).
- **Suggested fix:** restore the legacy distinction — let a `FileSystem.get` (FS-resolution) failure
  propagate loud while a per-directory `listStatus` failure stays skip-with-warn. (Options in the handoff.)

### 2. REFRESH DATABASE does not drop the connector-owned caches — accept-deferred (documented)
- **Where:** `RefreshManager.refreshDbInternal → ExternalDatabase.resetMetaToUninitialized →
  ExtMetaCacheMgr.invalidateDb`. There is no `Connector.invalidateDb` SPI, so for a flipped hive catalog
  `REFRESH DATABASE` drops the fe-core schema cache but **not** the connector's partition/file caches.
- Legacy `REFRESH DATABASE` dropped the engine-side caches for the whole db; post-flip it won't → a user
  who runs `REFRESH DATABASE` expecting fresh data across the db still sees stale partition/file listings
  until TTL / `REFRESH TABLE` / `REFRESH CATALOG`.
- **Verify verdict:** CONFIRMED, regression-vs-legacy = true. Severity medium.
- **Design status:** explicitly signed off as "accept per-table/all coverage; a db-level verb is
  Model-B-adjacent" (§4.5). A `Connector.invalidateDb` SPI + fe-core wiring is genuinely event-Model-B
  territory, not this step. **Recommend: keep deferred, but document loudly as a known post-flip gap**
  (covered in the interim by `REFRESH CATALOG`).

## Refuted / clean (what the review actively verified as correct)

- **§2.6 fe-core last-modified branch (C-f) — CLEAN.** The two sharp risks were examined with line-level
  evidence and resolved as **legacy parity**, not new bugs:
  - *Monotonicity on partition drop:* a decreasing max `transient_lastDdlTime` makes
    `Dictionary.hasNewerSourceVersion` throw in **both** the new and the legacy
    `HMSExternalTable.getNewestUpdateVersionOrTime` (`:1060`, `max(HivePartition::getLastModifiedTime)`) —
    `lastDdlMillis` is byte-parity with it. Pre-existing property of HMS, the flipped value **equals**
    legacy. (Worth listing as a known-issue for the flip e2e, not a regression.)
  - *NPE surface:* `pin.getConnectorSnapshot()` is provably never null — `materializeLatest` builds a
    non-null connector snapshot on every path (`beginQuerySnapshot.orElseGet(emptySnapshot)`, both degraded
    branches use `emptySnapshot()`, range-view passes it through).
  - *Dormancy:* the only `lastModifiedFreshness(true)` in the whole tree is `HiveConnectorMetadata:1014`;
    paimon/iceberg/empty pins leave it false; SPI_READY excludes hms/hive → no live pin is flagged. The new
    line is one boolean read for live connectors; the pre-change max/range paths are byte-unchanged.
- **CachingHmsClient decorator (C-b) — CLEAN.** All 25 SPI methods enumerated: exactly the 4 scan reads
  cached, 21 pass-through; no write/DDL/txn cached; every must-be-fresh read (`tableExists`,
  `getPartition`-by-values, `getValidWriteIds`, `partitionExists`, `list*`) is pass-through. Key
  equals/hashCode correct; list-key order-sensitivity is a non-issue because every call-site passes
  deterministic HMS-ordered or singleton lists. `flush(db,table)` reaches all 4 key types; returned list
  containers are only ever read-only-iterated by consumers (traced) → shared-reference caching can't
  corrupt. `getTableColumnStatistics` is newly cached (legacy did a raw RPC) but under the same TTL + REFRESH
  invalidation as the existing fe-core `StatisticsCache` → no new staleness window.
- **TCCL / classloader — CLEAN.** The listing loader runs on the calling (TCCL-pinned) thread; with
  `autoRefresh=false` the `ForkJoinPool.commonPool()` refresh executor is never used to run a loader off
  the pinned thread. Cache holds only JDK/plugin types → no cross-loader ClassCast.
- **Caffeine single-version packaging (C-a) — CLEAN.** Mirrors the paimon pattern; hive self-bundles exactly
  one Caffeine 2.9.3 child-first; no leak onto fe-core.
- **New tests are non-vacuous** — the three "untested tolerance" worries were refuted (the guards + degrade
  paths are in fact covered / present at HEAD); the freshness/neutrality tests distinguish hit-vs-reload via
  call-count and a `verify(never())` probe-gate.

## Minor / cleanliness item (low, optional hardening)

- **hudi plugin zip gains an unused, Caffeine-less `fe-connector-cache` jar** because C-b makes
  `fe-connector-cache` a compile-scope dep of `fe-connector-hms` and hudi bundles transitive deps.
  **Benign now** (JVM class-loading is lazy; hudi has zero `connector.cache.*` refs → never linked, no
  NoClassDefFoundError). But it deviates from the "byte-neutral for …hudi" wording in C-c's message and
  plants a latent trap for the day hudi reuses the caching client (the design *anticipates* this reuse).
  **Cheap fix:** mark `fe-connector-cache` `<optional>true</optional>` in `fe-connector-hms/pom.xml`
  (hms compiles against it; hive already declares it directly; it stops flowing into the hudi zip).

## e2e debt to assert at the flip (heterogeneous-HMS docker)

1. A misconfigured storage on a hive scan **must surface an error**, not an empty result (this is exactly
   finding #1 — if we adopt the fix, assert it fails loud; if we don't, assert the behavior consciously).
2. `REFRESH TABLE` / `REFRESH CATALOG` end-to-end drop both cache layers and show new data; document that
   `REFRESH DATABASE` does **not** (finding #2).
3. A hive-backed SQL dictionary / MV **auto-refreshes** after the base table changes (validates the §2.6
   fix end-to-end), and note the pre-existing "drop-newest-partition lowers the max" monotonicity property.
4. Cache hit under a real flipped hms catalog (metastore + file listing); hudi-on-HMS caching (separate item).
