# FIX-B-R2-be — memoize the schema-evolution dict's per-schema reads (NO PERF REGRESSION)

> Source: `task-list-P6-deviation-fixes.md` §B-R2-be + `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R2 (be).
> Single-task loop: design → design red-team → implement → impl-verify → build+UT → commit.
> **User decision (this session): Option A — memoize the reads, keep the eager superset emission** (the
> "narrow to referenced ids" option was found architecturally infeasible connector-only + BE-crash-prone;
> see below). Hard constraint: **NO performance regression.**

## Problem & why narrowing was rejected

`buildSchemaEvolutionParam` (`PaimonScanPlanProvider.java:1298-1317`) builds the BE `history_schema_info`
dict by reading **every** committed schema: `for (Long schemaId : schemaManager.listAllIds()) { history.add(
buildSchemaInfo(schemaId, schemaManager.schema(schemaId).fields(), false)); }` — one schema-file read per
committed schema, **every scan**, even when the query's files touch only one schema id. Legacy added
entries lazily, one per distinct file `schema_id` a split referenced (+ the `-1` current entry).

**Narrowing (the task-list's primary fix) is infeasible connector-only and was rejected** (verified against
code): the dict is built in `getScanNodeProperties`, but the referenced schema_ids are only known in
`planScan`; `connector.getScanPlanProvider()` returns a NEW provider per call (`PaimonConnector.java:108`)
so no instance field can bridge them; the dict build fires lazily and **often before** splits are planned
(triggers: `getNodeExplainString:258`, `getSerializedTable:925`, conjunct-pruning, or
`createScanRangeLocations:940` after `super`→`getSplits`); the referenced set is per-scan (depends on
partition-pruning + predicates) so a table-keyed cache is imprecise/racy; the generic bridge must not
collect `paimon.schema_id` (connector-agnostic rule); and re-planning in the props build is forbidden (new
I/O = regression). An under-covering narrowed set **hard-crashes BE** (`table_schema_change_helper.h`
"miss table/file schema info", cf. CI 969249). → **Memoize instead.**

## Design — Option A: memoize the per-schema-id read, emission UNCHANGED

Keep `listAllIds()` and the full dict emission **byte-identical** (the dict still covers every committed
schema → always covers any file's schema_id → **zero BE-crash risk**). Only change HOW each historical
entry's fields are obtained: read through a **connector-level, immutable, bounded memo** keyed by
`(table-identity, schemaId)`, so the schema-file reads are served from cache across scans (a committed
schemaId's fields are write-once → no TTL; cleared on REFRESH = connector rebuild).

**Reuse the existing `PaimonSchemaAtMemo` (the B-MC2 memo).** The fact being cached — "the fields of table T
at committed schema version S" — is EXACTLY what `PaimonSchemaAtMemo` already holds (`PaimonSchemaSnapshot`,
keyed by `MemoKey(handle, schemaId)`, immutable). Both features call the SAME underlying read
`schemaManager.schema(schemaId)`. Caching it ONCE and serving both the time-travel path (B-MC2) and the
schema-evolution dict (this fix) is the DRY, efficient design.

**Consistency proof (same key → same value across the two features) — MUST hold. PRIMARY proof = the
write-once invariant (NOT object identity):**
- **Write-once invariant (the load-bearing argument):** for any committed `schemaId`,
  `schemaManager.schema(schemaId)` reads the **immutable, write-once `schema-<id>` file** — its content is
  independent of WHICH `Table` instance's `schemaManager` reads it (B-MC2's UNPINNED `resolveTable`, or this
  fix's snapshot-PINNED `resolveScanTable` = `table.copy(scan.snapshot-id=…)`). `MemoKey` deliberately
  excludes `scanOptions` from identity (`PaimonTableHandle.equals`), so the pinned and unpinned reads
  legitimately share ONE key, and the `scan.snapshot-id` pin does NOT change which committed schema file a
  given `schemaId` resolves to. So the same `MemoKey` always yields the same `fields()`. (Object identity of
  the `Table` is NOT required and does NOT hold for a time-travel scan — only value-equality, which the
  write-once invariant guarantees.)
- `$ro`: B-MC2 NEVER writes `$ro` keys (system tables skip the at-snapshot path — `resolveTimeTravel`/
  `beginQuerySnapshot` return empty for sys, and `getTableSchema(snapshot)` short-circuits on the default
  `schemaId<0`). This fix writes the BASE table's schema under the `$ro`-handle key (handle sysName="ro",
  `schemaDictTable`=base). No B-MC2 value to conflict; internally consistent. (Minor: a `t` query and a
  `t$ro` query don't share — different sysName — so `$ro` re-reads the base schemas once. Acceptable; rare.)
- branch: both writers key by `(db, table, branch)` and both read the branch FileStoreTable's
  `schemaManager.schema(id)` (the same write-once branch schema file) — identical value.

**Loader keeps the DIRECT read (not `catalogOps.schemaAt`)** so the existing real-`FileStoreTable` +
fake-`catalogOps` tests are unaffected (the schema-evolution tests build a real `FileStoreTable` via
`FileSystemCatalog` but construct the provider with a `RecordingPaimonCatalogOps`; routing the read through
`catalogOps.schemaAt` would break them). The loader returns a `PaimonSchemaSnapshot` (the memo's value type)
built from the same direct read:
```java
List<DataField> fields = schemaAtMemo.getOrLoad(handle, schemaId, () -> {
    TableSchema ts = schemaManager.schema(schemaId);     // the read the finding flags
    return new PaimonCatalogOps.PaimonSchemaSnapshot(ts.fields(), ts.partitionKeys(), ts.primaryKeys());
}).fields();
history.add(buildSchemaInfo(schemaId, fields, false));
```
`schemaId` and `schemaManager` are effectively final per loop iteration. Loader exceptions propagate
uncached (`getOrLoad` puts only after the loader returns) → the "schema reads that throw fail loud" javadoc
contract is preserved.

### Components

1. **`PaimonScanPlanProvider`**: add a `private final PaimonSchemaAtMemo schemaAtMemo;` field; a new
   **package-private** 4-arg ctor `(properties, catalogOps, context, schemaAtMemo)`; the existing public
   2-arg and 3-arg ctors delegate to it with a **fresh** `new PaimonSchemaAtMemo(PaimonSchemaAtMemo
   .DEFAULT_MAX_SIZE)` (so the ~8 existing provider construction sites + any non-production caller are
   unchanged — a fresh per-instance memo is correct, just no cross-scan sharing they don't need).
2. **`PaimonConnector.getScanPlanProvider()`**: pass the connector's existing `schemaAtMemo` (the same one
   `getMetadata` injects) into the provider — so the time-travel path and the scan dict share ONE
   per-catalog cache.
3. **`buildSchemaEvolutionParam`**: take the `PaimonTableHandle` (thread it from `getScanNodeProperties:663`)
   and memo-wrap the per-id read as above. Everything else (the `-1` current entry via
   `resolveCurrentSchemaFields`, `listAllIds()`, the encoding) is UNCHANGED.

## NO-regression / safety (must hold + be red-team-verified)

1. **Emission unchanged → zero BE-crash risk.** The dict still has the `-1` entry + one entry per
   `listAllIds()` id — byte-identical to today. BE always finds any file's schema_id. The fix touches only
   the read mechanism, never WHAT is emitted.
2. **Immutable value, collision-free key.** A committed schemaId's fields are write-once; the
   `(handle-identity, schemaId)` key is collision-free (handle identity = db+table+sysName+branch). Cleared
   only on REFRESH (connector rebuild → fresh memo). No TTL, no stale read.
3. **Worst case = current.** Miss → the same direct read as today + an O(1) put; hit → the read is skipped
   (faster, the win); overflow/concurrent-double-load → a re-read = today. Never more work than today.
4. **No new I/O, order-independent.** The memo does not depend on planScan/props ordering (unlike
   narrowing); `listAllIds()` still runs each scan (a cheap directory listing — unchanged); only the
   per-schema-file reads are cached.

## Files

- `fe-connector-paimon/.../PaimonScanPlanProvider.java` (ctor + field + `buildSchemaEvolutionParam` memo + thread handle)
- `fe-connector-paimon/.../PaimonConnector.java` (`getScanPlanProvider` injects `schemaAtMemo`)
- `fe-connector-paimon/.../test/.../PaimonScanPlanProviderTest.java` (new memoization test)

## Test Plan (RED→GREEN; red-team's 6 findings folded in)

1. **Dict build populates the shared memo (core RED→GREEN):** evolve a real `FileStoreTable` to K committed
   schemas (via `FileSystemCatalog`, like the existing schema-evolution tests; `Catalog.alterTable` +
   `SchemaChange.addColumn`); build a provider with a shared `PaimonSchemaAtMemo` (4-arg ctor); call
   `getScanNodeProperties` → assert `memo.size() == K` (the K historical entries were read through the memo;
   the `-1` entry does NOT use it). **RED before:** `buildSchemaEvolutionParam` reads directly → `size == 0`.
   Also assert the memo's KEY SET is exactly `{(handle,0..K-1)}` (not just the count).
2. **Cache HIT is positively observable (sentinel pre-seed — fixes the false-pass gap):** seed the SHARED
   memo for ONE `(handle, schemaId=X)` with a **sentinel** `PaimonSchemaSnapshot` whose field list DIFFERS
   from the real schema (e.g. a renamed/extra `DataField` "SENTINEL_FROM_MEMO") via `memo.getOrLoad(handle,
   X, () -> sentinel)`; then call `getScanNodeProperties`, **decode** the emitted `paimon.schema_evolution`
   (via `applySchemaEvolutionParam` like the existing `$ro` test at `:602`), and assert the schemaId=X entry
   carries the SENTINEL field (proving the build returned the CACHED value and skipped the real
   `schemaManager.schema(X)` read). **RED before:** the real read overwrites → no SENTINEL → fails.
3. **Byte-identical emission vs the NON-memo baseline (safety):** on the SAME evolved table, capture
   `encodedA` from a provider built with the 2/3-arg ctor (fresh per-instance memo → first build = direct
   read = pre-fix behavior) and `encodedB` from the 4-arg-ctor provider (shared memo); assert
   `encodedA.equals(encodedB)`. Proves the memoized path emits a byte-identical dict (no order/dedup/membership
   change → zero BE-crash surface). The existing schema-evolution / `$ro` tests also stay green (2-arg ctor).
4. **force-JNI → memo NOT populated:** with a shared memo, call `getScanNodeProperties` (a) on a force-jni
   handle (`isForceJni()`, e.g. a binlog sys handle) and (b) with `force_jni_scanner=true`
   (`sessionWithProps`) → assert `paimon.schema_evolution` ABSENT AND `memo.size()==0` (the dict — and the
   read — is gated off; guards a future regression moving the read above the gate).
5. **Connector wiring (the perf benefit hinges on ONE edit):** assert `connector.getScanPlanProvider()` and
   `connector.getMetadata()` observe the SAME `PaimonSchemaAtMemo` instance, and two successive
   `getScanPlanProvider()` calls share it — pins the `getScanPlanProvider` 4-arg injection so the fix can't
   silently no-op (fresh per-provider memo) while still emitting a correct dict. Needs a package-private
   accessor on the provider (and possibly the connector) exposing the memo for the identity assertion.

### E2E
Gated (`enablePaimonTest=false`) — NOT run. The UTs (sentinel HIT proof + byte-identical baseline + wiring)
are the proof.

## Decision (post red-team): REUSE `PaimonSchemaAtMemo`
All four lensVerdicts judged the design correct and explicitly recommended **REUSE** (the consistency proof
holds via the write-once invariant; a dedicated memo is unnecessary). No reuse→corruption case was found.
