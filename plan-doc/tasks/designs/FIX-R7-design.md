# FIX-R7 — SHOW PARTITIONS serves stale partition-name cache

Suite: `test_hive_use_meta_cache_true` (sql09). Effort: M. Scope: fe-connector only.

## Problem

Under `use_meta_cache=true`, `SHOW PARTITIONS FROM <hive_table>` returns a stale (empty) partition
list even after partitions were added externally in Hive:

```
refresh catalog                         -> sql08: SHOW PARTITIONS -> {}  (0 partitions, caches empty)
hive: alter table add partition part1
hive: alter table add partition part2
                                        -> sql09: SHOW PARTITIONS -> STILL {}  (WRONG; expected part1, part2)
```

The test's own spec comment (`test_hive_use_meta_cache_true.groovy:130`) states the contract:
**"can see because partition file listing is not cached"**.

## Root Cause (HEAD-verified)

`ShowPartitionsCommand:325` → `metadata.listPartitionNames(session, handle)` →
`HiveConnectorMetadata.listPartitionNames` (:1050) → `collectPartitionNames` (:1094) →
`hmsClient.listPartitionNames(db, tbl, -1)`.

`hmsClient` is the `CachingHmsClient` decorator, whose `listPartitionNames` (:148-151) serves from
the `partitionNamesCache` (default 24h TTL). So `refresh` (sql08) flushes + repopulates the cache
with the empty list; the external `add partition` does not invalidate it (coarse REFRESH + TTL is the
only invalidation, by design); sql09 returns the stale empty list.

**Legacy parity (the regression):** legacy `ShowPartitionsCommand.handleShowHMSTablePartitions:375`
called `hmsCatalog.getClient().listPartitionNames(...)` — the **raw** metastore client, NOT the
`HiveExternalMetaCache`. So legacy SHOW PARTITIONS always listed **fresh** partition names, bypassing
the metadata cache. The metadata TVF path (`MetadataGenerator:1338`, legacy) likewise used the raw
client. The cutover to `CachingHmsClient` inadvertently routed SHOW PARTITIONS through the cache.

## The nuance (do NOT one-shot disable the cache)

`collectPartitionNames` is shared by THREE callers:

| Caller | Purpose | Legacy source | Required |
|---|---|---|---|
| `listPartitionNames` (:1050) | SHOW PARTITIONS + `partitions` metadata TVF (`MetadataGenerator:1358`) | raw client (fresh) | **FRESH** |
| `listPartitions` (:1072) | query partition pruning | `HiveExternalMetaCache` (cached) | **CACHED** (use_meta_cache contract) |
| `getTableFreshness` (:1177) | MTMV whole-table freshness | cached | **CACHED** (unchanged — surgical) |

Disabling the cache inside `collectPartitionNames` unconditionally would defeat `use_meta_cache` for
query pruning (a fresh HMS RPC on every partitioned SELECT) — a real regression. The fix must make
only the SHOW-PARTITIONS/TVF path (`listPartitionNames`) bypass the cache.

## Design

Add a **fresh (cache-bypassing) partition-name listing** to the `HmsClient` interface as a default
method, overridden by the caching decorator to go straight to the delegate. Route the
`listPartitionNames` SPI path through it; leave `listPartitions` and `getTableFreshness` on the cached
path.

### 1. `HmsClient` (interface) — new default method

```java
/**
 * Lists partition names bypassing any decorator cache (a fresh metastore listing). SHOW PARTITIONS
 * and the partitions metadata TVF need a fresh view (legacy read the raw client, never the cache);
 * the query-pruning path keeps {@link #listPartitionNames} (cached under use_meta_cache).
 * Default = the cached path: a non-decorating client (e.g. the raw ThriftHmsClient) has no cache to
 * bypass, so the two are identical for it.
 */
default List<String> listPartitionNamesFresh(String dbName, String tableName, int maxParts) {
    return listPartitionNames(dbName, tableName, maxParts);
}
```

### 2. `CachingHmsClient` — override to delegate directly

```java
@Override
public List<String> listPartitionNamesFresh(String dbName, String tableName, int maxParts) {
    // Fresh (cache-bypassing) listing for SHOW PARTITIONS / partitions TVF (legacy raw-client parity).
    // Neither reads nor writes partitionNamesCache: reading would serve a stale list; writing would let
    // a rare escaped-value edge repopulate the cache off a non-cache path. The query-pruning path keeps
    // listPartitionNames (cached).
    return delegate.listPartitionNames(dbName, tableName, maxParts);
}
```

### 3. `HiveConnectorMetadata.collectPartitionNames` — gain a `bypassCache` flag

```java
private List<String> collectPartitionNames(HiveTableHandle handle, boolean bypassCache) {
    List<String> partKeyNames = handle.getPartitionKeyNames();
    if (partKeyNames == null || partKeyNames.isEmpty()) {
        return Collections.emptyList();
    }
    return bypassCache
            ? hmsClient.listPartitionNamesFresh(handle.getDbName(), handle.getTableName(), -1)
            : hmsClient.listPartitionNames(handle.getDbName(), handle.getTableName(), -1);
}
```

Route the three callers:
- `listPartitionNames` (:1054) → `collectPartitionNames(handle, true)`  **(fresh — the fix)**
- `listPartitions` (:1079) → `collectPartitionNames(hiveHandle, false)`  (cached — unchanged)
- `getTableFreshness` (:1177) → `collectPartitionNames(hiveHandle, false)`  (cached — unchanged)

## Risk Analysis

- **Blast radius**: `HmsClient` interface + `CachingHmsClient` + `HiveConnectorMetadata`. The new
  interface method is a `default`, so no other `HmsClient` implementor breaks (ThriftHmsClient inherits
  the default = its own `listPartitionNames`). Iceberg/hudi siblings do not call these hive methods.
- **Query pruning perf**: unchanged — `listPartitions` stays cached. `use_meta_cache` contract intact.
- **MTMV**: unchanged — `getTableFreshness` stays cached (deliberately not touched; not part of this
  regression, and switching it to fresh would add an RPC per freshness probe).
- **Metadata TVF** (`partitions`, `MetadataGenerator:1358`): now fresh. This matches legacy (raw
  client) and the TVF is low-frequency. Not a perf concern.
- **Fresh does not repopulate the names cache**: preserves legacy independence (SHOW PARTITIONS never
  updated the value cache). Query pruning still bound by TTL/REFRESH as before.
- **Connector-agnostic**: fe-core unchanged. No `if(hive)` anywhere. The bypass is a connector-internal
  choice between two `HmsClient` methods.

## Test Plan

### Unit Tests (fe-connector-hms, no Mockito — recording fake)

`CachingHmsClientTest` — new tests using the existing recording `delegate` (`listPartitionNamesCalls`):
1. `listPartitionNamesFreshAlwaysHitsDelegate`: two `listPartitionNamesFresh("db","t",-1)` calls →
   `delegate.listPartitionNamesCalls == 2` (never served from cache).
2. `listPartitionNamesFreshDoesNotPopulateCache`: `listPartitionNamesFresh` then two
   `listPartitionNames` → total `delegate.listPartitionNamesCalls == 2` (fresh call #1 delegated and did
   NOT populate; cached call #2 delegated+populated; cached call #3 served from cache). Proves bypass
   both directions (read + write).
3. `listPartitionNamesStillCached` (regression guard for the cached path): unchanged existing behavior
   — two `listPartitionNames` → 1 delegate call.

Optionally a `HmsClient` default-method test: a bare `HmsClient` (no override) has
`listPartitionNamesFresh == listPartitionNames`.

### E2E

`test_hive_use_meta_cache_true` sql09 turns green (the failing assertion). Live-gated (needs external
hive docker); user reruns. No golden change (sql09 golden already expects part1/part2).

## Verification
- `mvn -o -f fe/pom.xml -pl :fe-connector-hms -am test-compile` + `-Dtest=CachingHmsClientTest`.
- `mvn -o -f fe/pom.xml -pl :fe-connector-hive -am test-compile` (interface change compiles against hive).
- 0 checkstyle; import gate clean.

## Red-team result (wf_d9882a60-f53, 4 independent lenses — all SOUND, 0 blocker/major)
- **C1 legacy parity — SOUND.** Legacy SHOW PARTITIONS → `HMSExternalCatalog.getClient()` = `ThriftHMSCachedClient`
  whose `listPartitionNames` issues a direct thrift RPC ("Cached" = connection pooling, NOT metadata cache); no
  caffeine read/write, never touches `HiveExternalMetaCache`. Partitions TVF (`MetadataGenerator:1335-1338`) same
  raw client. The name/value cache is populated only by the query-pruning loader `loadPartitionValues` — a separate
  path. Confirms: fresh + no repopulation is exact legacy parity.
- **C2 caller completeness — SOUND** (2 doc notes folded). SPI `listPartitionNames(session,handle)` has EXACTLY 2
  fe-core callers (SHOW PARTITIONS single-column `:325`, partitions TVF `:1358`) — both intended-fresh, low-freq.
  No pruning/stats/MTMV/cache-fill path calls it (hive pruning routes through `listPartitions`), so making it fresh
  cannot regress a hot path. Notes: (a) `listPartitions(session,...)` actually serves FOUR callers, ALL staying
  cached, none broken — `PluginDrivenExternalTable:781` (pruning), `:837` (`partition_values` TVF),
  `PluginDrivenMvccExternalTable:268` (paimon/iceberg MVCC), `ShowPartitionsCommand:308` (paimon 5-col SHOW; hive
  never reaches it — no `SUPPORTS_PARTITION_STATS`). (b) Benign freshness asymmetry: after the fix the `partitions`
  TVF is fresh while the `partition_values` TVF stays cached (pruning path). ACCEPTED as intended — it mirrors the
  legacy split (SHOW/list-names = fresh raw client; pruning/values = cached), and only the SHOW/list-names path is
  the regression under test. Not a code change.
- **C3 MTMV + iron-rule — SOUND.** Legacy `HiveDlaTable.getTableSnapshot` (partitioned) read names from the CACHED
  `HiveExternalMetaCache`, so leaving `getTableFreshness` cached is CORRECT parity (not a latent bug). Clarify:
  `getPartitionFreshnessMillis` never used `collectPartitionNames` (calls `getPartitions` directly) — it is simply
  UNTOUCHED, not "left cached". Fix is connector-internal (no fe-core edit) → iron rule trivially clean.
- **C4 decorator stack — SOUND.** Production stack = single `CachingHmsClient(ThriftHmsClient)`; no outer/auth/TCCL
  HmsClient wrapper, no double-wrap. Override returns `delegate.listPartitionNames` = raw pooled RPC (fresh, no cache
  re-entry). New `default` safe for all impls: `ThriftHmsClient` inherits → fresh; hudi uses raw `ThriftHmsClient`;
  iceberg has no HmsClient impl; test fakes inherit cleanly. Foot-gun note (not a HEAD defect): the Javadoc MUST
  keep the "any caching decorator must override this" instruction for future implementors.
