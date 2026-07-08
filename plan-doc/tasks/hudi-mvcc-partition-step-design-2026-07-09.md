# Hudi MVCC / listPartitions / freshness step — implementation design (no-regression Group C, step 1)

Authoritative, code-grounded design. HEAD = `catalog-spi-11-hive`. Recon `wf_1a09236d-ee0` (5 readers + synthesis,
all HEAD-verified; 2 readers hit the structured-output cap and the synthesis re-read those files directly). All the
load-bearing facts below were hand-verified against HEAD after the recon.

**Goal**: close the no-regression gap so a PARTITIONED hudi-on-HMS table, served post-flip through the GENERIC
`PluginDrivenScanNode` path (like paimon/iceberg), has a correct partition + MVCC-snapshot surface (partition
pruning / `selectedPartitionNum` / SHOW PARTITIONS / TVFs / MTMV freshness). **Zero fe-core changes** — every change
is a connector override consuming the existing SPI (IRON rule respected).

## The reconciled model: follow the PAIMON template, NOT the hive plumbing

Paimon (`PaimonConnectorMetadata`) overrides ONLY `beginQuerySnapshot` (:432) + `listPartitions` (:954) +
`listPartitionNames` (:938) + `listPartitionValues` (:960). It does **not** override `getMvccPartitionView`,
`getTableFreshness`, or `getPartitionFreshnessMillis`. Hudi is a snapshot-id connector like paimon, so it mirrors
exactly that surface. `HudiConnectorMetadata` currently overrides **none** of these seven.

**Correction to the earlier task brief** (verified in `PluginDrivenMvccExternalTable`): with
`lastModifiedFreshness == false`, fe-core NEVER calls `getTableFreshness` (gated at :627) nor
`getPartitionFreshnessMillis` (gated at :591). Overriding them would be DEAD CODE. The task's
"getTableFreshness/getPartitionFreshnessMillis → last-commit instant" bullet targets the wrong seam: the freshness
signal is delivered through `beginQuerySnapshot`'s snapshotId (table) + per-partition `lastModifiedMillis`
(partition). Also, "getPartitionSnapshot takes the snapshot-id branch" is FALSE: on the LIST path
(`getMvccPartitionView` empty) `getPartitionSnapshot` reaches the pin-TIMESTAMP `MTMVTimestampSnapshot(value)`
branch (:607), identical to paimon; the `MTMVSnapshotIdSnapshot` branch (:589) requires a RANGE
`getMvccPartitionView` (iceberg only) — hudi must NOT provide one.

## Per-method spec (HudiConnectorMetadata)

1. **`beginQuerySnapshot(session, handle)` — NEW.** Return
   `Optional.of(ConnectorMvccSnapshot.builder().snapshotId(instant).build())`, where `instant =
   Long.parseLong(timeline.filterCompletedInstants().lastInstant().get().requestedTime())` (the latest COMPLETED
   instant; port of `HudiUtils.getLastTimeStamp`, fe-core `HudiUtils.java:271-284`; same timeline the scan already
   uses at `HudiScanPlanProvider.java:131-139`). Empty timeline → pin `0L` (legacy parity; `0L >= 0` survives the
   `getNewestUpdateVersionOrTime` `v>=0` filter at `PluginDrivenMvccExternalTable.java:714`). **MUST NOT set
   `lastModifiedFreshness(true)`** (default false — the one bit separating hudi from the hive precedent). Do NOT set
   schemaId (time-travel is a later step). → `getTableSnapshot` returns `MTMVSnapshotIdSnapshot(instant)` (:634).
2. **`getMvccPartitionView` — DO NOT OVERRIDE.** Inherit the SPI default `Optional.empty()`. Keeps the LIST path
   (paimon parity). The task's "→ empty" is the inherited default; an explicit override is redundant.
3. **`listPartitions(session, handle, filter)` — NEW.** One `ConnectorPartitionInfo` per partition via a shared
   private `collectPartitions(handle)`; `filter` ignored (paimon parity). Unpartitioned → `emptyList()`.
   Partition-name SOURCE = port of `HudiExternalMetaCache.loadPartitionNames` (fe-core :195-217),
   `useHiveSyncPartition`-aware:
   - `true` → `hmsClient.listPartitionNames(db, table, -1)` (already wired at `HudiConnectorMetadata.java:176`) then
     unescape each.
   - `false` → `HoodieTableMetadata.getAllPartitionPaths` (port of `HudiPartitionUtils.getAllPartitionNames`,
     fe-core :42-50; logic already inline in `HudiScanPlanProvider.resolvePartitions:344-352`).
   - Defer the `getPartitionNamesBeforeOrEquals` (non-latest time-travel) branch — matches deferred time-travel scope.
   `useHiveSyncPartition = Boolean.parseBoolean(properties.getOrDefault("use_hive_sync_partition","false"))` (port of
   `HMSExternalTable.useHiveSyncPartition`; key `USE_HIVE_SYNC_PARTITION`). For each raw path: parse values with the
   ALREADY-ported `HudiScanPlanProvider.parsePartitionValues(rawPath, partKeyNames)` (HD-A3) and **render a
   hive-style name** (see the 7-arg fields below). 7-arg `ConnectorPartitionInfo`.
4. **`listPartitionNames(session, handle)` — NEW.** `collectPartitions` → `getPartitionName()` (paimon parity).
5. **`listPartitionValues(session, handle, cols)` — NEW (recommended, TVF parity).** `collectPartitions` → project
   `getPartitionValues()` into `cols` order (paimon parity).
6. **`getTableFreshness` / `getPartitionFreshnessMillis` — DO NOT OVERRIDE** (dead code under flag=false, see above).

### Per-partition `ConnectorPartitionInfo` (7-arg ctor)
- **partitionName** = HIVE-STYLE `col0=val0/col1=val1/...` in partition-key order (values from `parsePartitionValues`).
  **MANDATORY**: the generic consumer rebuilds the item by RE-PARSING the name via `HiveUtil.toPartitionValues` under
  `Preconditions.checkState(values.size()==types.size())` (`PluginDrivenMvccExternalTable.java:292-297`). A raw hudi
  path (`"2024/01"`, or single-col `"2024"` with no `=`) → wrong count → checkState throws → caught+skipped
  (:277-280) → item map short → `isPartitionInvalid()` → silent UNPARTITIONED degrade. So the connector MUST render
  hive-style. **Arity precondition**: `partKeyNames.size()` must equal the fe-core partition-column count.
- **partitionValues** = raw parsed `Map<col,val>` (LinkedHashMap, key order). Backs `listPartitionValues` TVF.
- **properties** = `emptyMap()`.
- **rowCount / sizeBytes / fileCount** = `-1` (UNKNOWN; all -1-tolerant).
- **lastModifiedMillis** = **the instant** (SAME `Long.parseLong(requestedTime())` as the pin; `0L` on empty
  timeline). Feeds `MTMVTimestampSnapshot(instant)` (:607). A monotonic non-negative instant is a STABLE marker
  (unchanged table → same marker → no refresh; new commit → new instant → refresh). Emitting `-1` (hive names-only
  default) → `MTMVTimestampSnapshot(-1)` never equals stored → MTMV always-stale OVER-refresh. Emitting legacy's `0L`
  → never detects change (the 0-stub). The instant is strictly more correct than both.

## Signed / to-sign decisions
- **DECISION (needs user sign-off) — hudi MTMV freshness scope.** Legacy `HudiDlaTable.getPartitionSnapshot`/
  `getTableSnapshot` return `MTMVTimestampSnapshot(0L)` (real logic commented out) — hudi CAN be an MV base but never
  auto-refreshes on a source commit. **Recommended = implement REAL instant freshness now** (paimon model above): a
  strict superset over the broken 0-stub, near-zero cost (the instant is already computed for the scan), and it
  naturally avoids the `-1` over-refresh landmine. The alternative (replicate the 0-stub) is worse (table pin =
  instant vs partition = 0 semantic split) and still risks `-1`. **Behavior change to accept: hudi MVs will now
  auto-refresh when the base hudi table gets a new commit.** Regardless of A/B, do NOT override
  getTableFreshness/getPartitionFreshnessMillis.

## Risks / landmines (verified)
- **R1 (HIGH) — partition NAME rendering.** Must render hive-style `col=val/...` or silent UNPARTITIONED degrade
  (checkState-and-skip). Unit-test the arity directly (`HiveUtil.toPartitionValues(name).size() == partKeys.size()`).
- **R2 (HIGH) — partition-source consistency ⇒ FE prune-to-zero data loss.** Hudi does NOT override
  `ignorePartitionPruneShortCircuit` (default false) and its `planScan` ignores `requiredPartitions`; a FE prune over
  the NEW listPartitions universe that empties to zero SHORT-CIRCUITS `getSplits` to zero rows WITHOUT calling
  planScan (`PluginDrivenScanNode.java:957-969`). If listPartitions omits/mis-names a partition that has data, a
  partition-predicate query wrongly returns zero/partial rows — a risk INTRODUCED by this step (today the empty
  universe ⇒ NOT_PRUNED ⇒ scan-all). Mitigation: make listPartitions' source byte-identical to the scan's partition
  source (same `useHiveSyncPartition` selection = `resolvePartitions`' `getAllPartitionPaths` for non-hive-sync), and
  close the TODO at `HudiScanPlanProvider.java:378-383`. **Flip-time e2e MUST verify** a partition-predicate query on
  a NON-hive-sync table returns complete rows; decide then whether hudi should set `ignorePartitionPruneShortCircuit
  = true`.
- **R3 (MEDIUM) — batch-mode OOM is DORMANT for hudi** (no `supportsBatchScan`); this step supplies the partition
  COUNT batch mode keys on but does NOT by itself prevent planning-time OOM. Do not claim it does.
- **R4 (HIGH) — TCCL + UGI on the metaClient calls.** beginQuerySnapshot/listPartitions build a
  `HoodieTableMetaClient` + `getAllPartitionPaths` (hudi-bundled reflection + secured storage). The metadata/
  materialize thread is NOT TCCL-pinned and hudi's context is NOT wrapped in `TcclPinningConnectorContext`; secured
  HMS/HDFS needs `pluginAuth.doAs` (post-flip `context.executeAuthenticated` is NOOP). **Wiring change required**:
  `HudiConnector.getMetadata` currently constructs `new HudiConnectorMetadata(getOrCreateClient(), properties)` with
  NO context/auth (`HudiConnector.java:87`). Inject a single `execute(Callable)` wrapper (built by HudiConnector) that
  does `pluginAuth.doAs` — or `context.executeAuthenticated` when null — INSIDE a TCCL pin to
  `HudiConnector.class.getClassLoader()` (restore-in-finally), so the new metaClient-touching methods run
  authenticated + pinned. MEMORY: `catalog-spi-plugin-tccl-classloader-gotcha`.
- **R5 (MEDIUM) — instant encoding.** `Long.parseLong(requestedTime())`; empty → `0L`; malformed → NumberFormatException
  fail-loud (legacy parity). requestedTime is a numeric `yyyyMMddHHmmssSSS` string, fits/monotonic in long.
- **R6 (LOW) — no dead overrides.** Do NOT add getMvccPartitionView/getTableFreshness/getPartitionFreshnessMillis;
  guard with a test asserting the SPI defaults hold.
- **R7 (LOW, out of scope) — planScan re-derives its own queryInstant** (doesn't read the pin off the handle); matches
  current/legacy for a LATEST read; note only.

## Ports / wiring
- Lift the latest-completed-instant derivation + metaClient build + `getAllPartitionPaths` into a shared
  package-private helper reachable from BOTH `HudiConnectorMetadata` and `HudiScanPlanProvider` (they must take the
  identical instant; avoid a 3rd copy of the `HoodieTableMetadata.create(...)` dance). The metaClient factory should
  run under the plugin auth/UGI + TCCL pin (R4).
- `parsePartitionValues` (HD-A3) + `unescapePathName` are already in the connector — reuse; do NOT pull `HiveUtil`
  into the plugin (it runs fe-core-side).
- `HudiConnectorMetadata` ctor gains the auth/TCCL execute-wrapper param; `HudiConnector.getMetadata` passes it.

## Test plan
- Same-loader unit (module already proves the static-method pattern with HudiPartitionValuesTest/PruningTest):
  1. instant→long (factor into a static helper): `"20240101120000000"` → `20240101120000000L`; empty → `0L`.
  2. hive-style NAME rendering + `HiveUtil.toPartitionValues(name).size()==partKeys.size()` (R1): `"2024/01"`+[year,
     month], single-col `"2024"`+[dt] (size 1 not 0), hive-style passthrough, `%`-escaped round-trip.
  3. listPartitions per-partition: `lastModifiedMillis == instant` (assert `!= -1` and non-empty table `!= 0`),
     values map, hive-style name.
  4. listPartitionNames == names(listPartitions); unpartitioned → emptyList.
  5. useHiveSyncPartition source selection (stub both sources; assert the branch).
  6. beginQuerySnapshot: `lastModifiedFreshness == false`, `snapshotId == instant`.
  7. dead-code guard: getMvccPartitionView/getTableFreshness/getPartitionFreshnessMillis return SPI defaults.
- Flip-time e2e (per `hms-iceberg-delegation-needs-e2e`): partitioned COW+MOR, hive-sync + non-hive-sync;
  `SELECT *` → `partition=N/N`; partition-predicate → complete rows + correct pruned count (R2 non-hive-sync guard);
  SHOW PARTITIONS / TVFs; MTMV over a partitioned hudi base (new commit → snapshot changes → refresh; unchanged →
  stable, no over-refresh); Kerberos HMS+HDFS (R4).
