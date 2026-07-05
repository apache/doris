# P6.2-T05 — iceberg COUNT(*) pushdown (`getCountFromSnapshot`) + batch-mode decision

> Branch `catalog-spi-10-iceberg`. Mirrors legacy `IcebergScanNode.getCountFromSnapshot` (`:1142-1171`),
> the `tableLevelPushDownCount` branch of `setIcebergParams` (`:297-302`), and the count short-circuit in
> `doGetSplits` (`:944-957`) + `isBatchMode` (`:1001-1013`). Predecessors: T01 skeleton, T02 predicate/split,
> T03 typed range-params (`P6-T03-iceberg-scan-rangeparams-design.md`), T04 delete files
> (`P6-T04-iceberg-scan-deletefiles-design.md`). **0 new SPI.** Iceberg stays out of `SPI_READY_TYPES`;
> nothing reaches BE this phase — parity verified by offline UT until P6.6.

## 1. Goal / scope

Make `SELECT COUNT(*) FROM t` (no grouping, no count-invalidating filter) serve the row count from the
iceberg snapshot summary instead of BE materializing rows — porting legacy `getCountFromSnapshot`.

In scope:
- Override the COUNT-pushdown-aware SPI overload
  `planScan(session, handle, columns, filter, limit, requiredPartitions, countPushdown)`. When
  `countPushdown` is true, compute the pushable count from the snapshot summary; when `>= 0`, emit a single
  **collapsed count range** carrying the full count; when `-1` (not pushable), fall through to the normal
  scan (T02/T03/T04) so BE reads and counts.
- `getCountFromSnapshot` port: equality-delete present (`total-equality-deletes != "0"`) → `-1`;
  `total-position-deletes == 0` → `total-records`; position-delete `> 0` and session
  `ignore_iceberg_dangling_delete=true` → `total-records - deleteCount`; else `-1`; empty (no snapshot)
  → `0`.
- `IcebergScanRange` carries a typed `pushDownRowCount` (default `-1`); `getPushDownRowCount()` override
  (the generic node's EXPLAIN `pushdown agg=COUNT (n)` reads it); `populateRangeParams` emits
  `setTableLevelRowCount(pushDownRowCount)` (T03 emitted a constant `-1`).
- The count range is emitted **whole** (`scan.planFiles()`, no byte tiling). This is result-identical to —
  not a literal copy of — legacy: legacy byte-splits the count file (`planFileScanTask`→`splitFiles`) and
  keeps the first split's byte-range, but under count pushdown BE's count reader never reads the file
  (start/length irrelevant) and sums `table_level_row_count` across ranges, so one whole-file range yields
  the identical total (§2 D-T05-1). (The `splittable=!applyCountPushdown` flag is paimon-specific — it does
  **not** exist in the iceberg legacy path.)

Out of scope (decided / later):
- **batch mode → DEFERRED** (user sign-off 2026-06-22, mirrors paimon). See §5.
- The legacy `>10000`-rows **parallel multi-split trim** (`needSplitCnt = parallelExecInstanceNum *
  numBackends`, `assignCountToSplits` even distribution) is intentionally dropped — collapse to one
  (paimon parity, §2). Perf-only; BE's summed count is identical.
- Time-travel / MVCC snapshot pinning is a later P6.2 task; the count uses the scan's snapshot (§2), so it
  follows the scan automatically once MVCC pins it.

## 2. Architecture & design decisions

**The generic node already has the COUNT-pushdown machinery (FIX-COUNT-PUSHDOWN) — 0 new SPI.**
`PluginDrivenScanNode.getSplits` (`:688-715`) computes
`countPushdown = getPushDownAggNoGroupingOp() == TPushAggOp.COUNT` and forwards it through the 7-arg
`planScan` overload. After planning it reads `ConnectorScanRange.getPushDownRowCount()` (SPI default `-1`)
via `resolvePushDownRowCount` for the EXPLAIN line, and each range's `populateRangeParams` sets the BE
thrift `table_level_row_count`. Paimon already implements exactly this surface; iceberg mirrors it.

- **D-T05-1 — collapse to one count range (mirror paimon, drop the parallel trim).** Iceberg's count is a
  single table-level number from the snapshot summary (not per-file like paimon's `DataSplit
  .mergedRowCount()`). Legacy distributes that number across `needSplitCnt` splits (`assignCountToSplits`),
  but BE simply **sums** every range's `table_level_row_count`, so the distribution is perf-only. Paimon
  already collapsed its count to one range and dropped the parallel trim as a documented perf deviation;
  iceberg does the same: take the **first** whole-file `FileScanTask` as the representative, build one
  `IcebergScanRange` carrying the full count, emit nothing else. BE's summed count == legacy's.
- **D-T05-2 — the count snapshot is the scan's snapshot (`scan.snapshot()`), MVCC-ready.** Legacy reads
  `getSpecifiedSnapshot()` then `currentSnapshot()`/`snapshot(id)`. The connector's scan is currently
  un-pinned (`table.newScan()` → current snapshot; time-travel deferred), so `scan.snapshot()` equals
  legacy's `currentSnapshot()` for every non-time-travel query (the only supported case today) **and**
  automatically follows the scan once MVCC pins it — count and scan can never diverge. `null`
  (empty table, no snapshots) → `0`, matching legacy.
- **D-T05-3 — local summary-key constants (legacy parity, no SDK coupling).** Declare
  `total-records` / `total-position-deletes` / `total-equality-deletes` as private constants in the
  provider — byte-identical to `IcebergUtils.TOTAL_*` (which are themselves local strings, not
  `org.apache.iceberg.SnapshotSummary.*` constants). These are the stable iceberg spec keys.
- **D-T05-4 — typed carrier, not a string property.** Consistent with T03/T04: `pushDownRowCount` is a
  typed `long` field on `IcebergScanRange`, consumed in `populateRangeParams`; `getProperties()` stays
  empty. (Paimon stashes `paimon.row_count` in its string props because paimon ranges are stringly-typed;
  iceberg's are numeric/typed.)

## 3. Implementation

### 3.1 `IcebergScanRange` — `pushDownRowCount` carrier
- New `final long pushDownRowCount` (builder default `-1`), `Builder.pushDownRowCount(long)`.
- `@Override public long getPushDownRowCount()` returns it.
- `populateRangeParams`: replace the constant `formatDesc.setTableLevelRowCount(-1)` with
  `formatDesc.setTableLevelRowCount(pushDownRowCount)`. Default `-1` keeps every normal/T03/T04 range
  byte-unchanged; only the collapsed count range carries a real count.

### 3.2 `IcebergScanPlanProvider` — count-pushdown overload
- Add private constants `TOTAL_RECORDS`/`TOTAL_POSITION_DELETES`/`TOTAL_EQUALITY_DELETES`/
  `IGNORE_ICEBERG_DANGLING_DELETE` (`"ignore_iceberg_dangling_delete"`).
- Extract the existing 4-arg `planScan` body into
  `planScanInternal(session, handle, columns, filter, countPushdown)`; the public 4-arg `planScan`
  delegates with `countPushdown=false`.
- Override the 7-arg overload → `planScanInternal(session, handle, columns, filter, countPushdown)`
  (`limit`/`requiredPartitions` not consumed by the iceberg read path — same as paimon).
- `planScanInternal`: resolve table; build the scan (extracted `buildScan(table, filter, session)` =
  predicate convert + `table.newScan()` + per-conjunct `scan.filter`); compute `formatVersion` /
  `orderedPartitionKeys` / `zone` / `partitioned`. Then:
  - if `countPushdown`: `long realCount = getCountFromSnapshot(scan, session);` if `realCount >= 0` →
    `return planCountPushdown(table, scan, realCount, formatVersion, partitioned, orderedKeys, zone);`
    else fall through.
  - normal path (unchanged): `splitFiles(scan, session)` → `buildRange(..., -1)` per task.
- `planCountPushdown(...)`: iterate `scan.planFiles()` (whole-file tasks, **not** `splitFiles`); return a
  `Collections.singletonList(buildRange(table, firstTask.file(), firstTask, ..., realCount))`; empty
  iterable → `Collections.emptyList()` (empty table → BE 0 ranges → COUNT 0).
- `getCountFromSnapshot(TableScan scan, ConnectorSession session)` — verbatim port of legacy `:1142-1171`,
  reading `scan.snapshot()`:
  ```
  Snapshot s = scan.snapshot();
  if (s == null) return 0;
  Map<String,String> summary = s.summary();
  if (!summary.get(TOTAL_EQUALITY_DELETES).equals("0")) return -1;
  long deleteCount = Long.parseLong(summary.get(TOTAL_POSITION_DELETES));
  if (deleteCount == 0) return Long.parseLong(summary.get(TOTAL_RECORDS));
  if (ignoreIcebergDanglingDelete(session)) return Long.parseLong(summary.get(TOTAL_RECORDS)) - deleteCount;
  return -1;
  ```
- `buildRange(...)` grows a trailing `long pushDownRowCount` param threaded to
  `.pushDownRowCount(pushDownRowCount)`; the normal-path caller passes `-1`, the count path passes
  `realCount`.
- `ignoreIcebergDanglingDelete(session)` = `session != null && Boolean.parseBoolean(raw.trim())` on
  `getSessionProperties().get(IGNORE_ICEBERG_DANGLING_DELETE)` (default `false`).

## 4. Parity nuances (write down to avoid re-deriving)
- **The count range still carries its T03/T04 carriers (incl. delete files), and they are inert.** When a
  count is pushed BE's CountReader returns `table_level_row_count` without opening the file or applying
  deletes, so emitting the representative file's deletes/partition-values is harmless — and faithful
  (legacy `setIcebergParams` runs the v2+ delete loop regardless of `tableLevelPushDownCount`).
- **Not-pushable ⇒ normal scan, not an error.** Equality deletes, or dangling position deletes without the
  ignore flag, return `-1` → fall through to the tiled normal scan → BE reads & counts (legacy
  `tableLevelPushDownCount=false`). Every range then has `pushDownRowCount=-1` and the EXPLAIN renders
  `(-1)` (load-bearing sentinel, `resolvePushDownRowCount`).
- **COUNT pushdown implies an unfiltered count.** `getPushDownAggNoGroupingOp()==COUNT` is only set by the
  planner for a count with no count-invalidating predicate, so reading whole-table snapshot totals is
  correct (legacy relies on the same invariant). The scan still applies whatever filter it is given for
  parity (empty in practice).
- **`summary.get(...)` is null-unsafe — kept verbatim.** Legacy does `summary.get(TOTAL_EQUALITY_DELETES)
  .equals("0")` and `Long.parseLong(summary.get(...))` with no null guard; real iceberg snapshots always
  carry these totals (iceberg `SnapshotSummary.Builder` always sets them, `"0"` when none). Porting the
  guard-free form preserves byte-exact behavior (incl. the same NPE on a pathological summary).

## 5. Batch mode — DEFERRED (user sign-off 2026-06-22; mirrors paimon)
Legacy iceberg `isBatchMode` triggers on a **manifest data-file count** threshold
(`num_files_in_batch_mode`, `:1029-1056`). The generic `PluginDrivenScanNode.computeBatchMode` triggers on
a **pruned-partition count** threshold (`num_partitions_in_batch_mode`) gated by the connector's
`supportsBatchScan()` (default `false`) — a different axis that legacy's manifest-count cannot map onto.
Faithfully porting the manifest-count gate would require a new "connector-decides-isBatchMode" SPI seam,
violating P6.2's **0-new-SPI** invariant. Paimon deferred batch mode for the same reason. Batch mode is a
scale/streaming optimization, **not** correctness: deferring means a very large iceberg scan materializes
all splits at once (exactly as the skeleton and paimon do today) with identical query results. Iceberg does
**not** override `supportsBatchScan`, so the generic partition-count batch mode stays off for iceberg.
Re-porting (with a new SPI seam) is a separate future task if ever needed.

## 6. Deviations (UT-invisible unless noted; P6.6 docker-only for the BE-facing ones)
- **Parallel multi-split count trim dropped** — collapse to one count range (D-T05-1, paimon parity).
  Perf-only; BE's summed count identical. UT-visible (range count) and asserted.
- **batch mode deferred** (§5). Manifest-count vs partition-count axis mismatch; needs a new SPI seam.
- **`scan.snapshot()` vs legacy `getSpecifiedSnapshot()`/`currentSnapshot()`** (D-T05-2) — equivalent for
  every non-time-travel query today; MVCC-ready. Time-travel COUNT is already a deferred scan gap.
- **Empty-table COUNT EXPLAIN nit** — no representative file → empty ranges → `resolvePushDownRowCount`
  returns `-1` → EXPLAIN `(-1)`; legacy `setPushDownCount(0)` shows `(0)`. BE result identical (`0`).
  Non-correctness, EXPLAIN-only.

## 7. Tests (TDD, no Mockito, fail-loud fakes; real `InMemoryCatalog`)
- `IcebergScanRangeTest`: `populateRangeParams` with `.pushDownRowCount(N)` → `table_level_row_count == N`;
  default (no count) → `getPushDownRowCount()==-1` and `table_level_row_count == -1` (normal range
  unchanged).
- `IcebergScanPlanProviderTest` (7-arg `planScan`, `countPushdown=true` unless noted):
  - no-delete multi-file table → exactly **one** range, `getPushDownRowCount()==Σ record counts`,
    whole-file (start 0, length == fileSize), `table_level_row_count` == total.
  - equality-delete table → not pushable → **normal** multi-range scan, every `getPushDownRowCount()==-1`.
  - position-delete table + session `ignore_iceberg_dangling_delete=true` → one range,
    count `== total-records - total-position-deletes`.
  - position-delete table + ignore flag absent/false → not pushable → normal scan.
  - empty table (no snapshot) → empty range list.
  - `countPushdown=false` on a multi-file table → normal multi-range scan (existing behavior preserved).

## 8. Acceptance gate
fe-connector-iceberg UT green (177 → 185, +8, 1 skip), fe-core `PluginDriven*` unaffected (no fe-core
change), checkstyle 0, import-gate net, iceberg NOT in `SPI_READY_TYPES`, no SPI / fe-core / pom change.
Adversarial parity review (workflow, 4 dims × skeptic-verify) vs legacy `getCountFromSnapshot` /
`setIcebergParams` count branch / `doGetSplits` count short-circuit.

## 9. Adversarial review outcome (1 nit confirmed, doc-only; 0 correctness findings)
The 4-dim review (`wf_e0afb564-38b`, each finding independently skeptic-verified) confirmed the three core
dimensions — `getCountFromSnapshot` faithfulness, count-range emission / `populateRangeParams` / BE-sum
semantics, and SPI plumbing / not-pushable fallback — are **fully faithful (0 findings)**. One **nit**
(doc-only, zero result impact) was confirmed: the original whole-file rationale borrowed paimon's
`splittable = !applyCountPushdown` flag, which does **not** exist in the iceberg legacy path — legacy
`planFileScanTask`→`splitFiles` byte-splits the count file and keeps the first split's byte-range. The
whole-file `scan.planFiles()` choice is correct and intended (result-identical: under count pushdown BE's
count reader never reads the file, start/length irrelevant, and BE sums `table_level_row_count`); only the
cited reason was wrong. **Fixed** the rationale in `IcebergScanPlanProvider.planCountPushdown` Javadoc and
§1/§2 of this doc — no code-logic change.
