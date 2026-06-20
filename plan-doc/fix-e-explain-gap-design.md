# Problem

Build 968994 (branch `catalog-spi-07-paimon`), 5 paimon regression tests fail with
`IllegalStateException: Explain and check failed`. The catalogs load, every `qt_*`
data query passes, and the COUNT values are correct — only the EXPLAIN string is
missing lines that the legacy `org.apache.doris.datasource.paimon.source.PaimonScanNode`
emitted. All 5 assertions are inline `explain { contains "..." }` checks (NOT `.out`-backed).

| # | test (file:line) | expected `contains` | actual plan has |
|---|---|---|---|
| 1 | `test_paimon_count.groovy:51` | `pushdown agg=COUNT (12)` | `VPluginDrivenScanNode` + a normal `VAGGREGATE count(*)`, NO `pushdown agg` line |
| 2 | `test_paimon_deletion_vector.groovy:54` | `pushdown agg=COUNT (-1)` | same — no `pushdown agg` line |
| 3 | `test_paimon_deletion_vector_oss.groovy:57` (VERBOSE) | `deleteFileNum` | no `dataFileNum/deleteFileNum/deleteSplitNum` block |
| 4 | `test_paimon_catalog_varbinary.groovy:44` (force_jni) | `paimonNativeReadSplits=0/1` | no `paimonNativeReadSplits` line |
| 5 | `test_paimon_catalog_timestamp_tz.groovy:37` (force_jni) | `paimonNativeReadSplits=0/1` | no `paimonNativeReadSplits` line |

The actual explain bodies (from `/mnt/disk1/yy/tmp/64445_..._external/doris-regression-test.20260613.165803.log`)
show `VPluginDrivenScanNode(NN)` with `TABLE`/`CONNECTOR: paimon`/`partition=0/0` but none of the four
line families above. `count(*)` is served by a regular VAGGREGATE (correct rows; the FE `pushdown agg`
display is just absent).

These 5 are GENUINELY display-only — see Risk Analysis §"Real regression vs display gap": 4 catalogs are
`hdfs://` filesystem warehouses, the 5th is `oss://` (jindo bundled). None touch the broken s3/obs/hms
packaging classes; the oss test already ran `qt_1..qt_6` reads before the explain assertion, proving its
catalog loads and reads work.

# Root Cause

`PluginDrivenScanNode.getNodeExplainString(prefix, detailLevel)`
(`fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java:228-280`)
is a **full override that does NOT call `super.getNodeExplainString`**
(`FileScanNode.getNodeExplainString`, `fe/fe-core/.../datasource/FileScanNode.java:129-245`).
It hand-rolls the `TABLE` / `CONNECTOR` / `QUERY` / `PREDICATES` / `partition=N/M` lines and then
delegates to `scanProvider.appendExplainInfo(output, prefix, props)` (line 264). Because it never calls
super, it silently drops every FileScanNode-produced line. This is the documented
`catalog-spi-plugindriven-explain-override-gap` pattern, now re-manifested for paimon's four extra lines.

The four missing line families and their legacy producers:

1. **`pushdown agg=COUNT (n)`** — produced by `FileScanNode.java:227-232`:
   ```
   output.append(prefix).append(String.format("pushdown agg=%s", pushDownAggNoGroupingOp));
   if (pushDownAggNoGroupingOp.equals(TPushAggOp.COUNT)) {
       output.append(" (").append(getPushDownCount()).append(")");
   }
   ```
   `getPushDownCount()` returns the field `tableLevelRowCount` (default `-1`), set only via
   `FileScanNode.setPushDownCount(long)` (`:102-104`). Legacy `PaimonScanNode.getSplits` calls
   `setPushDownCount(pushDownCountSum)` (`PaimonScanNode.java:492`) when count pushdown produces a sum.
   `PluginDrivenScanNode` NEVER calls `setPushDownCount` — it forwards `countPushdown` to the provider
   (`:571-574`) but the provider computes `countSum` internally and emits it only per-range via the BE-bound
   `paimon.row_count` property (`PaimonScanRange` builder → `formatDesc.setTableLevelRowCount`). The FE
   node's `tableLevelRowCount` stays `-1`. So even if super were called, count tables would print `(-1)`.
   - Expected `(12)`/`(8)` = real precomputed merged-count sums (append/merge tables); `.out` count
     results confirm 12 and 8.
   - Expected `(-1)` (deletion_vector tables) = NO precomputed merged count → no count-range emitted →
     `tableLevelRowCount` stays `-1` → BE counts by reading; `.out` confirms the query returns 3.

2. **`dataFileNum=, deleteFileNum=, deleteSplitNum=`** — produced by `FileScanNode.java:209-212`, but
   gated `detailLevel == VERBOSE && !isBatchMode()` (`:151`). The per-BE loop calls
   `getDeleteFiles(fileRangeDesc)` (`:179`); the base `FileScanNode.getDeleteFiles` returns `emptyList`
   (`:123-127`). Legacy `PaimonScanNode` OVERRIDES `getDeleteFiles` (`PaimonScanNode.java:337-357`) to read
   `TPaimonFileDesc.getDeletionFile().getPath()` from the thrift range. `PluginDrivenScanNode` does NOT
   override `getDeleteFiles`, so even with super, `deleteFileNum` would always be 0. Test #3 uses
   `verbose(true)` and just asserts the literal substring `deleteFileNum` exists.

3. **`paimonNativeReadSplits=<raw>/<total>`** — paimon-specific, produced by the legacy override
   `PaimonScanNode.getNodeExplainString:656-658`:
   ```
   sb.append(String.format("%spaimonNativeReadSplits=%d/%d\n",
       prefix, rawFileSplitNum, (paimonSplitNum + rawFileSplitNum)));
   ```
   `rawFileSplitNum` (native ORC/Parquet sub-splits) and `paimonSplitNum` (JNI + non-DataSplit + count
   splits) are accumulated in `PaimonScanNode.getSplits` (`:393,465,477`). In the SPI path, split
   classification (native vs JNI vs count) happens INSIDE `PaimonScanPlanProvider.planScanInternal`
   (`PaimonScanPlanProvider.java:288-439`); the node only receives the resulting `List<ConnectorScanRange>`.
   `PaimonScanPlanProvider` has NO `appendExplainInfo` override and tracks no counts. The counts must be
   re-derived by the node from the returned ranges. Under `force_jni_scanner=true` (tests #4/#5), the
   native arm is never taken → `rawFileSplitNum=0`, exactly one JNI split → `0/1`.

4. **`predicatesFromPaimon:` / `PaimonSplitStats:`** — also emitted by the legacy override
   (`PaimonScanNode.getNodeExplainString:660-687`). NOT asserted by any of the 5 failing tests, so out of
   scope (but see Risk §"completeness" — re-emitting them is harmless and improves parity).

**Ordering note (verified):** `FileQueryScanNode.finalizeForNereids` (`:236`) → `createScanRangeLocations`
(`:312`) → `getSplits(numBackends)` (`:415`) runs during planning, BEFORE explain renders. So the node can
accumulate counts in `getSplits` and read them in `getNodeExplainString`, exactly as legacy does.

# Design

The fix re-emits the four line families from `PluginDrivenScanNode`, paimon-gated so other plugin
connectors (es / jdbc / trino-connector / max_compute — `CatalogFactory.SPI_READY_TYPES`) are byte-unchanged.

Three deltas, all in `PluginDrivenScanNode` plus one tiny SPI seam:

### Change A — call `super` for the FileScanNode line families, behind a flag

Do NOT blanket-call `super.getNodeExplainString` (it would also emit `table:`, `inputSplitNum=`,
`numNodes=`, etc., perturbing the existing custom `TABLE`/`CONNECTOR`/`QUERY`/`PREDICATES`/`partition=`
format that maxcompute/es golden assertions match). Instead, **selectively re-emit** the two
FileScanNode line families that are paimon-asserted, keeping the existing custom header:

- **`pushdown agg=COUNT (n)`**: after the connector `appendExplainInfo` call, emit
  ```
  output.append(prefix).append("pushdown agg=").append(getPushDownAggNoGroupingOp());
  if (getPushDownAggNoGroupingOp() == TPushAggOp.COUNT) {
      output.append(" (").append(getTableLevelRowCountForExplain()).append(")");
  }
  output.append("\n");
  ```
  This line is connector-agnostic and safe to emit for ALL plugin connectors (it mirrors what
  FileScanNode prints for every other scan node — its absence on plugin nodes is itself an inconsistency).
  **No gating needed** for the `pushdown agg` line; it is universally correct.
  - Requires the count value to reach the node. See Change C.

- **`dataFileNum/deleteFileNum/deleteSplitNum`** (VERBOSE block): this is the expensive per-BE loop in
  `FileScanNode:151-213`. Rather than duplicate ~60 lines, factor the VERBOSE per-BE block out of
  `FileScanNode.getNodeExplainString` into a `protected` helper
  `appendBackendScanRangeDetail(StringBuilder, prefix)` and call it from `PluginDrivenScanNode` under the
  same `detailLevel == VERBOSE && !isBatchMode()` gate. (Surgical alternative if extraction is undesirable:
  copy the block; but extraction avoids drift and is preferred by Rule 3's "don't fork" reading.) The block
  calls `getDeleteFiles(rangeDesc)` — see Change B for the plugin override.

### Change B — `getDeleteFiles` override on the plugin node, via a generic seam

`PaimonScanRange.populateRangeParams` already sets `TPaimonFileDesc.setDeletionFile(...)` on the thrift
range from the `paimon.deletion_file.path` property. The deletion-file path is therefore present in the
serialized `TFileRangeDesc` at explain time. Override `getDeleteFiles(TFileRangeDesc rangeDesc)` on
`PluginDrivenScanNode` to read it.

Two options for keeping it generic (the node is shared):
- **Option B1 (preferred):** add a default SPI hook
  `ConnectorScanPlanProvider.getDeleteFiles(TFileRangeDesc) -> List<String>` returning `emptyList()` by
  default; `PaimonScanPlanProvider` overrides it to read `TTableFormatFileDesc.getPaimonParams()
  .getDeletionFile().getPath()` (a verbatim port of legacy `PaimonScanNode.getDeleteFiles:337-357`). The
  node's override delegates to `connector.getScanPlanProvider().getDeleteFiles(rangeDesc)`. This keeps the
  thrift-shape knowledge in the paimon connector and leaves es/jdbc/mc returning empty (no `deleteFileNum`
  change — though their VERBOSE block still won't print unless they also opt in, see gating below).
- **Option B2 (rejected):** read `TPaimonFileDesc` directly in fe-core's `PluginDrivenScanNode`. Rejected:
  bakes paimon thrift knowledge into the shared node, and the legacy fe-core PaimonScanNode already imports
  `TPaimonDeletionFileDesc`/`TPaimonFileDesc` so the precedent exists, but B1 is cleaner for the SPI.

### Change C — thread the count-pushdown sum back to the node

`PaimonScanPlanProvider` already encodes the summed count on the single collapsed count range as the
`paimon.row_count` property (`PaimonScanRange` builder `.rowCount(...)` → `props["paimon.row_count"]`,
consumed BE-side by `populateRangeParams:202-205`). In `PluginDrivenScanNode.getSplits`, after building the
`PluginDrivenSplit`s, scan the ranges and, if `countPushdown` is active, read `paimon.row_count` from the
range properties and call `setPushDownCount(sum)`. Generic implementation (no paimon import):
```
if (countPushdown) {
    for (ConnectorScanRange r : ranges) {
        String rc = r.getProperties().get("paimon.row_count");   // generic key lookup
        if (rc != null) { setPushDownCount(Long.parseLong(rc)); break; }
    }
}
```
For deletion_vector tables no count range is emitted → no `paimon.row_count` → `tableLevelRowCount` stays
`-1` → `pushdown agg=COUNT (-1)`. Correct.

The property key `paimon.row_count` is paimon-specific but harmless to look up generically (absent for
other connectors). To avoid hard-coding a paimon key in the shared node, optionally promote it to a generic
`ConnectorScanRange` getter `getPushDownRowCount() -> long (default -1)` that `PaimonScanRange` overrides
from its `rowCount` field; the node reads `r.getPushDownRowCount()`. **Preferred:** the generic getter, to
keep the shared node connector-agnostic (consistent with Rule 11).

### Change D — accumulate native/jni split counts and emit `paimonNativeReadSplits`

`paimonNativeReadSplits` is intrinsically paimon-specific. Emit it from the connector via a NEW
`appendExplainInfo` override on `PaimonScanPlanProvider` — BUT `appendExplainInfo` only receives the
`nodeProperties` map, NOT the per-scan split counts (those are computed in `planScan`, after
`getScanNodeProperties`). So the counts must be threaded through the node.

Chosen approach: classify ranges in `PluginDrivenScanNode.getSplits` (where ranges are already iterated)
and stash counts, then emit via the connector's `appendExplainInfo` by passing them through a small
augmented props map, OR — simpler and matching legacy — have the connector own the string but feed it the
counts. Concretely:

- A native range = `ConnectorScanRange` whose `getRangeType()` is `FILE_SCAN` with a `getPath()` present
  and NO `paimon.split` property (paimon native sub-splits set `path`/`fileFormat`, no `paimon.split`).
- A jni/count range = has the `paimon.split` property.

Cleanest generic seam: add `ConnectorScanRange.isNativeReadRange() -> boolean (default false)` that
`PaimonScanRange` overrides (`true` when `paimon.split == null && path != null`). In
`PluginDrivenScanNode.getSplits`, after building splits, compute
`nativeCount = count(isNativeReadRange)` and `totalCount = ranges.size()`, store in two node fields
(`int rawFileSplitNum`, `int totalSplitNum` — generic names; or a single `scanRangeReadStats` map). Then in
`getNodeExplainString`, pass these to the connector via `appendExplainInfo`. Since `appendExplainInfo`'s
signature is `(StringBuilder, String prefix, Map<String,String> nodeProperties)`, thread the counts by
**adding them into a copy of the props map** the node passes to `appendExplainInfo` (e.g.
`__native_read_splits` / `__total_read_splits` synthetic keys), and have `PaimonScanPlanProvider
.appendExplainInfo` read them and emit `paimonNativeReadSplits=raw/total`. This keeps the paimon string in
the paimon connector and needs no SPI signature change.

**Gating:** `appendExplainInfo` is already connector-dispatched (only the active connector's provider runs),
so `paimonNativeReadSplits` is emitted ONLY for paimon. es/jdbc/mc providers do not emit it. The
synthetic count keys are injected by the shared node for ALL connectors but consumed only by paimon's
override — no other connector reads them, no perturbation.

**Summary of emission sites:**
| line | emitted in | gating |
|---|---|---|
| `pushdown agg=COUNT (n)` | `PluginDrivenScanNode.getNodeExplainString` (new) | none — universally correct |
| `dataFileNum/deleteFileNum/deleteSplitNum` | `PluginDrivenScanNode.getNodeExplainString` via extracted `FileScanNode` helper, VERBOSE-gated; `getDeleteFiles` via SPI | VERBOSE level; non-paimon return empty delete list |
| `paimonNativeReadSplits=raw/total` | `PaimonScanPlanProvider.appendExplainInfo` (new) | connector-dispatched (paimon only) |
| count sum (`-1` default) | node field `tableLevelRowCount` set in `getSplits` from `ConnectorScanRange.getPushDownRowCount()` | only set when a count range carries it |
| native/total split counts | node fields set in `getSplits` from `ConnectorScanRange.isNativeReadRange()` | generic; consumed only by paimon's appendExplainInfo |

# Implementation Plan

1. **SPI (`fe-connector-api/.../scan/ConnectorScanRange.java`)**: add two default methods:
   `default long getPushDownRowCount() { return -1; }` and
   `default boolean isNativeReadRange() { return false; }`.
2. **SPI (`ConnectorScanPlanProvider.java`)**: (Option B1) add
   `default List<String> getDeleteFiles(TTableFormatFileDesc tableFormatParams) { return emptyList(); }`.
3. **`PaimonScanRange.java`**: override `getPushDownRowCount()` (return the `rowCount` field, else -1) and
   `isNativeReadRange()` (`paimonSplit == null && path != null`).
4. **`PaimonScanPlanProvider.java`**:
   - override `appendExplainInfo(output, prefix, props)`: read the two synthetic count keys the node
     injects and emit `paimonNativeReadSplits=<native>/<total>`; optionally also re-emit
     `predicatesFromPaimon:` (needs predicates — already serialized in `paimon.predicate`, decode or
     skip — out of scope for the 5 tests).
   - override `getDeleteFiles(TTableFormatFileDesc)`: verbatim port of legacy
     `PaimonScanNode.getDeleteFiles` reading `getPaimonParams().getDeletionFile().getPath()`.
5. **`FileScanNode.java`**: extract lines 151-213 (the `VERBOSE && !isBatch` per-BE block) into
   `protected void appendBackendScanRangeDetail(StringBuilder output, String prefix)`; call it from the
   existing `getNodeExplainString` (no behavior change for existing FileScanNode subclasses).
6. **`PluginDrivenScanNode.java`**:
   - add fields `private long pushDownRowCount = -1; private int nativeReadSplitNum; private int totalReadSplitNum;`
     (or reuse `setPushDownCount`).
   - in `getSplits` (after building `splits`): if `countPushdown`, set `setPushDownCount(firstRowCount)`;
     accumulate `nativeReadSplitNum`/`totalReadSplitNum` from `range.isNativeReadRange()`.
   - override `protected List<String> getDeleteFiles(TFileRangeDesc rangeDesc)`: delegate to
     `connector.getScanPlanProvider().getDeleteFiles(rangeDesc.getTableFormatParams())` (null-guarded).
   - in `getNodeExplainString` (after `appendExplainInfo`, inside the non-PassthroughQueryTableHandle
     branch): inject `__native_read_splits`/`__total_read_splits` into the props passed to
     `appendExplainInfo`; emit the `pushdown agg=...` line; under `VERBOSE && !isBatchMode()` call
     `appendBackendScanRangeDetail`.
   - **Ordering of the injected counts:** `appendExplainInfo` runs inside `getNodeExplainString`, after
     `getSplits` already ran (finalize order verified), so the count fields are populated.

# Risk Analysis

- **Shared-node perturbation (PRIMARY risk).** `PluginDrivenScanNode` is shared by jdbc/es/trino/max_compute.
  - `pushdown agg=COUNT (n)`: added for ALL plugin connectors. Verified no other connector's suite uses
    `checkNotContains "pushdown agg"`; the maxcompute partition-prune suite
    (`external_table_p2/maxcompute/test_max_compute_partition_prune.groovy`) only does positive
    `contains "partition=N/M"` / `contains "CONNECTOR: max_compute"` — additive lines don't break `contains`.
    No `.out` file captures a `VPluginDrivenScanNode` block (grep: zero hits across `regression-test/data/`),
    so no golden explain shifts.
  - VERBOSE `deleteFileNum` block: emitted only at VERBOSE for plugin nodes that opt into the helper. Other
    connectors' `getDeleteFiles` returns empty → `deleteFileNum=0` if their VERBOSE block prints. To be
    conservative, the VERBOSE block can be gated to paimon (`getCatalog().getType().equals("paimon")` —
    available at `PluginDrivenScanNode.java:244`) so es/jdbc/mc VERBOSE output is byte-unchanged. **Decision:
    gate the VERBOSE block to paimon** to minimize blast radius (the 3 paimon assertions are the only
    consumers; the `pushdown agg` line stays ungated since it is universally correct and already standard
    for every FileScanNode).
  - `paimonNativeReadSplits`: connector-dispatched via `appendExplainInfo`, paimon-only by construction.
- **Value correctness (genOut risk).** CI dumped values in genOut. Verification: the `.out` count results
  (`test_paimon_count.out`: append=12, merge_on_read=8, deletion_vector=3) cross-check the explain values —
  `(12)`/`(8)` equal the actual counts (pushdown happened); `(-1)` is the no-precomputed-count sentinel and
  the dv table still returns 3 by BE counting. `paimonNativeReadSplits=0/1` is asserted under
  `force_jni_scanner=true`, where the native arm is provably skipped (`shouldUseNativeReader` returns false
  when `force_jni_scanner` is set) → 0 native, 1 jni. These are semantic, not just text. See Test Plan for
  the comparison-mode reruns that turn genOut into a real check.
- **Real regression vs display gap (per the brief's question).** All 5 are PURE DISPLAY gaps, NOT read-path
  regressions:
  - #1/#2 count: the data query (`qt_*_count`) returns the correct count via a normal VAGGREGATE; only the
    FE `pushdown agg` display line is absent. The count-pushdown OPTIMIZATION still happens BE-side
    (`paimon.row_count` is emitted on the range and consumed by `populateRangeParams`); FE just doesn't
    render it. (If the optimization were broken, the data result would still be correct — so the explain
    line is the only signal; the comparison-mode rerun in Test Plan confirms the BE row-count path.)
  - #4/#5 `paimonNativeReadSplits=0/1`: with `force_jni_scanner=true` the reader-selection is correct
    (everything JNI); the count is simply not displayed. The `qt_*` reads pass.
  - #3 `deleteFileNum`: the deletion vector is correctly applied BE-side (the merge-on-read `qt_*` results
    pass); only the VERBOSE accounting line is missing. The deletion file IS threaded to BE
    (`paimon.deletion_file.path` → `setDeletionFile`), just not surfaced in FE explain.
  Conclusion: NONE of the 5 hides a real read-path regression. They are all the
  `plugindriven-explain-override-gap` (no-super) class.
- **oss catalog load risk.** `test_paimon_deletion_vector_oss` uses `oss://` + `oss.endpoint`/`oss.access_key`
  (jindo, bundled per `e881247857d` FIX-PAIMON-OSS-JINDO-SELFCONTAINED). The test ran `qt_1..qt_6` before the
  explain assertion, so the catalog loaded and the oss reads succeeded — the explain gap is the only failure.
- **FileScanNode helper extraction.** Refactoring lines 151-213 into a protected method changes no behavior
  for existing FileScanNode subclasses (Hive/Iceberg/Hudi/Tvf) — it is a pure extract-method. Verify by
  running the iceberg/hive explain suites that assert `pushdown agg` (1 iceberg + 1 hive suite found).

# Test Plan

## Unit Tests

New `fe-core` tests on `PluginDrivenScanNode` (Mockito, same infra as
`PluginDrivenScanNodePartitionCountTest`):
- `getNodeExplainString` with `pushDownAggNoGroupingOp = COUNT` and `setPushDownCount(12)` → output
  contains `pushdown agg=COUNT (12)`; with no count set → `pushdown agg=COUNT (-1)`.
- count accumulation: feed a fake `ConnectorScanRange` list where one range has
  `getPushDownRowCount()=12` under `countPushdown=true` → node's `tableLevelRowCount` == 12; with none →
  stays -1. (Encodes WHY: the -1 sentinel must survive when no count range exists — Rule 9.)
- native/total accumulation: ranges with `isNativeReadRange()` mixed true/false → fields equal the counts;
  all-jni (force_jni) → native=0, total=N.
- `getDeleteFiles` override delegates to the provider and returns the deletion path when
  `TPaimonFileDesc.getDeletionFile()` is set; empty when unset.
- VERBOSE gating: assert the `dataFileNum/deleteFileNum/deleteSplitNum` block appears for a paimon-typed
  catalog at VERBOSE and NOT for a non-paimon-typed catalog (locks the gating decision).

New `fe-connector-paimon` tests on `PaimonScanPlanProvider` (offline, `PaimonScanPlanProviderTest` infra):
- `appendExplainInfo` with synthetic `__native_read_splits=0`/`__total_read_splits=1` → emits
  `paimonNativeReadSplits=0/1`.
- `getDeleteFiles(TTableFormatFileDesc)` returns the deletion path (port of legacy
  `PaimonScanNodeTest` deletion-file assertions if present).
- `PaimonScanRange.getPushDownRowCount()`/`isNativeReadRange()` for builder permutations
  (paimonSplit set vs path set; rowCount set vs not).

Run: `mvn -pl fe-core,fe-connector/fe-connector-paimon -am test` (use absolute `-f`; include `-am` to avoid
the `${revision}` resolution false error per memory `doris-build-verify-gotchas`). Checkstyle binds to the
fe-core test build — keep new tests clean.

## E2E Tests

Docker regression (paimon suite is `enablePaimonTest=true`-gated; NOT run locally in this design):
- Re-run the 5 suites in COMPARISON mode (not genOut) so the inline `explain { contains ... }` asserts the
  re-emitted lines AND the `qt_*`/`order_qt_*` data results compare against the committed `.out`:
  `test_paimon_count`, `test_paimon_deletion_vector`, `test_paimon_deletion_vector_oss`,
  `test_paimon_catalog_varbinary`, `test_paimon_catalog_timestamp_tz`.
  - This is the VALUE-VERIFICATION step: the `.out` count rows (12/8/3) confirm count pushdown correctness,
    independent of the explain text — turning the genOut dump into a real check.
- Regression-guard the shared node: re-run the maxcompute partition-prune suite
  (`external_table_p2/maxcompute/test_max_compute_partition_prune`) and any es/jdbc explain suites to
  confirm `partition=N/M` / `CONNECTOR:` assertions still pass and no stray paimon lines appear.
- Run the iceberg + hive suites that assert `pushdown agg` to confirm the `FileScanNode` extract-method is
  behavior-neutral.
