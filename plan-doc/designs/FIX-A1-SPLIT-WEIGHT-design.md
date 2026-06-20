# FIX-A1 — thread proportional split weight to the FE FileSplit (BE-assignment parity)

> Source: `task-list-P6-deviation-fixes.md` §A1 + `reviews/P6-paimon-fullpath-cleanroom-2026-06-18.md` §R1 (scan).
> Single-task loop: design → design red-team → implement → impl-verify → build+UT → commit.
> MINOR / regression. FE BE-assignment only — **no rows / route / BE-read / result change.**

## Problem

`PluginDrivenSplit`'s ctor (`PluginDrivenSplit.java:39-48`) forwards path/start/length/fileSize/modTime/
hosts/partitionValues to `FileSplit` but **never sets `selfSplitWeight` / `targetSplitSize`**. So
`FileSplit.getSplitWeight()` (`FileSplit.java:104-113`) hits the `else` branch → `SplitWeight.standard()`
(uniform) for every plugin-driven split. Legacy paimon set both fields, so `FederationBackendPolicy`
distributed splits across BEs by **proportional** weight (bigger split = more weight). Under the SPI all
paimon splits get uniform weight → the BE assignment differs from legacy (a scheduling skew, no
correctness impact).

## Root cause & the legacy parity spec (verified against real code)

`FileSplit.getSplitWeight()` returns proportional weight **iff** `selfSplitWeight != null && targetSplitSize
!= null`, computing `fromProportion(clamp(selfSplitWeight / targetSplitSize, 0.01, 1.0))`. The SPI never
populates either field. The legacy values (which we must reproduce):

**Per-split `selfSplitWeight`** (`PaimonSplit.java`):
- JNI / count split (`PaimonSplit(Split)` :50-64): `DataSplit` → `Σ dataFiles.fileSize` (:60);
  non-`DataSplit` system table → `rowCount()` (:63).
- Native sub-split (`PaimonSplit(LocationPath,start,length,…)` :67-72, built by `FileSplitter.splitFile` +
  `PaimonSplitCreator`): `selfSplitWeight = length` (the sub-range byte length, :72), **plus**
  `+= deletionFile.length()` when a DV is attached (`setDeletionFile` :112).

**Scan-level `targetSplitSize`** (the weight denominator, `PaimonScanNode.java:497-500`): set on **all**
splits to `getFileSplitSize() > 0 ? getFileSplitSize() : getMaxSplitSize()`, where `getMaxSplitSize()` =
the `max_file_split_size` var (default 64 MB, `SessionVariable:2408,4729`). This is a **different** value
from the file-splitting granularity `determineTargetFileSplitSize` (the connector's
`resolveTargetSplitSize`), and overrides whatever `FileSplitCreator` set.

**What the connector computes today vs needs:**
| split type | connector `selfSplitWeight` today | legacy | gap |
|---|---|---|---|
| JNI (`buildJniScanRange`) | `computeSplitWeight` = Σ fileSize / rowCount (:728-733,747) | same | none |
| count (`buildCountRange`) | `computeSplitWeight` (:778) | Σ fileSize | none |
| **native (`buildNativeRange`)** | **unset → Builder default 0** (:472-489) | `length` (+DV) | **MISSING** |
| `targetSplitSize` (all) | **never carried** | `fileSplitSize>0 ? : maxFileSplitSize` | **MISSING** |

So the task-list's "already computed, just not threaded" holds only for JNI/count. **Native is the common
path** (default ORC/Parquet read); leaving its `selfSplitWeight = 0` would make every native split's weight
`clamp(0/denom)=0.01` (uniform-ish), so wiring the getters WITHOUT fixing native would not achieve
proportional distribution. Both the native weight and the denominator must be added.

## Design

**Generic SPI getters + connector populates them + fe-core wires them.** Connector-agnostic: other
connectors (jdbc/es/trino/maxcompute) inherit the sentinel default → keep `SplitWeight.standard()` (no
regression).

1. **SPI `ConnectorScanRange`** (fe-connector-api) — two new default methods, sentinel `-1` = "no weight":
   ```java
   /** Per-split weight numerator for proportional BE assignment, or -1 if the connector
    *  does not provide one (→ the engine falls back to SplitWeight.standard()). */
   default long getSelfSplitWeight() { return -1; }
   /** Weight denominator (scan-level target split size), or -1 if not provided. Proportional
    *  weight is applied only when BOTH this and getSelfSplitWeight() are present. */
   default long getTargetSplitSize() { return -1; }
   ```
   A connector with no weight model returns both `-1` → unchanged behavior.

2. **`PluginDrivenSplit` ctor** (fe-core) — after `super(...)`, set the FileSplit fields only when the
   connector provides BOTH (guards div-by-zero and the null branch):
   ```java
   long weight = scanRange.getSelfSplitWeight();
   long target = scanRange.getTargetSplitSize();
   if (weight >= 0 && target > 0) {        // weight may legitimately be 0 (empty file / sys table)
       this.selfSplitWeight = weight;
       this.targetSplitSize = target;
   }
   ```
   Generic — no source-specific branching (rule: keep `PluginDrivenScanNode`/generic node connector-agnostic).

3. **`PaimonScanRange`** (connector) — carry the denominator and expose both getters:
   - Add `targetSplitSize` field + `Builder.targetSplitSize(long)` + `@Override getTargetSplitSize()`.
     **Builder default = `-1`** (the SPI sentinel "not provided"), NOT primitive `0` — a `0` denominator is
     invalid (div-by-zero / would be gated out). This is deliberately asymmetric with `selfSplitWeight`
     (default `0`, since `0` is a legitimate empty-file / 0-row-sys-table weight, which the `weight >= 0`
     gate accepts). Production always sets `targetSplitSize`; the `-1` default just keeps a Builder that
     omits it honest to the SPI contract.
   - `getSelfSplitWeight()` already returns the field; mark it `@Override` (it had no SPI declaration to
     satisfy before — verified it has no current callers besides being the field's accessor, so `@Override`
     is behavior-neutral). The `selfSplitWeight` field is the FE weight; the BE-thrift
     `paimon.self_split_weight` prop stays gated on `paimonSplit != null` (A3) so native ranges still do not
     emit it to BE — setting the field for native ranges changes only the FE getter.

4. **`PaimonScanPlanProvider`** (connector) — compute the denominator once and thread it:
   - New `resolveSplitWeightDenominator(session)` = `fileSplitSize>0 ? fileSplitSize :
     sessionLong(MAX_FILE_SPLIT_SIZE, DEFAULT_MAX_FILE_SPLIT_SIZE)` — exact legacy `getFileSplitSize()>0 ? :
     getMaxSplitSize()` parity (both read `file_split_size` / `max_file_split_size`; defaults 0 / 64 MB
     match). Computed once in `planScanInternal` (session-only), passed to every builder.
   - The threaded param + local is named **`weightDenominator`** EVERYWHERE (never `targetSplitSize`) so it
     cannot transpose with the existing file-splitting `targetSplitSize` / `effectiveSplitSize` local — a
     two-adjacent-`long` positional-swap is the one real bug risk here, name-isolated by construction.
   - `buildNativeRange`: `.selfSplitWeight(length + (deletionFile != null ? deletionFile.length() : 0))`
     (legacy `selfSplitWeight = length` + `+= deletionFile.length()`), `.targetSplitSize(weightDenominator)`.
   - `buildJniScanRange` / `buildCountRange`: add `.targetSplitSize(weightDenominator)` (selfSplitWeight already set).
   - Thread `weightDenominator` as an explicit param through `buildNativeRanges`/`buildNativeRange`/
     `buildJniScanRange`/`buildCountRange`. It is the weight base, computed even under count pushdown where
     the file-splitting size (`effectiveSplitSize`) is 0.
   - **Existing test call-sites of the changed signatures MUST be updated** (else compile break):
     `PaimonScanPlanProviderTest` calls `buildNativeRange` (~4 sites) and `buildNativeRanges` (~2 sites)
     directly — append the `weightDenominator` arg (any value, e.g. `64L*1024*1024`; those tests assert only
     URI-normalization / DV-on-every-sub-range, both denom-independent). Confirm exact line numbers at impl.

   **Why Option A (connector-owned SPI `getTargetSplitSize`) over Option B (fe-core computes the denominator):**
   hive/iceberg use a DIFFERENT denominator — the file-splitting granularity set by `FileSplitCreator`
   (`FileSplit.java:94`) — not paimon's `getFileSplitSize()>0 ? : getMaxSplitSize()`. A single fe-core
   denominator would mis-weight other connectors. The connector owning its denominator is both simpler and
   correct; do NOT later "simplify" the SPI getter away.

## No-regression / correctness

- **Other connectors unchanged:** sentinel `-1` default → `PluginDrivenSplit` leaves both FileSplit fields
  null → `getSplitWeight()` = `standard()` exactly as today. Verified **all 6 non-paimon
  `ConnectorScanRange` impls (jdbc / es / trino / maxcompute / hive / hudi)** do not reference/override the
  new getters → inherit the `-1` sentinel → `standard()`. (Hive's own `getTargetSplitSize` is a private
  plan-provider method, not an SPI override — no collision.)
- **Paimon = legacy parity:** JNI/count `selfSplitWeight` already matches; native now matches (`length`+DV);
  denominator matches legacy line 499 exactly. So `getSplitWeight()` reproduces legacy `fromProportion`.
- **weight 0 is valid** (empty file / 0-row sys table): the gate is `weight >= 0` (not `> 0`), so a genuine
  0 still yields `clamp(0/denom)=0.01`, matching legacy (whose denominator path is identical). Distinct
  from A3, which fixed the same 0-vs-unset confusion on the BE-thrift channel.
- **No BE/route/result change:** the FileSplit weight feeds only `FederationBackendPolicy` (FE split→BE
  assignment). `targetSplitSize > 0` guards div-by-zero; `denominator` defaults to 64 MB.

## Files

- `fe/fe-connector/fe-connector-api/.../scan/ConnectorScanRange.java` (2 default getters)
- `fe/fe-core/.../datasource/PluginDrivenSplit.java` (ctor gate)
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanRange.java` (targetSplitSize field/Builder/getter, @Override)
- `fe/fe-connector/fe-connector-paimon/.../PaimonScanPlanProvider.java` (denominator helper + thread + native weight)
- `fe/fe-connector/fe-connector-paimon/.../test/.../PaimonScanPlanProviderTest.java` (update ~6 changed-signature call-sites + new tests)
- new/extended UT in fe-core (PluginDrivenSplit) + fe-connector-api (SPI defaults) + connector (PaimonScanRange)

## Test Plan (RED→GREEN, each pinned by a mutation)

### Unit tests (each pins a concrete, non-vacuous expected value)
1. **`PluginDrivenSplit` (fe-core, the core regression):** build a `PluginDrivenSplit` from a fake
   `ConnectorScanRange` and assert the EXACT `getSplitWeight().getRawValue()` so RED (standard, rawValue 100)
   is always distinguishable from GREEN — pin concrete values that do NOT collapse to standard:
   - mid: `W=50, T=100` → proportion 0.5 → assert `rawValue == 50` (NOT standard's 100).
   - clamp-low: `W=1, T=100` → 0.01 floor → assert `rawValue == 1`.
   - default: a fake returning `-1/-1` → assert `getSplitWeight()` is `standard()` (rawValue 100).
   (Avoid `W>=T` cases — they clamp to 1.0 == standard and would false-pass even in the RED state.)
   A fake `ConnectorScanRange` is trivial — the same minimal anonymous impl exists in
   `PluginDrivenScanNodeExplainStatsTest` (only `getRangeType` + `getProperties` need a body). **RED before:**
   ctor sets neither field → every case returns `standard()` (rawValue 100) → the `==50`/`==1` asserts fail.
2. **SPI default (fe-connector-api):** an anonymous `ConnectorScanRange` (no override) returns `-1` for both
   getters. Guards the no-regression default.
3. **`PaimonScanRange` sentinel + round-trip (connector):** (i) a Builder WITHOUT `.targetSplitSize()` →
   `getTargetSplitSize() == -1` (pins the sentinel default + SPI contract); (ii) a Builder WITH
   `.selfSplitWeight(W).targetSplitSize(T)` → both getters round-trip `W`/`T`.
4. **`buildNativeRange` weight (connector):** call `buildNativeRange(file, dv, …, start, length,
   weightDenominator)` → range `getSelfSplitWeight() == length (+ dv.length())` and `getTargetSplitSize() ==
   weightDenominator`. Constructible fully offline — `buildNativeRange` is package-private and existing
   tests already build `new RawFile(...)` / `new DeletionFile(...)` (no `FileSystemCatalog`). **MUTATION:**
   drop the native `.selfSplitWeight(...)` → weight 0 → RED.
5. **`buildNativeRanges` positional-swap guard (connector):** call `buildNativeRanges` with a file-split
   target (e.g. `33`) numerically DISTINCT from the `weightDenominator` (e.g. `64MB`) on a multi-sub-range
   file → assert (a) range COUNT == `computeFileSplitOffsets(fileLength, 33).size()` (splitting follows the
   file-split target) AND (b) every range `getTargetSplitSize() == 64MB` (the denominator). REDs on a swap
   of the two adjacent `long` args.
6. **`resolveSplitWeightDenominator` + count-pushdown (connector):** `file_split_size` set → returns it;
   unset → returns `max_file_split_size` (default 64 MB). Plus: a count-pushdown native range (file-split
   `effectiveSplitSize == 0`) still gets a POSITIVE `weightDenominator` → non-standard weight (guards the
   denominator being computed independently of the file-split size).

### E2E
Gated (`enablePaimonTest=false`) — NOT run. `FederationBackendPolicyTest` (existing) already covers the
weight→assignment mapping; the UT proves the weight is now non-standard, which is the regression.
