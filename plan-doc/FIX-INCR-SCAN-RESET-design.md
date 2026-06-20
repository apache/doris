# FIX-INCR-SCAN-RESET — Design

> Source: `reviews/P5-paimon-rereview3-2026-06-12.md` (P2-1, **MAJOR**; was NIT in rereview2);
> task `task-list-P5-rereview3-fixes.md` FIX-3. **Connector-only, no BE / no SPI change.**
> Design red-team (5 skeptics + completeness critic, `wf_ffd11631-ed2`): **DESIGN-SOUND**, unanimous
> **Option 2** (inject the reset at the `Table.copy` chokepoint; keep the shared SPI null-free).

## Problem
A Paimon `@incr(...)` incremental read can read the **wrong rows** — or hard-fail — when the base table
**persists** a `scan.snapshot-id` / `scan.mode` option (legal & mutable via `ALTER TABLE SET`,
`TBLPROPERTIES`, or `table-default.*` catalog options). The connector's `@incr` path produces only the
`incremental-between*` scan options and applies them with `Table.copy(...)`, but it **dropped** legacy's
defensive null-reset of `scan.snapshot-id` / `scan.mode`. So the freshly-loaded base table's persisted
`scan.snapshot-id` survives into the copied table and collides with `incremental-between`.

Two concrete failure modes (both verified against paimon 1.3.1; the second was reproduced **empirically**
offline by the red-team):
- **Hard throw** (persisted `scan.snapshot-id` present): `Table.copy({incremental-between=…})` throws
  `IllegalArgumentException: "[incremental-between] must be null when you set
  [scan.snapshot-id,scan.tag-name]"`.
- **Silent wrong rows** (persisted `scan.snapshot-id` with no `scan.mode`): `CoreOptions.setDefaultValues`
  sets `scan.mode=FROM_SNAPSHOT` *before* the `INCREMENTAL` branch, and `startupMode()` checks
  `SCAN_SNAPSHOT_ID` before `INCREMENTAL_BETWEEN` → the read becomes `FROM_SNAPSHOT` at the stale id and
  `incremental-between` is silently ignored. The stale `scan.snapshot-id` (a `SCAN_KEY`) also pins the
  schema to the wrong version via `tryTimeTravel`.

## Root Cause
`PaimonIncrementalScanParams.validate()` (lines 222-265) intentionally **strips** legacy's
`paimonScanParams.put("scan.snapshot-id", null)` and `put("scan.mode", null)` (legacy
`PaimonScanNode.validateIncrementalReadParams:842-843,846`, applied via
`baseTable.copy(getIncrReadParams())` at `:896`). The strip was justified by a rationale that is **wrong**:
the class javadoc (lines 39-49) and the inline note (222-229) claim the reset is "byte-parity in EFFECT on
a freshly-loaded base table" because the connector loads a fresh `Table` per query (so "nothing to reset")
and because `ConnectorMvccSnapshot` rejects null values.

Both premises fail:
- **Per-query freshness ≠ option freshness.** The base table comes straight from `catalog.getTable(...)`
  (`CatalogBackedPaimonCatalogOps.getTable`), whose options are built from the **persisted** `TableSchema`
  (`FileStoreTable.options() == schema().options()`). Persisted `scan.*` is therefore present on every
  fresh load. The connector strips nothing before `copy`.
- **Why the reset matters.** paimon 1.3.1 `AbstractFileStoreTable.copyInternal` merges dynamic options
  with exactly `v == null ? options.remove(k) : options.put(k, v)`. A **null** value is the SDK's documented
  reset (remove) mechanism — the only way to clear a persisted `scan.snapshot-id`/`scan.mode`. `scan.mode`
  and `scan.snapshot-id` are **not** `@Immutable` in 1.3.1, so `copy`'s `checkImmutability`
  (`Objects.equals(old,new)` → else `SchemaManager.checkAlterTableOption`) does **not** throw on the reset;
  for a non-persisted key (`old==null`, `new==null`) it is a pure no-op.

## Design — Option 2 (chosen)
Keep `validate()` emitting **only** the non-null `incremental-between*` keys (so the shared SPI type
`ConnectorMvccSnapshot` and `PaimonTableHandle.scanOptions` stay **null-free** — preserving the
`Builder.property(k,v)` `requireNonNull` contract, the `getProperties()` "never null" javadoc, and the two
existing tests that pin "no null values"). Reintroduce legacy's two null resets **locally at the single
`Table.copy` chokepoint**, where the nulls are created and immediately consumed by `copyInternal`'s
`options.remove(k)` — never stored, never serialized, never placed in the SPI.

Add a helper **owned by `PaimonIncrementalScanParams`** (the rightful home of the incremental-key
knowledge), gated on the presence of an incremental key:

```java
public static Map<String, String> applyResetsIfIncremental(Map<String, String> scanOptions) {
    if (scanOptions == null || scanOptions.isEmpty()) {
        return scanOptions;
    }
    if (!scanOptions.containsKey(PAIMON_INCREMENTAL_BETWEEN)
            && !scanOptions.containsKey(PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP)) {
        return scanOptions;                       // non-incremental pin → unchanged (no false positive)
    }
    Map<String, String> withResets = new HashMap<>();
    withResets.put(PAIMON_SCAN_SNAPSHOT_ID, null); // legacy reset: clear a persisted stale pin at copy time
    withResets.put(PAIMON_SCAN_MODE, null);
    withResets.putAll(scanOptions);
    return withResets;
}
```

Call it inside `PaimonScanPlanProvider.resolveScanTable` (the lone `table.copy(scanOptions)` site, lines
248-255):

```java
return table.copy(PaimonIncrementalScanParams.applyResetsIfIncremental(scanOptions));
```

This single edit covers **both** `resolveScanTable` callers — `planScanInternal:292` (native/JNI scan) and
`getScanNodeProperties:515` (JNI serialized-table for BE, which serializes the **post-copy** table) — through
the shared chokepoint, so native and JNI `@incr` reset identically.

**Detection soundness** (verified): every successful `validate()` output contains exactly one of
`incremental-between` / `incremental-between-timestamp` (snapshot group always emits `incremental-between`;
timestamp group always emits `incremental-between-timestamp`; `incremental-between-scan-mode` is only ever
emitted *alongside* `incremental-between`). And **no** non-incremental scan-options producer emits either
key (`SNAPSHOT_ID`/`TIMESTAMP` → `scan.snapshot-id`; `TAG` → `scan.tag-name`; `BRANCH` routed before the
properties path; latest-pin → `scan.snapshot-id` only). So the helper resets **iff** the scan is
incremental — it never clobbers a legitimate `scan.snapshot-id`/`scan.tag-name` pin.

**Scope = strict legacy parity:** reset **only** `scan.snapshot-id` + `scan.mode` (exactly legacy
`PaimonScanNode:842-843,846`). Do **not** broaden to the other `SCAN_KEYS` (`scan.timestamp`,
`scan.timestamp-millis`, `scan.tag-name`, `scan.watermark`) that could also hijack `startupMode` — legacy
did not reset those, and the task-list pins the two-key scope.

### Why not Option 1 (re-add the nulls in `validate()`, ride through the SPI)
Mechanically it works (the nulls survive `ConnectorMvccSnapshot.Builder.properties(Map)`'s `putAll` and
the handle, and `copy` resolves them), but it is the wrong design: it **breaks the shared SPI's null-free
contract** for future consumers (iceberg/hudi), depends on a **silent gap** in `properties(Map)` (it lacks
the per-value `requireNonNull` that `property(k,v)` has — a future, correct hardening would silently
re-break `@incr`), **inverts** two existing green tests + the `getProperties()` javadoc, and leaks a
paimon-SDK quirk (`copy`: null == remove) into a source-agnostic type. Option 2 achieves identical engine
behavior with none of that.

## Implementation Plan
1. **`PaimonIncrementalScanParams.java`** — add `public static Map<String,String>
   applyResetsIfIncremental(Map<String,String>)` using the existing private constants
   (`PAIMON_SCAN_SNAPSHOT_ID`, `PAIMON_SCAN_MODE`, `PAIMON_INCREMENTAL_BETWEEN`,
   `PAIMON_INCREMENTAL_BETWEEN_TIMESTAMP`) — **no string literals**, so the detector key set cannot drift
   from the emitter set. Javadoc the WHY (legacy reset at copy time; nulls consumed by `copyInternal`).
2. **`PaimonScanPlanProvider.java`** — in `resolveScanTable` (248-255), wrap the `table.copy(scanOptions)`
   argument with `PaimonIncrementalScanParams.applyResetsIfIncremental(...)`. Keep the existing
   `scanOptions != null && !scanOptions.isEmpty()` guard unchanged.
3. **Doc fanout (Rule 9/12)** — correct the now-refuted "byte-parity on a freshly-loaded base table"
   rationale: `PaimonIncrementalScanParams` class javadoc (39-49) + inline note (222-229/233); the
   `INCREMENTAL`-case comments in `PaimonConnectorMetadata` (≈410-413, 490-492, 505-506). Reword to: the
   snapshot/SPI stays null-free **by design**, and the legacy null resets are reapplied at the `Table.copy`
   chokepoint via `applyResetsIfIncremental`.

## Risk Analysis
- **No SPI / no BE change.** Connector-only; import-gate clean (helper uses only `java.util` + paimon SDK).
- **Common path unaffected:** `applyResetsIfIncremental` returns the input map **unchanged** for every
  non-incremental scan (snapshot/tag/timestamp pin, latest read) and for empty options — so
  `resolveScanTableAppliesSnapshotPinViaCopy` / `…WithoutScanOptionsDoesNotCopy` stay green. The extra map
  allocation happens only on `@incr` reads (negligible).
- **paimon-version coupling:** the reset relies on 1.3.1 semantics (`null` → remove; `scan.*` mutable). A
  future paimon that marks these `@Immutable` would make the reset throw. Mitigated by the real-table test
  asserting `copy` with the resets does **not** throw against the bundled jar (fails loud on upgrade).
- **Forward-compat:** if a *future* `ConnectorTimeTravelSpec.Kind` ever emitted an `incremental-between*`
  key as a side property **and** wanted a real `scan.snapshot-id` pin, the helper would clobber it. Today
  only `INCREMENTAL` emits these keys — a caveat, not a current defect; co-locating the detector keys with
  the emitter constants mitigates rename drift.

## Test Plan
### Unit Tests (connector, offline)
- **`PaimonScanPlanProviderTest.resolveScanTableResetsStalePinForIncrementalRead` (NEW, real table — the
  fail-before/pass-after gate).** Build a real paimon 1.3.1 `FileSystemCatalog` + `LocalFileIO` table under
  `@TempDir` (reuse the existing `buildRealDataSplit` recipe), created with
  `.option("scan.snapshot-id","1").option("scan.mode","from-snapshot")` and at least one committed row so
  `tableSchema.options()` persists `scan.snapshot-id`. Seat it on the handle
  (`handle.setPaimonTable(realTable)`), pin `handle.withScanOptions({incremental-between:"3,5"})`, call
  `provider.resolveScanTable(handle)`. **Before fix:** `copy` throws `IllegalArgumentException` (or returns
  a table still carrying `scan.snapshot-id`). **After fix:** returned `table.options()` has **no**
  `scan.snapshot-id` and `incremental-between=3,5`. (A `FakePaimonTable` test cannot be the gate —
  `FakePaimonTable.copy` is a no-op recorder that does not implement merge/remove, so it can't fail-before;
  Rule 9.)
- **`PaimonIncrementalScanParamsTest.applyResetsIfIncrementalSeedsNullResetsForIncremental` (NEW, unit).**
  `applyResetsIfIncremental({incremental-between:"3,5"})` → map contains key `scan.snapshot-id` with **null**
  value AND key `scan.mode` with **null** value AND `incremental-between=3,5`; same for
  `{incremental-between-timestamp:"100,200"}`. Encodes WHY: nulls reach `copy` to remove a stale persisted
  pin.
- **`PaimonIncrementalScanParamsTest.applyResetsIfIncrementalPassesThroughNonIncremental` (NEW, unit).**
  `applyResetsIfIncremental({scan.snapshot-id:"5"})`, `({scan.tag-name:"t"})`, and an empty/null map return
  the input **unchanged** (no `scan.mode` injected, no null values) — the no-false-positive invariant that
  protects the snapshot/tag/timestamp pin paths.

### Existing tests
- **No structural change.** `PaimonIncrementalScanParamsTest.nullResetKeysAreStrippedNotPresentWithNull`
  (228-242) and `PaimonConnectorMetadataMvccTest.resolveIncrementalDoesNotEmitScanSnapshotId` (684-693)
  **stay green** (validate()/snapshot are unchanged) — do **not** invert them. Reword only their inline
  WHY-comments (currently "byte-parity on a freshly-loaded base") to "the SPI/snapshot stays null-free by
  design; the legacy null resets are reapplied at the `Table.copy` chokepoint in `resolveScanTable`".

### E2E
- Live `@incr`-over-persisted-`scan.snapshot-id` regression is **CI-gated** (`enablePaimonTest=false`) — not
  run in this environment; noted as gated, not claimed. The offline real-table unit test above is the
  load-bearing proof.

## Build / Verify
- `mvn -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :fe-connector-paimon -am
  -Dmaven.build.cache.enabled=false -DfailIfNoTests=false test` → read surefire XML + `MVN_EXIT`.
- `mvn -pl :fe-connector-paimon checkstyle:check`; `bash tools/check-connector-imports.sh`.

## Commit
`fix: FIX-INCR-SCAN-RESET` — connector-only; carries this design doc (repo convention).
