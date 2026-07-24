# FIX: `__HIVE_DEFAULT_PARTITION__` on a non-string partition column drops the partition

Status: **DONE** (code `58f3e367ed6`) · Branch `catalog-spi-11-hive` · Verified against HEAD `e2fdd65b954`
Result: implemented per §7; UT green — fe-connector-api 6, fe-core 67 (PluginDrivenMvcc + PartitionTest),
fe-connector-hive 9, fe-connector-paimon 10 (install); 0 failures, 0 checkstyle across all four modules.
**e2e still owed by the user** (see §7 step 11 + §8): `test_hive_default_partition` (INT+DATE) · paimon
`qt_null_partition_*` · `test_paimon_mtmv` (golden 3→5 rows regenerate + MV null-partition-creation gate).
Decision: **Approach A + user-signed variant B** (2026-07-12) — connector-supplied per-value NULL flag via SPI,
applied to **both hive AND paimon** (paimon adopts genuine-NULL semantics, not just hive).

Recon+design workflow: `wf_c9f00bc2-470` (5 HEAD-verified recon angles + synthesis).
Design red-team: `wf_1cdaf44e-325` (5 adversarial lenses, all **SOUND_WITH_FIXES**, 0 blocker/UNSOUND). Findings
RT-F1..RT-F6 folded inline below (fail-loud arity, exact-sentinel hive detection, positional hive flag build,
paimon MTMV golden as impacted suite, RED-baseline wording, paimon TVF scoped out).

---

## 1. Problem

A partitioned external table whose rows land in the genuine-NULL partition — rendered as the literal
partition name `col=__HIVE_DEFAULT_PARTITION__` — is silently mis-planned when the partition column is
**non-string** (INT, DATE, …). The default partition is dropped from the in-memory partition universe; if
it is the only partition the table reports `UNPARTITIONED` and the scan explain shows `partition=0/0`, and
`WHERE col IS NULL` returns nothing.

- **hive**: the reported regression (`test_hive_default_partition`, INT partition column).
- **paimon**: the *same* latent bug on a non-string (e.g. DATE) default partition — paimon renders the
  sentinel into the name and fe-core log-skips it today (test fixture
  `PaimonConnectorMetadataPartitionTest.nullDatePartitionRendersSentinelInsteadOfCrashing`). Additionally,
  even on a string column paimon's genuine-null partition is currently a non-null `StringLiteral`, so MTMV
  refresh emits `col IN ('__HIVE_DEFAULT_PARTITION__')` and **silently drops the null rows from the MV**.

Legacy `HiveExternalMetaCache:309` handled hive correctly (`isNull = HIVE_DEFAULT_PARTITION.equals(value)`).
The SPI migration copied the *paimon* builder verbatim into shared fe-core, hardcoding `isNull=false`,
regressing hive and freezing paimon into the drop/`col IN` behavior.

## 2. Root Cause

`fe/fe-core/.../PluginDrivenMvccExternalTable.java:299-311` — the shared MVCC live/pruning partition-item
builder marks every value non-null:

```java
for (String partitionValue : partitionValues) {          // values re-parsed positionally from the NAME (L296)
    values.add(new PartitionValue(partitionValue, false));  // L310  isNull hardcoded false
}
PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);   // L312
```

Throw chain (verified):
1. `PartitionKey.createListPartitionKeyWithTypes` (`catalog/PartitionKey.java:189-202`): `isNullPartition()==false`
   and type ≠ TIMESTAMPTZ → L201 `values.get(i).getValue(INT)`.
2. `PartitionValue.getValue(Type)` (`analysis/PartitionValue.java:48-56`): not null →
   `LiteralExprUtils.createLiteral("__HIVE_DEFAULT_PARTITION__", INT)`.
3. → `new IntLiteral("__HIVE_DEFAULT_PARTITION__", INT)` (`fe-catalog/.../IntLiteral.java:65-72`) →
   `Long.parseLong(...)` throws `NumberFormatException` → wrapped `AnalysisException("Invalid number format: …")`.
4. Caught **per-partition** in `listLatestPartitions` (`PluginDrivenMvccExternalTable.java:272-280`,
   log-and-skip). The name already entered `nameToLastModifiedMillis` (L271) but the item map did not.
5. Size mismatch → `PluginDrivenMvccSnapshot.isPartitionInvalid()` (`nameToLastModifiedMillis.size() !=
   nameToPartitionItem.size()`) → `getPartitionType` returns `UNPARTITIONED` → `partition=0/0`.

The isNull branch (`PartitionKey.java:190-191`) instead produces a typed `NullLiteral.create(type)` and
never parses the sentinel string — INT/DATE-safe.

**Why fe-core cannot re-add the string compare (iron rule):** paimon (`PaimonConnectorMetadata.java:1057`)
also renders the *identical* string `__HIVE_DEFAULT_PARTITION__` into its name. fe-core cannot decide
nullness from the string; it must be connector-supplied. (Under variant B both connectors want the same
`isNull=true` answer, but they still detect their own null representation — hive's HMS sentinel vs paimon's
`partition.default-name` option — so the detection must stay connector-side.)

## 3. Design — connector-supplied per-value NULL flag (positional `List<Boolean>`)

### 3.1 SPI shape — positional `List<Boolean>` (reject name-keyed map, reject Java-null-in-map)

The fe-core seam derives its values by **re-parsing the rendered partition name positionally**
(`HiveUtil.toPartitionValues(partitionName)`, L296) — it does not consume `getPartitionValues()`. So the
flag carrier must align to that same positional order.

- **Reject Option B (map/set keyed by column name):** every existing `partitionValues` map is keyed by
  **remote** names, while fe-core would key by the local Doris name → breaks under name-mapping / casing
  divergence.
- **Reject Option C (Java-null value in the `partitionValues` map):** forces fe-core to abandon the
  name-reparse seam; breaks paimon byte-parity (paimon's map is the RAW un-rendered spec while the NAME
  carries rendered dates) and perturbs the two other map consumers (`PluginDrivenExternalTable.java:814,868`).
- **Accept Option A (positional `List<Boolean>`):** zips index-for-index at the seam; casing/order-immune
  (both sides derive order from the identical positional name parse — hive's `HiveWriteUtils.toPartitionValues`
  is a byte-faithful port of fe-core `HiveUtil.toPartitionValues`; paimon builds the flag in the same
  `spec.entrySet()` loop that builds the name). Empty ⇒ all-false ⇒ zero change for connectors that don't
  opt in and for both existing ctors. Mirrors the already-shipped precedent
  `ConnectorPartitionValues.Normalized{List<String> values, List<Boolean> isNull}`
  (`fe-connector-api/.../scan/ConnectorPartitionValues.java:32-44`).

### 3.2 Exact changes

**(a) SPI — `fe-connector-api/.../ConnectorPartitionInfo.java`** — add one field + one accessor + one new
ctor; thread through `equals`/`hashCode`/`toString`:
```java
private final List<Boolean> partitionValueNullFlags;   // positional, aligned to getPartitionName() parse order

// new 8-arg ctor:
public ConnectorPartitionInfo(String partitionName, Map<String,String> partitionValues,
        Map<String,String> properties, long rowCount, long sizeBytes, long lastModifiedMillis,
        long fileCount, List<Boolean> partitionValueNullFlags) { ... }

public List<Boolean> getPartitionValueNullFlags() { return partitionValueNullFlags; }
```
- Null arg ⇒ `Collections.emptyList()`; else `Collections.unmodifiableList(new ArrayList<>(...))`. Class stays `final`.
- 7-arg ctor delegates to the new 8-arg with `emptyList()`; 3-arg already delegates to 7-arg. Both existing
  ctors keep working unchanged.
- Add the field to `equals`/`hashCode`/`toString` (value-based contract; `ConnectorPartitionInfoTest` enforces it).

**(b) fe-core — `PluginDrivenMvccExternalTable.java`** — thread the flag through `toListPartitionItem`; the
lone caller is `listLatestPartitions` L276:
```java
// L276:
nameToPartitionItem.put(partitionName, toListPartitionItem(partitionName, types, part.getPartitionValueNullFlags()));

// builder:
private static ListPartitionItem toListPartitionItem(String partitionName, List<Type> types,
        List<Boolean> nullFlags) throws AnalysisException {
    List<String> partitionValues = HiveUtil.toPartitionValues(partitionName);
    Preconditions.checkState(partitionValues.size() == types.size(), partitionName + " vs. " + types);
    // Fail loud (RT-F1): a connector that supplies flags MUST supply one per value; a short list would
    // silently default the tail to isNull=false and re-introduce the drop bug. Empty = non-opted-in = OK.
    Preconditions.checkState(nullFlags.isEmpty() || nullFlags.size() == types.size(),
            "nullFlags " + nullFlags + " vs. " + types);
    List<PartitionValue> values = Lists.newArrayListWithExpectedSize(types.size());
    for (int i = 0; i < partitionValues.size(); i++) {
        boolean isNull = i < nullFlags.size() && nullFlags.get(i);   // empty ⇒ false ⇒ non-opted-in connectors unchanged
        values.add(new PartitionValue(partitionValues.get(i), isNull));
    }
    PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, true);
    return new ListPartitionItem(Lists.newArrayList(key));
}
```
- Rewrite the L300-309 comment: it currently rationalizes unconditional `isNull=false` for paimon. Under B it
  must describe the connector-supplied flag: a genuine-NULL value (hive's HMS sentinel, paimon's
  `partition.default-name`) is marked `isNull=true` by its connector → typed `NullLiteral` → `col IS NULL`
  selects it and MTMV refresh materializes the null rows; a connector that supplies no flag defaults to non-null.

**(c) hive connector — `HiveConnectorMetadata.java` (`listPartitions` ~L1105)** — build the positional flag
list by **iterating `HiveWriteUtils.toPartitionValues(partitionName)` directly** (RT-F5: the same positional
parse fe-core re-runs, so flag[i] zips to value[i] regardless of column casing/order — do NOT iterate the
value Map/partKeyNames). Detection uses an **exact** `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION.equals(value)`
compare, **not** `isNullPartitionValue` (RT-F2: byte-parity with legacy `HiveExternalMetaCache:309`, which
marks null on the sentinel only — `isNullPartitionValue` also widens to `\N`/Java-null, an unwanted parity
divergence; HMS partition names never carry `\N` anyway). Pass the 8-arg ctor. The sentinel constant already
lives connector-side (`ConnectorPartitionValues.HIVE_DEFAULT_PARTITION`, already a hive dependency).

**(d) paimon connector — `PaimonConnectorMetadata.java` (`collectPartitions` ~L1044-1080)** — build a
positional flag list in the **same `spec.entrySet()` loop** that builds the name; set `true` at the existing
null branch L1050 (`defaultPartitionName.equals(value)`) — paimon detects its own default-name, NOT the hive
sentinel. Pass the flag list to the ctor. **Keep the name normalization to `__HIVE_DEFAULT_PARTITION__`
(L1057) unchanged** — the partition NAME identity must not change (avoids re-keying existing partitions /
MTMV names). Update the L1050-1057 comment: the intent it describes ("bridge marks the partition isNull") is
now *realized* via the supplied flag.

**(e) fe-core — `PluginDrivenScanNode.java` opt-out comment (L238-280)** — comment-only. The
`ignorePartitionPruneShortCircuit` opt-out **code + capability stay** (still required for genuine predicate
prune-to-zero, e.g. paimon `col = <absent value>`). But its worked example — "with `isNull=false` a
genuine-null partition renders as a non-null sentinel, so `col IS NULL` prunes every partition away" (L244-246,
L272-275) — is **stale under B**: paimon's null partition is now a real `NullLiteral`, so `col IS NULL`
prunes *accurately* to it (non-empty selection → opt-out at L276 does not fire; planScan still re-plans).
Replace the `col IS NULL` example with a still-valid one (`col = <absent value>` → genuine prune-to-zero).

**(f) other connectors** — hudi (`HudiConnectorMetadata.java:709`), maxcompute
(`MaxComputeConnectorMetadata.java:274`), iceberg (`IcebergPartitionUtils.java:545`): keep their existing
ctors → flags default empty → `isNull=false`. **Zero change.**

### 3.3 Out of scope (noted follow-ups)

- **iceberg latent analogous bug**: identity/bucket/truncate LIST branch renders a genuine Java-null as the
  literal text `col=null` (`IcebergPartitionUtils.java:620`); on a non-string column fe-core throws and drops
  the partition — the same failure mode. The flag mechanism would let iceberg opt in at
  `IcebergPartitionUtils.java:542-543` (typed Java-null already in hand), but iceberg is not in any failing
  suite and this touches signed-off P6 code. **Deferred** to a separate task (user-agreed).

## 4. Iron-Rule Compliance

fe-core has **no** source-specific code after the fix: `toListPartitionItem` reads a connector-supplied
`List<Boolean>` and calls `new PartitionValue(value, isNull)`. No `if (hive/paimon)`, no `instanceof HMS*`,
no `HIVE_DEFAULT_PARTITION.equals(...)` in fe-core. The sentinel/default-name compare lives entirely in each
connector. The new field is a generic per-value nullness carrier, semantically identical to the existing
`ConnectorPartitionValues.Normalized.isNull` list already in the API module. (Pre-existing fe-core sentinel
compares at `TablePartitionValues.java:162` and the non-MVCC base path are **not reached by hive/paimon** —
overridden by the MVCC subclass — and are left untouched.)

## 5. Parity & Behavior Change

| | before | after (B) |
|---|---|---|
| **hive** INT/DATE null partition | dropped → `partition=0/0`, `col IS NULL` empty | `NullLiteral` partition; `col IS NULL` returns null rows; count correct (legacy `HiveExternalMetaCache:309` parity restored) |
| **hive** string null partition | `StringLiteral` sentinel | `NullLiteral` (legacy parity) |
| **paimon** string null partition | `StringLiteral` sentinel; MTMV `col IN (sentinel)` **drops null rows** | `NullLiteral`; MTMV `col IS NULL` **materializes null rows** (realizes the connector's stated intent) |
| **paimon** DATE/INT null partition | dropped (log-skip) | `NullLiteral` partition (fixed) |
| **paimon** `col IS NULL` query | prune-away → opt-out scan-all → planScan re-plan | prune *accurately* to null partition → planScan re-plan (same rows, more precise) |
| **paimon** `col = <absent>` query | opt-out scan-all → planScan → 0 rows | unchanged (opt-out still fires) |
| hudi / maxcompute / iceberg | — | unchanged (default-false) |

`test_hive_default_partition` (hive) and paimon null-partition regressions (`qt_null_partition_*`) are the
e2e gates. `qt_null_partition_4` (paimon `col IS NULL`) returns the same rows via the accurate-prune path;
must be re-run (user).

## 6. Risk Analysis / Blast Radius

- **paimon behavior change** (the wider-scope part of B): its genuine-null partition flips from
  `StringLiteral` to `NullLiteral`. Downstream touch points to verify in red-team: (1) MTMV refresh
  (`UpdateMvByPartitionCommand.java:183-202` — NullLiteral → `col IS NULL`, StringLiteral → `col IN`);
  (2) the `PluginDrivenScanNode` prune opt-out (still needed for `col=<absent>`, no longer needed for
  `col IS NULL`); (3) partition NAME identity unchanged (name normalization kept); (4) partition count
  display now counts the paimon null partition (was: string partition, also counted — no `0/0` regression
  since paimon string sentinel never threw).
- **`ConnectorPartitionInfo.equals/hashCode/toString`**: field added to all three; existing equal instances
  stay equal (both empty lists).
- **ctor compatibility**: both existing ctors preserved; only hive + paimon use the new 8-arg ctor. All other
  construction sites and tests compile unchanged.
- **Non-MVCC base path / SHOW PARTITIONS**: untouched — consume the value map or name only, never
  `toListPartitionItem`.
- **`partition_values()` TVF / `$partitions`** (RT-F6): the flag is **not** threaded here (separate surface —
  `getNameToPartitionValues` → `MetadataGenerator.partitionValuesRows`, string-compares the raw value against
  the hive sentinel at `:2166`). **hive** is unaffected (its value map carries `__HIVE_DEFAULT_PARTITION__`,
  matched at `:2166`). **paimon** is a **pre-existing** gap (NOT introduced or worsened by this fix): its value
  map carries the RAW `partition.default-name` (e.g. `__DEFAULT_PARTITION__`), which `:2166` does not match, so
  a non-string paimon default-partition column throws `Integer.valueOf("__DEFAULT_PARTITION__")` at `:2177`. The
  §5 correction: this fix restores the LIST/scan/prune surface only; the paimon TVF surface is **out of scope**
  (deferred follow-up, same iron-rule reason — fe-core can't string-detect paimon's default-name).
- **Impacted EXISTING e2e suite — `mtmv_p0/test_paimon_mtmv.groovy`** (RT-F3, MAJOR): it builds an MTMV over the
  paimon `null_partition` table (`partition by(region)`) with a `// Will lose null data` comment and a committed
  golden (`test_paimon_mtmv.out:138-141`) that deliberately **omits** the two genuine-NULL `region` rows. Under
  B the null partition becomes a `NullLiteral` → MTMV refresh emits `region IS NULL` → the MV **gains** those
  rows → golden flips 3→5 rows. This is an **expected** behavior change, not a new test: (1) update the stale
  `// Will lose null data` comment; (2) the `.out` golden must be **regenerated by the e2e run** (do NOT
  hand-write — `勿 rebless data-wrong`); (3) **e2e gate**: confirm the MV auto-creates the null list partition
  (`VALUES IN (NULL)`) without throwing — the `region` column is nullable (base STRING, no NOT NULL) so it
  should be allowed, but a NOT-NULL partition column would be rejected (`test_null_partition.groovy:44`); if it
  throws, B breaks paimon MTMV creation and must be gated.

## 7. Implementation Plan (ordered)

1. `fe-connector-api/.../ConnectorPartitionInfo.java` — field + 8-arg ctor + getter + equals/hashCode/toString; 7-arg delegates with `emptyList()`.
2. `fe-connector-api/.../ConnectorPartitionInfoTest.java` — ctor/getter, back-compat-default (3-arg & 7-arg → empty), equals/hashCode-with-flags.
3. `fe-core/.../PluginDrivenMvccExternalTable.java` — `toListPartitionItem` gains `List<Boolean> nullFlags`, empty-safe consume, caller L276; rewrite L300-309 comment.
4. `fe-core/.../PluginDrivenScanNode.java` — update the stale `col IS NULL` example in the opt-out comment (code unchanged).
5. `fe-core/.../PluginDrivenMvccExternalTableTest.java` — `cpiNull` helper + RED tests (INT/DATE sentinel+flag → LIST/NullLiteral); keep no-flag default test green; flag-absent-INT-still-drops guard.
6. `fe-connector-hive/.../HiveConnectorMetadata.java` — build positional flag via `ConnectorPartitionValues.isNullPartitionValue`, 8-arg ctor.
7. `fe-connector-hive/.../HiveConnectorMetadataPartitionListTest.java` — sentinel→`[true,false]` + ordinary→all-false.
8. `fe-connector-paimon/.../PaimonConnectorMetadata.java` — build flag in the `spec.entrySet()` loop, set true at L1050; update L1050-1057 comment.
9. `fe-connector-paimon/.../PaimonConnectorMetadataPartitionTest.java` — null partition now flags `true` (behavior change assertion).
10. `regression-test/suites/mtmv_p0/test_paimon_mtmv.groovy` — replace the stale `// Will lose null data` comment (RT-F3); the `.out` golden is regenerated by the e2e run, NOT hand-edited here.
11. e2e (user): `test_hive_default_partition` (INT+DATE) + paimon `qt_null_partition_*` + `test_paimon_mtmv` (golden 3→5 rows + MV null-partition-creation gate).

## 8. Test Plan

Constraints (verified): fe-core tests use Mockito (mock `ConnectorMetadata`); connector tests
(`ConnectorPartitionInfoTest`, `HiveConnectorMetadataPartitionListTest`, `PaimonConnectorMetadataPartitionTest`)
use hand-written recording fakes — **no Mockito**; extend the fakes. Checkstyle scans test sources: no static
imports, `AvoidStarImport`, `CustomImportOrder`.

**Unit (RED before fix, GREEN after):** RED baseline caveat (RT-F4) — the flag-dependent tests reference the
new 8-arg ctor/accessor, so they cannot compile against a full revert; their RED baseline is the *staged
mutation* (SPI + tests applied, fe-core seam still hardcoding `false`). The compile-independent opt-in guard is
`testDefaultSentinelWithoutFlagStillDrops` (no-flag INT → UNPARTITIONED), which is RED against unmodified fe-core.
- fe-core `PluginDrivenMvccExternalTableTest` (`cpiNull(name, lastModified, boolean… flags)`):
  - `testHiveDefaultSentinelOnIntColumnBuildsGenuineNullPartition` (Type.INT, flag `[true]`): `getPartitionType==LIST`, `getPartitionColumns` non-empty, `getNameToPartitionItems().size()==1`, single key `isNullLiteral()==true`. RED pre-fix (UNPARTITIONED/0).
  - `testHiveDefaultSentinelOnDateColumnBuildsGenuineNullPartition` (Type.DATEV2, flag `[true]`): same shape.
  - `testHiveDefaultSentinelBuildsNonNullStringKey` (existing, VARCHAR, **no flag**): keep GREEN — the no-flag default (non-opted-in connector) stays non-null. Re-comment: this is the default path, not "paimon parity".
  - `testDefaultSentinelWithoutFlagStillDrops` (Type.INT, no flag): pre-fix behavior preserved (UNPARTITIONED) — locks the fix as opt-in.
- fe-connector-api `ConnectorPartitionInfoTest`: `ctorCarriesPerValueNullFlags`, `backwardCompatCtorsDefaultNullFlagsEmpty`, `equalsAndHashCodeIncludeNullFlags`.
- fe-connector-hive `HiveConnectorMetadataPartitionListTest` (extend `FakeHmsClient`): `listPartitionsMarksHiveDefaultSentinelNull` (`year=__HIVE_DEFAULT_PARTITION__/month=01` → flags `[true,false]`, stats still UNKNOWN); `listPartitionsMarksNoNullForOrdinaryValues` (all false).
- fe-connector-paimon `PaimonConnectorMetadataPartitionTest` (extend `RecordingPaimonCatalogOps`): `nullPartitionFlagsTrue` (`category=__DEFAULT_PARTITION__` → emitted flag `true`, name still normalized to the sentinel) — the paimon behavior-change assertion.

**e2e (user):** `test_hive_default_partition` (INT+DATE default partition → partitioned, `col IS NULL`
returns null rows, counts match; VARCHAR control) + paimon `qt_null_partition_*` (esp. `_4`, `col IS NULL`
unchanged rows) + a paimon MTMV-over-null-partition check (now materializes null rows).

## Open Design Decisions

None. SPI shape (positional `List<Boolean>`), fe-core seam, and both connectors' opt-in points are agreed
across all five recon angles; the paimon-semantics fork was resolved by the user to **variant B** (paimon
adopts genuine-NULL). The iceberg latent bug is a deferred follow-up.
