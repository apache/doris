# FIX-JNI-FILE-FORMAT — Design

> Source: `reviews/P5-paimon-rereview3-2026-06-12.md` (P7-1, **MAJOR**); task `task-list-P5-rereview3-fixes.md` FIX-2.
> Connector-only, no BE change. Edits `PaimonScanPlanProvider` (same file as FIX-1; sequenced after it).

## Problem
A JNI-serialized Paimon split (the default reader path, and the COUNT(*)-pushdown collapse range)
emits `file_format="jni"` in `TPaimonFileDesc`. BE's `paimon_cpp_reader.cpp:397-411` backfills the
paimon `FILE_FORMAT` **and** `MANIFEST_FORMAT` options from this field (only when they are unset/empty,
guarded `!file_format.empty()`), to avoid paimon-cpp defaulting `manifest.format=avro`. With the value
`"jni"` (an invalid paimon format) the backfill injects `MANIFEST_FORMAT=jni` (and `FILE_FORMAT=jni` when
the option is not serialized) → the cpp reader's manifest read breaks.

## Root Cause
`PaimonScanPlanProvider.buildJniScanRange` and `buildCountRange` hardcode `.fileFormat("jni")` on the
`PaimonScanRange.Builder`. The correct `defaultFileFormat`
(`= table.options().getOrDefault(CoreOptions.FILE_FORMAT.key(), "parquet")`, computed once in
`planScanInternal`) is **passed into `buildJniScanRange` and ignored**, and is **not even passed into
`buildCountRange`**. `PaimonScanRange.populateRangeParams:186` then emits `fileDesc.setFileFormat("jni")`.

**Crucial mechanism (verified):** the JNI `formatType` routing is gated by **`paimon.split` presence**
(`PaimonScanRange.populateRangeParams:160` → `setFormatType(FORMAT_JNI)`), **NOT** by the `fileFormat`
string. The `fileFormat` string is used for `formatType` only on the **native** branch (`:174-178`,
where it is already the real orc/parquet). So changing the JNI/count `fileFormat` from `"jni"` to the
real format leaves JNI routing untouched and only corrects the inner `fileDesc.fileFormat` BE consumes.

## Legacy parity
`paimon/source/PaimonScanNode.setPaimonParams`: `rangeDesc.setFormatType(FORMAT_JNI)` (routing) **and**
`fileDesc.setFileFormat(fileFormat)` (`:288`) where
`fileFormat = getFileFormat(paimonSplit.getPathString())` (`:259`) =
`FileFormatUtils.getFileFormatBySuffix(path).orElse(source.getFileFormatFromTableProperties())`. For a
JNI whole-`DataSplit`, `getPathString()` resolves to the first data file's name, and the fallback is the
table `file.format` property. For a homogeneous paimon table (one `file.format` per table) that equals
`defaultFileFormat`. So `defaultFileFormat` is the correct, legacy-faithful value (and is exactly BE's
own `FILE_FORMAT` resolution source).

## Design
1. **`buildJniScanRange`**: `.fileFormat("jni")` → `.fileFormat(defaultFileFormat)` (the parameter it
   already receives, currently ignored). Covers both the non-DataSplit metadata-split call
   (`planScanInternal:357`) and the DataSplit JNI call (`:419`).
2. **`buildCountRange`**: add a `String defaultFileFormat` parameter; `.fileFormat("jni")` →
   `.fileFormat(defaultFileFormat)`; thread `defaultFileFormat` from the call site (`planScanInternal:430`,
   where it is in scope).
3. **`PaimonScanRange.Builder` default (`:244`)**: change `private String fileFormat = "jni"` →
   `private String fileFormat = ""`. Every production caller sets `fileFormat` explicitly, so the default
   is currently dead — but `"jni"` is the very invalid value this fix removes; an empty default is the
   safe one (BE's `!file_format.empty()` guard then **skips** the backfill rather than ever injecting an
   invalid format, and the native `formatType` branch only matches real `orc`/`parquet`). Pure safety net,
   no behavioral change for any existing path.

### Divergence note (accepted)
`defaultFileFormat` is the table's `file.format` option; legacy derives the JNI format path-suffix-first
(first data file), table-prop fallback. These differ only for a **mixed-format** table (e.g. after
`ALTER ... file.format` leaves old-format files), which paimon does not produce per-table; the table
option is the more correct per-table hint for the whole-split BE backfill. The native path keeps its
per-file suffix derivation (`buildNativeRange:450`, unchanged).

## Implementation Plan
1. `buildJniScanRange`: `"jni"` → `defaultFileFormat`.
2. `buildCountRange`: add param + use it; update call site `:430`.
3. `PaimonScanRange.Builder.fileFormat` default `"jni"` → `""`.
4. Test (below); build connector + checkstyle + import-gate.

## Risk Analysis
- **JNI routing**: unchanged — gated by `paimon.split` presence, not `fileFormat` (verified `:160`).
- **Native path**: untouched (already real per-file format).
- **BE**: no change; the fix makes the consumed value valid. Backfill only fires when the option is
  unset/empty and now backfills a real format instead of `"jni"`.
- **Builder default `""`**: dead for all current callers; safer than `"jni"`/`"parquet"`.
- **System tables (binlog/audit_log)** go JNI; their `defaultFileFormat` = underlying table option (same
  as legacy non-DataSplit fallback). Valid format emitted, not `"jni"`.

## Test Plan
### Unit Tests
**connector — `PaimonScanPlanProviderTest`** (+1, real-table harness like the count tests):
- `jniAndCountRangesCarryRealFileFormatNotJni`: create a real PK table via `FileSystemCatalog`/`LocalFileIO`
  with an explicit `.option("file.format", "orc")` (so `defaultFileFormat` is a deterministic `"orc"`,
  distinct from the `"parquet"` fallback — proves the real table option is read, not a constant):
  - `planScan(force_jni_scanner=true, countPushdown=false)` → every emitted range is a JNI data range
    (`buildJniScanRange`); assert each `((PaimonScanRange) r).getFileFormat()` equals `"orc"` and not
    `"jni"`. Also drives the call-site threading.
  - `planScan(countPushdown=true)` → the collapsed count range (`paimon.row_count` present;
    `buildCountRange`); assert `getFileFormat()` equals `"orc"`, not `"jni"` — pins the new `defaultFileFormat`
    parameter + its threading from the call site.
  - MUTATION: reverting either method to `.fileFormat("jni")`, or failing to thread `defaultFileFormat`
    into `buildCountRange` → the `"orc"` assertion → red.

### E2E Tests
None required (connector-only, no BE change). The BE backfill behavior is pre-existing; this fix only
changes the FE-emitted value from an invalid `"jni"` to the table's real format.

## Files touched
- `fe/fe-connector/.../paimon/PaimonScanPlanProvider.java` (2 sites + 1 call site)
- `fe/fe-connector/.../paimon/PaimonScanRange.java` (Builder default)
- `fe/fe-connector/.../paimon/PaimonScanPlanProviderTest.java` (+1 test)
