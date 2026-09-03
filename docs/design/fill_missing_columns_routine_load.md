# Design: `fill_missing_columns` for Routine Load (JSON)

Scope: FE-only. Routine-load JSON property propagation + Nereids load scan planning.

Implementation source: the feature commits on
`feature/auto-fill-missing-columns`:

- `25fb54ef1b1 [feature](routine-load) support fill_missing_columns for json format to auto-complete unspecified columns`
- `a457347b0a4 [fix](routine_load) add test cases`

The current `master` commit is the merge base of this branch. This document
therefore covers the complete `master...HEAD` functional diff: eight FE
production files, seven FE test files, and the JSON/Kafka regression fixture.

## 1. Background and Goal

Routine Load lets users declare a `COLUMNS(...)` clause to map JSON source fields
to target table columns. In the existing Nereids load planner
([NereidsLoadScanProvider](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadScanProvider.java)),
the base schema of the table is only auto-appended when the user did **not**
specify any plain file-field column (`!specifyFileFieldNames`). When the user
writes a `COLUMNS` clause that contains only derived expressions
(e.g. `COLUMNS(score_x2 = score * 2)`), `specifyFileFieldNames` is `false` and the
auto-fill path runs — but if the user lists at least one plain field, the
auto-fill is skipped and any unlisted columns (including the sequence column)
must be specified manually, otherwise planning fails with errors such as
`need to specify the sequence column`.

The goal of this feature is to add a JSON-only routine-load property
`fill_missing_columns`. When `true`, the planner auto-completes the remaining
base-schema columns (and the sequence-column input when applicable) even when
the user already specified some file fields or mappings.

The property does not synthesize values in the FE. It changes which source slots
and target expressions are present in the Nereids load plan. The existing JSON
reader and BE load path remain responsible for extracting JSON fields, applying
normal default/nullability rules, and reporting filtered rows. Consequently,
enabling the property does not make a missing JSON key valid for a `NOT NULL`
column without a default; that existing execution-time validation still applies.
No BE protocol, storage format, or BE configuration change is required.

## 2. User-visible Contract

The property belongs in the `PROPERTIES` clause of a JSON Routine Load:

```sql
CREATE ROUTINE LOAD job_name ON target_table
COLUMNS(id, derived_score = score * 2)
PROPERTIES
(
    "format" = "json",
    "fill_missing_columns" = "true"
)
FROM KAFKA (...);
```

| Item | Contract |
|------|----------|
| Property name | `fill_missing_columns` |
| Accepted values | Case-insensitive boolean literal `true` or `false` |
| Default | `false` |
| Supported format | JSON only |
| CREATE with CSV/default format | Rejected, including when the value itself is malformed |
| ALTER of a non-JSON job | Rejected |
| `false` | Preserves the legacy planner behavior |
| `true` | Adds omitted base-schema columns to the planned file-field descriptors, subject to mapping ownership rules below |
| Persisted form | Existing Routine Load `jobProperties` string map |
| `SHOW CREATE ROUTINE LOAD` | Emits the property only for JSON jobs, including explicit `false`, so the generated statement can be replayed |

`fill_missing_columns` means “complete the FE input/output column plan”; it
does not mean “invent a value for every absent JSON key.” For example, a nullable
or defaulted target column may be populated by existing JSON-load semantics when
the key is missing, while a missing `NOT NULL` target with no default remains
subject to the existing BE filtering behavior.

## 3. End-to-end Property Propagation

The property `fill_missing_columns` flows through the existing routine-load
property pipeline. New code mirrors the existing `fuzzy_parse` / `num_as_string`
handling at every hop:

| Stage | File | Change |
|------|------|--------|
| Parse/validate JSON format props | [JsonFileFormatProperties](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/datasource/property/fileformat/JsonFileFormatProperties.java) | Add `PROP_FILL_MISSING_COLUMNS` constant, `fillMissingColumns` field (default `false`), parse in `analyzeFileFormatProperties`, and `isFillMissingColumns()` getter. |
| CREATE allow-list | [CreateRoutineLoadInfo](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateRoutineLoadInfo.java) | Register property in the accepted-properties set. |
| ALTER allow-list + validation | [AlterRoutineLoadCommand](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/AlterRoutineLoadCommand.java) | Register property; validate value is `true`/`false`; write into `analyzedJobProperties`. |
| Persisted job properties | [RoutineLoadJob](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/load/routineload/RoutineLoadJob.java) | Put property into `jobProperties` from JSON format props; add `isFillMissingColumns()`; emit it in `getShowCreateInfo()`. |
| Task info copy (Kafka + Kinesis) | [NereidsRoutineLoadTaskInfo](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsRoutineLoadTaskInfo.java) | Read property from copied `jobProperties` via `isFillMissingColumns()` override. |
| Task-info interface default | [NereidsLoadTaskInfo](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsLoadTaskInfo.java) | New `default boolean isFillMissingColumns()` returning `false` (backward compatible for stream load and other callers). |
| Analysis map | [NereidsDataDescription](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/load/NereidsDataDescription.java) | Copy the flag into the analysis property map used by planning. |

Routine-load task construction passes a copy of the job-property map into
`NereidsRoutineLoadTaskInfo`; the task-specific implementation reads the
property from that map. The new default method on `NereidsLoadTaskInfo` keeps
other load-task implementations, such as broker load, on `false`.

## 4. CREATE and ALTER Validation

CREATE first verifies the supported-property allow-list, resolves the unique-key
update mode, and then validates the format and value. JSON parsing rejects empty
strings, numbers, and other non-boolean literals instead of relying on
`Boolean.parseBoolean`, which would silently turn them into `false`.

ALTER cannot change a job's format, so it obtains the existing
`RoutineLoadJob` and rejects the property unless that job is JSON. It also
evaluates the effective post-alter state rather than merely validating the
property named in the statement:

| Existing state | ALTER input | Result |
|----------------|-------------|--------|
| JSON, upsert | `fill_missing_columns=true` | Allowed |
| CSV | `fill_missing_columns=true` | Rejected as non-JSON |
| Fixed partial update | `fill_missing_columns=true` | Rejected |
| `fill_missing_columns=true` | `unique_key_update_mode=UPDATE_FIXED_COLUMNS` | Rejected |
| `fill_missing_columns=true` | legacy `partial_columns=true` | Rejected |
| Fixed partial update | `fill_missing_columns=false` | Allowed |
| Flexible partial update | `fill_missing_columns=true` | Allowed |

This requires two effective-value functions in `AlterRoutineLoadCommand`:

1. `effectiveFillMissingColumns` prefers the altered value, otherwise it reads
   the current job.
2. `effectiveUniqueKeyUpdateMode` prefers the explicit update mode, then the
   legacy `partial_columns` flag, otherwise it reads the current job.

Checking the resolved pair prevents an invalid state from being created in
either transition direction.

## 5. Planning Change in `NereidsLoadScanProvider`

Two behaviors change, both gated by the JSON-only helper `isFillMissingColumns()`:

1. **Sequence column auto-add** — `shouldAddSequenceColumn` now returns `true`
   when `fill_missing_columns` is enabled, so the sequence column is treated as
   auto-fillable rather than requiring explicit specification.

2. **Base-schema auto-fill** — the auto-fill block guarded by
   `!specifyFileFieldNames` is extended to also run when `fillMissing` is `true`.
   Because the user may already have listed some columns in this path, the fill
   must dedup against descriptors that already provide a file slot, otherwise a
   column would be added twice.

### 5.1 Descriptor construction order

`createLoadContext()` constructs the plan in this order:

1. Start with user `COLUMNS(...)` descriptors.
2. Add delete-sign descriptors for merge/delete loads.
3. Resolve and add the hidden sequence-column mapping where required.
4. Rewrite dependent mappings, for example replace a temporary mapping reference
   with its source expression.
5. Drop expression descriptors whose mapping target is not a real target-table
   column, while retaining valid plain file fields and valid target mappings.
6. Compute constant mappings, then append missing base-schema descriptors when
   the legacy condition or `fill_missing_columns` requires it.
7. Add applicable hidden columns and build `exprMap` plus `scanSlots`.

The property changes step 6 and the sequence-column decision in step 3. It does
not alter expression rewriting, JSON parsing, or BE sink behavior.

### 5.2 Dedup correctness for target mappings

The first implementation deduped against **every** existing descriptor's column
name:

```java
for (NereidsImportColumnDesc desc : copiedColumnExprs) {
    existingColumns.add(desc.getColumnName());   // BUG: includes mapping targets
}
```

This treats every descriptor as proof that the matching file slot is available.
It is wrong for a **self-referencing same-name mapping** such as
`COLUMNS(k1 = k1)`:

- `copiedColumnExprs` starts with `NereidsImportColumnDesc("k1", UnboundSlot("k1"))`.
- Adding `k1` to `existingColumns` makes the base-schema loop skip the plain `k1`
  file descriptor.
- The later scan-slot loop only creates file slots for descriptors whose
  `expr == null`, so no `k1` file slot is produced.

The reduced plan becomes:

```
LogicalLoadProject(k1 := UnboundSlot(k1))
  LogicalProject(scanSlots without k1)
    LogicalOneRowRelation(scanSlots without k1)
```

The mapping still references the input `k1`, but its child no longer outputs that
slot — an inconsistent plan. The old `!specifyFileFieldNames` path always added
the base-schema descriptor and kept this source slot available.

### 5.3 The fix

A descriptor only legitimately suppresses the base fill when it actually provides
the file slot for that column. That is true for:

- **true file fields** (`expr == null`), and
- **mappings that do not reference their own column**, e.g. `score_x2 = score * 2`.
  Such a derived target consumes *other* columns; the derivation is preserved
  precisely by suppressing the base descriptor (note base descriptors are
  appended *after* mappings, so a base `null` entry would otherwise overwrite the
  derivation in `columnExprMap`).

A **self-referencing** mapping such as `k1 = k1` or `k1 = k1 + 1` references the
same-named source column, so the base scan descriptor must be preserved.

```java
Map<String, String> selfRefSourceByColumn = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
Set<String> existingColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
if (fillMissing) {
    for (NereidsImportColumnDesc desc : copiedColumnExprs) {
        String source = selfReferenceSource(desc);
        if (source != null) {
            selfRefSourceByColumn.put(desc.getColumnName(), source);
        } else {
            existingColumns.add(desc.getColumnName());
        }
    }
}
```

with the single helper (also reused by §5.5 for the source-slot spelling):

```java
private String selfReferenceSource(NereidsImportColumnDesc desc) {
    if (desc.isColumn()) {
        return null;
    }
    for (Slot slot : desc.getExpr().getInputSlots()) {
        if (slot.getName().equalsIgnoreCase(desc.getColumnName())) {
            return slot.getName();
        }
    }
    return null;
}
```

Constant mappings (`k1 = 'v'`) are unaffected: they neither reference their own
column nor any column, so they are deduped here, and they are also separately
handled by the pre-existing `constantMappingColumns` skip. Their base descriptor
is correctly omitted because the constant fully provides the value.

### 5.4 Case matrix (fill_missing_columns = true)

| COLUMNS clause | Base `c` descriptor added? | Why |
|----------------|----------------------------|-----|
| `c` (plain field) | no (already a file field) | descriptor is a true file field |
| `x = c * 2` | yes for `c`, no for `x` | `x` is derived from other column; `c` not mentioned |
| `c = c + 1` | **yes** (fix) | mapping references its own source column `c` |
| `c = c` | **yes** (fix) | self-reference |
| `c = 'v'` (constant) | no | constant fully provides value; deduped twice |

### 5.5 Case-preserving source-slot spelling

For case-preserving formats (JSON/Arrow, `Util.isCasePreservingFormat`), the BE
`NewJsonReader` matches a non-Hive JSON key to the scan slot **name** exactly,
while Nereids binds a `COLUMNS(...)` mapping source case-insensitively. When the
base-schema fill emitted the descriptor using the **table column** spelling, a
self-referencing mixed-case mapping produced a NULL:

- Table column `Score`, JSON `{"score":10}`, `COLUMNS(Score = score + 1)`.
- The base loop added a scan slot named `Score`; the reader looked up the JSON key
  `Score`, missed the actual key `score`, and the mapping received NULL.

The fix keeps the mapping's own source-slot spelling for the base descriptor of a
self-referencing column under case-preserving formats:

```java
if (Util.isCasePreservingFormat(fileFormatType)) {
    String sourceSpelling = selfRefSourceByColumn.get(column.getName());
    columnDesc = new NereidsImportColumnDesc(
            sourceSpelling != null ? sourceSpelling : column.getName());
} else {
    columnDesc = new NereidsImportColumnDesc(column.getName().toLowerCase());
}
```

`selfRefSourceByColumn` is built once, in the same pass that computes the dedup
set, from a single `selfReferenceSource(desc)` helper: for a self-referencing
mapping (e.g. `COLUMNS(Score = score + 1)`) it returns the exact source-slot
spelling read by the mapping (`score`), and `null` otherwise. That one result
serves both needs — preserving the base descriptor for self-references (dedup) and
supplying the reader-matching spelling for case-preserving formats. Non-self-
referencing columns and non-case-preserving formats keep the table column
spelling.

## 6. Interaction with Partial Update

`fill_missing_columns` performs a full-row upsert: every omitted base-schema
column is injected into the scan/output tuple and null-checked by the
`FileScanner`. Fixed partial columns update (`UPDATE_FIXED_COLUMNS`, also reached
via the legacy `partial_columns=true`) only writes the explicitly listed columns,
and the BE writer drops the auto-filled columns from its index slots. The
combination lets an omitted non-null column be NULL in the output tuple even
though it is not being updated, so a valid fixed partial row can be wrongly
filtered.

The two semantics are mutually exclusive, so the combination is rejected up front
(fail-fast) at both entry points:

- [CreateRoutineLoadInfo#checkJobProperties](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/info/CreateRoutineLoadInfo.java):
  rejects `fill_missing_columns=true` when the resolved `uniqueKeyUpdateMode` is
  `UPDATE_FIXED_COLUMNS`.
- [AlterRoutineLoadCommand](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/AlterRoutineLoadCommand.java):
  resolves the *effective* fill-missing flag and update mode after the alter
  (either changed by this alter or kept from the existing job) and rejects the
  combination, covering both "enable fill_missing on a fixed partial job" and
  "switch a fill_missing job into fixed partial mode".

Flexible partial update (`UPDATE_FLEXIBLE_COLUMNS`) checks per-row column
integrity on the BE, so it stays compatible and is **not** rejected.

## 7. Compatibility and Operational Properties

- Default is `false`; absent property ⇒ unchanged behavior.
- The `!specifyFileFieldNames` legacy path is untouched when `fillMissing` is
  `false` (the new dedup loop only runs under `fillMissing`).
- JSON-only: `isFillMissingColumns()` short-circuits to `false` for non-JSON
  formats.
- No edit-log/metadata schema change; the flag is stored in the existing
  `jobProperties` string map and survives replay via the existing routine-load
  alter/write-lock/edit-log paths.
- No new shared mutable state, scheduling path, RPC field, or locking protocol
  is introduced. CREATE and ALTER reuse their existing job-property validation
  and persistence paths.
- The additional planning work is linear in the number of user descriptors plus
  base-schema columns. The case-insensitive map/set matches existing
  case-insensitive column resolution and avoids an order-dependent duplicate
  decision.

## 8. Test Coverage

Unit:
- [JsonFileFormatPropertiesTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/datasource/property/fileformat/JsonFileFormatPropertiesTest.java): parse true/false/default and the all-properties path.
- [NereidsRoutineLoadTaskInfoTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/nereids/load/NereidsRoutineLoadTaskInfoTest.java): property read true/false/default.
- [CreateRoutineLoadInfoTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/info/CreateRoutineLoadInfoTest.java): JSON-only rejection; fixed-partial rejection (mode + legacy `partial_columns`); `false` allowed with fixed partial; flexible partial allowed.
- [AlterRoutineLoadCommandTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/AlterRoutineLoadCommandTest.java): accepted/false/invalid value; fixed-partial rejection when enabling on a fixed-partial job or switching a fill_missing job into fixed partial; flexible partial allowed.
- [NereidsLoadScanProviderTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/nereids/load/NereidsLoadScanProviderTest.java): JSON-only enablement; sequence-column behavior; all descriptor ownership categories; end-to-end `COLUMNS(Score = score + 1)` on JSON keeps the `score` scan slot.
- [NereidsBrokerLoadTaskTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/nereids/load/NereidsBrokerLoadTaskTest.java): the interface default remains `false` for an unrelated load implementation.
- [RoutineLoadJobTest](file:///Users/yuanbin.me/Documents/codebase/github/doris/fe/fe-core/src/test/java/org/apache/doris/load/routineload/RoutineLoadJobTest.java): `SHOW CREATE` output includes the property.

Regression ([test_routine_load_fill_missing_columns.groovy](file:///Users/yuanbin.me/Documents/codebase/github/doris/regression-test/suites/load_p0/routine_load/test_routine_load_fill_missing_columns.groovy)):
- Positive: `COLUMNS(score_x2 = score * 2)` with `fill_missing_columns=true` —
  sequence column and unlisted base columns auto-filled, job runs, data correct.
- Contrast: same clause with `fill_missing_columns=false` — job pauses with the
  sequence-column error (legacy behavior preserved).
- Same-name mapping: `COLUMNS(score = score + 1)` with `fill_missing_columns=true`
  — verifies the source `score` slot stays available and the value is read from
  JSON and incremented.
- **Added** mixed-case mapping: table column `Score`, JSON key `score`,
  `COLUMNS(Score = score + 1)` with `fill_missing_columns=true` — verifies the
  case-preserving base scan slot uses the `score` spelling so the value is read
  and incremented (covers §5.5).

The asynchronous cases no longer drop their tables in `finally`, so a failed poll
or assertion preserves the table/rows for debugging (best-effort routine-load stop
is kept).

## 9. Non-goals

- Support for CSV, broker load, stream load, or non-Routine Load entry points.
- A new BE mechanism for materializing missing JSON keys.
- Relaxing existing `NOT NULL`, default-value, strict-mode, or row-filtering
  semantics.
- Changing the meaning of user-supplied mapping expressions. A mapping retains
  ownership of its target column; the fill logic must only provide source slots
  that the mapping still needs.
