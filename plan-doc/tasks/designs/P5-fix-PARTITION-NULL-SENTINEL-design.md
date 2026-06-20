# P5-fix FIX-PARTITION-NULL-SENTINEL (P4 / review §5 sentinel data-edge)

> The one P4 item the handoff reserved for a deliberate decision. User-signed FIX (2026-06-12).
> Pure-connector, scan-path. Adversarial-verified: the *common* genuine-NULL case is NOT a
> regression; the fix targets a narrow but real literal-value wrong-result.

## Problem
On the plugin scan path, a partition column whose **genuine, non-null** value is literally
`\N` (backslash-N, 2 chars) or `__HIVE_DEFAULT_PARTITION__` is materialized as SQL **NULL**
instead of the literal string. Legacy paimon keeps the literal. Reachable for a string
(VARCHAR/CHAR) partition column on the native ORC/Parquet read; `\N` is *not* a paimon-reserved
token (paimon's null marker is `__DEFAULT_PARTITION__`), so it is an ordinary value a user can
store. Result: wrong cell + a scan-vs-prune inconsistency (`WHERE col='\N'` / `col IS NULL`
return divergent rows).

The dominant case — a **genuine NULL** partition — is NOT affected: both sides set `isNull=true`
and BE ignores the rendered value string when `is_null==true`
(`be/src/format/table/partition_column_filler.h:40-44` early-returns NULL rows without reading
the value), so connector `\N` vs legacy `""` render is unobservable. (Re-verified by two
adversarial agents + a render-path skeptic in recon `wf_6884d37b-8ef`.)

## Root Cause
`PaimonScanRange.populateRangeParams` (`PaimonScanRange.java:212-226`) routes paimon partition
values through `ConnectorPartitionValues.normalize`, which applies **Hive-directory** null-sentinel
coercion:

```java
public static boolean isNullPartitionValue(String value) {
    return value == null || HIVE_DEFAULT_PARTITION.equals(value) || NULL_PARTITION_VALUE.equals(value);
}   // NULL_PARTITION_VALUE = "\\N"
```

That coercion is **correct for hudi** (`HudiScanRange.java:226`), whose partition values come from
Hive-style directory PATHS where a null partition is encoded as the `__HIVE_DEFAULT_PARTITION__`
directory name. It is **wrong for paimon**: paimon partition values are already *typed* — the
per-type serializer `serializePartitionValue` (`PaimonScanPlanProvider.java:843-885`) returns
Java-`null` for a genuine null and the literal `toString()` otherwise (`getPartitionInfoMap:801-829`
`put`s Java-null into the map). So a paimon null is a Java-null, never a sentinel string; the
coercion only ever bites a genuine literal value.

Legacy `PaimonScanNode.setScanParams` (`source/PaimonScanNode.java:323-326`) derives `isNull` from
the Java null **only**: `fromPathValues.add(value != null ? value : ""); fromPathIsNull.add(value == null);`.

## Design
Paimon-local fix: in `PaimonScanRange.populateRangeParams`, derive `isNull` from the Java null only
(legacy parity) instead of the Hive-aware `ConnectorPartitionValues.normalize`. Render a genuine
null as `""` (legacy-exact; unobservable since BE ignores it when `isNull`). Drop the now-unused
`ConnectorPartitionValues` import.

**Do NOT touch `ConnectorPartitionValues`** — it is shared API and hudi legitimately needs the
Hive-directory coercion.

### Out of scope (deliberately)
The Nereids **prune** path (`TablePartitionValues.toListPartitionItem:162`, fed via the generic
bridge `PluginDrivenExternalTable.getNameToPartitionItems:333`) coerces `__HIVE_DEFAULT_PARTITION__`
while legacy paimon `PaimonUtil.toListPartitionItem:214` hardcodes `isNull=false`. That divergence:
(a) is in generic fe-core shared with hive/iceberg, (b) is pre-existing and unchanged by this fix,
(c) for the genuine-null + `partition.default-name=__HIVE_DEFAULT_PARTITION__` case the connector
is arguably MORE correct than legacy's hardcoded-false. After this scan fix, the only residual
difference is the contrived literal-`__HIVE_DEFAULT_PARTITION__` value (THE Hive null marker,
effectively unreachable). Logged as a deviation; not fixed here (would require a paimon-specific
fe-core prune path — out of finding scope, and would regress hudi if done in the shared class).

## Implementation Plan
1 file (`PaimonScanRange.java`): replace the `normalize`-based block with the inline
`isNull = value==null` / `render = value!=null?value:""` computation; remove the unused import.

## Risk Analysis
- Genuine-null: byte-identical BE result (NULL cell) before/after — proven unobservable render diff.
- Non-null literal `\N` / `__HIVE_DEFAULT_PARTITION__`: now kept as literal (was wrongly NULL) →
  matches legacy scan exactly. Net strictly toward parity.
- No SPI/BE/thrift change; hudi untouched.

## Test Plan
### Unit Tests
Extend `PaimonPartitionValueRenderTest` (or add a focused test) exercising
`PaimonScanRange.populateRangeParams` via a built range's `TFileRangeDesc`:
- genuine null (map value Java-null) → `columnsFromPathIsNull[i]=true` (unchanged).
- literal `"\N"` → `isNull=false`, `columnsFromPath[i]="\N"` (**the fix**; fail-before: isNull=true).
- literal `"__HIVE_DEFAULT_PARTITION__"` → `isNull=false`, value kept (**the fix**).
- ordinary value `"cn"` → `isNull=false`, value `"cn"` (unchanged).

WHY (Rule 9): encodes that paimon partition nulls are Java-null-only (typed source), so a literal
sentinel string is real data, not a null marker — distinguishing the Hive-directory coercion
(wrong here) from the legacy `value==null` rule (correct). Fail-before: the `\N` /
`__HIVE_DEFAULT_PARTITION__` literal assertions are red (currently coerced to isNull=true).

### E2E Tests
None added; the genuine-null render parity and native partition materialization are covered by the
CI-gated legacy paimon partition regression. No BE change.
