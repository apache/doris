# P4-T06e FIX-ISKEY-METADATA — Design

> Issue: P3-10 / NG-6 / F3 / F10 (minor, read/metadata, regression). Source:
> `plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md` §NG-6.
> 用户定夺：**Fix（isKey=true，恢复 legacy parity）**（2026-06-08）。

## Problem

After cutover, every MaxCompute column is marked `isKey=false`, so `DESCRIBE <mc_table>` shows
`Key=NO`; legacy showed `Key=YES` for all columns. `DESCRIBE` is the path that genuinely reads the
catalog `Column.isKey()` — via `IndexSchemaProcNode.createResult` (`:92`).

> **Scope correction (design-validation `wa9t0emta`):** `information_schema.columns.COLUMN_KEY` is
> **NOT** affected. `FrontendServiceImpl.describeTables` gates `desc.setColumnKey(...)` on
> `if (table instanceof OlapTable)` (`:962-965`); MaxCompute tables (legacy **and** cutover) extend
> `ExternalTable`, not `OlapTable`, so `COLUMN_KEY` is empty before and after the fix — already at
> legacy parity, out of scope. The regression and fix are **DESCRIBE-only.**

This is **not purely cosmetic**, though the practical impact is small: besides DESCRIBE, a few
non-OLAP-guarded paths read `Column.isKey()` for external tables — `UnequalPredicateInfer` predicate
inference (`:278`) and the BE slot/column descriptors (`DescriptorToThriftConverter:67`,
`ColumnToThrift:59`). Legacy set `isKey=true` uniformly, so those paths always saw `true` in
production; the cutover's `isKey=false` silently changed them. The fix **restores the exact legacy
`isKey=true` value every planning/BE path already consumed** — so it is parity-correct and safe, and
closes a subtle planning divergence, not merely a display one.

`MaxComputeConnectorMetadata.getTableSchema` (`:138` data, `:150` partition) builds
`ConnectorColumn`s with the **5-arg** constructor, whose `isKey` defaults to `false`
(`ConnectorColumn:35-38`). The converter `ConnectorColumnConverter.convertColumn` already threads
`cc.isKey()` into the Doris `Column` (`:65-70`), and `PluginDrivenExternalTable.initSchema`
(`:132,:146`) is what turns this into the external table's FE schema used by DESCRIBE /
information_schema.

Legacy `MaxComputeExternalTable.initSchema` (`:177` data, `:189` partition) constructs each Doris
`Column(..., true /*isKey*/, ...)` → `isKey=true`. The `nullable` field already matches between
cutover and legacy (data = `col.isNullable()`; partition = `true`); **`isKey` is the only
divergence.**

## Root Cause

The connector port used the 5-arg `ConnectorColumn` ctor (isKey defaults false) and never set
`isKey=true`, dropping the legacy uniform `isKey=true` for external-table columns.

## Design

**Connector-local. No SPI change.** The converter already passes `isKey` through; only the two
construction sites need `isKey=true`.

Extract a small package-private static helper (mirrors the repo's pure-static-helper testability
convention, e.g. `toPartitionSpecs` / `shouldUseLimitOptimization`), so the `isKey=true` invariant
is directly unit-testable without a live ODPS `Table` (which `getTableSchema` otherwise requires —
the connector module has no fe-core/Mockito):

```java
/**
 * Builds a {@link ConnectorColumn} for a MaxCompute external-table column. {@code isKey=true}
 * mirrors legacy MaxComputeExternalTable.initSchema (every column was a Doris key column); for
 * external (non-OLAP) tables the flag is display-only metadata (DESCRIBE Key / information_schema
 * COLUMN_KEY), with no storage/aggregation semantics.
 */
static ConnectorColumn buildColumn(String name, ConnectorType type, String comment,
        boolean nullable) {
    return new ConnectorColumn(name, type, comment, nullable, null, true);
}
```

Replace the two loops:
```java
// data columns
columns.add(buildColumn(col.getName(), MCTypeMapping.toConnectorType(col.getTypeInfo()),
        col.getComment(), col.isNullable()));
// partition columns
columns.add(buildColumn(partCol.getName(), MCTypeMapping.toConnectorType(partCol.getTypeInfo()),
        partCol.getComment(), true));
```

(Partition column `nullable=true` preserved verbatim — legacy parity, unchanged.)

## Implementation Plan

1. `MaxComputeConnectorMetadata.java`: add `buildColumn(...)` static helper; replace the two inline
   `new ConnectorColumn(...)` (`:138`, `:150`) with `buildColumn(...)`. Import `ConnectorType`
   (the helper's param type) if not already imported.
2. No other prod files (no SPI, no fe-core, no converter change).

## Risk Analysis

- **Blast radius:** one connector method; only MaxCompute reaches it. Restores **exact legacy
  behavior** (`isKey=true` was production reality), so zero new risk.
- **Safety of `isKey=true` on external columns (validated by `wa9t0emta`):** every `isKey` branch
  that could affect external-MC planning is either OLAP/Schema-guarded and unreachable for MC
  (`BindRelation`, `OperativeColumnDerive` keysType, `StatisticsUtil` non-OlapTable early-return,
  `getBaseSchemaKeyColumns` callers all OLAP-only), **or** non-guarded
  (`UnequalPredicateInfer:278`, `DescriptorToThriftConverter:67`, `ColumnToThrift:59`) but **already
  received `isKey=true` from legacy** — so the fix introduces zero new behavior vs pre-cutover
  production. `buildColumn` uses the 6-arg ctor leaving `isAutoInc=false` (matches legacy); the
  `InsertUtils` `isKey && isAutoInc` branches never fire.
- **Completeness (validated):** the MC connector has exactly **2** `ConnectorColumn` sites
  (`:138`, `:150`), both in `getTableSchema`; `convertColumn` is the single FE conversion point
  threading `isKey`; no BE-descriptor / scan-slot / partitions-TVF path surfaces the catalog
  `isKey`. A **third** `ConnectorColumn` site exists downstream —
  `PluginDrivenExternalTable.initSchema:139-140` rebuilds a *renamed* column (the lowercase
  identifier-mapping path, which MC exercises via `fromRemoteColumnName`) via the 6-arg ctor
  **threading `col.isKey()`**, so it **preserves** the `isKey=true` we set (no extra change needed).
- **Helper retention (vs inline `,true`×2):** the 6-arg ctor's `isKey=true` is already pinned
  generically by `ConnectorColumnTest:63`, so `buildColumn` is a thin seam. Kept because (a) it
  gives a mutation-killable assertion of the **MC-module** `isKey=true` decision (consistent with
  the per-issue UT+mutation methodology), (b) it centralizes the decision across the 2 sites and
  documents the legacy-parity intent (Rule 9). Cost: one static method + one `ConnectorType` import.

## Test Plan

### Unit (`MaxComputeConnectorMetadataIsKeyTest`, connector module — no fe-core/Mockito)

`buildColumn` is pure static → exercise directly (no live ODPS `Table`).

1. `buildColumn("c", ConnectorType.of("INT"), "cmt", true).isKey()` → **true** (kills the
   `isKey true→false` mutation — the core regression guard).
2. Same call preserves `name` / `type` / `comment` / `nullable` and leaves `isAutoInc=false`
   (non-vacuous: confirms the helper builds the column correctly, not just the key flag).
3. `buildColumn(..., false).isKey()` → still **true** and `nullable=false` (isKey independent of
   nullable — guards against accidentally wiring isKey to the nullable arg).

Add a Rule-9 "why" comment in the test class: it pins the `buildColumn` invariant; the
`getTableSchema → buildColumn` wiring is e2e-only because `com.aliyun.odps.Table` is unmockable in
this Mockito-free module. **Residual risk (acknowledged, `wa9t0emta` test-quality shouldFix):** the
unit test cannot catch a future call site that *bypasses* `buildColumn` (reverts to the 5-arg ctor,
re-introducing `Key=NO`); the **e2e DESCRIBE assertion is the load-bearing regression gate** for the
wiring.

### Mutation

- `buildColumn` `isKey=true` → `false` → test 1 red.

### E2E (CI-skipped; live ODPS truth-gate — record as DV)

`DESCRIBE <mc_table>` shows `Key=YES` for MaxCompute columns (data + partition). Mirrors the
rereview's suggested regression assertion. **Note:** do **not** assert on
`information_schema.columns.COLUMN_KEY` — it is OlapTable-gated (`FrontendServiceImpl:962-965`) and
empty for MC regardless, already at legacy parity (see Problem). The `getTableSchema → DESCRIBE`
wiring is e2e-only because `getTableSchema` needs a live ODPS `Table` — same posture as DV-016.

## Doc-sync (with commit)

- `task-list-P4-rereview.md`: P3-10 row → DONE (+ round summary); RESUME → P3-11.
- `decisions-log.md`: D-033 — isKey=true restored (connector-local, no SPI).
- `deviations-log.md`: DV-017 — isKey=true unit-pinned via `buildColumn`; getTableSchema→DESCRIBE
  wiring e2e-only (live truth-gate), same posture as DV-016.
- review-rounds: `plan-doc/reviews/P4-T06e-FIX-ISKEY-METADATA-review-rounds.md`.

## Outcome ✅ DONE (commit `1b44cd4f065`)

Implemented as designed (`buildColumn` helper + 2 call-site swaps in `MaxComputeConnectorMetadata`;
no SPI/fe-core change). Design-validation `wa9t0emta` 0 mustFix (folded in: DESCRIBE-only scope,
restores-legacy framing, 3rd-site note, helper rationale); impl-review `wrx0n11ol` 0 mustFix /
0 shouldFix (only a test-javadoc wording precision). Guards: build SUCCESS, **UT 3/3 (+37/37
collateral)**, checkstyle 0, import-gate clean, mutation killed (`isKey true→false` → Failures 2).
Decision **D-033**; wiring-coverage + scope deviation **DV-017**.
