# P4-T06d — FIX-DDL-ENGINE — no-ENGINE CREATE TABLE under PluginDriven max_compute

Status: design (revised, post-critic). Scope: fe-core only, single file
`CreateTableInfo.java` (1 import + 2 branch insertions + 1 private helper) + 1 CI-runnable UT.
Severity: blocker. Layer: fe-core. Depends on / unblocks: Batch-D removal ordering (see §Batch-D).

This per-issue doc supersedes the parent `P4-cutover-fix-design.md` FIX-DDL-ENGINE section. It
folds in the parent critic's `needs-revision` corrections (verbatim verified against the current
tree) **and** one design refinement the parent missed (null-returning helper — §Design 3).

## Problem
After the `max_compute` cutover (T06b), a `max_compute` catalog instantiates as
`PluginDrivenExternalCatalog` (`CatalogFactory` SPI_READY_TYPES contains `max_compute`). A user
running a `CREATE TABLE` **without an explicit `ENGINE=maxcompute` clause** (the most common MC
form — `test_max_compute_create_table.groovy` Test1/2/3 all omit ENGINE) gets, at **analysis
time**, `AnalysisException: Current catalog does not support create table: <ctl>`. It never reaches
`PluginDrivenExternalCatalog.createTable` (which IS implemented and works). Legacy-usable,
cutover-broken → blocker regression. CTAS (`CREATE TABLE ... AS SELECT`) is broken identically
(§Root Cause).

## Root Cause
`CreateTableInfo` infers a missing engine and validates engine/catalog consistency by `instanceof`
on the **legacy concrete subclass** `MaxComputeExternalCatalog`. After cutover the catalog is
`PluginDrivenExternalCatalog`, matching none of the branches:

1. `paddingEngineName` (`CreateTableInfo.java:896-918`): when `engineName` is empty it walks
   `InternalCatalog`/`HMSExternalCatalog`/`IcebergExternalCatalog`/`PaimonExternalCatalog`/
   `MaxComputeExternalCatalog` (MC branch `:912-913 → ENGINE_MAXCOMPUTE`); no match →
   `:914-915 throw "Current catalog does not support create table"`.
2. `checkEngineWithCatalog` (`:376-393`): the symmetric consistency check;
   `:390 else if (catalog instanceof MaxComputeExternalCatalog && !engineName.equals(ENGINE_MAXCOMPUTE)) throw`.
   After cutover an **explicitly** written `ENGINE=maxcompute` silently bypasses this check (no
   `throw`, not a crash) — a mirror gap that should be fixed for parity.
3. `validateCreateTableAsSelect` (`:923-926`) also calls `paddingEngineName(catalogName, ctx)` →
   **CTAS into a max_compute PluginDriven catalog is equally broken pre-fix and equally fixed.**

Both gate methods re-fetch the catalog **by name** via
`Env.getCurrentEnv().getCatalogMgr().getCatalog(ctlName)` (`:899`, `:383`) — they ignore any
directly-constructed catalog object (drives the UT design, §Test Plan).

Verified type-string facts:
- `PluginDrivenExternalCatalog.getType()` returns the lowercase catalog-type prop
  (`catalogProperty.getOrDefault(CatalogMgr.CATALOG_TYPE_PROP="type", …)`) → `"max_compute"` for a
  MC catalog — the same key `PluginDrivenExternalTable.getEngine()/getEngineTableTypeName()` switch
  on.
- `ENGINE_MAXCOMPUTE = "maxcompute"` (`:125`) — **no underscore**; getType is `"max_compute"` **with**
  underscore. The helper must map between them.
- **Must NOT** reuse `TableType.MAX_COMPUTE_EXTERNAL_TABLE.toEngineName()`: that enum has no case in
  `TableIf.toEngineName()` → returns `null` (confirmed; `PluginDrivenExternalTable.getEngine()`
  itself documents this null). Mapping must be a direct literal `"max_compute" → ENGINE_MAXCOMPUTE`.
- Downstream is satisfied once padded: `checkEngineName` (`:940-944`) and `analyzeEngine`
  (`:1121-1127`) whitelist `ENGINE_MAXCOMPUTE`, so producing `"maxcompute"` makes the rest of the
  path byte-identical to legacy with zero further edits.

## Design
Mirror the in-repo convention `PluginDrivenExternalTable.getEngine()` (switch on
`((PluginDrivenExternalCatalog) catalog).getType()`): add a `PluginDrivenExternalCatalog` branch to
both gate methods, keyed on `getType()` (not a hardcoded `instanceof MaxComputeExternalCatalog`), so
it generalizes to any future full-adopter and survives Batch-D deleting the legacy MC branch.

1. **Import** — add `import org.apache.doris.datasource.PluginDrivenExternalCatalog;`. **Placement
   (critic correction):** immediately after `:49 org.apache.doris.datasource.InternalCatalog` and
   **before** `:50 org.apache.doris.datasource.hive.HMSExternalCatalog`. Rationale: Checkstyle
   `CustomImportOrder` is ASCII-case-sensitive; after `org.apache.doris.datasource.` the next char is
   uppercase `P` (0x50) for PluginDriven vs lowercase `h`/`i`/`m`/`p` (≥0x68) for the
   `hive.`/`iceberg.`/`maxcompute.`/`paimon.` sub-packages, so `P` sorts **before** all of them and
   **after** `I` (InternalCatalog, 0x49). Grouped with the top-level `datasource.*` classes
   (`CatalogIf :48`, `InternalCatalog :49`), NOT after the sub-packages. (The parent design's
   "between :51/:52, after MaxCompute" was off-by-two and would put it after `hive`/`iceberg` →
   Checkstyle reject.)

2. **`paddingEngineName`** — insert after the MC branch (`:913`), before the `:914 else`:
   ```java
   } else if (catalog instanceof PluginDrivenExternalCatalog
           && pluginCatalogTypeToEngine((PluginDrivenExternalCatalog) catalog) != null) {
       engineName = pluginCatalogTypeToEngine((PluginDrivenExternalCatalog) catalog);
   } else {
       throw new AnalysisException("Current catalog does not support create table: " + ctlName);
   }
   ```
   A max_compute PluginDriven catalog gets `engineName = "maxcompute"`. A jdbc/es/trino PluginDriven
   catalog (helper returns `null`) falls through to the existing `else` and throws the **same**
   "does not support create table" message it already throws today — byte-identical pre/post for
   those types.

3. **`pluginCatalogTypeToEngine` (new `private static`) — RETURNS `null` for unmapped types, does
   NOT throw** (refinement over parent design — Rule 7):
   ```java
   // Maps a PluginDriven (SPI) catalog's type to the legacy engine name used for DDL
   // engine-padding / catalog-engine consistency. Keyed on getType() (CatalogFactory key),
   // mirroring PluginDrivenExternalTable.getEngine()/getEngineTableTypeName(); the two switches
   // must stay in sync if SPI_READY_TYPES gains a CREATE-TABLE-capable full-adopter.
   // Returns null for SPI types that do not support CREATE TABLE (jdbc/es/trino-connector),
   // so callers preserve their existing behavior for those types.
   private static String pluginCatalogTypeToEngine(PluginDrivenExternalCatalog catalog) {
       switch (catalog.getType()) {
           case "max_compute":
               return ENGINE_MAXCOMPUTE;
           default:
               return null;
       }
   }
   ```
   **Why null, not default-throw (the parent design's version):** the helper is shared by BOTH gate
   methods. If it threw for non-max_compute types, then `checkEngineWithCatalog` — which legacy lets
   jdbc/es/trino pass through unconditionally (they are not in legacy's instanceof chain) — would
   newly throw for a jdbc catalog with an explicit engine. Returning null lets each caller keep
   legacy semantics: `paddingEngineName` falls to its existing else-throw; `checkEngineWithCatalog`
   simply skips. This makes jdbc/es/trino byte-identical to legacy in **both** methods; only
   max_compute gains behavior.

4. **`checkEngineWithCatalog`** — insert after the MC branch (`:391`), before the `:392` close:
   ```java
   } else if (catalog instanceof PluginDrivenExternalCatalog) {
       String pluginEngine = pluginCatalogTypeToEngine((PluginDrivenExternalCatalog) catalog);
       if (pluginEngine != null && !engineName.equals(pluginEngine)) {
           throw new AnalysisException("MaxCompute type catalog can only use `maxcompute` engine.");
       }
   }
   ```
   `pluginEngine` is non-null only for max_compute, so the message is only ever reachable for
   max_compute and is the **verbatim legacy MC message** (`:391`) — matching the established
   convention asserted for sibling catalogs (`test_iceberg_create_table.groovy` / `test_hive_ddl.groovy`:
   `"<Type> type catalog can only use \`<engine>\` engine."`). jdbc/es/trino (pluginEngine == null)
   fall through with no throw, exactly as legacy.

5. **SPI / connector / thrift / BE: no change.** The `Connector` SPI has no engine-name concept;
   adding one is over-design. `getType()` is sufficient. Pure fe-core.

## Implementation Plan (fe-core only)
1. `CreateTableInfo.java`: add the import (§Design 1, exact placement).
2. `CreateTableInfo.java:896-918 paddingEngineName`: insert the PluginDriven branch (§Design 2).
3. `CreateTableInfo.java:376-393 checkEngineWithCatalog`: insert the PluginDriven branch (§Design 4).
4. `CreateTableInfo.java`: add `private static pluginCatalogTypeToEngine` (§Design 3).
5. Gate: `mvn -pl :fe-core -am` (compile + UT); `fe-code-style` Checkstyle (new import must be used +
   correctly ordered). Independent commit `[P4-T06d] FIX-DDL-ENGINE`.

## Risk
- **Regression surface is narrow**: a new branch fires only when (padding) `engineName` is empty AND
  catalog is PluginDriven, or (check) catalog is PluginDriven. HMS/Iceberg/Paimon/Internal and
  legacy-MC (`instanceof MaxComputeExternalCatalog`, still in keep-set) paths are byte-unchanged
  (branch order: after MC, before else / before close).
- **jdbc/es/trino**: byte-identical to legacy in both methods (null-returning helper, §Design 3).
  No new capability, no new breakage. Verified non-regressive: pre-fix they already hit the same
  `:915` "does not support create table" throw; `ConnectorTableOps.createTable` default also throws.
- **CTAS**: fixed transitively (validateCreateTableAsSelect → paddingEngineName); covered by UT.
- **Follower replay / master sync**: not a concern — engine padding is analysis-time on the receiving
  FE; persistence uses `logCreateTable` independent of engineName.
- **getType() string fragility**: depends on the `"max_compute"` literal (CatalogFactory key), same
  convention as `PluginDrivenExternalTable.getEngine()`. The helper comment cross-references both so a
  future SPI_READY_TYPES key change updates both switches.
- **Checkstyle/import-gate**: one new import, used in 3 places; placement verified (§Design 1).

## Batch-D ordering (keep-set dependency — must record in decisions-log)
`P4-batchD-maxcompute-removal-design.md:100` plans to delete both
`instanceof MaxComputeExternalCatalog` branches in `CreateTableInfo`. This fix **must land first**;
Batch-D then degrades to "delete only the legacy MC instanceof branches + the
`maxcompute.MaxComputeExternalCatalog` import", leaving the PluginDriven branches (keyed on
getType()) in place. If Batch-D runs first, no-ENGINE CREATE TABLE is permanently broken (the
"amendment self-triggers" pattern). This fix depends on the `MaxComputeExternalCatalog` import
staying in keep-set until Batch-D. (Confirmed `UnboundTableSinkCreator` already has PluginDriven
branches from T06c, so `CreateTableInfo` is the last unwired analysis-time CREATE TABLE gate.)

## Test Plan
**UT (CI-runnable, fe-core)** — new file
`fe/fe-core/src/test/java/org/apache/doris/nereids/trees/plans/commands/info/CreateTableInfoEngineCatalogTest.java`
(same package → can construct `CreateTableInfo`; private gate methods invoked via reflection).
Infra (mirrors `PluginDrivenExternalCatalogDdlRoutingTest`): `MockedStatic<Env>` →
`mockEnv.getCatalogMgr()` → `mockCatalogMgr.getCatalog("mc_ctl")` returns a
`Mockito.mock(PluginDrivenExternalCatalog.class)` with `getType()` stubbed to `"max_compute"` (a
Mockito mock IS an `instanceof PluginDrivenExternalCatalog`; getType() is non-final). **This
registration via the mocked CatalogMgr is mandatory** because both gate methods look the catalog up
by name (critic correction — a directly-constructed catalog would be ignored).

Cases (each assertion message encodes WHY — Rule 9; each fails if its branch is reverted):
1. `noEnginePaddedToMaxcomputeForPluginDriven` — `CreateTableInfo` with empty engine, ctl `"mc_ctl"`;
   reflectively invoke `paddingEngineName("mc_ctl", null)`; assert `getEngineName() == "maxcompute"`.
   WHY: no-ENGINE CREATE TABLE must auto-pad maxcompute, not throw. (Revert branch → throws "does not
   support create table" → red.)
2. `ctasNoEnginePaddedToMaxcompute` — drive the **CTAS** entry point
   `validateCreateTableAsSelect(["mc_ctl"], <cols>, ctx)` far enough to assert padding ran (assert
   `getEngineName() == "maxcompute"` after the call, or that it does not throw "does not support
   create table"). Covers the CTAS path the parent design omitted. (If `validate(ctx)` downstream is
   too heavy to run headless, assert via `paddingEngineName` re-invocation parity + a focused
   `assertDoesNotThrow` up to the padding point; final form decided at implementation against what
   runs offline.)
3. `wrongExplicitEngineRejectedForPluginDriven` (Rule-9 mirror test) — set `engineName="hive"`,
   ctl `"mc_ctl"`; reflectively invoke `checkEngineWithCatalog()`; assert `AnalysisException`
   thrown. WHY: catalog-engine consistency must still hold under PluginDriven. **This fails (no
   throw) if the checkEngineWithCatalog branch is absent** — proving the mirror branch against its
   intent (the parent design had no such test; the branch would otherwise be untestable per Rule 9).
4. `correctExplicitEnginePassesForPluginDriven` — `engineName="maxcompute"`,
   `checkEngineWithCatalog()` does not throw (locks that the check is a consistency gate, not a
   blanket reject).
5. `jdbcPluginDrivenStillUnsupported` — getType() stubbed `"jdbc"`; `paddingEngineName` (empty
   engine) throws "does not support create table" (helper returns null → existing else); and
   `checkEngineWithCatalog` with any explicit engine does NOT throw (mirrors legacy pass-through).
   Locks the null-returning-helper decision (§Design 3) against regression.

Reflection helper unwraps `InvocationTargetException` to rethrow the cause so `assertThrows`
sees `AnalysisException` directly.

**E2E (NOT run in normal CI — needs live ODPS):**
`regression-test/suites/external_table_p2/maxcompute/test_max_compute_create_table.groovy` Test1
(`:62-71`, no-ENGINE Basic CREATE TABLE) is the natural assertion point: under cutover it must go
from FAIL → PASS (CREATE TABLE succeeds, `show tables like` hits, `qt_test1_show_create_table`
renders without error). **Do NOT add the parent design's proposed extra assertion
"SHOW CREATE TABLE output contains `ENGINE=maxcompute`"** (critic correction): SHOW CREATE TABLE
renders `ENGINE=` + `getEngineTableTypeName()` = `"MAX_COMPUTE_EXTERNAL_TABLE"` (the recorded `.out`
baseline line 3 confirms `ENGINE=MAX_COMPUTE_EXTERNAL_TABLE`), NOT `maxcompute`. The analysis-time
engineName (`"maxcompute"`, used for padding/validation) is a different value from the display-time
table-type name. The existing `qt_test1_show_create_table` already covers the regression correctly;
no extra assertion needed.

## Resolved Open Questions
- Helper default for jdbc/es/trino: **return null** (not throw) so both gates mirror legacy for those
  types; revisit when a second connector full-adopts CREATE TABLE.
- `checkEngineWithCatalog` message: **verbatim legacy MC string** (`"MaxCompute type catalog can only
  use \`maxcompute\` engine."`) — only reachable for max_compute, matches the in-repo convention;
  UT asserts on exception **type**, not the string, to avoid brittleness.
- `"max_compute"→"maxcompute"` mapping kept as a local literal in the helper (minimal change) with a
  cross-reference comment to `PluginDrivenExternalTable.getEngine()` rather than extracting a shared
  constant.

## Summary (post-implementation, 2026-06-07)
**Status: DONE — implemented, verified, reviewed (sound, 1 round), ready to commit.**

- **Change**: `CreateTableInfo.java` — `import org.apache.doris.datasource.PluginDrivenExternalCatalog;`
  (after `:49 InternalCatalog`); PluginDriven branch in `paddingEngineName` (after the MC branch) and
  in `checkEngineWithCatalog` (after the MC branch); new `private static pluginCatalogTypeToEngine`
  (`"max_compute"`→`ENGINE_MAXCOMPUTE`, else `null`). New UT
  `CreateTableInfoEngineCatalogTest` (5 cases). fe-core only; no SPI/connector/thrift/BE change.
- **Verification** (real Maven exits, not background-task echoes):
  - `mvn -pl :fe-core -am test -Dtest=CreateTableInfoEngineCatalogTest` → Tests run: 5, Failures: 0,
    Errors: 0; BUILD SUCCESS.
  - Rule-9 mutation (helper returns `null` for `max_compute`): tests 1/2/3 go red (no-ENGINE throw /
    `expected:<maxcompute> but was:<null>` / "nothing was thrown"); restore → 5/5 green.
  - `mvn -pl :fe-core checkstyle:check` → 0 violations.
- **Adversarial review** (`wf_e8887334-53a`, 4 clean-room reviewers → verify → cross-check):
  verdict **sound**, 1 round. 6 raw findings → 1 confirmed = a single **nit**
  (`correctExplicitEnginePassesForPluginDriven` is vacuous as a regression detector for its branch),
  disposition **acceptable-as-is** — the real guard for that branch is the sibling
  `wrongExplicitEngineRejectedForPluginDriven` (confirmed pre-fix-red). All 6 design corrections
  confirmed present in code; no code↔design contradictions; no blocker/major. See
  `plan-doc/reviews/P4-T06d-FIX-DDL-ENGINE-review-rounds.md`.
- **Batch-D ordering** (must record in decisions-log): this fix lands the PluginDriven branches FIRST;
  Batch-D then deletes only the legacy `instanceof MaxComputeExternalCatalog` branches + the
  `maxcompute.MaxComputeExternalCatalog` import, leaving the getType()-keyed PluginDriven branches.
