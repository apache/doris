# P4 Batch D — MaxCompute legacy removal + fe-core odps-dep drop (design)

> **Design-first, verified.** Closure produced 2026-06-07 by a parallel re-grep + adversarial-verify
> workflow (OQ-3 "入口先完整 re-grep" satisfied). Full per-line detail (84 refs) saved at the recon
> output `tasks/wzlnjgj64.output` (session transcript). This doc is the execution source for
> P4-T07/T08/T09 + the fe-core pom drop.
> **Gate before executing any of this:** the user must report the live ODPS cutover test green
> (`OdpsLiveConnectivityTest` + manual smoke) — per [D-027], removal is sequenced *after*
> live-validation so the T06b flip stays independently revertable until then.
> Mirrors the completed trino-connector removal: `524097e38d3` (code) + `c4ac2c5911d` (pom drop).
>
> ⚠️ **[D-028] UPDATE (2026-06-07) — gate raised + §2 amended.** Live-verification recon (code-verified)
> found the cutover **functionally incomplete**: only read(SELECT)/CREATE TABLE/write(INSERT) route
> through SPI; **DROP TABLE / CREATE DB / DROP DB / SHOW PARTITIONS / partitions() TVF FE-dispatch was
> never wired** (connector impls exist since P4-T01/T02, FE has zero callers). So **P4-T06c must land
> first** (wire those FE sites to the SPI, generically on `PluginDrivenExternalCatalog`), then live
> verification must be **all-green**, *then* Batch D. Consequence for §2: the `ShowPartitionsCommand`
> / `MetadataGenerator` / `PartitionsTableValuedFunction` entries change from **delete-branch** to
> **delete only the residual legacy `MaxComputeExternalCatalog` reference** — the working dispatch is
> the `PluginDrivenExternalCatalog` branch T06c adds (do NOT delete that). See §2 note.

---

## 0. Why / scope

After the T06b flip ([P4-T06b]), a `max_compute` catalog deserializes to `PluginDrivenExternalCatalog`
/ `PluginDrivenExternalTable`; **no legacy `MaxComputeExternal*` object is ever instantiated again**
(factory case gone, GSON → `PluginDriven*` via T05). The entire legacy MaxCompute subsystem in
fe-core is therefore dead code. Removing it is the only way to drop fe-core's `odps-sdk-*` jars
(the user's requirement): the two deps are reachable **only** through that legacy code (7 files
`import com.aliyun.odps.*`, all under the deletion set; `feCoreOdpsResidualAfterDeletion` = ∅).

Batch D = **T07** (clean mechanical reverse-refs) + **T08** (clean live reverse-refs + verify
`MCInsertExecutor` dead, OQ-1) + **T09** (delete legacy dir + plumbing + tests) + **pom drop**.
In practice the reverse-ref removal and the file deletion must land as **one compiling unit**
(every `instanceof MaxCompute*` references a class symbol — Java does not dead-strip source refs).

---

## 1. Deletion set — 21 fe-core files (all verified dead-after-flip, zero survivor risks)

> **⚠️ 红线限定（P3-11 补，2026-06-08）— `source/MaxComputeScanNode`：** 「zero survivor / dead-after-flip」
> 仅就**实例化链**成立；该类还承载两段**行为逻辑副本**，删除前各须有 PluginDriven 侧 live 等价物：
> ① **读裁剪**（`MaxComputeScanNode:718-731`）—— 已由 FIX-PRUNE-PUSHDOWN（`072cd545c54` / [D-031]）清除；
> ② **batch-mode 异步分批 split**（`MaxComputeScanNode:214-298`）—— 已由 FIX-BATCH-MODE-SPLIT（`ac8f0fc15eb` /
> [D-035]）在 `PluginDrivenScanNode` 落通用等价。两者**均已落**，故本类现可纳入删除单元；但删前仍须 live e2e
> 终验（[D-027] Batch-D 执行门）。交叉引用 HANDOFF §横切「Batch-D 红线扩充」+ 各 per-fix 红线。

**`datasource/maxcompute/` (10):** `MaxComputeExternalCatalog`, `MaxComputeExternalDatabase`,
`MaxComputeExternalTable`, `MaxComputeMetadataOps`, `MaxComputeExternalMetaCache`,
`MaxComputeSchemaCacheValue`, `McStructureHelper` (+inner `ProjectSchemaTableHelper`/`ProjectTableHelper`),
`MCTransaction`, `source/MaxComputeScanNode`, `source/MaxComputeSplit`.

**Write/txn plumbing (8):**
- `planner/MaxComputeTableSink`
- `nereids/trees/plans/logical/LogicalMaxComputeTableSink`
- `nereids/trees/plans/physical/PhysicalMaxComputeTableSink`
- `nereids/analyzer/UnboundMaxComputeTableSink`
- `nereids/trees/plans/commands/insert/MCInsertExecutor`  *(OQ-1: confirm dead — only built from `instanceof UnboundMaxComputeTableSink`/`PhysicalMaxComputeTableSink`, both gone)*
- `nereids/trees/plans/commands/insert/MCInsertCommandContext`
- `nereids/rules/implementation/LogicalMaxComputeTableSinkToPhysicalMaxComputeTableSink`
- `transaction/MCTransactionManager`

**Tests (2, deleted whole):** `datasource/maxcompute/MaxComputeExternalMetaCacheTest`,
`datasource/maxcompute/source/MaxComputeScanNodeTest`.

Instantiation-chain proof (all roots dead post-flip): `MaxComputeExternalCatalog` was built **only**
at `CatalogFactory:147` (removed by flip); everything else is reachable only from it or via
`instanceof MaxCompute*` gates that become false. `MaxComputeScanNode` only via
`instanceof MaxComputeExternalTable`. The sinks/executor/context/rule only via `instanceof` on
`UnboundMaxComputeTableSink` / `PhysicalMaxComputeTableSink` / `LogicalMaxComputeTableSink`.

---

## 2. Reverse-ref cleanup — ~30 files, 84 refs (32 remove-import · 43 delete-branch · 9 keep)

> ⚠️ **[D-028] amendment:** for the 3 partition/show dispatch sites below —
> `ShowPartitionsCommand` (:203/:286/:415), `MetadataGenerator` (:1310/:1337 `dealMaxComputeCatalog`),
> `PartitionsTableValuedFunction` (:173/:200) — **P4-T06c adds a `PluginDrivenExternalCatalog` branch
> that routes to the connector SPI** (the actual functionality). After T06c, Batch D must **delete
> only the residual legacy `MaxComputeExternalCatalog`/`MaxComputeExternalTable` branch + import**, and
> **KEEP** the new PluginDriven branch. (Pre-T06c this table said "delete-branch" outright, which would
> have permanently broken SHOW PARTITIONS / partitions TVF — see [D-028].) The DDL gap (createDb/dropDb/
> dropTable) is fixed by T06c via `PluginDrivenExternalCatalog` overrides, not by any §2 edit here.

Per file (edit, NOT delete) — remove the import(s) + delete the now-dead `instanceof`/visitor/rule branch:

| File | What to remove |
|---|---|
| `datasource/CatalogFactory.java` | *(done in T06b: import + case)* |
| `datasource/ExternalCatalog.java` | import + `MaxComputeExternalDatabase` db-build branch (~:939) → mirror JDBC/trino (`PluginDrivenExternalDatabase`) |
| `datasource/ExternalMetaCacheMgr.java` | import + **eager** `MaxComputeExternalMetaCache` registration (~:183 + :310) — ⚠ constructed at ctor, must be removed (adversarial finding) |
| `datasource/metacache/ExternalMetaCacheRouteResolver.java` | import + `instanceof MaxComputeExternalCatalog` (~:75) |
| `nereids/analyzer/UnboundTableSinkCreator.java` | import + 3× `instanceof MaxComputeExternalCatalog` branches (:66/:105/:146) |
| `nereids/glue/translator/PhysicalPlanTranslator.java` | 4 imports + `visitPhysicalMaxComputeTableSink` (~:593) + `instanceof MaxComputeExternalTable` scan branch (~:795) |
| `nereids/rules/analysis/BindSink.java` | 4 imports + `unboundMaxComputeTableSink`/`bindMaxComputeTableSink`/`bind`/`instanceof MaxComputeExternalTable` branches (~:170/:863/:1078/:1084) |
| `nereids/trees/plans/commands/insert/InsertIntoTableCommand.java` | 3 imports + `instanceof PhysicalMaxComputeTableSink` MCInsertExecutor branch (~:562) |
| `nereids/trees/plans/commands/insert/InsertOverwriteTableCommand.java` | 2 imports + `instanceof MaxComputeExternalTable` (~:321) + `instanceof UnboundMaxComputeTableSink` (~:399) |
| `nereids/trees/plans/commands/insert/InsertUtils.java` | import + 2× `instanceof UnboundMaxComputeTableSink` (~:380/:607) |
| `nereids/trees/plans/visitor/SinkVisitor.java` | 3 imports + 3 visit methods (Unbound/Logical/Physical, ~:104/:136/:200) |
| `nereids/processor/post/ShuffleKeyPruner.java` | import + `visitPhysicalMaxComputeTableSink` (~:272) |
| `nereids/processor/pre/TurnOffPageCacheForInsertIntoSelect.java` | import + `visitLogicalMaxComputeTableSink` (~:72) |
| `nereids/properties/RequestPropertyDeriver.java` | import + `visitPhysicalMaxComputeTableSink` (~:180) |
| `nereids/rules/RuleSet.java` | import + 2× `LogicalMaxComputeTableSinkToPhysicalMaxComputeTableSink` registration (~:233/:281) |
| `nereids/rules/expression/ExpressionRewrite.java` | `LogicalMaxComputeTableSinkRewrite` entries (~:113/:522) |
| `nereids/trees/plans/commands/ShowPartitionsCommand.java` | import + `instanceof MaxComputeExternalCatalog` (:203/:415) + `handleShowMaxComputeTablePartitions` (~:286) |
| `nereids/trees/plans/commands/info/CreateTableInfo.java` | import + 2× `instanceof MaxComputeExternalCatalog` (~:390/:912) |
| `tablefunction/MetadataGenerator.java` | import + `instanceof MaxComputeExternalCatalog` (~:1310) + `dealMaxComputeCatalog` (~:1337) |
| `tablefunction/PartitionsTableValuedFunction.java` | 2 imports + `instanceof MaxComputeExternalCatalog`/`Table` (~:173/:200) |
| `tablefunction/PartitionValuesTableValuedFunction.java` | import + `instanceof MaxComputeExternalCatalog` (~:115) |
| `transaction/TransactionManagerFactory.java` | import + `createMCTransactionManager` branch (~:38) |

**Test trims (~6):** `ExternalMetaCacheRouteResolverTest`, `CommitDataSerializerTest` (MCTransaction
case), `FrontendServiceImplTest` (testGetMaxComputeBlockIdRange — keep if it exercises the *plugin*
RPC; only drop the legacy-MCTransaction wiring), `PluginDrivenExternalTableEngineTest`
(keeps the max_compute engine cases — those are plugin, NOT legacy; re-check before trimming),
`PluginDrivenInsertExecutorTest`, `PluginDrivenTableSinkBindingTest` (comment only).
⚠ Re-verify each test against the keep-set before editing — several "MaxCompute" test refs are the
**plugin** path (keep), not legacy.

---

## 3. KEEP set — image / plan / thrift compat (do NOT delete)

- `catalog/TableIf.TableType.MAX_COMPUTE_EXTERNAL_TABLE` — used by `PluginDrivenExternalTable` post-flip + old-image replay.
- `datasource/InitCatalogLog.Type.MAX_COMPUTE`, `datasource/InitDatabaseLog.Type.MAX_COMPUTE` — init-log replay (`legacyLogTypeToCatalogType` default → `"max_compute"`).
- `transaction/TransactionType.MAXCOMPUTE` — plugin executor `transactionType()` returns it (T06a) + state persistence.
- `datasource/TableFormatType.MAX_COMPUTE` — `PluginDrivenExternalTable.getTableFormatType()`.
- `persist/gson/GsonUtils` 3× `registerCompatibleSubtype("MaxComputeExternal{Catalog,Database,Table}")` — T05 image compat (string literals only, no odps).
- `nereids/.../PlanType.{LOGICAL,LOGICAL_UNBOUND,PHYSICAL}_MAX_COMPUTE_TABLE_SINK`,
  `nereids/rules/RuleType.{BINDING_INSERT_MAX_COMPUTE_TABLE, LOGICAL_MAX_COMPUTE_TABLE_SINK_TO_PHYSICAL...}` —
  enum constants; leave them (harmless dormant; removing risks churn). They become unused once the
  classes are deleted; that is fine.
- `service/FrontendServiceImpl.getMaxComputeBlockIdRange` + `TMaxComputeBlockIdRequest/Result` thrift —
  **the plugin write path's BE→FE block-alloc RPC** (T06a), NOT legacy. Keep.
- `transaction/PluginDrivenTransactionManager` — the live txn manager (T06a). Keep.
- `datasource/PluginDrivenExternalTable` `max_compute` engine cases (T05) + `PluginDrivenExternalCatalog.legacyLogTypeToCatalogType` default (no MC case). Keep.
- `fe-common` `common/maxcompute/MCProperties` + `MCUtils` — fe-common, used by the connector +
  be-java-extensions; not in scope. `DatasourcePrintableMap` imports only `MCProperties` (no odps).

---

## 4. pom drop (mirror `c4ac2c5911d`)

`fe/fe-core/pom.xml` — remove the two dependency blocks (~lines 362–381):
`com.aliyun.odps:odps-sdk-core` (with its `<exclusions>`) and `com.aliyun.odps:odps-sdk-table-api`.
After deletion fe-core has **zero** odps source refs. fe-core still receives `odps-sdk-core`
**transitively via fe-common** (which keeps it for `MCUtils`) — accepted per [D-027] decision 2
(direct-declarations-only). `odps-sdk-table-api` is fe-core-only and disappears entirely from
fe-core's classpath. Verify with `mvn -pl :fe-core dependency:tree | grep odps` (expect only the
transitive `odps-sdk-core` via fe-common).

---

## 5. Ordered TODO (execute after live-validation gate)

1. **T07+T08+T09 as one compiling change:** apply all §2 edits (imports + dead branches) **and**
   delete the §1 21 files together. Keep §3 untouched.
2. Trim §2 test files (re-verify each against §3 keep-set first).
3. Gate: `mvn -f fe/pom.xml -pl :fe-core -am -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true
   -DskipTests test-compile` (compiles main+test against odps-less-of-legacy classpath; read real
   `BUILD`/`MVN_EXIT`) → `checkstyle:check` → `bash tools/check-connector-imports.sh`.
4. Grep-empty assertion (acceptance): `grep -rn "MaxComputeExternal\|MCTransaction\b\|MCInsert" fe/fe-core/src/main` returns **only** the §3 keep-set lines (enums/gson/thrift/plugin). `grep -rn "com.aliyun.odps" fe/fe-core/src` → empty.
5. Commit `[P4-T07/T08/T09] remove legacy MaxCompute subsystem from fe-core`.
6. **pom drop** (§4): remove the two blocks; re-run test-compile (BUILD SUCCESS) + `dependency:tree`
   check. Commit `[P4-T09] drop leftover odps-sdk deps from fe-core`.
7. doc-sync 5 steps (PROGRESS / tasks-P4 / connectors-maxcompute / decisions / deviations) + grep-empty
   evidence in the 阶段日志.

---

## 6. Risks

| Risk | Mitigation |
|---|---|
| Missed reverse-ref → compile break | §2 is the verified 84-ref closure; gate test-compile catches any residue. |
| Deleting a *plugin*-path symbol thinking it's legacy | §3 keep-set is explicit; re-verify each "MaxCompute" test/thrift ref before touching. |
| `ExternalMetaCacheMgr` eager init NPE/CNFE | §2 flags the ctor-time registration — remove the line, do not assume dead-strip. |
| `MCInsertExecutor` still reachable (OQ-1) | Verified: only built from now-dead `instanceof` gates; confirm with the grep-empty step before deleting. |
| Removing fe-core odps breaks an unseen consumer | `feCoreOdpsResidualAfterDeletion` = ∅; `dependency:tree` + test-compile confirm. |
