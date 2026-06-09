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

## 1. Deletion set — 20 fe-core files (all verified dead-after-flip, zero survivor risks)

> **⚠️ 红线限定（P3-11 补，2026-06-08）— `source/MaxComputeScanNode`：** 「zero survivor / dead-after-flip」
> 仅就**实例化链**成立；该类还承载三段**行为逻辑副本**，删除前各须有 PluginDriven 侧 live 等价物：
> ① **读裁剪**（`MaxComputeScanNode:718-731`）—— 已由 FIX-PRUNE-PUSHDOWN（`072cd545c54` / [D-031]）清除；
> ② **batch-mode 异步分批 split**（`MaxComputeScanNode:214-298`）—— 已由 FIX-BATCH-MODE-SPLIT（`ac8f0fc15eb` /
> [D-035]）在 `PluginDrivenScanNode` 落通用等价；③ **LIMIT-split 优化**（`MaxComputeScanNode` 内第 3 段行为副本）
> —— 通用等价已在 P3-9 / 连接器 `MaxComputeScanPlanProvider`（session var `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION`
> 门控，commit `952b08e0cc8`）落地（**DOC task 2026-06-09 补**：原注只列 ①② 漏此第 3 副本）。三者**均已落**，故本类现可纳入删除单元；但删前仍须 live e2e
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
- `fe-common` `common/maxcompute/MCProperties` — **KEEP**（odps-free 常量；`DatasourcePrintableMap` 仅引它、无 odps）。
  `MCUtils` —— **不再属 KEEP**：见 **§8**（方案 A，2026-06-09 用户定），删 legacy 后下沉到 be-java-extensions、并删 fe-common 的 odps（使 fe-core 依赖树彻底无 odps）。

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

> ✅ **EXECUTED 2026-06-09** (branch `catalog-spi-06`, off upstream `9ed49571b20` / #64253). Steps 1–6 landed in 2 commits: `7a4db351100` (delete 20 files + reverse-refs + test trims/rewires) and `409300a75b8` (drop fe-core+fe-common odps, sink MCUtils into be-java-ext). All gates green: test-compile (main+test), checkstyle 0, import-gate, grep-empty (`com.aliyun.odps` in fe-core/src = ∅, no non-comment refs), and `mvn -pl :fe-core dependency:tree | grep odps` = ∅. §8 surfaced a hidden transitive leak — odps-sdk-core was also providing netty/protobuf to fe-common's own DorisHttpException/GsonUtilsBase; fixed by declaring them directly (see [DV-022]). Step 7 (doc-sync) = this update + PROGRESS/HANDOFF/deviations-log.

1. **T07+T08+T09 as one compiling change:** apply all §2 edits (imports + dead branches) **and**
   delete the §1 20 files together. Keep §3 untouched.
2. Trim §2 test files (re-verify each against §3 keep-set first).
3. Gate: `mvn -f fe/pom.xml -pl :fe-core -am -Dmaven.build.cache.enabled=false -Dcheckstyle.skip=true
   -DskipTests test-compile` (compiles main+test against odps-less-of-legacy classpath; read real
   `BUILD`/`MVN_EXIT`) → `checkstyle:check` → `bash tools/check-connector-imports.sh`.
4. Grep-empty assertion (acceptance): `grep -rn "MaxComputeExternal\|MCTransaction\b\|MCInsert" fe/fe-core/src/main` returns **only** the §3 keep-set lines (enums/gson/thrift/plugin). `grep -rn "com.aliyun.odps" fe/fe-core/src` → empty.
5. Commit `[P4-T07/T08/T09] remove legacy MaxCompute subsystem from fe-core`.
6. **pom drop** (§4) **+ fe-common 解耦** (§8)：remove fe-core 的两个 odps 块；**并**按 §8 下沉 `MCUtils`
   到 be-java-ext + 删 `fe-common/pom.xml` 的 `odps-sdk-core`。re-run test-compile (BUILD SUCCESS) +
   `dependency:tree | grep odps` = **∅**。Commit `[P4-T09] drop fe-core odps + sink MCUtils into BE ext, drop fe-common odps`.
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

---

## 7. 现状校验 + 范围确认 + 前置门（2026-06-09，design-only refresh）

> 2026-06-09 session 增补：用户要求「完整移除 fe-core 下老的 maxcompute（零代码 + 零依赖）」。本 session **只分析 + finalize 方案 + 确认前置，不动代码**（用户定：实际删除放下个 session）。

### 7.1 用户范围定夺（2026-06-09）= 重申 [D-027]
- **Q1 = 只删老实现（本 Batch-D），非 full-purge。** 保留服务于新 SPI 插件路径的 `max_compute` 词元（§3 KEEP 集：`TableType.MAX_COMPUTE_EXTERNAL_TABLE` / `TransactionType.MAXCOMPUTE` / `TableFormatType.MAX_COMPUTE` / block-id thrift `TMaxComputeBlockId*` / session var `ENABLE_MC_LIMIT_SPLIT_OPTIMIZATION` / `ConnectorSessionBuilder` 的 `max_compute_write_max_block_count` 注入 / `DatasourcePrintableMap`→`MCProperties` / GsonUtils 镜像兼容串）—— 这些是 live 路径在用，非 legacy。
- **Q2 = fe-core 依赖树彻底无 odps（2026-06-09 升级，覆盖 [D-027] 决定 2）。** 不止删 fe-core/pom 两个直接 odps 块，**外加**经**方案 A**（§8）把 `MCUtils` 下沉到 be-java-extensions、再从 `fe-common/pom.xml` 删 `odps-sdk-core` → fe-core 不再有任何**直接或传递**的 odps。原 [D-027] 决定 2「direct-only、接受 fe-common 传递」被用户 2026-06-09 反转。
- **后果（须知，by design，非缺陷）**：删后 `grep -rn "com.aliyun.odps" fe/fe-core/src` = **∅**，但 `grep -rni "maxcompute\|max_compute\|odps" fe/fe-core/src/main` 仍 **>0**（SPI 胶水 + 镜像兼容串保留）。若日后要真正「零词元」= 另起 full-purge 任务（泛化 block-id thrift / 各 MC 枚举 / session var；fe-common 的 odps 已由 §8 解耦、不在此列），本 session 已评估其代价与**升级兼容下限**（GsonUtils 3 兼容子类串 + `InitCatalogLog.Type.MAX_COMPUTE` + 已持久化 `TransactionType.MAXCOMPUTE` 须留，否则断 pre-SPI 镜像/editlog 滚动升级），用户当前**不取**。

### 7.2 @HEAD 校验结果（删除单元仍准确，2026-06-09）
- ✅ **删除单元 = 20 文件**（非 §0/§1 早先写的「21」—— 其自身枚举即 10+8+2=20，off-by-one 已就地修正）；20 文件全部存在于当前 HEAD。
- ✅ **Linchpin（pom-drop 安全性）**：`fe-core/src` 内 import `com.aliyun.odps.*` 的文件 = **8**（7 main + 1 test），**全部**在删除单元内；删除单元外 residual = **∅** → 删完 fe-core 零 odps 源引用，pom drop 不破编译。
- ✅ fe-core/pom 两个 odps 块在 `:364`(odps-sdk-core) / `:379`(odps-sdk-table-api)（§4 的「~362–381」仍准）。
- ✅ 自本 doc（2026-06-07）后近 commit `effd8edbfdb`(fix explain) / `2b8a732682c`(add connector type to explain) **只动 `PluginDrivenScanNode`（KEEP 集，通用 SPI）** + 新增分区计数测 + 审计 md，**未改 legacy footprint**。
- ✅ **任务 0（静态分发完整性审计）已 DONE** —— `plan-doc/reviews/P4-cutover-completeness-audit-2026-06-08.md`（裁决 PASS：24/24 op 全路由、零 legacy 运行时回退）。即 🅱 删除的**静态前置门已绿**。
- ⚠️ **§2 行号已漂移**：多处 `~:NNN` 较 HEAD 偏 +5~+43（doc 写于 2026-06-07）。照 §5 要求——执行前按符号 re-grep，**勿信行号**。

### 7.3 执行前置门（下个 session 开删前须全绿）
1. 🅰 **live ODPS e2e 验证绿（用户跑，硬门，当前 OPEN）** —— `OdpsLiveConnectivityTest`（4 个 `MC_*` env）+ 手测 smoke（读/裁剪/下推/limit-split/batch/CAST + 写/INSERT/OVERWRITE/txn/动静态分区 + 全 DDL/元数据）。[D-027]：删 legacy = 去掉易回退的 fallback，故须 live 绿后才删。
2. ⬜ **T3**（登记 4 条 Tier-3 DV：GAP3/4/9/10，doc-only）—— 可与删除同批或前置；非编译阻塞。
3. ✅ **DOC**（本 Batch-D redline 扩充）—— 本 session 完成（§1 补 LIMIT-split 第 3 副本红线 + 本 §7 校验）。

### 7.4 验收基线（删除 + pom drop 后须满足）
- `grep -rn "MaxComputeExternal\|MCTransaction\b\|MCInsert" fe/fe-core/src/main`：当前 **151** 行 → 删后须**仅剩 §3 KEEP 集**（`GsonUtils` 3 个字符串字面量 `"MaxComputeExternal{Catalog,Database,Table}"` + PluginDriven* 内引用 legacy 行为的注释）。
- `grep -rn "com.aliyun.odps" fe/fe-core/src` → **∅**。
- `mvn -pl :fe-core dependency:tree | grep odps` → **完全为空**（§8 方案 A 下沉 MCUtils + 删 fe-common odps 后，直接 + 传递 odps 均消失）。
- 总词元 `grep -rni "maxcompute\|max_compute\|odps" fe/fe-core/src/main`：当前 **703** → 删后仍 >0（Q1 保留胶水，非缺陷）。

---

## 8. fe-common odps 解耦（方案 A，用户定 2026-06-09）—— 使 fe-core 依赖树彻底无 odps

> 背景：`fe-common` 装 `odps-sdk-core` 仅为 `common/maxcompute/MCUtils`（odps 客户端工厂）服务，而 MCUtils 删 legacy 后**唯一消费者 = be-java-extensions**（`MaxComputeJniScanner` / `MaxComputeJniWriter`）。fe-core 经 fe-common 白拿 odps 是假耦合。用户定**方案 A**：把 MCUtils 下沉到真正用它的 BE 扩展，fe-common 去 odps。

**消费者核实（2026-06-09 @HEAD）**：
- `MCUtils`（import `com.aliyun.odps.*` + `com.aliyun.auth.credentials.*`）：be-java-ext 2 处 `createMcClient` + fe-core `MaxComputeExternalCatalog`（§1 删）→ **删后仅 be-java-ext**。
- `MCProperties`（纯常量、零 import）：be-java-ext `MaxComputeJniWriter` + fe-core `DatasourcePrintableMap`（KEEP）→ **留 fe-common**。
- 新 FE 连接器 `fe-connector-maxcompute` **不用** fe-common 的 MC 类（有自有 `MCConnectorClientFactory`）。
- be-java-ext 经 `java-common→fe-common`（`provided`）拿 MC 类，且自带 `odps-sdk-core` / `odps-sdk-table-api`。

**步骤（须在 §1 删除 `MaxComputeExternalCatalog` 之后 / 同批，否则 fe-core 仍需 MCUtils）**：
1. 移 `fe/fe-common/.../common/maxcompute/MCUtils.java` → `fe/be-java-extensions/max-compute-connector/.../org/apache/doris/maxcompute/MCUtils.java`；包名 `org.apache.doris.common.maxcompute` → `org.apache.doris.maxcompute`。`MCUtils` 内保留 `import org.apache.doris.common.maxcompute.MCProperties`（仍在 fe-common、be-java-ext 可达）。
2. `MaxComputeJniScanner` / `MaxComputeJniWriter` 删 `import org.apache.doris.common.maxcompute.MCUtils`（与 MCUtils 同包后无需 import）。
3. `MCProperties.java` **留 fe-common**（odps-free 常量；fe-core `DatasourcePrintableMap` 仍需）。
4. 删 `fe/fe-common/pom.xml` 的 `odps-sdk-core` 块（~:137–154）。
5. 守门：`grep -rn "com.aliyun.odps" fe/fe-common/src` = **∅**（验 MCUtils 是 fe-common 唯一 odps 用户）→ `mvn -pl :fe-common -am compile`（fe-common 无 odps 仍编译）→ `mvn -pl be-java-extensions/max-compute-connector compile`（MCUtils 在新家编译；确认 `com.aliyun.auth.credentials.*` 经 odps-sdk-core 传递的 credentials-java 可达，**若报缺则给该模块 pom 显式补 aliyun-auth 依赖**）→ `mvn -pl :fe-core dependency:tree | grep odps` = **∅**。
6. commit `[P4-T09] sink MCUtils into BE extension; drop odps from fe-common`（与 §4 fe-core pom drop 同批或紧随）。

**与 §4 关系**：§4 删 fe-core **直接** odps；§8 断 fe-common→fe-core 的**传递** odps。二者合一 = fe-core 依赖树**彻底无 odps**（需求 ② 严格满足）。**运行时安全性**：删 legacy 后 fe-core 不再 class-load 任何 odps/MCUtils 符号（仅 `DatasourcePrintableMap` 引 odps-free 的 `MCProperties`），故 fe-core 无 odps 不会 `NoClassDefFoundError`。
