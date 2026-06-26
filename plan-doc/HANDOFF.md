# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.6 C3b-core 耦合核心 step 6：commit-bridge（最后一步，最深）** —— step 3（③ row-id 注入）✅ 本 session

**step 3 ✅ 本 session（1 commit，additive/dormant；3a 新 SPI getSyntheticWriteColumns + 3b getFullSchema override + 3c injector guard 中立化；27/16/6/60+14 全绿 + 11-mutation 全杀 + 对抗 review GO）**。**P6.1–P6.5 = ✅ 全 DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core 设计 ✅ + impl-recon ✅ + ③-infra ✅ + ctx 重命名 ✅ + step1（① handles）✅ + step2（② cast+partitioning）✅ + step3（③ row-id 注入）✅ → 余下 step6（commit-bridge）待办**。翻闸 = 5 commit-stream（C1/C2/C3/C4/C5），**C5 最后翻闸（唯一不可逆点）**。iceberg **仍不在** `SPI_READY_TYPES`。

## ✅ 本 session 完成（step 3，1 commit + 对抗 review GO；详设计 §11.6）
**③ row-id 注入 = 两处 IcebergExternalTable post-flip 耦合双治**（carrier=A 下 v3-lineage 已连接器侧 emit〔③-infra part2〕→ 本 step 仅请求级 STRUCT `__DORIS_ICEBERG_ROWID_COL__`）：
- **3a 新 tiny SPI accessor**：`ConnectorWritePlanProvider.getSyntheticWriteColumns(session, tableHandle)` default `emptyList()`；`IcebergWritePlanProvider` override 声明 STRUCT row-id（invisible，连接器本地字面量 5 常量）。STRUCT round-trip parity 钉死=`IcebergRowId.createHiddenColumn()`（两侧契约钉）。
- **3b `PluginDrivenExternalTable.getFullSchema`/`needInternalHiddenColumns` override**：gate(`Util.showHiddenColumns()||ctx.needsSyntheticWriteColForTable`)→经 SPI fetch+convert+append（null/absent 全降级不抛；连接器无关，iron-law 合规）。
- **3c `IcebergNereidsUtils.IcebergRowIdInjector` guard 放宽**：`isRowIdInjectionTarget(ExternalTable)`（中立=legacy IcebergExternalTable || PluginDriven+supportsDelete/Merge）+ 内 cast/targetTable/`getRowIdColumn` widen→`ExternalTable`；`pluginConnectorSupportsRowLevelDml` 加 null-connector guard（review Finding）。
- **验证**：连接器 `IcebergWritePlanProviderTest` 27/0/0 + 契约钉 `ConnectorColumnConverterTest` 16/0/0 + `PluginDrivenExternalTableTest`（新）6/0/0 + `IcebergNereidsUtilsTest` 60/0/0 + pre-flip parity `IcebergDDLAndDMLPlanTest` 14/0/0；**11-mutation 全杀**；checkstyle/import-gate PASS；`SPI_READY_TYPES` 未改；**0 新 DV**。
- **对抗 review GO**（`wf_cae63ac6-ca9`，3 lens + synthesis 全 GO；merge-only 漏 mutation + null-conn 不对称已补，余 finding 非阻塞）。

## 🟡 已登记 follow-up（非阻塞，**勿在本增量做**）
- **[FU-step1-nullconn → cutover]** `IcebergRowLevelDmlTransform.pluginConnectorSupportsRowLevelDml`（step-1）unguarded `getConnector()`；step-3 同名 helper 已 guard → cutover 前对齐两处（dropped-catalog race，dormant）。
- **[FU-order → flip e2e]** post-flip `getFullSchema` 列序 = [base, v3-lineage, row-id]（v3 来自 schema-cache，row-id 末尾 append）≠ legacy [base, row-id, v3-lineage]。**benign**（按名/field-id 匹配非位置）+ dormant；中立设计无法在不识别 v3-lineage 列（iron-law）前提下插 row-id 居中。翻闸 e2e 前加 v3 列序 parity 测或接受。
- **[FU-remap → cutover/bridge]**（step-2 登记，仍 open）`PluginDrivenExternalTable.toSchemaCacheValue:188` remap 分支丢 `.invisible()`/`.withUniqueId()`（只重应用 `.withTimeZone()`）；对 iceberg 安全（identity 名走直通）+ 全 SPI_READY 连接器不可达。

## 🔑 impl-recon 决定性结论（动 step-6 前必读，详 design §11/§10.6 O1·O2）
- **O1 解：合成列无 carrier 需求**。`$operation/$row_id` 不进 thrift sink，按名 `setMaterializedColumnName:615`→`colName` BE 按名匹配；连接器 `planWrite` 不读 `handle.getColumns()`。bridge 只需 (1) `WriteOperation=MERGE/DELETE` 透到写 handle（`ConnectorWriteHandle.getWriteOperation` 现默认 INSERT）+ (2) post-flip 走 `PluginDrivenTableSink` 时把 `:615` slot-name loop 复制到 `visitPhysicalConnectorTableSink:629-681`。⚠️**子项**：`visitPhysicalIcebergDeleteSink:588-598` 今天不跑 slot-name loop，impl 期须先证 DELETE 合成 slot colName 怎么到 BE。
- **O2 解（最深/危险）**：post-flip `IcebergRewritableDeletePlanner.collect():64` 按 `instanceof IcebergScanNode` 过滤，scan 变 `PluginDrivenScanNode`（translator `:750`先于`:790`）→ **静默 empty()** → v3 DV delete files 不 `removeDeletes`=**正确性回归（silent）**。native `DeleteFile` 过不了 classloader。**修=收集迁连接器**（`IcebergScanPlanProvider.buildDeleteFiles:515` 现转 Serializable carrier 丢弃 native → 须新增保留 `Map<originalPath,List<native DeleteFile>>` 喂 `IcebergConnectorTransaction.setRewrittenDeleteFilesByReferencedDataFile:271`，iceberg-only seam）。**做到时专门 recon**（per-request 连接器 ctx vs `beginWrite` 内按 pinned snapshot 重扫）。
- **bridge 事务**：事务管理器**按 catalog**（`BaseExternalTableInsertExecutor:69`），post-flip→`PluginDrivenTransactionManager`，legacy executor `(IcebergTransaction)` cast post-flip **必 CCE(loud)**→legacy 仅 pre-flip。dormant SPI commit 链（`RowLevelDmlCommand.run:98-100`→`connectorTx.commit`）就绪、**无需新 commit 管线**。`handles:69` 须与 `newExecutor:110`/`setupConflictDetection:166`/`finalizeSink:178` 的 legacy-executor downcast **lockstep** dual-mode。**conflict-detection**（native `Expression` 存 legacy executor）post-flip 须迁连接器经 `ConnectorTransaction.applyWriteConstraint`（中立 `ConnectorPredicate`，dormant 路）。

## C3b-core 剩余实现顺序（每完一 sub-step 即 green+mutation+HANDOFF+commit；PR 时 squash）
1. **③-infra part1+part2 ✅ DONE** / 2. **ctx 中立重命名 ✅ DONE** / 3. **step1 ① handles ✅ DONE** / 4. **step2 ② cast+partitioning ✅ DONE** / 5. **step3 ③ row-id 注入 ✅ DONE 本 session**（详上 + 设计 §11.6）。
6. **step6 commit-bridge ⟵ 下个 session 起点（最后一步，最深）**：translator `visitPhysicalIcebergMergeSink:601-627`/`DeleteSink:588-598` dual-mode（post-flip→`PluginDrivenTableSink`+connector `planWrite`，slot-name loop 透传，`WriteOperation` 经 `ConnectorWriteHandle.getWriteOperation` 透传）+ DML executor 改道 plugin-driven（connector `ConnectorTransaction` + `RowLevelDmlCommand` SPI commit）+ **O2 V3 DeleteFile 收集迁连接器**（`collectFor*` + `buildDeleteFiles:515`→`setRewrittenDeleteFilesByReferencedDataFile:271`，**专门 recon**）+ conflict-detection 迁连接器。**O2 做到时专门 recon；C5 前切忌动 `SPI_READY_TYPES`**。

## ⚠️⚠️ 用户铁律（C2 确立）：**fe-core 不得 `if(iceberg)` / `instanceof Iceberg*` / `import IcebergUtils`（新 seam）**
iceberg 逻辑落 `fe-connector` 经 SPI。保留的 `PhysicalIcebergMergeSink`/`DeleteSink`/`IcebergRowLevelDmlTransform`/`Iceberg*Command`/`IcebergNereidsUtils`/`IcebergTransaction`/`IcebergRewritableDeletePlanner` = **legacy 豁免**（dual-mode helper + `instanceof IcebergExternalTable` pre-flip 分支落这些豁免类合法）；V3 DeleteFile seam 的 `instanceof IcebergConnectorTransaction` downcast 在 finalize 钩子（iceberg-only seam）。**step-3 注入点 `PluginDrivenExternalTable` 是通用 fe-core 类（非豁免）→ 全经中立 SPI + 中立 ctx，已合规。**

---

# 🔴🔴 开放问题 — P6.6 翻闸（C1+C2+C3a+C3b-pre 已闭，C3b-core 设计+recon 闭、step1-3 已实现，step6+C4–C5 待办）

> 5 commit-stream（C1 ✅ / C2 ✅ / C3 WS-WRITE / C4 WS-REWRITE / C5 FLIP）。

- **[C1 ✅]** sys 表时间旅行 pin-feed（[D-068]）。
- **[C2 ✅]** 合成列读路径 classifyColumn SPI 化（[D-069]）。
- **[C3a ✅]** INSERT OVERWRITE 能力 + 完整接通 @branch（[D-070]，闭 DV-T06-branch）。
- **[C3b-pre ✅]** partition_columns emit + ④b parallel-write（[D-071]）。
- **[C3b-core 设计 ✅ / impl-recon ✅ / step1-3 已实现 / step6 待办]**（[D-072]+[D-073]）：① handles ✅ + ②[getWritePartitioning dual-mode helper] ✅ + ③ row-id 双注入[新 SPI getSyntheticWriteColumns + getFullSchema override + injector guard] ✅ + commit-bridge[Option(a) 路由连接器] 待。详 §10+§11（§11.6 = 本 session step3）。
- **[C4 待办，最重]** rewrite_data_files 执行半接 PluginDriven（连接器 body 端口 + 5 seam 泛化 + `IcebergProcedureOps` 注册去 throw）；`IcebergRewriteDataFilesAction:173,196` 翻闸即 CCE。
- **[C5 FLIP，不可逆]** `SPI_READY_TYPES`+iceberg / 删 `CatalogFactory case` / GSON compat / mvcc+partition-stats capability 核 / Show* parity。**C5 前须 C1–C4 全绿 + 用户二签。**

## 🆕 翻闸前置项（RFC 漏，已登记）
- **[GAP-A → C5]** 翻闸后 iceberg 表类掉出 `MaterializeProbeVisitor.SUPPORT_RELATION_TYPES`（精确类白名单）→ lazy-top-N 静默失效。修须 capability/engine 判别。
- **[GAP-B = C3b-core ③] ✅ 本 session 闭**：隐藏列注入——v3-lineage 经 carrier=A 连接器 emit（③-infra part2）；请求级 STRUCT row-id 经 fe-core `getFullSchema` override + injector guard（step3）。

**[pre-flip 行为偏差中央登记]**：P6.4=DV-045/046/047；P6.5=DV-048/049。C1/C2/C3a/C3b-pre/C3b-core(③-infra+step1+step2+step3) 无新 DV。**commit-bridge 预计可能引入 DV（post-flip 路由）——做到时评估**。

**⚠️ C5 才动 `SPI_READY_TYPES`**（`CatalogFactory:50-51`，现 = {jdbc,es,trino-connector,max_compute,paimon}）。

---

# 🗺️ 代码脚手架（iceberg read/sys/write，全 DONE/dormant + step6 实现锚点）

- **step6 commit-bridge 实现锚点（impl 期 re-grep 防漂移）**：
  - translator `visitPhysicalIcebergMergeSink:601-627`/`DeleteSink:588-598` dual-mode（post-flip→`PluginDrivenTableSink`，仿 `visitPhysicalConnectorTableSink:629-681`；slot-name loop `:615`）；`PluginDrivenInsertExecutor.beginTransaction:81`/`finalizeSink`/`getConnectorTransactionOrNull`；`RowLevelDmlCommand.run:98-100` SPI commit；连接器 `IcebergConnectorTransaction`（已全建）；`IcebergRewritableDeletePlanner.collect:52-86`（V3 DeleteFile，O2 迁连接器）；`ConnectorWriteHandle.getWriteOperation`（默认 INSERT，须透传 MERGE/DELETE）。
- **① handles（step1）/ ② getWritePartitioning+cast（step2）/ ③ row-id 注入（step3）/ partition_columns+parallel-write（C3b-pre）/ 写@branch+OVERWRITE（C3a）/ classifyColumn（C2）/ sys pin-feed（C1）**：dormant，详 git log + design §11.4/§11.5/§11.6。
- **step3 ③ 实现位（本 session）**：`ConnectorWritePlanProvider.getSyntheticWriteColumns`（SPI）；`IcebergWritePlanProvider.getSyntheticWriteColumns`+`buildRowIdColumn`（连接器）；`PluginDrivenExternalTable.getFullSchema`/`needInternalHiddenColumns`/`fetchSyntheticWriteColumns`（fe-core）；`IcebergNereidsUtils.isRowIdInjectionTarget`/`pluginConnectorSupportsRowLevelDml`+injector widen（fe-core legacy 豁免）。
- **连接器**：`IcebergConnector`/`IcebergConnectorMetadata`〔`supportsDelete:496`/`supportsMerge:501`/`buildTableSchema:227`〕/`IcebergConnectorTransaction`〔全 commit + `setRewrittenDeleteFilesByReferencedDataFile:271`〕/`IcebergWritePlanProvider`〔`planWrite`/`getWritePartitioning:195`/`getSyntheticWriteColumns`〕/`IcebergScanPlanProvider`〔`buildDeleteFiles:515`〕。
- **fe-core legacy 豁免（pre-flip 路 + P6.7 删）**：`IcebergTransaction` + `IcebergDeleteExecutor`/`MergeExecutor`/`InsertExecutor` + `IcebergNereidsUtils` + `IcebergRewritableDeletePlanner` + `IcebergExternalTable.getFullSchema:293`。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错）；连接器=`fe-connector-iceberg`、SPI=`fe-connector-api`、dispatch/guard/scan/sink/command/executor/transaction/PluginDrivenExternalTable=`fe-core`。验证读 surefire **XML**（python ET 聚合）。**全量前 `rm -f target/surefire-reports/TEST-*.xml`**。
- **iceberg 连接器全量 UT** 须 `package -Dassembly.skipAssembly=true`；单类/多类 `test -Dtest=A,B`。**fe-core -am 单类 ~2.5min（首次 -am 全链 ~5-7min）**。后台 task exit code 是 echo 的非 maven 的，读 surefire XML。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**脚本在仓根 `tools/`，非 `fe/tools/`**；exit 0=PASS）。**连接器须本地字面量复制 Doris 常量**（如 `__DORIS_ICEBERG_ROWID_COL__`）+ fe-core contract UT pin 值防漂移（C2 范式）。**classloader 隔离**：native `org.apache.iceberg.*` 跨连接器↔fe-core 必 CCE。测试：连接器**无 Mockito**（fail-loud fake + 真 `InMemoryCatalog`）；fe-core Mockito〔`CALLS_REAL_METHODS`+stub 包私 seam；jmockit `Deencapsulation` import = `org.apache.doris.common.jmockit.Deencapsulation`〕。live-e2e CI-gated，勿谎称跑过。
- **mutation-check（Rule 9/12）**：dormant 路 + assertFalse/doesNotThrow 易 trivially-pass → 必变异验真。批量变异脚本范式：cp 备份→exact-string mutate→`mvn test -Dtest=<class>`→查 surefire 期望测试 fail（KILLED）→restore（见 scratchpad/mutate.py）。**OR 能力须双 arm 各测一遍**（merge-only 漏测教训）。**⚠️变异须用「行为禁用形」（如 `… && false`）而非「删引用形」**：删唯一引用某 import/local 的表达式→checkstyle UnusedImport 阻断 build→无 surefire XML→脚本误判「SURVIVED」假阴（step-3 M3b-gate/ctx 踩过：删 `Util.showHiddenColumns()`/`ctx…` 触发；改 `&&false` 保引用复验 KILLED）。同理 surefire XML 缺失须与「测试通过」区分（脚本 failed() 返 None=inconclusive）。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·**仓根游离 `fe/IcebergScanPlanProvider.java`**〔review 重申勿提交，真文件在 `fe/fe-connector/...`〕·`plan-doc/reviews/P5-paimon-rereview3-*`〔非本线〕）。
- **本 session commit（step3）= 5 改 + 1 新测 + 2 doc**：`fe/fe-connector/fe-connector-api/.../write/ConnectorWritePlanProvider.java`、`fe/fe-connector/fe-connector-iceberg/.../IcebergWritePlanProvider.java`〔+测 `IcebergWritePlanProviderTest.java`〕、`fe/fe-core/.../datasource/PluginDrivenExternalTable.java`、`fe/fe-core/.../datasource/iceberg/IcebergNereidsUtils.java`、`fe/fe-core/.../test/datasource/ConnectorColumnConverterTest.java`、`fe/fe-core/.../test/datasource/iceberg/IcebergNereidsUtilsTest.java`；**新文件** `fe/fe-core/.../test/datasource/PluginDrivenExternalTableTest.java`；`plan-doc/HANDOFF.md` + `plan-doc/tasks/designs/P6.6-C3-ws-write-design.md`〔§11.6〕。**勿** `git add -A`。
- commit message：见 `git log` 上一条范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游剥离）。PR base = `branch-catalog-spi`，squash。

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。
- **⚠️ 推送状态**：P6.4 T01–T06+arg-move 已推 `origin`；**其后全部（含本 session）未 push**。**用户未要求 push**——留用户裁量。
- **P6.1–P6.5 = ✅ DONE**。**P6.6：C1 ✅ / C2 ✅ / C3a ✅ / C3b-pre ✅ / C3b-core step1·2·3 ✅ → step6 commit-bridge 待办**。
- iceberg **不在** `SPI_READY_TYPES`（pre-flip 零行为变更）。metastore 子线已 CLOSED（勿读）。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计/RFC 的依赖名/行号/不变式/测试前提可能过时或错** —— 动码前先 recon（grep+实证）再信文档。
- **🆕 用户铁律：fe-core 不得 `if (iceberg)` / `instanceof Iceberg*`（新 seam）** —— dual-mode helper 的 `instanceof IcebergExternalTable` pre-flip 分支落 legacy 豁免类合法；通用类（`PluginDrivenExternalTable`）须全经中立 SPI。
- **clean-room 对抗 review 偏好**：大改动多 agent 对抗（本 session `wf_cae63ac6-ca9` 3 lens+synthesis）+ 先 code 独立判断、后交叉核历史结论。
- **commit-bridge 是大活、最深、跨 session**：fresh session 全 budget 做；**O2（V3 DeleteFile 迁连接器）做到时专门 recon**；逐 sub-step green+mutation；**C5 前切忌动 `SPI_READY_TYPES`**。
- **上下文超 30% 即交接**。本 session = step 3（③ row-id 注入，27/16/6/60+14 全绿 + 11-mutation 全杀 + 对抗 review GO），在干净节点交接。**下个 session 起点 = step 6（commit-bridge）**：先读 design §10.2/§10.6（O1·O2）+ §11.4-bridge + 本 HANDOFF「impl-recon 决定性结论」；动码前 re-grep translator/executor/collectFor* 锚点；O2 专门 recon。
