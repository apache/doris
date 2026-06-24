# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4-T07 = dispatch rewire（EXECUTE → `getProcedureOps()`）**

**P6.4a（T01–T04）+ P6.4b（T05 规划半 + T06 事务半）已完成**（均未 push）。**T06 = 用户裁 Option 1**：只做 **① 连接器事务 `WriteOperation.REWRITE` 变体**（已实现，dormant），**②③④（rewrite 执行半翻闸接线）= R-B 推后给专门写路径 RFC**（见下「翻闸阻塞」DV-T06r-rb）。**下一 = T07 dispatch rewire**，仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **T07 内容**（设计 §7 / task 表 P6.4-T07）：把通用派发层（fe-core，引擎中立）的反向 `instanceof IcebergExternalTable` 改走 `catalog.getConnector().getProcedureOps()`，**dormant 保 legacy 分支**（pre-flip iceberg 是 `IcebergExternalTable` 非 PluginDriven → 走 legacy `IcebergExecuteActionFactory`，连接器 procedure 路 dormant 直到 P6.6）：
  - ① `ExecuteActionFactory.createAction`（`commands/execute/ExecuteActionFactory.java:56`）反向 `instanceof IcebergExternalTable` → 加 `table instanceof PluginDrivenExternalTable` 分支 → `((PluginDrivenExternalCatalog)catalog).getConnector().getProcedureOps().execute(session, tableHandle, name, props, where, partitions)`；null→throw（镜像 `PhysicalPlanTranslator:657-661`）；**保** legacy `IcebergExternalTable` 分支（P6.7 删）。
  - ② `ExecuteActionFactory.getSupportedActions`（**通用 overload** `:77`，**无 live caller** → 先 reroute 当 pathfinder）→ `getProcedureOps().getSupportedProcedures()`。注：inner `IcebergExecuteActionFactory.getSupportedActions()`（无参，`:93`）**有** live caller，留。
  - ③ 引擎保 `PrivPredicate.ALTER` + `BaseExecuteAction` 单行 `CommonResultSet` 包装 + `ExecuteActionCommand:153-166` `logRefreshTable`（已核 flip-safe，`PluginDrivenExternalTable` 是 `ExternalTable`，editlog 键不变）。
  - **结果适配**：连接器返 `ConnectorProcedureResult{schema, rows}`（已建，T02）→ 引擎包成单行 `CommonResultSet`（`BaseExecuteAction:106-108` 硬 `Preconditions.checkState(columnCount==row.size())` 单行不变式）。
  - **WHERE lower**：`ExecuteActionCommand` 持 nereids `Optional<Expression>`→引擎侧经 P6.3 O5-2 `ExprToConnectorExpressionConverter`/`NereidsToConnectorExpressionConverter` lower 成 `ConnectorPredicate` 后传入（连接器不 lower nereids；仅 `rewrite_data_files` 用，其余 8 个拒非 null）。**但 rewrite 走 R-B 留 legacy**，故 T07 dispatch 只覆盖 8 pure-SDK，WHERE 透传可暂置空/拒。
- **rewrite（②③④）不在 T07**：`rewrite_data_files` 翻闸接线 = R-B 专门写路径 RFC（见「翻闸阻塞」）。T07 dispatch 仅 8 pure-SDK procedure。
- **关键先读**（subagent 总结大文件，playbook §3.1）：fe-core `nereids/.../commands/execute/{ExecuteActionFactory, ExecuteAction, BaseExecuteAction}` + `commands/ExecuteActionCommand`〔`logRefreshTable`/priv/`CommonResultSet`〕 + 连接器 `connector.iceberg.procedure.IcebergProcedureOps`〔已建 dispatch + `getSupportedProcedures`〕 + SPI `connector.api.procedure.{ConnectorProcedureOps, ConnectorProcedureResult}` + `Connector.getProcedureOps()`。
- **起步必读**：设计 [`designs/P6.4-T01-procedure-spi-design.md`](./tasks/designs/P6.4-T01-procedure-spi-design.md)（§2 架构总览 dispatch + §7 old→new + flip 账本）+ task 表 [`§P6.4 拆解 + T01..T06 实现记录`](./tasks/P6-iceberg-migration.md) + [D-062]。
- **节奏**（playbook §5.1 / 7.3）：T07 涉 fe-core dispatch（用 Mockito：PluginDriven 表 + fake connector 走 `getProcedureOps()` 路；legacy `IcebergExternalTable` 路 byte-parity oracle 不回归）+ 连接器侧无 Mockito。`getSupportedActions` 通用 overload 无 live caller → pathfinder。T08 = parity 审计 + **DV-T05r-where / DV-T06r-{rb,scanpool,zone,rollback} 批量中央登记**、T09 收口。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 进行中 / P6.5 未做）。翻闸前必修下述 BLOCKER：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[DV-T06r-rb] 🔴 NEW（写路径，T06 实证）= `rewrite_data_files` 执行半翻闸接线（R-B，推后专门写路径 RFC）**。**recon 证伪设计 §5 / D-062 的 R-A 前提「连接器从 pinned snapshot+WHERE 重规划」**：① 连接器 scan SPI（`ConnectorScanPlanProvider.planScan` + `IcebergTableHandle` snapshot-pin）只能按 snapshot/谓词/分区收窄，**无法表达 legacy bin-pack 的「分区内任意文件子集」** → 重规划 **over-scan → 重写组外文件 → 破坏 rewrite 正确性**（非仅成本）；忠实保 file-level 分组须**新增中立 scan-范围 SPI**（违 P6.2「禁 SDK-vending / 0 新 SPI」+ 原设计前提）。② `FileScanTask` 侧信道（`RewriteGroupTask:117`→`IcebergScanNode.getFileScanTasksFromContext:498`）翻闸后走 `PluginDrivenScanNode` **端到端死**（grep 实证零引用）。③ **SPI 模块边界**：`fe-core` 只依赖 `fe-connector-api/-spi`，连接器 `RewriteDataGroup`（裹 iceberg `FileScanTask`/`DataFile`）**不能**跨进 fe-core 执行半。④ multi-sink-per-txn 生命周期（一 begin、N 组 INSERT-SELECT 不重 begin/commit、一 commit）须重设计。⑤ bind：`BindSink.bind(UnboundIcebergTableSink):1057` 对 PluginDriven 抛错 → 须改绑 `UnboundConnectorTableSink`→`visitPhysicalConnectorTableSink`；`RewriteGroupTask:175` `instanceof IcebergRewriteExecutor` + executor 选择 `instanceof PhysicalIcebergTableSink`（翻闸后须连接器 sink）；`RewriteDataFileExecutor:61` `(IcebergTransaction)` 下转 → 经通用 `PluginDrivenTransactionManager` 取连接器 REWRITE txn〔T06 ① 已建〕+ `setTxnId` 喂 commit-fragment。**用户裁 Option 1（D-062「超预算→R-B」预设被实证触发）**：T06 只做 ① 事务半（dormant，已做），②③④ 推后。

**[P6.4 翻闸阻塞预登记（dispatch 侧）]**：T07 dispatch（PluginDriven→`getProcedureOps()`）须在 P6.6 与 legacy 分支切换同步（dormant 直到翻闸）。

**[pre-flip 行为偏差（T08 批量中央登记 DV，非翻闸阻塞）]**：(T04) 8 snapshot mutator commit 现裹 `executeAuthenticated`（legacy 缺）/ cache 失效搬 dispatch 级 + 短路时多一次幂等失效 / `executeAction` 加 `ConnectorSession` 参；**(T05) DV-T05r-where** = `rewrite_data_files` WHERE 走 P6.3 conflict-mode 通路（用户签字 Option A）⇒ 不可转节点静默丢（legacy fail-loud 抛）=rewrite 变宽/极端重写全表 + conflict-matrix 收窄；**(T06) DV-T06r-{scanpool,zone,rollback}** = `commitRewriteTxn` 丢 legacy `scanManifestsWith(threadPool)`（perf-only，对齐 append 路）/ rewrite-added 文件分区值经 session-TZ 解析（复用 INSERT `convertToWriterResult(...,zone)` = 既有 DV-T04-f 路新触发，timestamp-分区表 benign）/ `rollback()` 不清 `filesToDelete`/`filesToAdd`（单 txn/语句生命周期下中性）。常见 WHERE/路径零差异、休眠至 P6.6。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，fe-connector-iceberg UT 389/0/1 + fe-core 30/0）。
- **P6.4 = 🟢 进行中**：T01 ✅（recon+设计+三签字 [D-062]）；T02 ✅（`ConnectorProcedureOps` SPI 骨架）；T03 ✅（base+factory+dispatch 骨架 + arg 框架移 `fe-foundation`）；T04 ✅（8 pure-SDK 体 + `RewriteManifestExecutor`，iceberg 444）；T05 ✅（`rewrite_data_files` 规划半 → `connector.iceberg.rewrite` 3 类，iceberg 467）；**T06 🟢（用户裁 Option 1）= ① 连接器事务 `WriteOperation.REWRITE` 变体已实现（dormant，净 0 新 verb，iceberg 475/0/1 + api 37/0），②③④ rewrite 执行半翻闸接线 = R-B 推后专门写路径 RFC（DV-T06r-rb）**；T07–T09 未做。**七 commit 待 push**（T01/T02/T03 + arg-framework fe-foundation 化 + T04 + T05 + T06）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:51` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T06（① 事务半 + ②③④ R-B），1 commit，未 push

**T06**（用户裁 **Option 1**：5-reader 全量 recon → AskUserQuestion → ① now + ②③④ R-B；dormant）：
- **① 事务半（已做，`IcebergConnectorTransaction`）**：`WriteOperation.REWRITE` 变体——新枚举值（api，6 项，`ConnectorWriteHandleTest` guard 同步）+ `filesToDelete`/`filesToAdd`/`startingSnapshotId(-1L)` 状态 + `applyBeginGuards` REWRITE 分支（捕获 `startingSnapshotId`，无 branch/`baseSnapshotId`，不走 fmt≥2/branch-resolution）+ `updateRewriteFiles(List<DataFile>)`（synchronized 累积，package-visible——fe-core 不能传 iceberg `DataFile`，未来连接器侧 coordinator 喂）+ `commit()` 折 legacy `finishRewrite`→`buildPendingOperation` 加 `case REWRITE: commitRewriteTxn()`（`convertCommitDataToFilesToAdd` 复用 INSERT `convertToWriterResult` → 空-skip → `newRewrite().validateFromSnapshot(startingSnapshotId).deleteFile(old)·addFile(new).commit()`，裹既有 `executeAuthenticated`）+ count/size 访问器。**净 0 新事务 verb**（commit-fragment 通道 P6.3 已统一）。dormant（`planWrite` 无 REWRITE case→default-throw，翻闸后 R-B RFC 接线）。
- **🔴 关键 recon 发现 = ②③④ R-B 的依据**（见上「翻闸阻塞 DV-T06r-rb」）：设计 §5「pinned snapshot+WHERE 重规划」被代码证伪（over-scan→破正确性 + SPI 模块边界 + 侧信道翻闸后死 + multi-sink-per-txn）。
- **🟡 诚实测试覆盖限制（mutation-check 实证，Rule 12）**：注掉 `commitRewriteTxn` 的 `validateFromSnapshot(startingSnapshotId)` 单跑并发-delete OCC 测**仍 GREEN**（冲突由 iceberg RewriteFiles 从 txn begin-时基快照固有校验抛，**不由显式行隔离**）→ 该测验「rewrite 冲突 fail-loud 行为」（有价值）但**不 pin** 显式 OCC 行；已据 mutation 结果**诚实修正测试名+注释，不 overclaim**；显式行忠实 legacy 港、跨-refresh 独立价值离线不可分辨 → **P6.6 docker/并发门**。
- **验证**（`-Dmaven.build.cache.enabled=false` + clean surefire + 核对 mtime）：fe-connector-iceberg **475/0/0/1**（467→+8 rewrite 测）；fe-connector-api **37/0/0/0**（enum guard +REWRITE）；`BUILD SUCCESS`；checkstyle 0；`check-connector-imports.sh` exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；**0 BE / 0 fe-core / 0 pom 改**（仅 api enum + iceberg 事务 + 两测）。
- **faithfulness 对抗 `wf_2efb10dc-1a2`**（5 finder + refute-by-default skeptic + completeness critic）= **4 raw → 0 confirmed**。critic 2 scope-确认（非 bug，T08 登记）：DV-T06r-zone / DV-T06r-rollback；另 DV-T06r-scanpool。全部 DV-T06r-* → T08 批量中央登记（同 DV-T05r-where 体例）。
- **文档同步**：task 表（T06 行 🟢 + 实现记录）/ PROGRESS（header + P6 行 475 + Option 1 + DV-T06r-rb）/ connectors/iceberg.md（当前状态 475 + 完成度 ~82% + T06 progress-log）/ decisions-log（**无新 D**，Option 1 = R-B 系 D-062「超预算→R-B」预设触发，记 task record）/ deviations-log（**无新中央 DV**，DV-T06r-{rb,scanpool,zone,rollback} 前向引用 → T08 批量登记，同 DV-T05r-where）。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`〔scan + **conflict** 双模式〕/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`〔**T06 加 `WriteOperation.REWRITE` 变体：`updateRewriteFiles`/`commitRewriteTxn`/`convertCommitDataToFilesToAdd`/`startingSnapshotId` 捕获/count·size 访问器，dormant**〕/`IcebergWriterHelper`/`IcebergWriteContext`；procedure `IcebergProcedureOps`〔dispatch + `runInAuthScope` + post-commit invalidate + session 透 `executeAction`〕 + `action/{BaseIcebergAction, IcebergExecuteActionFactory, 8 pure-SDK actions, RewriteManifestExecutor}`；rewrite `rewrite/{RewriteDataFilePlanner〔规划半，T05〕, RewriteDataGroup, RewriteResult}`。**arg 框架** `NamedArguments`/`ArgumentParsers`/`ArgumentParser` 在 **`fe-foundation`**（`org.apache.doris.foundation.util`）。
- **procedure SPI（已建，`fe-connector-api`）**：`procedure/{ConnectorProcedureOps, ConnectorProcedureResult}` + `Connector.getProcedureOps()` default-null。`handle/WriteOperation` 含 **REWRITE**（T06）。
- **T07 待改**（fe-core，dispatch rewire）：`nereids/.../commands/execute/ExecuteActionFactory`〔`createAction:56` + `getSupportedActions:77` 加 PluginDriven→`getProcedureOps()` 分支，dormant，保 legacy〕；`commands/ExecuteActionCommand`〔解析 `getProcedureOps()` null→throw，引擎保 priv+`CommonResultSet`+`logRefreshTable`〕。
- **rewrite ②③④（R-B，专门写路径 RFC，非 T07）**：fe-core `RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteTableCommand`/`IcebergRewriteExecutor` 留 fe-core；翻闸接线见「翻闸阻塞 DV-T06r-rb」。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/action/`（`BaseIcebergAction`+9 action+`IcebergExecuteActionFactory`）+ `rewrite/`（6 文件）+ 通用 dispatch `nereids/.../commands/{ExecuteActionCommand,execute/{ExecuteAction,BaseExecuteAction,ExecuteActionFactory}}`。**现 iceberg EXECUTE/rewrite 仍走 legacy**（连接器 procedure/rewrite dormant：`IcebergExternalTable` 非 PluginDriven，直到 P6.6）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`${revision}` 假错**）；offline 加 `-o`；procedure-SPI art = `fe-connector-api`、连接器 = `fe-connector-iceberg`、dispatch rewire = `fe-core`。checkstyle 在 `validate` phase（编译前）跑。验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）+ `BUILD SUCCESS`。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现，勿轻信 stale 计数。**⚠️ 单方法跑会留单测 XML 污染聚合计数**——全量验证前 `rm -f target/surefire-reports/TEST-*.xml` 再 clean 跑。**⚠️ surefire txt 字段位**：用正则提全字段，勿按 `[:,]` split 错位。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。fe-core 用 `test` 即可。**全量 `package` 偶超 2min**（assembly 相）——验证可单跑测类 + 读 XML，或给足超时（≥420s）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`）。测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog；共享 fixture `RecordingIcebergCatalogOps`/`RecordingConnectorContext`/`action/ActionTestTables`；delete/rewrite-bearing 用真 v2 `newRowDelta`/`newRewrite`+`FileMetadata.deleteFileBuilder`，见 `IcebergConnectorTransactionTest` rewrite 节 / `rewrite/RewriteDataFilePlannerTest`）；fe-core command/datasource 侧 UT 用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- **mutation-check（验证测试 pin 了意图，Rule 9/12）**：dormant 写路径里「测试是否真 pin 某行」非显然——临时删该行、单跑该测、确认转红、再恢复。T06 实证 OCC 测**不** pin `validateFromSnapshot`（被 iceberg 固有校验掩盖）→ 据实修正注释不 overclaim。
- cwd 跨 Bash 持久；一律绝对路径（Bash heredoc `cd` 会改 cwd → 后续相对路径失效）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。
- message `[refactor](catalog) P6.4 iceberg: <subj>`（纯文档用 `[doc]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。**T06 实证**：设计 §5「pinned snapshot+WHERE 重规划=忠实」对 rewrite **不成立**（over-scan 破正确性 + SPI 模块边界 fe-core 够不到连接器 `RewriteDataGroup`），是真实设计-vs-代码冲突 → 须 surface 给用户裁（Option 1/2/3），勿盲从设计措辞。
- **大文件（`rewrite/*`/`IcebergTransaction`/legacy action/`RewriteGroupTask`/`BindSink` 等）用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新（非累积）。**DV 中央登记延后到 Tn8 批量**（前向引用 DV-Tnnr-* 在 code + task record），同 P6.3-T08 / DV-T05r-where 体例。
- **faithfulness 对抗 workflow 范式**（T06 = `wf_2efb10dc-1a2`，5 finder + refute-by-default skeptic + completeness critic）：每发现独立 skeptic（默认 isReal=false，须引双文件原文），critic 查漏报类别。**T06 经验**：critic 的「能否 pin 某行」质疑值得用 mutation-check **实测**（而非静态推断）——实测推翻了 critic 与我的静态假设，得诚实结论；refuted 为 test-coverage 观察时仍可补测/修注释，**绝不 overclaim**（Rule 12）。
