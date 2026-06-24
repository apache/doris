# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4-T06 = `rewrite_data_files` 写路径耦合（长杆）**

**P6.4a（T01 设计三签字 + T02 SPI 骨架 + T03 base/factory/dispatch + T04 8 pure-SDK 体）+ P6.4b 开篇 T05（规划半）已完成**（均未 push）。**下一 = T06**，仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。**T06 = P6.4 最高风险长杆**，超预算 → **R-B 回退**（`rewrite_data_files` 整体留 fe-core 登记有界 DV，但 bind 路仍须改造以备翻闸）。

- **T06 内容**（设计 §5 / task 表 P6.4-T06）：① **事务半** = `IcebergConnectorTransaction` 加 `WriteOperation.REWRITE` 变体（**净 0 新事务 verb**——commit-fragment 通道 P6.3 已统一）：`commit()` 内新增 REWRITE 分支做 `transaction.newRewrite().validateFromSnapshot(startingSnapshotId).commit()`（OCC），`startingSnapshotId` 于 `beginWrite` 捕获（移植 legacy `IcebergTransaction.beginRewrite:175`/`updateRewriteFiles:137`/`finishRewrite:207`/`:253-255`/`getFilesToAddCount:610` 进既有生命周期）。② **scan-task 获取**（critic 更正：翻闸后侧信道死）：legacy `StatementContext.setIcebergRewriteFileScanTasks`→`IcebergScanNode.getFileScanTasksFromContext:491-505` 在 `PluginDrivenScanNode` 路径**端到端死**。**忠实方案 = 连接器从 pinned snapshot-id + WHERE 重规划**（**不**钉 `FileScanTask` 成新 carrier=重引 SDK-vending，违 P6.2 禁）。③ **bind flip-trap**：`BindSink.bind(UnboundIcebergTableSink):1057` 对 `PluginDrivenExternalTable` 抛错 → rewrite INSERT-SELECT 改绑 `UnboundConnectorTableSink` → P6.3 `visitPhysicalConnectorTableSink`。④ **执行半留 fe-core**（`RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteTableCommand`/`IcebergRewriteExecutor`——`StmtExecutor`/`TransientTaskManager`/nereids，不可离；镜像 P6.3 `RowLevelDmlCommand` plan 合成留 fe-core），经 SPI 调连接器的 **T05 已就位的规划半** + 事务半。
- **T05/T06 边界**：**T05 已做规划半**（连接器 `connector.iceberg.rewrite.{RewriteDataFilePlanner core, RewriteDataGroup, RewriteResult}`，SDK-only，离线 InMemoryCatalog 测、dormant 未接 dispatch）；T06 = 事务半 + bind + scan 重规划 + 把执行半（fe-core）接到连接器规划/事务半。`IcebergRewriteDataFilesAction` body（legacy fe-core action）= T06 不 port〔执行半〕但读以识别规划/执行二分点；factory `rewrite_data_files` case 现 default-throw，T06 接线。
- **关键先读**（subagent 总结大文件，playbook §3.1）：legacy `datasource/iceberg/rewrite/{RewriteDataFileExecutor, RewriteGroupTask}`（执行半，T06 边界）+ `IcebergTransaction.java`（rewrite verbs `beginRewrite/updateRewriteFiles/finishRewrite/getFilesToAddCount/newRewrite().validateFromSnapshot`）+ 连接器 `IcebergConnectorTransaction`（P6.3，加 REWRITE 变体落点）+ `IcebergScanNode.getFileScanTasksFromContext` + `BindSink.bind(UnboundIcebergTableSink)`〔翻闸 trap〕+ `nereids/.../commands/insert/RewriteTableCommand`/`IcebergRewriteExecutor`（执行半 nereids）。**T05 已建** `connector.iceberg.rewrite.RewriteDataFilePlanner`（消费 `ConnectorPredicate` WHERE + conflict-mode 转换；T06 给它供 pinned-snapshot 重规划入口）。
- **起步必读**：设计 [`designs/P6.4-T01-procedure-spi-design.md`](./tasks/designs/P6.4-T01-procedure-spi-design.md)（**§5 = `rewrite_data_files` 二分 + scan-task 翻闸后侧信道死 + bind flip-trap + R-B 回退**）+ recon [`research/p6.4-iceberg-procedures-recon.md`](./research/p6.4-iceberg-procedures-recon.md)（§4 rewrite 二分）+ task 表 [`§P6.4 拆解 + T01..T05 实现记录`](./tasks/P6-iceberg-migration.md) + [D-062]。
- **节奏**（playbook §5.1 / 7.3）：T06 涉 fe-core（执行半接线 + bind），fe-core 侧 UT 用 Mockito、连接器侧 InMemoryCatalog 无 Mockito。**T06 = P6.4b 收尾**；T07a（dispatch rewire）可 pathfinder（无 live caller）；T08 parity 审计（auth 补 + cache 搬家 + session-签名 + **DV-T05r-where** 批量中央登记）、T09 收口。**超预算（T06 写路径耦合长杆）→ R-B 回退**：`rewrite_data_files` 整体留 fe-core 登记有界 DV，但 bind 路仍须改造以备翻闸（设计 §5）。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 进行中 / P6.5 未做）。翻闸前必修下述 BLOCKER：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[P6.4 翻闸阻塞预登记]**：①T07 dispatch（PluginDriven→`getProcedureOps()`）须在 P6.6 与 legacy 分支切换同步；②`rewrite_data_files`（**T06**）的 `WriteOperation.REWRITE` sink + scan 从 pinned snapshot 重规划〔**侧信道 `IcebergScanNode:498` 翻闸后端到端死**——忠实方案=连接器从 pinned snapshot-id + WHERE **重规划**，**不**钉 `FileScanTask` 成 carrier〕 + bind 改 `UnboundConnectorTableSink`〔`BindSink.bind:1057` 对 `PluginDrivenExternalTable` 抛错〕须接线（dormant 直到翻闸）；③**pre-flip 行为偏差**（T08 批量登记 DV，**非翻闸阻塞**）：(T04) 8 snapshot mutator commit 现裹 `executeAuthenticated`（legacy 缺）/ cache 失效搬 dispatch 级 + 短路时多一次幂等失效 / `executeAction` 加 `ConnectorSession` 参；**(T05) DV-T05r-where** = `rewrite_data_files` WHERE 走 P6.3 conflict-mode 通路（用户签字 Option A）⇒ 不可转节点静默丢（legacy fail-loud 抛）=rewrite 变宽/极端重写全表 + conflict-matrix 收窄跨列 OR/非-IsNull NOT/NE；常见 WHERE 零差异、休眠至 P6.6。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，fe-connector-iceberg UT 389/0/1 + fe-core 30/0）。
- **P6.4 = 🟢 进行中**：T01 ✅（recon+设计+三签字 [D-062]）；T02 ✅（`ConnectorProcedureOps` SPI 骨架）；T03 ✅（base+factory+dispatch 骨架 + arg 框架移 `fe-foundation`）；T04 ✅（8 pure-SDK 体 + `RewriteManifestExecutor`，iceberg 444）；**T05 ✅（`rewrite_data_files` 规划半 → `connector.iceberg.rewrite` 3 类，iceberg 467/0/1，faithfulness wf 8→0 confirmed/0 critic gaps）**；T06–T09 未做。**六 commit 待 push**（T01/T02/T03 + arg-framework fe-foundation 化 + T04 + T05）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T05（`rewrite_data_files` 规划半 → `connector.iceberg.rewrite`），1 commit，未 push

**T05**（规划半，SDK-only，机械可移，dormant）：新包 `connector.iceberg.rewrite` 3 类 + 3 测类。**0 fe-core / 0 BE / 0 pom**；factory `rewrite_data_files` 仍 default-throw（未接 dispatch，直到 T06）。
- **3 类港**：① `RewriteResult` + `RewriteDataGroup` = **逐字 POJO**（仅 package + javadoc；无 fe-core import）。② `RewriteDataFilePlanner` = bin-pack/分区分组/file+group filter 逐字保真（`BinPacking.ListPacker(maxFileGroupSizeBytes,1,false)`、全 `>=`/`>` 算子、`tooHighDeleteRatio` file-scoped sum+min+ratio、`groupTasksByPartition` specId-兼容判+empty-struct 回退、`useSnapshot`/`ignoreResiduals`/`planFiles` 序）+ **3 处有意换型** + 1 bug-for-bug。
- **3 处有意换型**：① checked `UserException`→unchecked `DorisConnectorException`（去 `throws`；catch-wrap 串 `"Failed to plan file scan tasks: "` **字节同**）；② `Parameters.whereCondition` nereids `Optional<Expression>`→中立 `ConnectorPredicate`；③ WHERE 转换 `IcebergNereidsUtils.convertNereidsToIcebergExpression`→`new IcebergPredicateConverter(schema, sessionZone, **true** /*conflict-mode*/).convert(pred.getExpression())`（每合取独立 `scan.filter`，镜像 `IcebergScanPlanProvider.planScan`）+ 线程 `ZoneId sessionZone`。bug-for-bug：死 `outputSpecId` ctor 参保留忽略。
- **🟡 DV-T05r-where（用户签字 Option A，T08 批量中央登记）**：conflict-mode 通路对 legacy `IcebergNereidsUtils` 两处有意发散——① **不可转节点静默丢**（legacy **抛**）→ scan 变宽 → 重写比 WHERE 多的文件（极端=全表），不报错；② conflict-matrix 收窄跨列 OR/非-IsNull NOT/NE。**关键认知（设计 §5 论据更正）**：「safe over-approximation」对**扫描下推**成立（BE 残差再过滤）但对 **rewrite 不成立**（planner `scan.filter()` 直接即重写集，无下游再过滤），与 O5-2「变宽=更保守」安全性**反号**。常见 WHERE（等值/范围/IN/BETWEEN）两路一致零差异；发散仅罕见 WHERE（函数/NE/跨列 OR/NOT）。**用户裁 Option A**（复用 conflict-mode + 登记 DV）vs Option B（忠实 fail-loud 新转换器）——理由：贴合已签设计「走 P6.3 通路」、最少代码、休眠至 P6.6、常见路无差异。
- **验证**（`-Dmaven.build.cache.enabled=false`）：3 测类 23 测〔`RewriteDataFilePlannerTest` 17：rewriteAll-分组/bin-pack-不超 cap/分区不混/current-snapshot/空表无 NPE/file filter 选超范围·跳范围内/组 filter 三 OR-arm 隔离/边界 `==` 不选/WHERE 分区裁剪/**跨列 OR 静默丢=DV 过宽**/**BETWEEN conflict-mode 裁剪=钉 Option A**〔scan-mode 会丢→红〕/**delete 阈值·比率门**〔真 v2 `newRowDelta` equality/position delete fixture〕；`RewriteResultTest` 4；`RewriteDataGroupTest` 2〕。**iceberg 全模块 467/0/0/1**（444→467）；checkstyle 0；import-gate exit 0；iceberg 仍**不在** `SPI_READY_TYPES`；**0 BE/fe-core/pom 改**。
- **faithfulness 对抗 `wf_40ae73fd-3ef`**（5 finder + 每发现独立 refute-by-default skeptic + completeness critic）= **8 raw → 0 confirmed / 8 refuted + 0 critic gaps**。8 refuted 全 test-coverage 观察（产品码逐字一致非行为发散）；最强 1 项（delete-filter 覆盖 legacy 有·港丢）**当场补**真 delete fixture 复原 `testDeleteFileThreshold`/`testDeleteRatioThreshold` parity；其余（lookback/largestBinFirst 不可区分、incompatible-specId 回退、ignoreResiduals 效果在执行半、catch-block 串难离线触发）= legacy 测亦无 / 离线难测，留注。
- **文档同步五步**：task 表（T05 行 ✅ + 实现记录）/ PROGRESS（header 467 + Option A + DV-T05r-where）/ connectors/iceberg.md（当前状态 467 + 完成度 ~80% + T05 dated 详情）/ decisions-log（无新 D，Option A 记 task record，同 D-062 体例）/ deviations-log（**DV-T05r-where → T08 批量中央登记**，同 T04/P6.3-T05 体例，code 内已前向引用）。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`〔scan + **conflict** 双模式〕/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`〔`msTimeStringToLong` + `resolveSessionZone` public〕；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`/`IcebergWriterHelper`/`IcebergWriteContext`；procedure `IcebergProcedureOps`〔dispatch + `runInAuthScope` + post-commit invalidate + session 透 `executeAction`〕 + `action/{BaseIcebergAction, IcebergExecuteActionFactory, 8 pure-SDK actions, RewriteManifestExecutor}`；**rewrite `rewrite/{RewriteDataFilePlanner〔规划半，T05〕, RewriteDataGroup, RewriteResult}`**。**arg 框架** `NamedArguments`/`ArgumentParsers`/`ArgumentParser` 在 **`fe-foundation`**（`org.apache.doris.foundation.util`）。**T06 待建/改**：`IcebergConnectorTransaction` 加 `WriteOperation.REWRITE` 变体 + `RewriteDataFilePlanner` 的 pinned-snapshot 重规划入口 + fe-core 侧执行半接线（`RewriteDataFileExecutor`/`RewriteGroupTask` 调连接器规划/事务半）+ bind 改 `UnboundConnectorTableSink` + factory `rewrite_data_files` case 接线。
- **procedure SPI（已建，`fe-connector-api`）**：`procedure/{ConnectorProcedureOps, ConnectorProcedureResult}` + `Connector.getProcedureOps()` default-null。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/action/`（`BaseIcebergAction`+9 action+`IcebergExecuteActionFactory`）+ `rewrite/`（6 文件，`RewriteDataFilePlanner`/`RewriteDataFileExecutor`/`RewriteGroupTask`/`RewriteDataGroup`/`RewriteResult`/`RewriteManifestExecutor`）+ 通用 dispatch `nereids/.../commands/{ExecuteActionCommand,execute/{ExecuteAction,BaseExecuteAction,ExecuteActionFactory}}`。**现 iceberg EXECUTE/rewrite 仍走 legacy**（连接器 procedure/rewrite dormant：`IcebergExternalTable` 非 PluginDriven，直到 P6.6）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`${revision}` 假错**）；offline 加 `-o`；procedure-SPI art = `fe-connector-api`、连接器 = `fe-connector-iceberg`、dispatch/执行半 rewire = `fe-core`。checkstyle 在 `validate` phase（编译前）跑。验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）+ `BUILD SUCCESS`。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现，勿轻信 stale 计数。**⚠️ surefire txt 字段位**：`Tests run: N, Failures: F, Errors: E, Skipped: S` —— 用正则提全字段，勿按 `[:,]` split 错位（failures/errors 会被读成 label→恒 0 假绿）。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。fe-core 用 `test` 即可。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}`）。测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog；共享 fixture `RecordingIcebergCatalogOps`/`RecordingConnectorContext`/`action/ActionTestTables`；delete-bearing 用真 v2 `newRowDelta`+`FileMetadata.deleteFileBuilder`，见 `rewrite/RewriteDataFilePlannerTest` / `IcebergScanPlanProviderTest`）；fe-core command/datasource 侧 UT 用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 持久；一律绝对路径（Bash heredoc `cd` 会改 cwd → 后续相对路径失效）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。
- message `[refactor](catalog) P6.4 iceberg: <subj>`（纯文档用 `[doc]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号/不变式可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。**T05 实证**：设计 §5「WHERE 走 P6.3 通路（safe over-approximation）」的安全论据对 **rewrite 不成立**（无下游再过滤），是真实安全发散 → 须 surface 给用户裁（Option A/B），勿盲从设计措辞。
- **大文件（`rewrite/*`/`IcebergTransaction`/legacy action 等）用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新（非累积）。
- **faithfulness 对抗 workflow 范式**（T05 = `wf_40ae73fd-3ef`，5 finder + refute-by-default skeptic + completeness critic）：每发现独立 skeptic（默认 isReal=false，须引双文件原文），critic 查漏报类别；镜像 T04/P6.2-T11/P6.3-T08。**T05 经验**：refuted 为「test-coverage 观察」而非「行为发散」时，仍值得当场补测（delete-filter 覆盖复原），而非仅记 refuted。
