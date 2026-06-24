# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4-T02 = `ConnectorProcedureOps` SPI 骨架（dormant）**

**P6.4-T01 已完成**（recon + 设计 + 用户三签字 [D-062]，纯文档 0 产品码，待 commit）。**下一 = T02**（SPI 骨架），仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **T02 内容**（设计 §9 / task 表 P6.4-T02）：新 `fe-connector-api` 子包 `connector.api.procedure.{ConnectorProcedureOps, ConnectorProcedureResult}`；`Connector.java:50`（`getWritePlanProvider()` 之后）加 `default getProcedureOps() { return null; }`；`IcebergConnector` 惰性 override（镜像 `getWritePlanProvider`@`IcebergConnector.java:194` = `new IcebergProcedureOps(properties, new CatalogBackedIcebergCatalogOps(getOrCreateCatalog()), context)`，T02 可先返空/throw impl 占位）。**验 jdbc/es/maxcompute/paimon/trino 0 影响**（全继承 null 默认）。connector-api UT + checkstyle 0 + import-gate 净 + grep iceberg 不在 `SPI_READY_TYPES`。
- **SPI 形状（S-1 扁平，已签 [D-062]）**：`ConnectorProcedureResult execute(ConnectorSession, ConnectorTableHandle, String name, Map<String,String> props, ConnectorPredicate where, List<String> partitions)` + `List<String> getSupportedProcedures()`。`ConnectorProcedureResult = {List<ConnectorColumn> resultSchema, List<List<String>> rows}`。`executionMode` **不进 S-1 接口**（P6.4b 接线时增量加 `getExecutionMode(name)` 默认 COORDINATOR_LOCAL）。类型均现存：`ConnectorSession`/`ConnectorTableHandle`(api/handle)/`ConnectorPredicate`(api/pushdown)/`ConnectorColumn`(api)。
- **起步必读**：设计 [`designs/P6.4-T01-procedure-spi-design.md`](./tasks/designs/P6.4-T01-procedure-spi-design.md)（§3 SPI / §9 TODO）+ recon [`research/p6.4-iceberg-procedures-recon.md`](./research/p6.4-iceberg-procedures-recon.md) + task 表 [`tasks/P6 §P6.4 拆解`](./tasks/P6-iceberg-migration.md) + [D-062]。镜像 P6.3 `ConnectorWritePlanProvider` 加法（`Connector.getWritePlanProvider` default-null 范式）。
- **节奏**（playbook §5.1 / 7.3）：逐 task TDD（无 Mockito + InMemoryCatalog）→ 文档同步五步 → commit + **覆盖式** HANDOFF。T02→T03(port base/factory)→T04(8 pure-SDK)→T07a(dispatch rewire) = **P6.4a**；T05/T06/T07b = **P6.4b**（rewrite_data_files）；T08 parity 审计、T09 收口。

## P6.4 三签字（[D-062]，本 session 用户 AskUserQuestion ×2）

- **Q1 = R-A 分相位**：P6.4a 先发 8 pure-SDK；P6.4b `rewrite_data_files`（长杆=分布式 INSERT-SELECT 写）混合切——规划半进连接器、事务半 `WriteOperation.REWRITE` 变体（**净 0 新事务 verb**，commit 通道 P6.3 已统一）、scan 从 pinned snapshot+WHERE 重规划（侧信道 `IcebergScanNode:498` 翻闸后端到端死，**非** P6.2 carrier 类比）、bind 改 `UnboundConnectorTableSink`、执行半（`StmtExecutor`/`TransientTaskManager`/nereids）留 fe-core。超预算→R-B 回退（rewrite 整体留 fe-core 登记有界 DV，但 bind 路仍须改造以备翻闸）。
- **Q2 = S-1 扁平 `execute()`**（非 S-2 注册表/非 Trino CALL）：引擎保 `PrivPredicate.ALTER`+`CommonResultSet` 包装+`logRefreshTable`（已核 flip-safe），连接器拥 procedure 体。Doris `ALTER TABLE EXECUTE` 唯对应 Trino `TableProcedureMetadata`（表级）→ 保 Doris 扁平 `ExecuteAction` 模型。
- **§4 = 4-A 连接器自包含 arg 校验**（**import-gate 冲突更正**：`NamedArguments`/`ArgumentParsers` 在 `org.apache.doris.common`，连接器禁 import → 原"引擎保 NamedArguments"不成立 → per-arg 校验落连接器，逐字 port error 串、TZ 用 P6.2 `IcebergTimeUtils` alias-map，**T08 byte-parity UT 硬门**兜；翻闸后 fe-core 0 iceberg-arg 知识=干净切）。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 进行中 / P6.5 未做）。翻闸前必修下述 BLOCKER：

**[DV-038]**（读路径）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径）= DV-038 同主题新面。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` → iceberg DELETE/MERGE 经通用 sink 走通前须先长出。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`/branch-INSERT thread-through/REST vended overlay/O5-2 `getConnectorTransactionOrNull()`→null 休眠/FILE_BROKER 地址。

**[P6.4 新增翻闸阻塞预登记]**：①T07 dispatch（PluginDriven→`getProcedureOps()`）须在 P6.6 与 legacy 分支切换同步；②`rewrite_data_files`（P6.4b）的 `WriteOperation.REWRITE` sink + scan 重规划 + bind 改 `UnboundConnectorTableSink` 须接线（dormant 直到翻闸）；③8 snapshot mutator 缺 `executeAuthenticated` 翻闸顺带修（T08 登记 DV）。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10）。**P6.2 = ✅ DONE**（T01–T11，UT 278/0/1）。**P6.3 = ✅ DONE**（T01–T09，fe-connector-iceberg UT 389/0/1 + fe-core 30/0）。
- **P6.4 = 🟢 进行中**：T01 ✅（recon + 设计 + 三签字 [D-062]，0 产品码，待 commit）；T02–T09 未做。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T01（recon + SPI 设计 + 用户三签字，待 commit，未 push）

纯文档 0 产品码。新 **`research/p6.4-iceberg-procedures-recon.md`**（10 节：Trino 参照 / legacy 9-action+rewrite 清册 / rewrite 引擎问题 / `ConnectorProcedureOps` SPI 三选项 / old→new 映射 / flip 账本 / task 拆解 / 开放决策）+ **`designs/P6.4-T01-procedure-spi-design.md`**（11 节：目标 / 架构 / S-1 SPI / §4 arg 校验冲突 / rewrite_data_files P6.4b / 不变式 / 映射 / 测试 / TODO / 风险 / 签字）。recon workflow `wf_cb757c7c-708`（10 reader + 对抗 completeness critic），critic **3 处源码核实更正**已并入：①instanceof 计数 = **3 instanceof + 11 downcast**（非初稿"14 instanceof"，grep 实证）；②`rewrite_data_files` commit 半已 P6.3 统一 → `WriteOperation.REWRITE` 变体而**非 4 新 verb**（实证 `IcebergConnectorTransaction.java:114/163/219`）；③`FileScanTask` 侧信道**非** P6.2 carrier 类比（live SDK 对象 FE 侧 vs thrift POJO BE 侧）+ 消费者 `IcebergScanNode:498` 翻闸后端到端死。**文档同步**：decisions-log [D-062]（三签字）/ master plan §P6.4 task 拆解（T01–T09）+ 状态 / PROGRESS（header+phase-row+connector-row）/ connectors/iceberg.md（T02 code 落地时更新）/ deviations-log（无新 DV，设计 §10 预登记，T08 批量登记，镜像 P6.2-T11/P6.3-T08）。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`/`IcebergWriterHelper`/`IcebergWriteContext`。**P6.4 待建**：`procedure/IcebergProcedureOps` + `action/{BaseIcebergAction,IcebergExecuteActionFactory,8 pure-SDK actions,RewriteManifestExecutor}` + `rewrite/{RewriteDataFilePlanner core,RewriteDataGroup,RewriteResult}`（P6.4b）。
- **procedure SPI（待建，`fe-connector-api`）**：`procedure/{ConnectorProcedureOps,ConnectorProcedureResult}` + `Connector.getProcedureOps()` default-null。镜像 `write/ConnectorWritePlanProvider` 加法 + `Connector.getWritePlanProvider`@`Connector.java:50`。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/action/`（`BaseIcebergAction`+9 action+`IcebergExecuteActionFactory`）+ `rewrite/`（6 文件，`RewriteDataFilePlanner`/`RewriteDataFileExecutor`/`RewriteGroupTask`/etc）+ 通用 dispatch `nereids/.../commands/{ExecuteActionCommand,execute/{ExecuteAction,BaseExecuteAction,ExecuteActionFactory}}`（**ExecuteAction/BaseExecuteAction/ExecuteActionFactory/NamedArguments 留 fe-core**）。**现 iceberg EXECUTE 仍走 legacy**（连接器 procedure dormant：`IcebergExternalTable` 非 PluginDriven，直到 P6.6）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`${revision}` 假错**）；offline 加 `-o`；procedure-SPI art = `fe-connector-api`、连接器 = `fe-connector-iceberg`、dispatch rewire = `fe-core`。checkstyle 在 `validate` phase（编译前）跑。验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）+ `BUILD SUCCESS`。
- **⚠️ build-cache 坑**：验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现，勿轻信 stale 计数。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath）。fe-core 用 `test` 即可。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（禁 `org.apache.doris.{catalog,common,datasource,qe,analysis,nereids,planner}` —— **故 `NamedArguments`/`ArgumentParsers` 连接器用不了，§4=4-A 自包含校验**）。测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog）；fe-core command/datasource 侧 UT 用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。
- message `[doc](catalog) P6.4 iceberg: <subj>`（T01 纯文档用 `[doc]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。本 session 实证：recon 初稿 instanceof 计数错（critic 抓到→已修）。
- **大文件（`IcebergTransaction`/`action/*`/`rewrite/*` 等）用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新（非累积）。
