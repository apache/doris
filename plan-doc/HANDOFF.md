# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4-T04 = port 8 个 pure-SDK procedure 体 → `connector.iceberg.action`**

**P6.4-T01（recon+设计+三签字 [D-062]）+ T02（SPI 骨架）+ T03（base+factory+arg 框架 port + dispatch 骨架）已完成**（均未 push）。**下一 = T04**，仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **T04 内容**（设计 §9 / task 表 P6.4-T04）：port 8 个 pure-SDK action（`rollback_to_snapshot`/`rollback_to_timestamp`/`set_current_snapshot`/`cherrypick_snapshot`/`fast_forward`/`publish_changes`/`expire_snapshots`/`rewrite_manifests`，**不含 `rewrite_data_files`=T05/T06**）→ `connector.iceberg.action`，各 `extends` T03 的 `BaseIcebergAction`，并把对应 `case` 接进 `IcebergExecuteActionFactory.createAction` switch（T03 现只有 default-throw）。**逐字保 parity**：TZ 路（`rollback_to_timestamp` 用 P6.2 已有连接器 `IcebergTimeUtils` alias-map 替 fe-core `TimeUtils.msTimeStringToLong`，CST→Asia/Shanghai）、`publish_changes` STRING result+字面 `"null"`、`fast_forward`(branch,to) 参序+无 guard commit 后读、各短路不对称、**全 error 串字节同**（自定义校验/body 失败抛 `DorisConnectorException`〔unchecked〕，message 字节同 legacy `AnalysisException`/`UserException` 串）。
- **🔑 T04 关键已就位（T03 已落，T04 无须改 base/factory/dispatch 签名）**：① body 的 SDK 调用链 + `.commit()` 已由 dispatch `IcebergProcedureOps.runInAuthScope` 裹在 `executeAuthenticated` 内（recon §7 auth 补，legacy 8 个 snapshot mutator 缺）——**body 自身不再调 auth**；② cache 失效 = **dispatch 级**加 `context.getMetaInvalidator()`（seam 已存在 default-NOOP，`ConnectorContext.java:105`），替 legacy fe-core-only `ExtMetaCacheMgr.invalidateTableCache`——body 不调；③ `executeAction(org.apache.iceberg.Table)` 收**已加载 SDK 表**（非 `(IcebergExternalTable) table).getIcebergTable()` downcast）；④ result schema 用 `ConnectorColumn`（非 `Column`，`ConnectorType.of("BIGINT")` 等）；⑤ arg 注册用 **fe-foundation** 的 `namedArguments`/`ArgumentParsers`（`org.apache.doris.foundation.util`，引擎与连接器共享同一份；body 自定义校验/error 失败抛 `DorisConnectorException` unchecked）。⇒ T04 body = legacy body **去 fe-core import + 上述 5 机械换型**，逻辑/SDK 调用链/error 串照搬。
- **关键先读**（subagent 总结大文件，playbook §3.1）：legacy 8 个 action 体（`datasource/iceberg/action/Iceberg*Action.java`）+ T03 连接器 `action/BaseIcebergAction`·`IcebergExecuteActionFactory`·`IcebergProcedureOps`（继承/接线点，已读则免）+ P6.2 连接器 `IcebergTimeUtils`（TZ alias-map）。`expire_snapshots` 最复杂（5 可选 arg + 自定义校验 + FE-local `FixedThreadPool` + 6×BIGINT result + `deleteWith` 回调计数）；`rollback_to_timestamp` 唯一 TZ 耦合；`publish_changes`/`fast_forward` 有 bug-for-bug 保留点（设计 §10）。
- **起步必读**：设计 [`designs/P6.4-T01-procedure-spi-design.md`](./tasks/designs/P6.4-T01-procedure-spi-design.md)（§3 SPI / §6 单行不变式·auth·flip-safe / §10 bug-for-bug 保留）+ recon [`research/p6.4-iceberg-procedures-recon.md`](./research/p6.4-iceberg-procedures-recon.md)（§3 9-action 清册矩阵 / §6 映射）+ task 表 [`§P6.4 拆解 + T03 实现记录`](./tasks/P6-iceberg-migration.md) + [D-062]。
- **节奏**（playbook §5.1 / 7.3）：逐 task TDD（无 Mockito + InMemoryCatalog；body 用真 `InMemoryCatalog` + `FakeIcebergTable`，断 SDK 调用链/result schema 列数==行 size/短路/error 串）→ 文档同步五步 → commit + **覆盖式** HANDOFF。T04(8 pure-SDK) + T07a(dispatch rewire) = **P6.4a 收尾**；T05/T06/T07b = **P6.4b**（`rewrite_data_files`）；T08 parity 审计（auth 补 DV 批量登记）、T09 收口。

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
- **P6.4 = 🟢 进行中**：T01 ✅（recon + 设计 + 三签字 [D-062]，0 产品码）；T02 ✅（`ConnectorProcedureOps` SPI 骨架 + dormant 占位，connector-api 37/0）；T03 ✅（base+factory port + `IcebergProcedureOps` dispatch 骨架 + arg 框架移 `fe-foundation` 共享，iceberg 401/0/1，faithfulness wf 4→0 confirmed）；T04–T09 未做。**四 commit 待 push**（T01/T02/T03 + arg-framework fe-foundation 化重构）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.4-T03（base+factory + dispatch 骨架）+ arg 框架 fe-foundation 化重构，2 commit，未 push

**T03**（连接器自包含 procedure 脚手架，dormant）：新包 `connector.iceberg.action`（4 产品文件）+ 改 `IcebergProcedureOps`。**0 fe-core / 0 BE / 0 pom**。
- **arg 框架（§4=4-A 逐字 port）**：`ArgumentParser`/`ArgumentParsers`/`NamedArguments` 从 `org.apache.doris.common` 整体搬入连接器（import-gate 禁 `common`）。唯一改 = `NamedArguments.validate` 抛 `DorisConnectorException`（unchecked）替 `AnalysisException`（去 `throws`）；**所有校验/parser error 串字节不变**（"Unknown argument: "/"Missing required argument: "/"Invalid value for argument '%s': %s. %s"/各 parser 串）——T08 byte-parity 兜。
- **`BaseIcebergAction`（独立基类，非 extends 被禁 `BaseExecuteAction`）**：折入 `BaseExecuteAction` 被消费机器，SPI 中立型〔`List<String> partitionNames`〔空=无〕/ `ConnectorPredicate where`〔null=无〕/ `List<ConnectorColumn> getResultSchema` / `executeAction(org.apache.iceberg.Table)`〕。`validate()`=`namedArguments.validate`+`validateIcebergAction()`（**无 priv**——引擎保 `PrivPredicate.ALTER`，D-062 §2）；`execute(Table)`=单行包装+`Preconditions.checkState(schema.size()==row.size())`（legacy 单行不变式，message 字节同）；4 partition/where 守卫 message 字节同；去无消费者 `getDescription`（grep 实证）。
- **`IcebergExecuteActionFactory`（去死 `table` 参）**：9 名常量+`getSupportedActions()`〔legacy 序〕+`createAction→BaseIcebergAction`。**T03 switch 只 faithful default-throw**（"Unsupported Iceberg procedure: X. Supported procedures: ..." 字节同，`DorisConnectorException` 替 `DdlException`）；9 case=T04/T05-T06。
- **`IcebergProcedureOps` 接线**：`getSupportedProcedures()`=`Arrays.asList(factory.getSupportedActions())`；`execute()`=dispatch 骨架（downcast handle→`createAction`〔拒 unknown〕→`validate`→**`runInAuthScope`**：load+body+commit 同一 `executeAuthenticated` 作用域〔recon §7 auth 补〕→ body `DorisConnectorException` verbatim 透出，引擎壳 T07 加前缀）。
- **验证**：5 新测类 23/0/0、fe-connector-iceberg 全模块 **412/0/1**（389→412）、checkstyle 0、import-gate exit 0、iceberg 仍**不在** `SPI_READY_TYPES`、0 BE/fe-core/pom 改。**faithfulness 对抗 `wf_009434eb-5a7`**（4 dim + per-finding refute-by-default verify + critic）= **4 raw→0 confirmed**；critic CRITIC-0〔HIGH：commit 须在 auth 内〕自查为真→已落 `runInAuthScope`，余 4 NIT。**auth 补 = pre-flip 行为偏差**→设计 §10/recon §7 **T08 批量登记 DV**（镜像 P6.2-T11/P6.3-T08，T03 不单列）。
- **文档同步五步**：task 表（T03 行 ✅ + 实现记录）/ PROGRESS（header+phase-row+connector-row + 活动日志）/ connectors/iceberg.md（当前状态+完成度）/ decisions-log（无新 D，D-062 已签）/ deviations-log（无新 DV，T08 批量）。

**arg 框架 fe-foundation 化（T03 后续重构，同 session，用户要求，2nd commit）**：把 `ArgumentParser`/`ArgumentParsers`/`NamedArguments` 从 fe-core `org.apache.doris.common` 提升到最底层 **`fe-foundation` `org.apache.doris.foundation.util`**，引擎与连接器**共享一份**、删 T03 复制进连接器的副本（这 3 个是基础工具类，复制造成重复）。
- **异常（用户 AskUserQuestion 选 A）**：fe-foundation 够不到 fe-common 的 `AnalysisException`（连接器也被 gate 禁 import 它）⇒ `NamedArguments.validate` 改抛 **unchecked `IllegalArgumentException`**（所有 error 串字节不变），两侧 catch 后 re-wrap：fe-core `BaseExecuteAction.validate`→`AnalysisException(msg)`〔保 legacy 错误类型+消息 parity，live 路径〕、连接器 `BaseIcebergAction.validate`→`DorisConnectorException(msg)`。`ArgumentParsers` 的 Guava `Preconditions.checkNotNull`→JDK `Objects.requireNonNull`（fe-foundation 无 Guava；同 NPE+消息）。
- **改动面**：fe-foundation +3 类 +2 测试（断 `IllegalArgumentException`/NPE-IAE，各 20/0）；fe-core −3 类 −2 测试 + `BaseExecuteAction`〔import+re-wrap〕+ 8 iceberg action〔`ArgumentParsers` import 改 foundation，alphabetical 重排；`RollbackToTimestamp` inline lambda parser 无改〕；连接器 −3 副本 −2 测试 + `BaseIcebergAction`〔import+re-wrap+javadoc〕+ `BaseIcebergActionTest`〔import〕+ **pom 加 `fe-foundation` 依赖**。
- **验收**：fe-foundation 20+20/0；fe-connector-iceberg **401/0/1**（cache 禁用+`skipCache` fresh，核对 mtime；412→401=389+12，删 2 测类 11 测）；fe-core test-compile 绿（含 checkstyle validate 相）；checkstyle 0；import-gate exit 0（`foundation` 非 gated）；iceberg 仍**不在** `SPI_READY_TYPES`；0 BE / 0 pom-dM 改。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`/`IcebergWriterHelper`/`IcebergWriteContext`；procedure `IcebergProcedureOps`（T02 占位 → T03 dispatch 骨架，含 `runInAuthScope`）+ `action/{BaseIcebergAction, IcebergExecuteActionFactory}`（**T03 已建**：base+factory 骨架）。**arg 框架** `NamedArguments`/`ArgumentParsers`/`ArgumentParser` 在 **`fe-foundation`**（`org.apache.doris.foundation.util`，引擎+连接器共享，T03 后续重构；连接器 pom 直接依赖 fe-foundation）。**P6.4 待建**：`action/{8 pure-SDK actions, RewriteManifestExecutor}`（T04，各 extends `BaseIcebergAction` + 接 factory case）+ `rewrite/{RewriteDataFilePlanner core, RewriteDataGroup, RewriteResult}`（P6.4b）。
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
