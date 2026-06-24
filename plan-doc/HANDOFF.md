# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.4 = procedures（10 个 action + 新 `ConnectorProcedureOps` SPI E2）**

**P6.3 写路径已全完**（T01–T09 ✅，全未 push）。**下一阶段 = P6.4 procedures**，仍 behind gate（iceberg 不在 `SPI_READY_TYPES`，零行为变更直到 P6.6）。

- **P6.4 内容**（task 表 line 50/69/81/92）：迁 fe-core `datasource/iceberg/action/`（11：`BaseIcebergAction` + 9 action + `IcebergExecuteActionFactory`）+ `rewrite/`（6，含 legacy `RewriteFiles`/`updateRewriteFiles` 写半）→ 连接器；fe-core `ExecuteActionCommand` 走通用 dispatch；**新建 `ConnectorProcedureOps` SPI（E2，`listProcedures`/`callProcedure`）**——这是 P6.1 recon 标记的 **3 缺失 SPI 之一**（另两 = `ConnectorCredentials`〔P6.2 已用既有接缝复用解决〕、写路径 RFC〔P6.3 已做〕）。10 action = `RewriteDataFiles`/`ExpireSnapshots`/`RollbackToSnapshot`/`CherrypickSnapshot`/`FastForward`/`PublishChanges`/`RewriteManifests`/`RollbackToTimestamp`/`SetCurrentSnapshot`（+`rewrite_data_files`）经 dispatch 行为 parity。
- **⚠️ 前置（R3，risks.md）**：`ConnectorProcedureOps` 形态**先看 Trino Iceberg connector**（`/mnt/disk1/yy/git/trino`）再定——10 action 行为不齐风险；P6.4 起前须新建该 SPI。**新 SPI ⇒ 大概率须先 design-doc/mini-RFC + 用户签字**（参 P6.3 RFC 先行先例），勿直接写实现。
- **起步必读**：master plan `tasks/P6-iceberg-migration.md` §P6.4 行 + §「3 缺失 SPI」（line 34/81）+ recon `research/p6.1-iceberg-metadata-recon.md`；legacy `datasource/iceberg/action/` + `rewrite/`（大文件**用 subagent 总结**，playbook §3.1）。
- **节奏**（playbook §5.1 / 7.3）：code-grounded recon（Trino 形态 + legacy action/rewrite subagent 总结）→ `ConnectorProcedureOps` SPI design + 用户签字 → 逐 task 实现（TDD，无 Mockito fake + InMemoryCatalog）→ 文档同步五步 → commit + **覆盖式** HANDOFF。P6.4 后 = P6.5 sys-table + 元数据列。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 / P6.5 都还没做）。翻闸前必修下述 BLOCKER，否则 BE 崩 / DML 挂：

**[DV-038]**（读路径，P6.2 登记）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径，T08 登记）= **DV-038 同主题新面**。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink`（`PhysicalPlanTranslator:630-681`）缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge`——这两者仅 legacy `visitPhysicalIcebergDeleteSink`/`visitPhysicalIcebergMergeSink`（`:589-627`）有 → iceberg DELETE/MERGE 经通用 sink 走通前须先长出，否则同款 BE StructNode DCHECK（T07 有意不碰 legacy `:589-627`、也不把它物化进通用 sink）。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`（DV-T06-a）/ branch-INSERT thread-through / REST 对象存储 vended overlay（不接 403，DV-T07-vended）/ O5-2 `getConnectorTransactionOrNull()`→null 休眠（DV-T07c-o5seam）/ FILE_BROKER 地址。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10：连接器地基 + 读元数据 + 7-flavor 校验 + metastore 模块拆分）。
- **P6.2 = ✅ DONE**（T01–T11：scan + 谓词下推 + delete + COUNT 下推 + field-id 字典 + MVCC + cache + vended + parity 审计 + 收口）。UT 278/0/1。
- **P6.3 = ✅ DONE**（T01–T09：写框架统一·SPI 收口 + jdbc planWrite + `IcebergConnectorTransaction`〔骨架/op/WriterHelper/begin guards〕+ commit 校验套件·O5-2·V3 DV + sink 统一〔INSERT/OVERWRITE〕 + DELETE/MERGE sink 方言〔T07a〕+ O5-2 生产半〔T07b〕+ 通用 `RowLevelDmlCommand` 壳·注册表〔T07c〕+ parity 审计·DV-041..044 登记〔T08〕+ 收口汇总设计〔T09〕）。fe-connector-iceberg UT **389/0/1** + fe-core 30/0。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.3-T09（收口 = P6.3 DONE，待 commit，未 push）

纯文档 0 产品码（镜像 P6.2-T11）。新 **`designs/P6.3-T09-iceberg-write-summary-design.md`**（7 节）：①架构总览 + T01–T08 逐 task 索引（含各 commit `2c789a0257c`..`5db5ac1d087` + 对抗 wf 结论）；②**写路径 SPI 收口核对**（与 P6.2「净 0 新 SPI」**相反**——P6.3 有意 SPI 统一：删 `usesConnectorTransaction` 双模型 fork + insert-handle 面 + config-bag 三件套，收敛为单 `ConnectorTransaction` 写模型 + capability 派发）；③deviation 回指 DV-041..044；④P6.6 翻闸阻塞汇总；⑤验收门；⑥下一 = P6.4。**code-grounded 核实**（grep 实证非凭文档）：gate state + config-bag 三件套 grep 净 + 统一写 SPI 面逐一存在。**faithfulness 对抗验证 `wf_9234a18e-1d9`**（6 cluster verifier refute-by-default + 1 completeness critic）= **全 CONFIRMED**，唯 1 真错（双标）= §5 `:589-627` 误挂**通用** `visitPhysicalConnectorTableSink`（实测通用在 `:630-681`，`:589-627` 是 legacy delete+merge visitor）→ **已修**（deviations-log:115 本就只把 :589-627 挂 legacy 处，无需改）；critic cheap-check 证 UT 计数静态精确（iceberg 389 `@Test`、jdbc 190/mc 102(1skip)/paimon 318(1skip)、fe-core 11/19/14）。**文档同步五步全做**（task 表 P6.3✅+T09✅+记录 / PROGRESS §header+§phase+§connector+§log / connectors/iceberg.md / decisions-log 无新 D / deviations-log 无新 DV）。**0 BE / 0 pom**（git diff `84a00c56dea..HEAD` 仅 fe/、plan-doc/、regression-test/）。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/.../connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`〔含 conflictMode〕/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；**写（终态）** `IcebergWritePlanProvider`/`IcebergConnectorTransaction`/`IcebergWriterHelper`/`IcebergWriteContext`。
- **写路径壳（T07c，fe-core）**：`nereids/trees/plans/commands/` `RowLevelDmlCommand`/`RowLevelDmlOp`/`RowLevelDmlArgs`/`RowLevelDmlTransform`/`RowLevelDmlRegistry`/`IcebergRowLevelDmlTransform` + 委派目标 `Iceberg{Delete,Update,Merge}Command`（合成原地，P6.7 删）；O5-2 生产半 `datasource/WriteConstraintExtractor`+`NereidsToConnectorExpressionConverter`。
- **写 SPI（终态，`fe-connector-api`）**：`ConnectorTransaction`〔commit/rollback abstract + addCommitData/getUpdateCnt/applyWriteConstraint/profileLabel default〕/`NoOpConnectorTransaction`/`WriteOperation`/`ConnectorWriteHandle`〔getWriteOperation/getSortInfo default〕/`ConnectorWritePlanProvider`〔planWrite/appendExplainInfo〕/`ConnectorPredicate`/`ConnectorWriteSortColumn` + capability `SINK_REQUIRE_FULL_SCHEMA_ORDER`。
- **paimon 模板**（镜像源）：`fe/fe-connector/fe-connector-paimon/`。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/`（含 `IcebergTransaction`/`IcebergMetadataOps`/`action/`〔P6.4 迁〕/`rewrite/`〔P6.4 迁〕）+ legacy planner `Iceberg{Table,Delete,Merge}Sink` + executor 链。**现 iceberg 写仍走 legacy**（连接器写 dormant：`IcebergExternalTable` 非 `PluginDriven` + `visitPhysicalConnectorTableSink` 强转 `PluginDrivenExternalTable`〔`:636-637`〕故 iceberg 进不去，直到 P6.6）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`${revision}` 假错**）；offline 加 `-o`；写路径壳/O5-2/procedure-SPI art = `fe-core`、连接器 = `fe-connector-iceberg`。checkstyle 在 `validate` phase（编译前）跑。验证读 surefire **XML**（`<testsuite tests=.. failures=..>`）+ `BUILD SUCCESS`。
- **⚠️ build-cache 坑**：maven-build-cache 偶发静默服务 **stale surefire**。验证加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现，勿轻信 stale 计数。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath；plain `test` 报 `NoClassDefFoundError: HiveConf`）。fe-core 用 `test` 即可。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog）；fe-core command/datasource 侧 UT 用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `tasks/.*.swp`）。
- message `[doc](catalog) P6.3 iceberg: <subj>`（T09 纯文档用 `[doc]`）+ 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。本 session 实证：T09 草稿 §5 行号 `:589-627` 误挂通用 sink（实测 `:630-681`），对抗验证 wf 抓到→已修。
- **大文件（`IcebergTransaction`/`action/*`/`rewrite/*` 等）用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新（非累积）。
