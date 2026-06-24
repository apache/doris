# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.3-T09 = 收口（T09 完成 = P6.3 DONE）**

**P6.3 写路径**：T01–T08 ✅ DONE（全未 push）；**下一 = T09 = P6.3 最后一个 task**。

- **T09 内容**（任务表 line 240）：写**汇总设计文档**（建议 `designs/P6.3-T09-iceberg-write-summary-design.md`，镜像 P6.2 的 `P6-T11-iceberg-scan-summary-design.md`：①架构总览 + T01–T08 逐 task 索引；②写路径 capability/SPI 收口核对；③deviation 中央注册回指 DV-041/042/043/044；④P6.6 翻闸阻塞汇总；⑤验收门状态）+ **文档同步五步**（task 表/PROGRESS/connectors/iceberg；decisions-log 如有 D；deviations-log 已 DV-041..044）+ **gate 核对**（iceberg 仍**不在** `SPI_READY_TYPES`）。**T09 ≈ 纯文档 0 产品码**（镜像 P6.2-T11）。
- **起步必读**：RFC `06-iceberg-write-path-rfc.md` + 总纲设计 `designs/P6.3-T07-rowlevel-dml-unification-design.md` + **本 session 的 `designs/P6.3-T08-write-parity-audit-design.md`** + 任务表 `tasks/P6-iceberg-migration.md` §P6.3。
- **节奏**（playbook §5.1）：code-grounded recon（大文件用 subagent 总结）→ 汇总设计 → 文档同步五步 → commit + **覆盖式** HANDOFF。T09 后 = **P6.4 procedures**（`ConnectorProcedureOps` E2，10 action），仍 behind gate。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

翻闸（P6.6 加 iceberg 进 `SPI_READY_TYPES`）是**全有或全无**（`CatalogFactory:104-113`），须等 P6.1–P6.5 全实现完（P6.4 procedure / P6.5 sys-table 都还没做）。翻闸前必修下述 BLOCKER，否则 BE 崩 / DML 挂：

**[DV-038]**（读路径，P6.2 登记）BE `iceberg_reader.cpp` field-id 路径 StructNode `DCHECK`→整 BE 崩。2 面：①GLOBAL_ROWID top-N 合成列被通用 `classifyColumn` 归 REGULAR（修在共享 fe-core，但 `paimon_reader.cpp` 无对应处理器→盲改破 paimon top-N）；②`getColumnHandles` 无 snapshot 重载（rename+time-travel，**共享 fe-core seam 仍潜伏 PAIMON**）。

**[DV-041]**（写路径，本 session T08 登记）= **DV-038 同主题新面**。主阻塞 **DV-T07-materialize**：通用 `visitPhysicalConnectorTableSink` 缺合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge`（仅 legacy `visitPhysicalIcebergMergeSink` 有）→ iceberg DELETE/MERGE 经通用 sink 走通前须先长出，否则同款 BE StructNode DCHECK（T07 有意不碰 `PhysicalPlanTranslator:589-627`）。+ 休眠-至-翻闸激活集（P6.6 必接线）：写分布 `getRequirePhysicalProperties`（DV-T06-a）/ branch-INSERT thread-through / REST 对象存储 vended overlay（不接 403，DV-T07-vended）/ O5-2 `getConnectorTransactionOrNull()`→null 休眠（DV-T07c-o5seam）/ FILE_BROKER 地址。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（现 scan/write/procedure/sys-table 未齐，翻闸即全断）。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10：连接器地基 + 读元数据 + 7-flavor 校验 + metastore 模块拆分）。
- **P6.2 = ✅ DONE**（T01–T11：scan + 谓词下推 + delete + COUNT 下推 + field-id 字典 + MVCC + cache + vended + parity 审计 + 收口）。fe-connector-iceberg UT 278/0/1。
- **P6.3 = 进行中**：T01–T08 ✅；**T09 收口剩余（= P6.3 DONE）**。最近 commit = 本 session T08（待 commit）；T07c = `a61cd9262b9`。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.3-T08（写路径 parity 审计 + deviation 中央登记，待 commit，未 push）

10 维对抗审计 workflow `wf_c1067212-ab8`（132 agents，每 gap 3-lens refute-by-default skeptic + 2 critic）= **40 报告 → 20 confirmed / 20 refuted → 11 交付**（8 新测 + 3 强化断言）。gap-fill：连接器 `IcebergConnectorTransactionTest` +4（分区 identity 冲突 filter 窄化〔同/异分区并发〕/ 非-identity 禁窄化 / snapshot 隔离跳 validate / PUFFIN DV dedup by path#offset#size）、`IcebergWritePlanProviderTest` +2（dataLocation 级联 fallback / ORC·codec 矩阵）+ WP-001 强化（partitionSpecsJson 字节断）；fe-core `WriteConstraintExtractorTest` +1（per-conjunct drop）、`NereidsToConnectorExpressionConverterTest` +1（OR all-or-nothing）、`IcebergDDLAndDMLPlanTest` 2 强化（DELETE/UPDATE operation-literal 值断）。**有意不补（Rule 12 附理由，见设计 §「不补」）**：MERGE branch-label / 执行器路由（REDUNDANT-WITH-ORACLE）/ combine 两侧 AND（in-proc SDK 非 BE-observable）/ null→isNull。**deviation 中央登记 DV-041/042/043/044**（4 条镜像 P6.2-T11 DV-038/039/040 分层，用户签字 4 条方案）。验收：fe-connector-iceberg **389/0/1**、fe-core 3 测类绿（WriteConstraintExtractor 11/0、NereidsToConnectorExpressionConverter 19/0、IcebergDDLAndDMLPlanTest 14/0）、checkstyle 0、import-gate 0、**0 SPI / 0 BE / 0 fe-core 产品 / 0 pom 改**（仅 5 测试文件）、iceberg 不在 `SPI_READY_TYPES`。**mutation 实证**（Rule 9）：去 `buildDeleteFileDedupKey` PUFFIN 的 `#offset#size` → PUFFIN dedup 测变红（2→1），已 revert（prod 0 diff）。详 = 设计 `designs/P6.3-T08-write-parity-audit-design.md`。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`〔含 conflictMode〕/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`/`IcebergWriterHelper`/`IcebergWriteContext`。
- **写路径壳（T07c，fe-core）**：`nereids/trees/plans/commands/` `RowLevelDmlCommand`/`RowLevelDmlOp`/`RowLevelDmlArgs`/`RowLevelDmlTransform`/`RowLevelDmlRegistry`/`IcebergRowLevelDmlTransform` + 委派目标 `Iceberg{Delete,Update,Merge}Command`（合成原地，P6.7 删）；O5-2 生产半 `datasource/WriteConstraintExtractor`+`NereidsToConnectorExpressionConverter`。
- **paimon 模板**（镜像源）：`fe/fe-connector/fe-connector-paimon/`。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`/`IcebergUtils`/`IcebergTransaction`/`IcebergConflictDetectionFilterUtils`/`IcebergNereidsUtils` + 7 flavor catalog）+ legacy sink/executor 链。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`${revision}` 假错**）；offline 加 `-o`；写路径壳/O5-2 art = `fe-core`、连接器 = `fe-connector-iceberg`。checkstyle 在 `validate` phase（编译前）跑。验证读 surefire **XML**（`<testsuite tests=.. failures=..>`，本 session 实测 `.txt` 有时不生成）+ `BUILD SUCCESS`。
- **⚠️ build-cache 坑**：maven-build-cache 偶发 unzip 损坏会**静默服务 stale surefire**（本 session 实遇：writeplan 测显 20 而非 22）。验证时加 **`-Dmaven.build.cache.enabled=false`** 并核对 surefire mtime / 新测方法名实际出现，勿轻信 stale 计数。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath；plain `test` 报 `NoClassDefFoundError: HiveConf`）。fe-core 用 `test` 即可。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog）；fe-core command/datasource 侧 UT 用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 持久；一律绝对路径。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`）。
- message `[refactor](catalog) P6.3 iceberg: <subj>` + 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。
- **大文件（`IcebergUtils`/`IcebergMergeCommand`/`IcebergTransaction` 等）用 subagent 总结**（playbook §3.1）。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新（非累积）。
