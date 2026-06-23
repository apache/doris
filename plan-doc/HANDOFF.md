# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（2026-06-22 合入 #64446/#64653/#64655）——[`metastore-storage-refactor/`](./metastore-storage-refactor/) 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 下一个 session 的任务 = **P6.3-T08**（parity 审计 + deviation 中央登记）

**P6.3 写路径**：T01–T07〔a/b/c〕 ✅ DONE（全未 push）；**下一 = T08** → **T09 收口 = P6.3 DONE**。

- **T08 第一步**（精确）：读各 P6.3 task 设计 `tasks/designs/P6.3-T0n-*.md` 的 §6 / §4.5.8 收集全部 DV → 核对 `deviations-log.md` 现有条目（DV-038/039/040 已在）→ 新增 **DV-04x / DV-T07-* / DV-T07b-* / DV-T07c-***（各 task DONE 段已列）→ parity 覆盖审计（镜像 P6.2-T10/T11 收口流程）。
- **起步必读**：RFC `06-iceberg-write-path-rfc.md` + 总纲设计 `tasks/designs/P6.3-T07-rowlevel-dml-unification-design.md`（§4.5 = T07c 实现锁定）+ 任务表 `tasks/P6-iceberg-migration.md` §P6.3。
- **每 task 节奏**（playbook §5.1）：code-grounded recon（大文件用 subagent 总结）→ TDD RED→GREEN → 对抗 parity workflow（每发现独立 refute-by-default skeptic）→ UT/checkstyle/import-gate 绿 + 断 assembled Thrift/校验套件 vs legacy → **文档同步五步** → commit + **覆盖式** HANDOFF。

---

# 🔴🔴 开放问题 — P6.6 翻闸阻塞（须翻闸前 holistic 修）

**[DV-038]** BE `iceberg_reader.cpp` field-id 路径 `children_column_exists` StructNode `DCHECK` → 整 BE 崩（CI #969249 类）。1 主题 / 2 面，跨 paimon：
- **面 1（GLOBAL_ROWID）**：top-N 延迟物化合成列被通用 `PluginDrivenScanNode.classifyColumn` 归 REGULAR → 不在 field-id 字典 → DCHECK。修在共享 fe-core `classifyColumn`（GLOBAL_ROWID→SYNTHESIZED），但 `paimon_reader.cpp` 无对应处理器 → 盲改破 paimon top-N。
- **面 2（`getColumnHandles` 无 snapshot 重载）**：rename + time-travel pin 从 current schema 建字典丢被重命名 slot field-id → 同 DCHECK。**iceberg 侧 T07 Option A 已闭合**，但**共享 fe-core seam 仍潜伏 PAIMON**（snapshot-id time-travel + rename）。

**[DV-T07-materialize]**（写路径翻闸）：通用 `visitPhysicalConnectorTableSink` 无合成列 `setMaterializedColumnName`（`$operation`/`$row_id`）+ `DistributionSpecMerge` 分布；iceberg DELETE/MERGE 经通用 sink 走通前须先长出（T07 不碰 `PhysicalPlanTranslator:589-627`）。

**⚠️ P6.1–P6.5 切忌动 `SPI_READY_TYPES`**（翻闸全有或全无 `CatalogFactory:104-113`，只在 P6.6；现 scan/write/procedure/sys-table 未齐，翻闸即全断）。翻闸前必修上述两面 + 写路径阻塞，否则 iceberg top-N / paimon·iceberg time-travel+rename / iceberg DML 挂 BE。

---

# 📦 阶段状态

- **工作分支 = `catalog-spi-10-iceberg`**（off `branch-catalog-spi` @ `e5959e1b53d`，PR base = `branch-catalog-spi`，squash 合并）。**所有 commit 均未 push。**
- **P6.1 = ✅ DONE**（T01–T10：连接器地基 + 读元数据 + 7-flavor 校验 + metastore 模块拆分）。
- **P6.2 = ✅ DONE**（T01–T11：scan + 谓词下推 + delete files + COUNT 下推 + field-id 字典 + MVCC/time-travel + 连接器内 cache + vended/静态凭据 + parity 审计 + 收口）。fe-connector-iceberg UT 278/0/1。
- **P6.3 = 进行中**：T01–T07〔a/b/c〕 ✅；**T08 / T09 剩余**。最近 commit `a61cd9262b9`（T07c）。
- iceberg **不在** `SPI_READY_TYPES`（`CatalogFactory:50` = {jdbc,es,trino-connector,max_compute,paimon}），仍走 switch-case（`:137 case "iceberg"`）。
- metastore 子线 **已 CLOSED**（勿读）。

## 本 session 完成 = P6.3-T07c（commit `a61cd9262b9`，未 push）

通用 `RowLevelDmlCommand` 壳 + `RowLevelDmlTransform` 注册表 + 6 instanceof 派发重接 + O5-2 dormant 接线。checkpoint 2 决策：**D1 完整 shell + 委派合成**（合成留 `IcebergXCommand` 原地经 transform 委派，仅放宽 3 private 合成法到包级；单 live 循环；legacy loop transitional-dead → P6.7 删）；**D2 O5-2 现接 dormant**（新 `BaseExternalTableInsertExecutor.getConnectorTransactionOrNull()`，iceberg 走 legacy txn→null→不可达直到 P6.6）。验收：fe-core 目标测 **104/0/0**（oracle `IcebergDDLAndDMLPlanTest` 14/0 byte-parity 铁证 + 新 `IcebergRowLevelDmlTransformTest` 7/0）、checkstyle 0、import-gate 0、**0 BE/thrift/pom**、iceberg 不在 SPI_READY_TYPES。对抗 parity `wf_a80f8edb-bed` = 24 raw / 0 REAL / 24 refuted。详 = `git show a61cd9262b9` + 设计 §4.5 + memory `catalog-spi-p6t3-t07c-done`。**deviation 待 T08 登记**：DV-T07c-{conflict-order, resolve, dormant-loop, o5seam}。

---

# 🗺️ 代码脚手架（iceberg）

- **连接器（终态）**：`fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/`：基础 `IcebergConnector`/`Provider`/`ConnectorMetadata`/`ConnectorProperties`/`TableHandle`/`TypeMapping`/`CatalogFactory`/`CatalogOps` + `dlf/`；scan `IcebergScanPlanProvider`/`IcebergScanRange`/`IcebergPredicateConverter`/`IcebergPartitionUtils`/`IcebergSchemaUtils`/`IcebergColumnHandle`/`IcebergTimeUtils`；cache `IcebergLatestSnapshotCache`/`IcebergManifestCache`；写 `IcebergWritePlanProvider`/`IcebergConnectorTransaction`/`IcebergWriterHelper`/`IcebergWriteContext`。
- **写路径壳（T07c，fe-core）**：`nereids/trees/plans/commands/` 新 `RowLevelDmlCommand`/`RowLevelDmlOp`/`RowLevelDmlArgs`/`RowLevelDmlTransform`/`RowLevelDmlRegistry`/`IcebergRowLevelDmlTransform` + 委派目标 `Iceberg{Delete,Update,Merge}Command`（合成原地，P6.7 删）。
- **paimon 模板**（镜像源）：`fe/fe-connector/fe-connector-paimon/`。
- **legacy 对照（STILL-CONSUMED，P6.7 删）**：fe-core `datasource/iceberg/`（`IcebergMetadataOps`/`IcebergUtils`/`IcebergTransaction`/`IcebergConflictDetectionFilterUtils`/`IcebergNereidsUtils` + 7 flavor catalog）+ legacy sink/executor 链。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false`（**漏 `-am`→`${revision}` 假错**）；offline 加 `-o`；写路径壳 art = `fe-core`、连接器 = `fe-connector-iceberg`。checkstyle 在 `validate` phase（编译前）跑。验证读 surefire XML + `BUILD SUCCESS`。
- **iceberg/paimon 连接器 UT** 须 `package -Dassembly.skipAssembly=true`（HiveConf 仅 package 相在 test-classpath；plain `test` 报 `NoClassDefFoundError: HiveConf`）。
- plugin-zip 实查：`mvn -pl :fe-connector-iceberg -am package -DskipTests` → `unzip -l target/*.zip | grep lib/`。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（仅 `org.apache.doris.{thrift,connector,extension,filesystem}`）。
- 测试：连接器侧偏好无 Mockito（fail-loud fake + InMemoryCatalog）；fe-core command/datasource 侧 UT 用 Mockito。live-e2e CI-gated（docker），勿谎称跑过。
- cwd 跨 Bash 持久；一律绝对路径（`cd` 破相对路径 + 触权限提示）。

# ⚠️ Commit 须知（任何 `git add` 前必读）

- **path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 Aliyun key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`）。
- message `[refactor](catalog) P6.3 iceberg: <subj>` + 根因/解法/测试 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`（squash 入上游时剥离）。
- PR base = `branch-catalog-spi`，squash 合并。

# 🧠 给下一个 agent 的 meta

- **删除/parity 前必 grep 全调用方 + 区分 DEAD vs STILL-CONSUMED**（P5-T29 教训）。
- **HANDOFF/设计的依赖名/行号可能过时** —— 动 pom/代码前先 recon（grep + unzip 实证）再信文档。
- **大文件（`IcebergUtils`/`IcebergMergeCommand`/`IcebergTransaction` 等）用 subagent 总结**（playbook §3.1），勿主线整读。
- **文档同步五步**（playbook §5.1，每 task 缺一不可）：`tasks/Pn-*.md` 状态 + `PROGRESS.md` + `connectors/iceberg.md` + decisions-log（如有 D）+ deviations-log（如有 DV）；HANDOFF **覆盖式**更新（非累积）。
