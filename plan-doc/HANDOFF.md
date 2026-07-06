# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.3 原子批 INC-1..INC-5 全部完成 + 全绿（写链 + 读链都齐了）。下一步 = FINAL：INC-1..INC-5 一次原子 feature commit（path-whitelist，待用户点头）。全批仍 UNCOMMITTED 在工作树里（跨 session WIP 载体，勿 commit / 勿 revert / 勿 `git checkout`）。本轮已 commit 的只有 HANDOFF + 设计文档 + INC-5 port-map（文档，不含功能码）。**

> **本轮做了什么（INC-5：读侧 ACID 生产端 + 插件读事务生命周期，dormant）**：新建 `HiveAcidUtil`（把 HEAD `AcidUtil.getAcidState` 的**纯目录名解析 + 快照挑选算法**逐条移到插件：`parseBase`/`parseDelta`/`isValidBase`/`isValidMetaDataFile` + FullAcid/InsertOnly 文件过滤 + best-base/working-delta 选择环；fe-core `FileCacheValue`/`LocationPath`/`AcidInfo`/`HivePartition` 全落地为原生 Hadoop `FileSystem`/`FileStatus`；`globList(...,"/*")`→`fs.listStatus`[文件+目录]、`globList(bareDir)`→`fs.listStatus`+仅留文件；读 `HmsAcidConstants` 两键）+ `HiveReadTransaction`/`HiveReadTransactionManager`（移植 `HiveTransaction`/`HiveTransactionMgr`，用 INC-1 的 `openTxn`/`acquireSharedLock`/`getValidWriteIds`/`commitTxn`；`TableNameInfo`→(db,table) 串）+ `HiveScanPlanProvider.planAcidScan` 下潜 + `.acidInfo("dir|file1,file2")` 生产端接线（含 full-acid 非 ORC fail-loud）+ `HiveTableHandle.isTransactional/isFullAcid` 从 `getTableParameters()` 派生（D8）+ `HiveConnector` 持有（dormant）读事务管理器。

> **本轮验证（独立重跑，不信自述）**：`fe-connector-hive` 全模块 `clean test` **104 测试 0 失败 0 错误 BUILD SUCCESS（原 87 + 新 17：`HiveAcidUtilTest`=9、`HiveReadTransactionTest`=3、`HiveTableHandleAcidTest`=5，既有 `HiveScanRangeAcidTest`=3 保绿）**；`checkstyle:check` 0 违规；`check-connector-imports.sh` 净（新码只碰 Hadoop/hive-common/thrift/`HmsAcidConstants`，无 fe-core）。**净室对抗复审（`wf-inc5-review.js`，6 维 × find→对抗 verify，13 agent 0 error）：1 条 CONFIRMED（已修）+ 1 条自查先修 + 测试补强**：(a) **CONFIRMED** insert-only 事务表被错标 `transactional_hive`（HEAD 只在 `isFullAcid` 才挂 `acidInfo`；insert-only 存的是平文件 → 错走 BE merge-on-read）→ 修为 `planAcidScan` 仅 full-acid 才挂 `acidInfo`（insert-only 仍按快照选文件、但标平 `hive`）；(b) 复审前**自查**发现读事务管理器按 queryId 跨不同表复用一个事务 → 第二张表拿到第一张的快照，改为**每表各开各事务**（`register` 非复用，对齐 HEAD）；(c) 按复审 test-quality 发现补了 4 个 descent 测试（可见性 txn 过滤 / `isValidBase` 甄别 / not-enough-history）+ insert_only 大小写。其余 5 条 refuted 为 benign/有意偏差。**遗留（交割 cutover 集成门）**：`planScan` 级 insert-only 标记门禁的回归测试（需 live session/txn/FS 管线，设计 §7 把集成测试推到 cutover）。

> **⚠️ 工作树未提交的 INC-1..INC-4 代码**（`git status` 会显示；原子批模型，设计文档 §8——**若工作树被清就照各 port-map 重建**）：
> - INC-1（`fe-connector-hms`）：`HmsClient`/`ThriftHmsClient`/`HmsWriteConverter`/`pom.xml` 改 + `HmsAcidConstants`/`HmsCommonStatistics`/`HmsPartitionStatistics`/`HmsPartitionWithStatistics`/`HmsPartitionInfo` 新 + 2 test。
> - INC-2（`fe-connector-hive`）：`HiveWriteUtils.java` + `NameMapping.java`（+ 2 test）。
> - **INC-3（`fe-connector-hive`）**：**新** `HiveWriteContext.java`(89) + `HiveConnectorTransaction.java`(1704，含 dup-key 修复；本轮 INC-4 又改了 guard→`rejectTransactionalWrite`) + `HiveConnectorTransactionTest.java`(14 例) + `FakeConnectorContext.java`(64)；**改** `HiveConnectorMetadata.java`(`beginTransaction` override) + `pom.xml` + `HiveWriteUtils.java`。
> - **INC-4（`fe-connector-hive` + 通用层）**：**新** `HiveWritePlanProvider.java`(362) + `HiveSinkHelper.java`(246) + `HiveWritePlanProviderTest.java`(716,20 例) + `RecordingConnectorContext.java`(98)；**改** `HiveConnector.java`(+getWritePlanProvider) + `HiveConnectorTransaction.java`(guard 放宽) + `HiveConnectorTransactionTest.java`(+insert-only 拒绝)。**通用层（DEC-1，本批唯一 fe-core 改动）**：`fe-connector-api` `ConnectorWritePlanProvider`/`Connector`/`ConnectorContractValidator` + `fe-core` `PluginDrivenExternalTable`/`PhysicalConnectorTableSink` + 2 个 fe-core test 文件各加用例 + `fe-connector-hms` `HmsTableInfo`(+bucketCols/numBuckets)/`ThriftHmsClient`(convertTable 填桶)。
> - **INC-5（`fe-connector-hive`）**：**新** `HiveAcidUtil.java` + `HiveReadTransaction.java` + `HiveReadTransactionManager.java` + 3 test（`HiveAcidUtilTest`/`HiveReadTransactionTest`/`HiveTableHandleAcidTest`）；**改** `HiveScanPlanProvider.java`(+`planAcidScan`/`splitFile`+acid 参/`newRangeBuilder`/`encodeDeleteDeltas`/`buildPartitionName`) + `HiveTableHandle.java`(+`isTransactional`/`isFullAcid`) + `HiveConnector.java`(+读事务管理器) + `pom.xml`(+`commons-lang` test dep)。

> **权威实现依据**（信 HEAD 控制流，不信行号）：
> - `tasks/P7.3-hive-write-txn-implementation-design.md`（§2 决策 D1–D12、§4 签名、§5 移植细节、§6 构建顺序）。
> - **`tasks/P7.3-INC-3-portmap.md`（本轮新落地，595 行）**：逐行移植地图 + §7 12 处 GAP + **§9 已核实的实现前检查**（GAP-11/12 结论、D6 MPU map 重载确认、FS 构建/鉴权/iceberg 模板签名全确认）。INC-4/INC-5 也可续用其读侧 §5.4 + 测试计划 §6。
> - 移植源 = HEAD `fe/fe-core/.../datasource/hive/HMSTransaction.java`；模板 = `fe-connector-iceberg/.../IcebergConnectorTransaction.java`+`IcebergWriteContext.java`。

---

# 🚀 下个 session 任务 = **FINAL：INC-1..INC-5 一次原子 feature commit（待用户点头）**

> **① INC-1..INC-5 —— ✅ 全部完成 + 全绿**（写链 INC-1..4 + 读链 INC-5，见 🎯）。`fe-connector-hive` **104 测试** + `fe-core` 16 测试全绿；checkstyle 0；import-gate 净；INC-5 已过净室对抗复审（1 CONFIRMED 已修 + 1 自查已修 + 测试补强）。**不再回炒任何增量。** port-map：INC-3=`tasks/P7.3-INC-3-portmap.md`、INC-4=`tasks/P7.3-INC-4-portmap.md`、INC-5=`tasks/P7.3-INC-5-portmap.md`。

> **② FINAL —— 一次原子 feature commit**（§8；**path-whitelist `git add`，严禁 `-A`**）。白名单 = INC-1..INC-5 全部功能码 + 通用层（DEC-1）：
> - `fe/fe-connector/fe-connector-hms/`（`HmsClient`/`ThriftHmsClient`/`HmsWriteConverter`/`HmsTableInfo`/`pom.xml` 改 + `HmsAcidConstants`/`HmsCommonStatistics`/`HmsPartitionStatistics`/`HmsPartitionWithStatistics` 新 + 2 test）
> - `fe/fe-connector/fe-connector-hive/`（INC-2/3/4/5 全部新+改文件 + `pom.xml`，见 🎯 各段清单）
> - `fe/fe-connector/fe-connector-api/`（`ConnectorWritePlanProvider`/`Connector`/`ConnectorContractValidator`）
> - `fe/fe-core/`（`datasource/PluginDrivenExternalTable.java`、`nereids/.../physical/PhysicalConnectorTableSink.java` + 两个对应 test）
> - **切勿混入**：`regression-test/conf/regression-conf.groovy*`、`*.bak`、`.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`、`.claude/*.js`（workflow 脚本，非仓内）、`plan-doc/reviews/...`。commit message 见 `git log` 范式 + `Co-Authored-By`/`Claude-Session`。HANDOFF/doc/port-map **另起单独 commit**（本轮已 commit 文档）。
> - **提交前**务必 `git status` 核对暂存清单，且再跑一次 `fe-connector-hive` fresh `clean test` 确认无 stale `.class`。

> **③ cutover（P7.4/P7.5，另起原子批，勿在 FINAL 混入）**：翻闸（加 `HIVE` 到 `SPI_READY_TYPES`）、fe-core 写链 retype、读侧把 `Env.getCurrentHiveTransactionMgr()` 换成插件 `HiveReadTransactionManager` + 把 `QueryFinishCallbackRegistry` 接到 `deregister`（放锁）、摘 legacy `HiveTransactionMgr`、删 `datasource/hive`（+hudi+23 HMS-iceberg 类，守跨连接器删序）、**跑 ACID 集成测试套件**（R-002 最大风险，含 insert-only 标记门禁的 planScan 级回归——INC-5 已注留为 cutover 门）。

## 开场要点（承接）

1. **起步先读**本文顶部 🎯 段 + 设计文档 §6/§8。**若要做 FINAL commit**：严格按 🚀②的白名单 path-whitelist add，提交前 `git status` 核对 + fresh `clean test`。
2. **INC-1..INC-5 已全部完成 + 全绿 + 复审，勿重写/勿再复审现有代码**。全批仍 UNCOMMITTED 在工作树（原子批 WIP 载体，勿 `git checkout`/`revert`）。
3. **范围锁定（勿重议）**：hive **不在** `SPI_READY_TYPES`（整批天然 dormant，编译+单测但零线上路由）；翻闸 / fe-core 写链 retype / 摘 `HiveTransactionMgr` / 读侧放锁接线 / 删 legacy **均属后续 P7.4/P7.5，另起原子批**（见 🚀③）。full-ACID **写**继续硬拒（D7），full-ACID + insert-only **读**在范围（INC-5，已落地）。
4. **硬门 = ACID 集成测试套件**（R-002 项目最大风险，需 live 读/写路径 → P7.4/P7.5 翻闸时跑，勿静默跳过——Rule 12）。
5. **纪律**：每轮完成即更新本 HANDOFF + commit（memory `handoff-discipline-per-phase`）；上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**（复用 P5-T29 #64653 / P6 #64688 范式）。
- **公开 tracking issue = apache/doris#65185**（catalog-SPI 迁移 umbrella）；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `plan-doc/reviews/P5-paimon-rereview3-*`；`.claude/` 是 memory、非仓内）。
- **⚠️ 本批 FINAL 原子 commit 的白名单现已跨通用层**（DEC-1，用户 2026-07-06 授权）：除 `fe-connector-hms`/`fe-connector-hive` 外，还含 `fe-connector-api`（`ConnectorWritePlanProvider`/`Connector`/`ConnectorContractValidator`）+ `fe-core`（`datasource/PluginDrivenExternalTable.java`、`nereids/.../physical/PhysicalConnectorTableSink.java` + 两个对应 test）。设计文档 §4.5"fe-core ZERO changes"就此**作废**（改为一处通用、connector-agnostic、默认关的新能力）。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每阶段/每条 fix = 独立 commit**；HANDOFF + 任务清单 + 设计文档 + port-map 单独 commit。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**checkstyle 别加 `-am`**：`mvn -pl :<art> checkstyle:check`。
- **⚠️ bash 工具默认 timeout 120s**：fe-connector-hive 全模块 build/test ~2min → 调 `timeout` ~580000ms。**后台 task 通知的 "exit code" 是末尾 echo 的、非 maven 的**——读 LOG 的 `BUILD SUCCESS` 行或 surefire XML（`Tests run=/Failures=/Errors=`），别信通知 exit。maven 经管道 `$?` 是管道尾的 → grep `BUILD SUCCESS`。**改代码后 commit 前务必 fresh recompile**（stale `.class` 假红）。
- **连接器测试无 Mockito**（真 recording fakes；本轮 `HiveConnectorTransactionTest` 即用手写 recording `HmsClient` fake + `FakeConnectorContext`，`HmsClient` 多数 Phase-3+ 方法 default-throw、fake 只覆盖用到的）。checkstyle **禁 static import**（用 `Assertions.assertX`）、**扫 test 源**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**HMS `HiveVersionUtil` 命中 = 误报非违规**（memory `catalog-spi-hms-hiveversionutil-gate-false-positive`）。
- **cwd 会被 harness 重置** → 一律绝对路径。**⚠️ `/mnt/disk1` 紧**（2.0T ~82% used，360G free）；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够；本轮实现 agent 即在主树工作）。

# 🧠 起步必读

1. 本文档顶部 🎯/🚀 段 + `tasks/P7.3-INC-3-portmap.md` + 设计文档 `tasks/P7.3-hive-write-txn-implementation-design.md` §6。
2. **样板**：`tasks/P5-paimon-migration.md`（翻闸+删 legacy 全流程）；`tasks/P6-iceberg-migration.md`（阶段拆分范式）。模板事务 = `IcebergConnectorTransaction`/`IcebergWriteContext`。
3. **铁律**：fe-core 不得新增 `if(hive)`/`instanceof HMSExternal*`/引擎名判别；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）；插件跨边界须 pin TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
4. **memory 相关项**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`memory-keep-only-general-or-requested`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-plugin-tccl-classloader-gotcha`、`catalog-spi-hms-hiveversionutil-gate-false-positive`、`catalog-spi-tracking-issue`。

---

## 背景：#64688（P6 iceberg 收官，已合入 `branch-catalog-spi`）+ P7 关键遗留

整条 catalog-SPI 主线阶段链均已合入 upstream `branch-catalog-spi`：P0 #63582 · P1 #63641 · P2 trino #64096 · P3 hudi #64143 · P4 maxcompute #64300 · P5 paimon #64446+#64653 · P3b kerberos #64655 · **P6 iceberg #64688**。#64688 把原生 iceberg 整体迁到自包含 `fe-connector-iceberg` + 翻闸 + 删 fe-core 原生 iceberg 子系统 + 属性/鉴权全归插件（用户 2026-07-05 架构裁定，memory `catalog-spi-no-property-parsing-in-fecore`）。

**⚠️ P7 必须接手的遗留**：`#64688` 删的是原生 iceberg；但 **iceberg-on-HMS**（`type=hms` 下 `DlaType.ICEBERG`）仍走 fe-core，故 fe-core `datasource/iceberg/` 还**故意保活 23 个 HMS-iceberg 支撑类**（`IcebergUtils`/`IcebergMetadataOps`/`source/IcebergScanNode`+…/`cache/`/`IcebergMvccSnapshot`/… ）。→ P7 hive 迁移把 HMS-iceberg 挪到连接器路径后，这 23 文件才能删（P7.4/P7.5 阶段四）。同理 fe-core `datasource/hudi/`、`datasource/hive/` 也在 P7 范围。**删除排序（最硬约束）**：`datasource/hive/` 删不掉，直到 `HudiUtils`/`HudiScanNode`/`IcebergHMSSource`/`HMSAnalysisTask`/`StatisticsUtil.getIcebergColumnStats` 等全 retype 到 generic。
