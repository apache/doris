# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.3 原子批 INC-1 + INC-2 + INC-3 全部完成（主体 + 复审 + FS 重活单测全绿）。下一步 = INC-4 → INC-5。全批仍 UNCOMMITTED 在工作树里（跨 session WIP 载体，勿 commit / 勿 revert / 勿 `git checkout`）。本轮只 commit HANDOFF + 设计文档。**

> **本轮做了什么（INC-3 收尾 a：提交器 FS 重活单测）**：给 `HiveConnectorTransactionTest` 加了 4 个 committer FS 单测，覆盖 D6/D9 对象存储那半（净室复审 b 已在上一轮做完，本轮无需再跑）。注入方式 = 匿名子类 override `resolveObjectStoreFileSystem(...)` 返回一个 recording 假 `ObjFileSystem`（内含 recording 假 `ObjStorage`，同时记录 complete+abort；假件的非 MPU FS 方法一律 fail-loud，走到即测试失败）。4 例：① commit 时 MPU `completeMultipartUpload`（无分区 APPEND、`targetPath==writePath` 走 `objCommit`；并断言 ETag 按 part 号排序）；② `rollback()`（committer-null 路径）MPU `abortMultipartUpload`；③ rollback **幂等**（第二次 `rollback()` 不抛、且 abort 恰好一次）；④ `addPartitions` **只调一次**且带整表分区 SD（GAP-7 批处理在 client 内、GAP-4 列从表 schema 重建）——第④例 FILE_S3 下 `targetPath==writePath` 不触发任何 FS，故连假件都不需要。同步把测试类头 Javadoc 与设计文档 §6 INC-3 (a) 勾成 done。

> **本轮验证（独立重跑，不信自述）**：`fe-connector-hive` 全模块 `test` **67 测试 0 失败 0 错误 BUILD SUCCESS（原 63 + 新 4；hive/hms 全绿、无回归）**；`checkstyle:check` 0 违规；`check-connector-imports.sh` 净（仅 `HiveVersionUtil` 已知误报，gate 现已自动 skip）。

> **⚠️ 工作树未提交的 INC-1 + INC-2 + INC-3 代码**（`git status` 会显示；原子批模型，设计文档 §8——**若工作树被清就照 port-map 重建**）：
> - INC-1（`fe-connector-hms`）：`HmsClient`/`ThriftHmsClient`/`HmsWriteConverter`/`pom.xml` 改 + `HmsAcidConstants`/`HmsCommonStatistics`/`HmsPartitionStatistics`/`HmsPartitionWithStatistics`/`HmsPartitionInfo` 新 + 2 test。
> - INC-2（`fe-connector-hive`）：`HiveWriteUtils.java` + `NameMapping.java`（+ 2 test）。
> - **INC-3（`fe-connector-hive`）**：**新** `HiveWriteContext.java`(89) + `HiveConnectorTransaction.java`(1704，含 dup-key 修复) + `HiveConnectorTransactionTest.java`(654，14 例：10 分类/SPI + 4 committer FS) + `FakeConnectorContext.java`(64)；**改** `HiveConnectorMetadata.java`(+15，`beginTransaction` override) + `pom.xml`(+11，`fe-filesystem-spi` scope `provided`) + `HiveWriteUtils.java`（`equalsIgnoreSchemeIfOneIsS3`/`toPartitionValues` + 单测）。

> **权威实现依据**（信 HEAD 控制流，不信行号）：
> - `tasks/P7.3-hive-write-txn-implementation-design.md`（§2 决策 D1–D12、§4 签名、§5 移植细节、§6 构建顺序）。
> - **`tasks/P7.3-INC-3-portmap.md`（本轮新落地，595 行）**：逐行移植地图 + §7 12 处 GAP + **§9 已核实的实现前检查**（GAP-11/12 结论、D6 MPU map 重载确认、FS 构建/鉴权/iceberg 模板签名全确认）。INC-4/INC-5 也可续用其读侧 §5.4 + 测试计划 §6。
> - 移植源 = HEAD `fe/fe-core/.../datasource/hive/HMSTransaction.java`；模板 = `fe-connector-iceberg/.../IcebergConnectorTransaction.java`+`IcebergWriteContext.java`。

---

# 🚀 下个 session 任务 = **INC-4 → INC-5，（全绿后）全批一次原子 feature commit**

> **① INC-3 —— ✅ 全部完成**（主体 + 复审 b + FS 重活单测 a，见 🎯；`fe-connector-hive` 67 测试全绿）。下步是 **INC-4 → INC-5**，不再回炒 INC-3。INC-4/INC-5 落地后可对**新代码**再起一轮同款净室对抗复审。

> **② INC-4 —— `HiveWritePlanProvider.planWrite` + `buildSink` + capabilities**（`fe-connector-hive`，port-map §4.4/§5.2）：`planWrite` 调 `tx.beginWrite(...)` + `buildSink`（= 忠实移植 HEAD `planner.HiveTableSink.bindDataSink`：PARTITION_KEY/REGULAR 按表序、bucket、按格式压缩、staging-vs-in-place location、serde、`hadoopConfig` 经 `context.getStorageProperties().toBackendProperties()` + `vendStorageCredentials`）+ `HiveConnector.getWritePlanProvider()` + `supportedOperations={INSERT,OVERWRITE}` + capability gates；LZO-INSERT / 事务表拒写作 begin-guard。测试 = `HiveWritePlanProviderTest`（假 `ConnectorWriteHandle`，断 `THiveTableSink` 形状 + INSERT→OVERWRITE 提升 + guard）。

> **③ INC-5 —— 读侧 ACID 生产半 + 插件读事务生命周期（dormant）**（`fe-connector-hive`，port-map §5.4）：移植 `getAcidState` **纯**目录名解析 + `hive-common Valid*` 算法；插件 `HiveTransaction`（openTxn/acquireSharedLock/getValidWriteIds/commitTxn，用 INC-1 已落地的读原语）；`HiveScanPlanProvider` 下潜 + `HiveScanRange.acidInfo(...)` 生产端接线；`HiveTableHandle` 的 `isTransactional`/`isFullAcid` 从 `getTableParameters()` 派生（D8）。测试 = `HiveAcidDescentTest` + 既有 `HiveScanRangeAcidTest` 保绿。**依赖仅 INC-1，独立于 INC-3/4**，也可先做。

> **④ FINAL** —— INC-1..INC-5 全绿后**一次原子 feature commit**（path-whitelist `git add`，勿 `-A`）+ HANDOFF/doc 单独 commit。

## 开场要点（承接）

1. **起步先读**本文顶部 🎯 段 + 设计文档 §6 构建顺序 + INC-4 段 port-map §4.4/§5.2（写 sink 侧）/ INC-5 段 port-map §5.4（读侧 ACID）。**信 HEAD 控制流不信行号**——每处编辑前重读对应 HEAD 段。（`P7.3-INC-3-portmap.md` 仅 INC-3 专用，已全部落地，下轮不必再读。）
2. **INC-3 已全部完成，勿重写/勿再复审现有代码**：`HiveConnectorTransaction` 已编译 + 67 单测（10 分类/SPI + 4 committer FS）+ checkstyle + import-gate 全绿，且**已过 12 路净室对抗复审（11/12 忠实，唯一 low 缺陷已修+测）**。下步是 **INC-4 → INC-5**（可择序：写 spine 先或读侧先，INC-5 仅依赖 INC-1）。
3. **范围锁定（勿重议）**：hive **不在** `SPI_READY_TYPES`（整批天然 dormant，编译+单测但零线上路由）；翻闸 / fe-core 写链 retype / 摘 `HiveTransactionMgr` / 删 legacy **均属后续 P7.4/P7.5，另起原子批**。full-ACID **写**继续硬拒（D7），full-ACID **读**在范围（INC-5）。
4. **硬门 = ACID 集成测试套件**（R-002 项目最大风险，需 live 写路径 → P7.4/P7.5 翻闸时跑，勿静默跳过——Rule 12）。
5. **纪律**：每轮完成即更新本 HANDOFF + commit（memory `handoff-discipline-per-phase`）；上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**（复用 P5-T29 #64653 / P6 #64688 范式）。
- **公开 tracking issue = apache/doris#65185**（catalog-SPI 迁移 umbrella）；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `plan-doc/reviews/P5-paimon-rereview3-*`；`.claude/` 是 memory、非仓内）。
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
