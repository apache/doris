# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.3 已落地中立读事务生命周期 seam（T08 连接器无关部分，commit `21aa30683dc`）+ 前序 delete-delta 回归修复（T07 隔离必修，commit `c30fa15d99a`）；核心写/事务批（T01–T05）+ 6 文件 retype（T06）+ 读侧 ACID 生产半搬迁（T07 剩余，本轮侦察已知与写批耦合）+ HiveTransactionMgr 入插件（T08 剩余）+ 测试/门（T09–T11）待做**

> **本轮做了什么** —— **1 实现 commit（`21aa30683dc`）+ 1 文档 commit**，落地 T08 的**中立读事务生命周期 seam**（OQ-RTX=a 的连接器无关部分）：
> - **新 `QueryFinishCallbackRegistry`**（fe-core `qe/`）：`register(queryId, Runnable)` / `runAndClear(queryId)`（幂等 + 单回调异常隔离，一个连接器清理失败不阻断其它）。
> - **`QeProcessor` 加 `registerQueryFinishCallback` SPI**；`QeProcessorImpl.unregisterQuery` 原 `:210` 硬编 `Env.getCurrentHiveTransactionMgr().deregister(...)`（**每条查询都点名 hive** 的通用清理代码）改为通用 drain `runAndClear(printId(queryId))` → **通用查询生命周期代码不再点名任何数据源**（架构铁律进展；对齐 Trino 引擎驱动的 query/txn 生命周期）。
> - **`HiveScanNode.doInitialize`** 经 seam 注册 deregister 回调。**行为保持**：drain key `printId(queryId)` == 回调 key `hiveTransaction.getQueryId()`（老 deregister 依赖的同一等式）；早释放路径仍即时 commit、finish 回调再走幂等 deregister 即 no-op → 单次 commit。
> - **6 单测（无 Mockito）**：run-once / 空查询 no-op / 多回调按序 / 幂等 / 失败隔离 / per-query 作用域。**build SUCCESS / checkstyle 0 / import-gate 净 / 6 pass**。非 live（翻闸 P7.5）。
> - **T08 剩余**：`HiveTransactionMgr` **仍留 `Env`**（legacy fe-core 读路径 `HiveScanNode` 在用）；入插件须待 T07 读侧 ACID 迁移（届时插件经**同一 seam** 注册），故与 T07 同批。
> - **为何本轮只做 T08（用户拍板）**：T01–T05 是**互锁原子批**（写原语签名由事务实际调用反推，不得先作死代码——P7.1 教训），照 spec 移植 1895 行 `HMSTransaction`+`HiveTableSink.bindDataSink` 是**多 session** 且中途拿不出可编译可提交结果。T08 中立 seam 是本 session 唯一「干净、有界、可独立编译+提交+测试」的片。
> - **⚠️ 本轮侦察修正 T07 边界（已写进 spec T07 行，勿低估）**：读侧 ACID 的**消费/解码半已在插件做完+单测**（`HiveScanRange`，`.acidInfo(` 目前无生产者调用）；**生产半仍是活且与写批耦合**——`AcidUtil.getAcidState`(427) 返回 fe-core 缓存内类 `FileCacheValue`、拖入 Doris `filesystem`/`StorageProperties`/`LocationPath`/`HivePartition`；生产实调在 `HiveExternalMetaCache:816`（非 `HiveScanNode`）；`HiveTransaction` 的 `openTxn`/`getValidWriteIds`/`acquireSharedLock` **与写路径共用同批 ACID HmsClient 原语（T01/T02）**；插件 `HiveScanPlanProvider` 现**整段跳过 ACID 目录**，产 ACID 读须新增扫描下潜逻辑。⇒ **T07 生产半宜与 T01/T02 写批同批推进，非独立并行小片**。
>
> **上一轮（P7.3 recon+设计）已定的产出仍有效**（写进 `tasks/P7-hive-migration.md` 末尾「**P7.3 逐 task 拆解**」块 + OQ 段 ✅）：
> - **通用写入通路已由 iceberg P6 建好**（`PhysicalConnectorTableSink → PluginDrivenTableSink → PluginDrivenInsertExecutor` + `ConnectorTransaction`/`ConnectorWritePlanProvider`/`PluginDrivenTransactionManager`，**fe-core 桥零改动**）⇒ P7.3 = 把 hive INSERT 折进现成通路 + 照抄 `IcebergConnectorTransaction`/`IcebergWriteContext`，非从零发明 seam。
> - **缺的 HMS 写原语底层全有**（vendored `HiveMetaStoreClient` 已实现 addPartitions/stats/ACID 全套）→ 缺的只是 `HmsClient` 接口开口 + 一行 `execute` 转发。
> - **三项决策（2026-07-06，spec OQ 段 ✅）**：① **OQ-RTX = 主干加通用 per-query finish 回调**（连接器无关，替 `QeProcessorImpl:210→Env.getCurrentHiveTransactionMgr` 硬编，对齐 Trino）；② **OQ-ACID-WRITE = 迁移行为保持**（非-ACID 写 + ACID 读迁到位，full-ACID 写继续硬拒，控 R-002）；③ **OQ-LOCK = 保持现状**（读侧共享 HMS 锁无 heartbeat）。
>
> **P7.1 交付的地基（已在工作分支）**：`fe-connector-hms` 有 DDL 写客户端（create/drop db+table、truncate，default-throw seam + `ThriftHmsClient` 实现）；`fe-connector-hive` `HiveConnectorMetadata` DDL override 齐（txn/planWrite override 仍 0）；env channel 通；shared converter 带列默认值 + 显式分区标志。

> **整条 catalog-SPI 主线阶段链均已合入 upstream `branch-catalog-spi`**：
> P0 #63582 · P1 #63641 · P2 trino #64096 · P3 hudi(hybrid) #64143 · P4 maxcompute #64300 · P5 paimon 迁移+翻闸 #64446 + 删 legacy #64653 · P3b kerberos→fe-kerberos #64655 · **P6 iceberg #64688（本轮收官）**。
>
> **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`，工作树干净）。P6 期间所有"未 push（[DEC-FLIP-1] 铁律）"的翻闸/删死码/属性迁移工作**已全部随 #64688 一次性合入**——**该铁律现已解除**，P7 走常规「连接器内实现 → 翻闸 → 删 legacy → squash-合入」流程（复用 P5/P6 样板）。

## #64688 做了什么（685 文件，+79738/−23744）

把原生 iceberg（元数据/scan/write/procedures/sys-tables/行级 DML）整体迁到自包含 `fe-connector-iceberg` + 翻闸（iceberg 入 `CatalogFactory.SPI_READY_TYPES`）+ 删 fe-core 原生 iceberg 子系统：
- **删除**：`IcebergExternalCatalog`/`IcebergExternalTable`/`IcebergTransaction`/`IcebergNereidsUtils`、7 catalog flavor + factory、`broker/`·`dlf/`·`fileio/`·`action/`·`rewrite/`·`helper/`、四个 DML 执行器、planner 三 sink、`IcebergTransactionManager` + fe-core Iceberg/Paimon 元存储属性簇（S7，−4914 行）等。
- **翻闸 + GSON 迁移**：旧 8 catalog 变体 + db + table 标签 `registerCompatibleSubtype`→`PluginDriven*`（保升级老集群反序列化）。
- **属性/鉴权全归插件**（S1–S10，用户 2026-07-05 架构裁定）：fe-core 不再解析任何属性；已迁连接器（iceberg+paimon）走「插件解析 → BE thrift → 回传 fe-core」。详见 memory `catalog-spi-no-property-parsing-in-fecore`。

## ⚠️ 关键遗留（P7 必须接手）= fe-core `datasource/iceberg/` 还剩 **23 个 HMS-iceberg 支撑类**

`#64688` 删的是**原生 iceberg 子系统**；但 **iceberg-on-HMS**（`type=hms` 下 `DlaType.ICEBERG` 的表）仍走 fe-core，因此以下 23 文件**故意保活**（decision D5 / Q3=B：HMS-iceberg 随 hive 一起迁）：
`IcebergUtils`、`IcebergMetadataOps`、`source/IcebergScanNode`(+`IcebergHMSSource`/`IcebergSource`/`IcebergSplit`/…)、`cache/`、`IcebergMvccSnapshot`、`IcebergSchema/Snapshot/Partition*CacheValue`、`profile/IcebergMetricsReporter`、`DorisTypeToIcebergType`、`IcebergCatalogConstants`。
→ **P7 hive 迁移把 HMS-iceberg 挪到连接器路径后，这 23 文件才能删（阶段四）**。同理 fe-core `datasource/hudi/` 与 `datasource/hive/` 也在 P7 范围。

---

# 🚀 下个 session 任务 = **实现 P7.3 核心写/事务批（T01–T05 原子批 → T06 retype → T07 剩余 + T08 剩余 → T09–T11）；T07 delete-delta 回归修复（`c30fa15d99a`）+ T08 中立 seam（`21aa30683dc`）已落地，勿重做**

> **权威计划**：`tasks/P7-hive-migration.md` 末尾「**P7.3 逐 task 拆解**」块（recon 结论 + 3 决策 + T01–T11 任务表 + 移植指针 + HEAD 行号）。**信 HEAD 控制流不信本文/spec 行号。** 模板 = `IcebergConnectorTransaction`/`IcebergWriteContext`/`IcebergConnectorMetadata.beginTransaction+planWrite`；fe-core 桥 `PluginDrivenTransactionManager`/`PluginDrivenInsertExecutor` **零改动**。

## 开场要点（P7.3 实现）

1. **不发明中立 seam**：通用写入通路现成（iceberg P6 建）。P7.3 = 把 hive INSERT 折进现成通路 + 照抄 iceberg 事务。
2. **落地顺序**（spec 已排 + 本轮修正）：**组1 写原语（T01/T02）与组2 连接器事务（T03–T05）同批 commit**（P7.1 教训：写原语不得先于事务作死代码，签名由实际调用反推）；组3 6 文件 retype（T06）依赖组2 provider/txn 就位。**组5 读事务中立 seam（T08）的连接器无关部分已落地（`21aa30683dc`）**——通用 `QueryFinishCallbackRegistry` + `QeProcessorImpl` 通用 drain 替 hive 硬编 + `HiveScanNode` 经 seam 注册；其剩余（`HiveTransactionMgr` 入插件）随 T07 走。**⚠️ 组4 读侧 ACID（T07）经本轮侦察已知非独立小切片**：消费半已在插件+单测，生产半（`AcidUtil.getAcidState` + 插件扫描下潜 + `openTxn`/`getValidWriteIds`）拖入 fe-core `filesystem`/`StorageProperties`/`FileCacheValue` 且**与写批共用 ACID HmsClient 原语**→ 宜与组1/组2 写批**同批**推进，非并行独立片（详见 spec T07 行 ⚠️ 段）。T07 delete-delta 回归修复已落地（`c30fa15d99a`），复用已修好的 populate。
3. **3 硬耦合结**（成为插件事务时必断）：注入 profile/queryId（去 `ConnectContext.get()`）；thrift `THivePartitionUpdate` 簇随插件走；fs 抽象须暴露对象存储 MPU complete/abort（`SpiSwitchingFileSystem.forPath→ObjFileSystem`）。
4. **范围锁定**：full-ACID **表**写继续硬拒（OQ-ACID-WRITE）；读侧共享锁**不加 heartbeat**（OQ-LOCK）。
5. **硬门 = 独立 ACID 集成测试套件**（T10，R-002 项目最大风险）：INSERT/OVERWRITE/分区写/delete-delta 读/rollback/多 FE 失效。⚠️端到端写测试需插件写路径 live，但翻闸在 P7.5 → 须本地/scoped 翻闸跑该套件（或与 P7.5 联跑），**勿静默跳过**（Rule 12）。
6. **勿重议的已定决策**（P7 全局）：**D-004** event→fe-connector-hms；**D-005** tableFormatType；**D-020** per-table SPI provider；**D-019** hudi live cutover 并入 P7；事务桥接 `PluginDrivenTransactionManager`（已存，零改动）。
7. **纪律**：每 task 独立 commit + build + test + checkstyle 0（不带 -am）+ import-gate 净；**每轮完成即更新本 HANDOFF + commit**（memory `handoff-discipline-per-phase`）。
8. **删除排序（最硬约束，spec §跨连接器删除排序，P7.4/P7.5 才触发）**：`datasource/hive/` 删不掉，直到 `HudiUtils`/`HudiExternalMetaCache`/`HudiScanNode`/`IcebergHMSSource`/`HMSAnalysisTask`/`StatisticsUtil.getIcebergColumnStats` 全 retype 到 generic。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**（复用 P5-T29 #64653 / P6 #64688 范式）。
- **公开 tracking issue = apache/doris#65185**（catalog-SPI 迁移 umbrella，大步骤勾选清单 + 连接器表）；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（scrub `regression-test/conf/regression-conf.groovy` 明文 key + `*.bak` + scratch `.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...` + `plan-doc/reviews/P5-paimon-rereview3-*`；`.claude/` 是 memory、非仓内）。
- commit message：见 `git log` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每阶段/每条 fix = 独立 commit**；HANDOFF + 任务清单 + 设计文档单独 commit。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**fe-core 只依赖 `fe-connector-api`**。**fe-connector-hive/hms 单独 build 注意 optional-shade 依赖**（如 HiveConf；参照 paimon `package` 而非 `test-compile` 的坑）。
- **⚠️ checkstyle 别加 `-am`**：`-am` 把 `fe-common`（大量既存 error）拖进假红 → `mvn -pl :<art> checkstyle:check`（不带 -am）。
- **⚠️ bash 工具默认 timeout 120s**：fe-core build 超时 → 调 `timeout` ~590000ms 或后台跑（全模块 ~2min）。**后台 task 通知的 "exit code" 是末尾 echo/df 的、非 maven 的**——读 LOG 里 `BUILD SUCCESS`/`MAVEN_EXIT=` 行或 surefire XML（`tests=`/`failures=`），别信通知 exit。
- **⚠️ maven 经管道 `$?` 是管道尾的** → 用 `${PIPESTATUS[0]}` 或 grep `BUILD SUCCESS`。**mutation/review 后 commit 前务必 fresh recompile**（stale `.class` 假红坑）。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`。**HMS `HiveVersionUtil` import 命中 = 误报非违规**（`fe-connector-hms` 内 vendored 同名副本，非 fe-core）——勿改，详见 memory `catalog-spi-hms-hiveversionutil-gate-false-positive`。
- **连接器测试无 Mockito**（真 InMemoryCatalog/Recording fakes）；**fe-core 用 Mockito**（`CALLS_REAL_METHODS` + `Deencapsulation.setField`；`anyString()` 不匹配 null）。详见 memory `catalog-spi-fe-core-test-infra`。
- **cwd 会被 harness 重置** → 一律绝对路径。
- **⚠️ 环境**：`/mnt/disk1` 紧（2.0T，~96% used）。**起步先 `df -h /mnt/disk1`**；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 起步必读

1. **本文档顶部 🎯/🚀 段** + master plan [§3.8](./00-connector-migration-master-plan.md)（P7 战略）+ [`connectors/hive.md`](./connectors/hive.md)（P7 子阶段/SPI 缺口/特殊性）+ master plan §4（13 步 playbook）。
2. **样板**：`tasks/P5-paimon-migration.md`（full-adopter + 翻闸 + 删 legacy 全流程）；`tasks/P6-iceberg-migration.md`（阶段拆分 spec 范式，P7 spec 照此建）。
3. **铁律**：fe-core 不得新增 `if(hive)` / `instanceof HMSExternal*` / 引擎名字符串判别（新 seam 走中立 SPI / `ConnectorCapability`）；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。
4. **memory（现存相关项）**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`memory-keep-only-general-or-requested`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-plugindriven-no-source-specific-code`、`catalog-spi-no-property-parsing-in-fecore`、`catalog-spi-be-java-ext-shared-classpath`、`catalog-spi-plugin-tccl-classloader-gotcha`、`catalog-spi-connector-session-tz-gotcha`、`catalog-spi-history-schema-info-lowercase-nested-names`、`catalog-spi-connector-cache-framework-caffeine-coherence`、`catalog-spi-hms-hiveversionutil-gate-false-positive`、`catalog-spi-tracking-issue`。
5. **上下文超 30% 即交接**（memory `session-handoff-at-30pct-context`）：找干净节点覆写本 HANDOFF + 通知用户开新 session。
