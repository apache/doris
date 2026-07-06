# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.1（DDL 写）已完成合入前状态；P7.3（写/事务，关键路径）已完成 recon + 3 项决策 + 逐 task 拆解（写进 spec，无实现代码）；下一步 = 实现 P7.3-T01…T11**

> **本轮（P7.3 开场 recon + 设计 session）做了什么** —— **无实现代码**，产出 = P7.3 逐 task 拆解 + 决策，全部写进 `tasks/P7-hive-migration.md` 末尾「**P7.3 逐 task 拆解**」块（OQ 段已标 ✅）：
> 1. **选定子阶段**：用户在 P7.2（event）/ P7.3（写/事务）二选一中选 **P7.3（关键路径、最难、R-002）**。
> 2. **4-agent 并行 code-grounded recon**（直读 HEAD）：写引擎 `HMSTransaction` / 6 文件写链 / 读侧 ACID / 目标 SPI+桥接。
> 3. **关键去风险发现**：**通用写入通路已由 iceberg P6 建好**（`PhysicalConnectorTableSink → PluginDrivenTableSink → PluginDrivenInsertExecutor` + `ConnectorTransaction`/`ConnectorWritePlanProvider`/`PluginDrivenTransactionManager`，**fe-core 桥零改动**）。⇒ P7.3 = 把 hive INSERT 折进现成通路 + 照抄 `IcebergConnectorTransaction`/`IcebergWriteContext`，**非从零发明 seam**。
> 4. **缺的 HMS 写原语底层全有**（vendored `HiveMetaStoreClient` 已实现 addPartitions/stats/ACID 全套）→ 缺的只是 `HmsClient` 接口开口 + 一行 `execute` 转发。老写引擎写原语已干净隔离在 `hiveOps` 后。
> 5. **⚠️ 揪出一处已存真实回归（必修，T07）**：插件 `HiveScanRange.populateTransactionalHiveParams` 处理 ACID delete-delta **只填目录、丢文件名**（老 fe-core 两者都填）→ 翻闸后事务表静默读错。
>
> **本轮三项决策（2026-07-06，spec OQ 段已标 ✅）**：① **OQ-RTX = 主干加通用 per-query finish 回调**（连接器无关，替 `QeProcessorImpl:210→Env.getCurrentHiveTransactionMgr` 硬编，对齐 Trino；hive 读事务管理器随之从 `Env` 摘掉入插件）；② **OQ-ACID-WRITE = 迁移行为保持**（非-ACID 写 + ACID 读迁到位，full-ACID 写继续硬拒，控 R-002）；③ **OQ-LOCK = 保持现状**（读侧共享 HMS 锁无 heartbeat，原样保留）。
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

# 🚀 下个 session 任务 = **实现 P7.3（写/事务，关键路径）——照 spec「P7.3 逐 task 拆解」块 T01–T11 逐 task commit+build+test+checkstyle；recon+决策已完成，直接写代码**

> **权威计划**：`tasks/P7-hive-migration.md` 末尾「**P7.3 逐 task 拆解**」块（recon 结论 + 3 决策 + T01–T11 任务表 + 移植指针 + HEAD 行号）。**信 HEAD 控制流不信本文/spec 行号。** 模板 = `IcebergConnectorTransaction`/`IcebergWriteContext`/`IcebergConnectorMetadata.beginTransaction+planWrite`；fe-core 桥 `PluginDrivenTransactionManager`/`PluginDrivenInsertExecutor` **零改动**。

## 开场要点（P7.3 实现）

1. **不发明中立 seam**：通用写入通路现成（iceberg P6 建）。P7.3 = 把 hive INSERT 折进现成通路 + 照抄 iceberg 事务。
2. **落地顺序**（spec 已排）：**组1 写原语（T01/T02）与组2 连接器事务（T03–T05）同批 commit**（P7.1 教训：写原语不得先于事务作死代码，签名由实际调用反推）；组3 6 文件 retype（T06）依赖组2 provider/txn 就位；组4 读侧 ACID（T07，含**回归修复**）+ 组5 读事务中立 seam（T08）相对独立、可并行。
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
