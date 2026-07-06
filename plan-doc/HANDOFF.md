# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-06）= **P7.1 完成：hive DDL 写路径（create/drop db+table、truncate）全部落地 + 全绿 + 提交（非 live，翻闸在 P7.5）；下一步 = 进入 P7.2（event）或 P7.3（txn，关键路径）**

> **本轮（P7.1 T05–T11 收官 session）做了什么** —— 3 个实现 commit（`4a92…` T08 / `0713…` T09 / `c17b…` T05–T07+T10）+ 本 HANDOFF：
> 1. **T05–T07 插件 DDL override**（`c17b…`，`fe-connector-hive`）：`HiveConnectorMetadata` 加 `supportsCreateDatabase/createDatabase/dropDatabase(force cascade)/createTable/dropTable/truncateTable`——忠实移植 legacy `HiveMetadataOps`。`createTable` 全套插件侧解析：owner 默认（`session.getUser()`）、transactional 建表拒绝、file_format 默认（env，用户 per-table 属性胜）、`doris.`-前缀 round-trip 参数、LIST-only 分区 + 显式分区取值拒绝、DLF 列默认值 guard、bucket gate（env enable + hash-only）、`doris.version`/text 压缩默认线程进写请求。事务表拒绝用 handle 的 `tableParameters` 复刻 `AcidUtils.isTransactionalTable`（**不引 hive-exec**）。**`renameTable` 保持 SPI default throw**（hive 无 rename，parity）。`HiveConnector` 传 `ConnectorContext`。
> 2. **T08 shared converter 富化**（`4a92…`，fe-core+api）：`ColumnDefinition.getDefaultValueString()` + converter 填 `ConnectorColumn.defaultValue`（原恒 null）；`ConnectorPartitionSpec.hasExplicitPartitionValues()` + converter 从 `PartitionTableInfo.getPartitionDefs()` 置位。**已核实 additive**：iceberg create `buildSchema` 不读默认值（读默认值的三处都在 ADD COLUMN 路径），paimon/maxcompute create 不读——只 hive 消费。
> 3. **T09 config threading**（`0713…`，fe-core）：`DefaultConnectorContext.buildEnvironment` 加 `hive_default_file_format`/`enable_create_hive_bucket_table`/`doris_version` 三键，**走运行环境**（非写 catalog 属性）。
> 4. **T10 单测**（并入 `c17b…`）：`HiveConnectorMetadataDdlTest`（20 条，recording-fake `HmsClient` + fake session/context，无 Mockito）覆盖各 guard/默认值/round-trip/force cascade；修既存 pruning 测 ctor。
> 5. **验收全绿**：fe-core compile + fe-connector-hive test-compile SUCCESS；单测 20+8=28 条 0 fail/0 skip；checkstyle 0（api+hive+core）；import-gate 净。**非 live**（翻闸在 P7.5），无 e2e DDL（符合本阶段验收门）。
>
> **本轮两个用户签字决策（2026-07-06，见 spec「P7.1 实现进度」块）**：① **config threading 机制 = 走运行环境**（非写 catalog 属性；在方案 A 内的机制细化——不落镜像/edit-log、不污染 SHOW CREATE、随全局配置刷新；有现成 `hive_metastore_client_timeout_second` 先例）。**注意**：legacy-exact（全局默认 + per-table 覆盖），**未**新增 per-catalog 覆盖属性。② **显式分区取值 = 保持报错**（converter thread 标志，hive 照 legacy 报错，非静默忽略）。

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

# 🚀 下个 session 任务 = **进入 P7.2（event pipeline，可与 P7.3 并行）或 P7.3（HMSTransaction/写路径，关键路径、最难、R-002）；先 code-grounded recon → 用户签字 OQ-* → 再写代码**

> **权威计划**：**`tasks/P7-hive-migration.md`**——P7.2/P7.3 子阶段 spec（上半）+「跨连接器删除排序」+「SPI 缺口（按子阶段）」块 +「开放决策 OQ-*」块 + [`connectors/hive.md`](./connectors/hive.md)。**「P7.1 逐 task 拆解」块已标 ✅ 完成（勿重做）。** 流程样板照搬 P7.1：recon → 中文讲背景+示例+推荐 → 用户签字 OQ-* → 逐 task commit+build+test+checkstyle。
> **P7.1 已交付的地基**：`fe-connector-hms` 有完整 DDL 写客户端（create/drop db+table、truncate）；`fe-connector-hive` `HiveConnectorMetadata` DDL override 齐；env channel 已通（`DefaultConnectorContext.buildEnvironment` → `ConnectorContext.getEnvironment()`）；shared converter 已带列默认值 + 显式分区标志。**通用桥 = `PluginDrivenExternalCatalog`**；**模板 = `IcebergConnectorMetadata`**。

## 下一子阶段选择 + 开场要点

1. **关键路径 = P7.3**（P7.1→P7.3→P7.4→P7.5）；P7.2（event）是可并行侧支。二选一按精力：P7.3 最难（ACID 写路径 6 文件耦合链 + `HMSTransaction` 1895 行重表达，R-002 项目最大风险，**硬门 = 独立 ACID 集成测试套件**）；P7.2 较独立（21 event 类 + processor 搬 `fe-connector-hms`，核心 fork = OQ-EVT）。
2. **进入子阶段先 recon**（**信 HEAD 控制流不信 spec 行号**）：读该子阶段 spec +「SPI 缺口」对应块 + 真实代码，产出逐 task 拆解追加 spec 末尾（仿「P7.1 逐 task 拆解」块）。**OQ-* 到 recon 后用户签字**（中文讲背景+示例+推荐、不引任务代号）——P7.3 = OQ-RTX/OQ-ACID-WRITE/OQ-LOCK；P7.2 = OQ-EVT。
3. **P7.3 需补的写原语**（随 `HMSTransaction` 落地才非死代码）：`HmsClient` 加 `updateTableStatistics/updatePartitionStatistics/addPartitions/dropPartition` + txn/writeId/lock/commit 方法。写路径 6 文件耦合链 retype `HMSExternalTable`→generic：`BindSink`→`LogicalHiveTableSink`→`PhysicalHiveTableSink`→`PhysicalPlanTranslator:569`→`planner/HiveTableSink`→`HiveInsertExecutor`。读侧 ACID（delete-delta）须移入插件。
4. **勿重议的已定决策**：**D-004** event→fe-connector-hms；**D-005** tableFormatType；**D-020** per-table SPI provider；**D-019** hudi live cutover 并入 P7；事务桥接 `PluginDrivenTransaction`。
5. **纪律**：每 task/每条 fix 独立 commit + build + test + checkstyle 0（不带 -am）+ import-gate 净；**每轮完成即更新本 HANDOFF + commit**（memory `handoff-discipline-per-phase`）。**P7.3 ACID 写路径必须有独立集成测试作 gate（R-002 项目最大风险）**。
6. **删除排序（最硬约束，spec §跨连接器删除排序）**：`datasource/hive/` 删不掉，直到 `HudiUtils`(5 方法)/`HudiExternalMetaCache`/`HudiScanNode`(extends HiveScanNode)/`IcebergHMSSource`/`HMSAnalysisTask`/`StatisticsUtil.getIcebergColumnStats` 全 retype 到 generic（P7.4）。

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
