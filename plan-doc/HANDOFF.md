# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-05）= **P6 iceberg 迁移 + 测试全部完成，已 squash-合入 `branch-catalog-spi`（#64688 `8b391c7459d`）；下一阶段 = P7 hive/HMS**

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

# 🚀 下个 session 任务 = **P7 hive (+HMS) 迁移（最复杂、最后做的连接器）**

> **权威计划**：[`00-connector-migration-master-plan.md` §3.8](./00-connector-migration-master-plan.md)（P7.1–P7.5 子阶段 + 风险）+ [`connectors/hive.md`](./connectors/hive.md)（子阶段/SPI 缺口/特殊性）。**单连接器 13 步 playbook = master plan §4。**
> **模块已就绪**：`fe-connector-hive` + `fe-connector-hms`（后者是 hive/hudi/iceberg-HMS/paimon-HMS 的**共享元存储库**，P3/P5/P6 已在用、稳定）。

## P7 为什么是复杂度顶点（master plan §3.8）

- **HMS 是共同元数据后端**：hive、hudi、iceberg-HMS-flavor、paimon-HMS-flavor 共用。`fe-connector-hms` 须持续稳定。
- **21 个 metastore event 类 + `MetastoreEventsProcessor`** → 用 P0 已建的 `ConnectorMetaInvalidator` 承接，event pipeline 整体搬进 `fe-connector-hms`（决策 D4）。
- **`HMSTransaction`（1866 行）+ `HiveTransactionMgr`** —— ACID 写路径，**最难**，须重写。**R-002（ACID 数据不一致，High）= 项目最大风险**；gate = 专门的 ACID 集成测试套件。
- **`HMSExternalTable`（1293 行）** —— hive/hudi/iceberg 三种 `dlaType` 分流；由 DLA 模型吸收（决策 D5：`ConnectorTableSchema.tableFormatType` 区分）。
- **31 处反向 `instanceof HMSExternalCatalog/HMSExternalTable`** 散落热路径（`nereids/glue/translator`、`tablefunction/MetadataGenerator`、`AnalyzeTableCommand`、`ShowPartitionsCommand`）。
- **子阶段（见 connectors/hive.md §子阶段）**：P7.1 HiveMetadataOps 全功能搬迁（2 周）→ P7.2 event pipeline 搬 + `ConnectorMetaInvalidator`（1.5 周）→ P7.3 HMSTransaction+HiveTransactionMgr ACID 写路径（2 周）→ P7.4 DLA 分流改造（0.5 周）→ P7.5 删 fe-core `datasource/hive/` + 31 instanceof（0.5 周）。
- **连带清理**：P7 收尾要一并删掉上面那 23 个 HMS-iceberg 支撑类 + fe-core `datasource/hudi/`；**hudi 批 E（live cutover）并入 P7**（PROGRESS §一 P3 note）。
- **Kerberos UGI**：`fe-kerberos`（P3b #64655 已收口）+ `ConnectorContext.executeAuthenticated` 已支持，P7 逐条审查复用（R-004）。

## P7 起步要点（下个 session 开场）

1. **先读**：master plan §3.8 + `connectors/hive.md` + `git show master:` 对照 fe-core `datasource/hive/` 真实代码 recon（**HANDOFF/计划的行号、依赖名、缺口清单可能过时；信控制流不信注释**）。
2. **建 P7 阶段拆分 spec** `tasks/P7-hive-migration.md`（phase-level plan，镜像 `tasks/P6-iceberg-migration.md`）+ 起步 code-grounded recon（AGENT-PLAYBOOK §7）。
3. **决策已定（master plan §5，勿重议）**：D4=event pipeline 放 `fe-connector-hms`；D5=DLA 用 `tableFormatType` 区分；写路径 SPI / procedure SPI / DLA 模型复用 P6 已建成果。
4. **纪律**：每阶段独立 commit + build + test + checkstyle 0 + import-gate 净；**每一轮完成后即更新本 HANDOFF + commit**（memory `handoff-discipline-per-phase`）。ACID 写路径必须有独立集成测试作 gate（R-002）。

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
