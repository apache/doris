# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-07）

**已提交（休眠）**：P7.1（DDL 元数据）+ P7.3（`0b19506acfe`：hive/hms 写事务+提交器+写计划+读侧 ACID 生产者+读事务管理器）+ P7.4 子批 A（`0923077fe67`：`Connector.getScanPlanProvider(handle)` 按表选扫描器接缝）。hive 尚未进 `SPI_READY_TYPES`，线上零路由。

**本 session 关键发现（重大计划校正，务必先读）**：对 iceberg-on-HMS 委派 + 事件管道两轮 code-grounded 侦察（`wf_24c2052f-198` / `wf_46c0c020-08f`）+ 亲手核对 HEAD 证实——**交接旧叙述把剩余工作拆成"翻闸前可独立落地的 6 子批（A–F）"，这个前提基本是错的**。真实结构性事实：

- **`type=hms` 目录今天 100% 是旧类**：`CatalogFactory` case `"hms"` → `new HMSExternalCatalog`（`CatalogFactory.java:133-134`）；`HMSExternalTable extends ExternalTable`（非 `PluginDrivenExternalTable`）；`HMSExternalCatalog extends ExternalCatalog`（非 `PluginDrivenExternalCatalog`）。
- **按表选扫描器接缝只在 `PluginDrivenScanNode` 内被调用**，而它只对 `instanceof PluginDrivenExternalTable` 的表生效（`PhysicalPlanTranslator.java:808`）；HMS 表走旧 `switch(dlaType)`→`IcebergScanNode` 分支（`:818-847`）。
- **只有 `PluginDrivenExternalCatalog` 持有插件连接器**（`getConnector()`）；旧 `HMSExternalCatalog` 无连接器，用旧 `HMSCachedClient`。fe-core 只 maven 依赖 `fe-connector-api/-spi`，插件类经**独立 child-first 类加载器**加载。
- **⇒ 结论**：iceberg/hudi 格式委派、事件管道搬迁，**都需要"HMS 目录先变成通用目录类（持有插件连接器）"才可达/可测**。它们不是翻闸前的独立步骤，而是**同一次翻闸的不同侧面**。翻闸前只能预置休眠脚手架或落"翻闸前减负"小修复。跨插件委派**不能靠加 maven 依赖/共包**（会出现第二份 AWS SDK，JVM 级毒化 S3，Paimon RC-3 有记录），须新增"网关拿到兄弟连接器"的中立 SPI。

**本 session 已落地（翻闸前唯一干净独立的减负项）**：`a6dc782d816` `[fix](catalog)` 消除编辑日志重放的 CCE 隐患——`ExternalMetaIdMgr.replayMetaIdMappingsLog` 原来强转 `(HMSExternalCatalog)` 只为取 id（日志本就带 catalogId），翻闸后会 CCE 使 FE 重放崩溃起不来；改为按 `catalogId(long)` 传，行为今天逐字不变，拆掉翻闸时必爆的雷。全绿（fe-core 编译 BUILD SUCCESS + `ExternalMetaIdMgrTest` 2 测过 + checkstyle 0）。

**两份侦察存档（起步必读，别重炒）**：
- `plan-doc/tasks/iceberg-on-hms-delegation-findings-2026-07-07.md` — 委派为何 flip-gated + 跨插件 handoff 机制（对照 Trino）+ 具体设计草图 + 删 23 类/按列统计(建议 DROP)/`IcebergUtils` 抽 6 个纯 helper 的推迟清单。
- `plan-doc/tasks/hms-event-pipeline-findings-2026-07-07.md` — 事件管道 flip-gated + 主障碍（事件做**结构性变更**非仅失效，中立 `ConnectorMetaInvalidator` 缺 register/rename 动词 + 分区 NAME 粒度缺口）+ 推荐 Model B（薄 fe-core 角色驱动 + 插件 pollOnce SPI）+ `ExternalMetaIdMgr` 可对 HMS 弃用（但 opcode 470 须保重放）。

---

# 🚀 下个 session 任务 = **执行翻闸（把 HMS 目录/表改造成通用类）**

翻闸是全项目最大最险的一块，是解锁一切（格式委派/事件/删旧类）的**真正关口**。样板 = P5 paimon（`P5-paimon-migration.md`）、P6 iceberg（`P6-iceberg-migration.md` + `P6.6-iceberg-flip-blockers-tasklist.md`），机制相同、但 HMS 因**异构（三格式）+ 事件管道 + 写链 + DLA** 而更大。

**开工方式（守纪律，别一上来写代码）**：先做一轮**针对"HMS 目录/表 retype 到通用类"的 code-grounded 侦察 + 设计文档**——把旧 `HMSExternalCatalog`/`HMSExternalTable` 提供的每项能力（元数据 init、schema、分区、统计、DLA 分派、MVCC、系统表、事件、写/DDL）逐项映射到连接器 SPI，标出缺口（跨插件兄弟连接器 SPI、事件 Model-B 驱动 + 结构性变更 SPI、GSON `registerCompatibleSubtype` 兼容、按表 MVCC 能力），并把翻闸拆成**内部有序阶段**（连接器补齐→加 `"hms"` 进 `SPI_READY_TYPES`→GSON 兼容→删旧类→ACID/事件/异构 e2e 硬门 R-002）。**每个开放决策先中文讲背景+示例+推荐、不引任务代号，请用户签字**再实现。

翻闸内部要覆盖的特性清单（把旧"6 子批"当**特性 inventory** 用，但**不再当作翻闸前的独立步骤**）见 `P7-cutover-scope-map-2026-07-06.md`（其 A–F 分解仍是有用的能力地图；**其"翻闸前可独立落地"的排序前提已被本 session 校正**）。

## 开场要点（承接）
1. **先读两份 findings 文档 + 本文顶部 🎯 段**。剩余 HMS 迁移 ≈ 一次翻闸；B/C/D 是它的侧面，非独立前置步。
2. **已提交勿回炒**：P7.1 / P7.3(`0b19506acfe`) / A(`0923077fe67`) 全休眠；replay-CCE fix(`a6dc782d816`) 已合。
3. **纪律**：先 code-grounded recon → 设计文档 → 用户签字该阶段专属决策 → 实现 → 独立 commit → 更新本 HANDOFF。上下文超 30% 找干净节点交接。**path-whitelist `git add`，严禁 `-A`**。
4. **硬门 = ACID/事件/异构集成测试**（R-002 最大风险，需 live 路径，勿静默跳过——Rule 12）。full-ACID **写**继续硬拒；full-ACID + insert-only **读**在范围（已落地插件侧）。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**。**打包/复审策略（翻闸前/后、单 PR vs 分 PR）= 翻闸阶段的开放决策**（paimon 分 PR vs iceberg 合并 squash 两先例）。
- **公开 tracking issue = apache/doris#65185**；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（工作树有历史遗留 scratch：`*.bak`·`regression-test/conf/regression-conf.groovy` 明文 key·`.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·`plan-doc/reviews/P5-*`·`.claude/`·`failed-cases.out`——均**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat](catalog) …` / `[fix](catalog) …` / `[doc](catalog) …` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每阶段/每条 fix = 独立 commit**；HANDOFF + 设计/findings 文档单独 commit（与 code 分开）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。跑单测加 `-Dtest=<Class>`。**checkstyle 别加 `-am`**：`mvn -pl :<art> checkstyle:check`。artifactId：`fe-core` / `fe-connector-api` / `fe-connector-hive` / `fe-connector-hms` / `fe-connector-iceberg` / `fe-connector-hudi`。
- **⚠️ bash 工具默认 timeout 120s**：fe-core 全量编译 ~1.5–2min → **务必**把 `timeout` 调到 ~580000ms。**后台/管道 exit 不可信**——读 LOG 的 `BUILD SUCCESS` 行 + surefire `Tests run=/Failures=/Errors=`。改代码后 commit 前 fresh recompile 杜绝 stale `.class`。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**（`mockStatic(Env.class)` 是本仓惯用法，191 处；stub `Env.getCurrentEnv()`→自定义 `TestingEnv extends Env{super(true)}` 覆写 getter，见 `ExternalMetaCacheRouteResolverTest` / 本轮 `ExternalMetaIdMgrTest`）。⚠️ `Mockito.mock(接口)` **不跑 default 方法**（返 null）。checkstyle **禁 static import**、**扫 test 源**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**repo 根**，非 `fe/`）。**HMS `HiveVersionUtil` 命中 = 误报非违规**（memory）。
- **cwd 会被 harness 重置** → 一律绝对路径。**⚠️ `/mnt/disk1` 紧**（~82% used）；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 起步必读

1. **两份 findings 文档**（`iceberg-on-hms-delegation-findings-2026-07-07.md` + `hms-event-pipeline-findings-2026-07-07.md`）+ 本文顶部 🎯/🚀。
2. **样板**：`P5-paimon-migration.md`（翻闸+删 legacy 全流程）；`P6-iceberg-migration.md` + `P6.6-iceberg-flip-blockers-tasklist.md`（净室复审 + 能力孪生审计 + GSON replay 范式）。委派/缝模板 = A 的设计文档 `P7.4-scan-provider-per-table-seam-design.md`。特性地图 = `P7-cutover-scope-map-2026-07-06.md`（排序前提已校正）。
3. **铁律**：fe-core 不得新增 `if(hive/iceberg/hudi)`/`instanceof HMSExternal*`/`switch(dlaType)`/引擎名判别（翻闸靠"表类=通用类 + 网关按句柄委派"，不靠在 `PhysicalPlanTranslator` 加分支）；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）；跨插件/跨边界 pin TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha`，事件轮询后台线程 R-010 亦需）；history_schema_info nested 名 lowercase（memory）。
4. **memory 相关项**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-tracking-issue`。

---

## 背景：跨连接器删除排序（翻闸最硬约束）

`datasource/hive/` **删不掉**，直到非-hive 消费者全 retype 到 generic：`datasource/hudi/HudiUtils`/`HudiScanNode`(extends `HiveScanNode`)/`HudiExternalMetaCache`；`datasource/iceberg/source/IcebergHMSSource`、`statistics/HMSAnalysisTask`、`statistics/util/StatisticsUtil.getIcebergColumnStats`、`datasource/systable/IcebergSysTable`。P6 #64688 删的是原生 iceberg，但 iceberg-on-HMS 仍走 fe-core，故 `datasource/iceberg/` 还保活 ~23 个 HMS-iceberg 支撑类（两 tier，见委派 findings 文档）——翻闸把它们切到连接器路径后才能删。同理 `datasource/hudi/`、`datasource/hive/`。整条 catalog-SPI 阶段链已合入 upstream `branch-catalog-spi`：P0 #63582 · P1 #63641 · P2 #64096 · P3 #64143 · P4 #64300 · P5 #64446+#64653 · P3b #64655 · P6 #64688。
