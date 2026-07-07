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

# 🚀 下个 session 任务 = **实现翻闸前的"休眠期"连接器 SPI 补齐（设计文档第 4/5 章 阶段 1）**

**本 session 已完成（recon + 设计 + 签字）**：针对"HMS 目录/表 retype 到通用类"的聚焦 code-grounded 侦察（`wf_e0586006-60f`：8 维读者 + 完整性/排序两个 critic，全 HEAD 核对）→ 落地权威设计文档 **`hms-cutover-retype-design-2026-07-07.md`（起步必读 #1）**。核心结论：翻闸是**一次原子、整库同时生效**的切换（靠类身份 `instanceof PluginDriven*` 分派，非运行时开关），前置一大批**休眠期**连接器补齐，末尾一次**循环依赖单元**的机械删除。**已 HEAD 证实**：`HiveConnectorProvider.getType()=="hms"` 已存在；`createSiblingConnector` 全树 0 处（跨插件兄弟 SPI 真缺）；GSON `registerSubtype` HMS 三处（catalog:366/db:447/table:471）；`buildTableInternal` 按**目录级** `SUPPORTS_MVCC_SNAPSHOT` 选基类/Mvcc 子类；`PhysicalPlanTranslator` `:808` PluginDriven 臂先于 `:818` HMS 臂命中。

**已签字决策（2026-07-07，设计文档 §6）**：
- **D1 统计 = 保留免扫描列统计**（新增 `ConnectorStatisticsOps.getColumnStatistics` SPI + Hive/Iceberg 各实现；表级行数照旧）——**列统计 SPI 进入休眠期补齐清单（§4.2a）**。
- **D2 缓存 = 连接器自持**（退休 fe-core 的 Hive/hudi/iceberg 元数据缓存，删 route/loader 的 `instanceof` 站点；随翻闸集落地）。
- **D3 混合 Iceberg 表 = 翻闸前先做好跨插件委派，无倒退**（兄弟连接器 SPI §4.4 是硬前置，滑期则翻闸顺延，不走 fail-loud 拒绝）。
- **D4 打包**（翻闸时再定）：倾向两 PR（可回滚翻闸 + 机械删除）。

**下一步 = 开始休眠期补齐（设计文档 §5 阶段 1，每子步一个独立休眠 commit，线上零影响）**。推荐**首个 commit = §4.1 Hive 连接器 Kerberos 认证器 + 属性校验对齐**（自包含 BLOCKER，有 `IcebergConnector` 现成模板 `:162-177,783-821`，测试门清晰）。其余子步：§4.2 读侧 SPI（`partition_columns`/`listPartitions`/`getTableStatistics`/视图/capabilities）、§4.2a 列统计 SPI、§4.3 MVCC/系统表 + freshness-aware `getTableSnapshot`、§4.4 兄弟 SPI + 网关委派、§4.5 读-ACID 收尾 + 写前检查 + `BIND_BROKER_NAME` 搬家 + engine-map。**翻闸集（原子）/删除单元/硬门 见设计文档 §2/§5/§7，勿在翻闸前动**。

## 开场要点（承接）
1. **先读设计文档 `hms-cutover-retype-design-2026-07-07.md`（权威计划）+ 两份 findings + 本文顶部 🎯 段**。剩余 HMS 迁移 = 一次原子翻闸 + 前置休眠补齐 + 末尾循环删除。
2. **已提交勿回炒**：P7.1 / P7.3(`0b19506acfe`) / 按表 scan seam(`0923077fe67`) 全休眠；replay-CCE fix(`a6dc782d816`) 已合；设计文档 commit `5bfc55f6d59`。
3. **纪律**：设计已签字 → **现在进入实现**：每子步独立休眠 commit（fresh recompile 杜绝 stale `.class`）→ 更新本 HANDOFF。上下文超 30% 找干净节点交接。**path-whitelist `git add`，严禁 `-A`**。铁律见 🧠 起步必读 #3（fe-core 不加 `if(format)`/`instanceof HMSExternal*`/`switch(dlaType)`；不解析属性；跨插件 pin TCCL）。
4. **硬门 = ACID/事件/异构集成测试 + Kerberos-HMS 冒烟**（R-002 最大风险，需 live 路径，勿静默跳过——Rule 12）。full-ACID **写**继续硬拒；full-ACID + insert-only **读**在范围（已落地插件侧）。

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

1. **权威计划 = `hms-cutover-retype-design-2026-07-07.md`**（原子翻闸模型 + 休眠补齐清单 §4 + 能力孪生图 + 阶段序 §5 + 已签字决策 §6 + 硬门 §7；全 recon 明细在 `tool-results/w0bg9i509.output`）。再读**两份 findings 文档**（`iceberg-on-hms-delegation-findings-2026-07-07.md` + `hms-event-pipeline-findings-2026-07-07.md`）+ 本文顶部 🎯/🚀。
2. **样板**：`P5-paimon-migration.md`（翻闸+删 legacy 全流程）；`P6-iceberg-migration.md` + `P6.6-iceberg-flip-blockers-tasklist.md`（净室复审 + 能力孪生审计 + GSON replay 范式）。委派/缝模板 = A 的设计文档 `P7.4-scan-provider-per-table-seam-design.md`。特性地图 = `P7-cutover-scope-map-2026-07-06.md`（排序前提已校正）。
3. **铁律**：fe-core 不得新增 `if(hive/iceberg/hudi)`/`instanceof HMSExternal*`/`switch(dlaType)`/引擎名判别（翻闸靠"表类=通用类 + 网关按句柄委派"，不靠在 `PhysicalPlanTranslator` 加分支）；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）；跨插件/跨边界 pin TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha`，事件轮询后台线程 R-010 亦需）；history_schema_info nested 名 lowercase（memory）。
4. **memory 相关项**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-tracking-issue`。

---

## 背景：跨连接器删除排序（翻闸最硬约束）

`datasource/hive/` **删不掉**，直到非-hive 消费者全 retype 到 generic：`datasource/hudi/HudiUtils`/`HudiScanNode`(extends `HiveScanNode`)/`HudiExternalMetaCache`；`datasource/iceberg/source/IcebergHMSSource`、`statistics/HMSAnalysisTask`、`statistics/util/StatisticsUtil.getIcebergColumnStats`、`datasource/systable/IcebergSysTable`。P6 #64688 删的是原生 iceberg，但 iceberg-on-HMS 仍走 fe-core，故 `datasource/iceberg/` 还保活 ~23 个 HMS-iceberg 支撑类（两 tier，见委派 findings 文档）——翻闸把它们切到连接器路径后才能删。同理 `datasource/hudi/`、`datasource/hive/`。整条 catalog-SPI 阶段链已合入 upstream `branch-catalog-spi`：P0 #63582 · P1 #63641 · P2 #64096 · P3 #64143 · P4 #64300 · P5 #64446+#64653 · P3b #64655 · P6 #64688。
