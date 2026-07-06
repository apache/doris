# 🤝 Session Handoff

> 滚动文档：每次 session 结束**覆盖式更新**（不累积历史；历史见 `git log plan-doc/HANDOFF.md`）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)
> **范围** = catalog-spi **主线** handoff。metastore/storage 抽取子线**已 CLOSED**（合入 #64446/#64653/#64655）——`metastore-storage-refactor/` 仅历史留存、勿读；metastore-spi 现状直接读代码 `fe/fe-connector/fe-connector-metastore-spi/`。

---

# 🎯 当前状态（2026-07-07）

**插件侧写/读能力已全部提交（休眠）= `0b19506acfe`**（hive/hms 写事务 + 提交器 + 写计划 + 读侧 ACID 生产者 + 读事务管理器；hive 尚未进 `SPI_READY_TYPES`，线上零路由）。P7.1（DDL 元数据）也已提交。

**关键校正（勿再误读）**：交接旧叙述把"下一步 = cutover"写成一次轻量翻闸，**实为错**。对照 HEAD 的 6-agent 侦察（`wf_536a2968-2c8`）证实：剩余"cutover"是**整个 P7 尾段、全项目最大最险的一块**，已拆成 **6 个子批 A–F**，完整地图 + 每子批开放决策见 **`plan-doc/tasks/P7-cutover-scope-map-2026-07-06.md`**（起步必读）。

**子批清单 + 进度**（依赖序）：
- **A（多格式分发地基 / 按表选 scan-provider 缝）—— ✅ DONE `0923077fe67`（2026-07-07）**。加 `Connector.getScanPlanProvider(ConnectorTableHandle)` 默认委派无参版；`PluginDrivenScanNode` 11 个取 provider 点收敛到 `resolveScanProvider()`（按 `currentHandle`）。**休眠**、对现有 7 连接器逐字不变。设计 = `plan-doc/tasks/P7.4-scan-provider-per-table-seam-design.md`。验证：api 58 测 + core `PluginDrivenScanNode*` 76 测 + checkstyle 0 + import-gate 净。用户签字：纯地基、放 `Connector`（非 `ConnectorMetadata`）、tableFormatType threading 延后。
- **B（iceberg-on-HMS 委派）—— ⏭ 下一步**。把"Hive 元存储上的 Iceberg 表"经 hms 网关按表委派给 iceberg 连接器（用 A 的缝）；补按列统计 SPI（保 `enableFetchIcebergStats`）；抽 `IcebergUtils` 纯 helper 使 fe-core 去 iceberg-SDK。骑在 A 上。
- **C（hudi 上线切换）**：搬 `datasource/hudi`(15 文件)入 `fe-connector-hudi`；拆 `HudiDlaTable`/`HudiScanNode extends HiveScanNode`；延后特性（增量/schema-evo/time-travel）移植或保持 fail-loud。骑在 A 上。
- **D（事件管道搬迁）**：21 事件类 + processor 搬 `fe-connector-hms`；中立 `ConnectorMetaInvalidator`；fe-core 留薄 role-aware driver；R-010 线程生命周期 + TCCL。**独立、可并行**。
- **E（表类改造 + 写链改造 + 读事务接线）**：中立化 42 `instanceof HMSExternal*` + 22 `dlaType`；`HMSExternalTable`→generic；删旧 hive sink（6-8 文件）；`ConnectorSession` 加 query-finish 缝；`Env` 摘 `HiveTransactionMgr`；`HMSAnalysisTask` 中立；搬 `BIND_BROKER_NAME`。骑在 A + 部分 D。
- **F（翻闸 + GSON 兼容 + 删旧 + 硬门）**：`SPI_READY_TYPES` 加 `"hms"`；`GsonUtils` 三工厂 `registerCompatibleSubtype`；分批跨连接器删 `datasource/hive`+`hudi`+23 iceberg 类；净室对抗复审 + 能力孪生审计 + GSON replay 测 + **docker ACID/事件/异构 e2e 硬门（R-002）** + 用户二签。**严格最后**。

---

# 🚀 下个 session 任务 = **子批 B（iceberg-on-HMS 委派）**

> 起步先读 `P7-cutover-scope-map-2026-07-06.md`（§B 段 + 删序约束）+ A 的设计文档（缝的契约）。B 是**首个真实委派**，验证 A 的地基。

- **做什么**：hms 网关连接器（`fe-connector-hive`）**依赖 `fe-connector-iceberg`**；其 `getScanPlanProvider(handle)` 覆写，对 iceberg-on-HMS 句柄委派给 iceberg 连接器的 scan provider（普通 hive 表仍走 hive provider）。网关需在 `getTableHandle` 探测格式并把它编进**自己的**句柄子类（判别标识是**连接器内部**事，fe-core 不看格式）。
- **连带（B 范围）**：补 `ConnectorStatisticsOps` 按列统计方法 + iceberg 实现（保 `enableFetchIcebergStats` 快路径）；抽 `IcebergUtils` 纯 helper（`isIcebergRowLineageColumn`/`getEffectiveIcebergFormatVersion`/行血缘常量）到 SDK-free util，使删 23 类后 fe-core 去 iceberg-SDK。
- **删序**：`IcebergHMSSource`/`IcebergScanNode`/`IcebergSysTable`/`StatisticsUtil.getIcebergColumnStats`/`HMSAnalysisTask` 等**retype 到 generic 后**才能删 `datasource/iceberg` 23 类；`datasource/hive` 要等 B + C 都完成（HudiScanNode/HudiUtils 也解绑）。
- **B 的开放决策（recon 后用户签字，先中文讲背景+示例+推荐，不引任务代号）**：格式判别标识形态（枚举 vs 串 vs capability，连接器内部）；按列统计 SPI vs 放弃 per-column HMS-iceberg 统计；`IcebergUtils` 抽纯 helper vs 留 SDK-linked shrunk 版。

## 开场要点（承接）
1. **起步先读** `P7-cutover-scope-map-2026-07-06.md`（6 子批全图 + 删序约束 + 每子批开放决策）+ 本文顶部 🎯 段。cutover ≠ 一次翻闸，是 A–F 六批。
2. **A 已提交（`0923077fe67`）+ 全绿 + 休眠，勿重写/勿再复审**。P7.1 + P7.3（`0b19506acfe`）同样已提交、勿回炒。
3. **纪律**：每子批 = 先 code-grounded recon → 设计文档 → 用户签字该批专属决策 → 实现 → 独立 commit → 更新本 HANDOFF。上下文超 30% 找干净节点交接。**path-whitelist `git add`，严禁 `-A`**。
4. **硬门 = ACID/事件/异构集成测试**在 F（R-002 最大风险，需 live 路径，勿静默跳过——Rule 12）。full-ACID **写**继续硬拒；full-ACID + insert-only **读**在范围（已落地插件侧）。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi` @ `8b391c7459d`）。PR base = `branch-catalog-spi`，**squash 合并**。**打包/复审策略（flip 前/后、单 PR vs 分 PR）= F 的开放决策**（paimon 分 PR vs iceberg 合并 squash 两先例）。
- **公开 tracking issue = apache/doris#65185**；P7 PR 应引用它。进度按已合入 `branch-catalog-spi` PR 口径。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**（工作树有历史遗留 scratch：`*.bak`·`regression-test/conf/regression-conf.groovy` 明文 key·`.audit-scratch/`·`conf.cmy/`·`META-INF/`·`docker/...`·`plan-doc/reviews/P5-*`·`.claude/`——均**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat](catalog) P7.x …` / `[doc](catalog) …` 范式 + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每子批/每条 fix = 独立 commit**；HANDOFF + 设计文档单独 commit（与 code 分开）。A 已是普通独立 commit（非 P7.3 那种原子批）。

# ⚙️ 操作须知（复用）

- maven：`-f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl :<art> **-am** -DfailIfNoTests=false -Dmaven.build.cache.enabled=false`（漏 `-am`→假错 `${revision}`）。**checkstyle 别加 `-am`**：`mvn -pl :<art> checkstyle:check`。artifactId：`fe-core` / `fe-connector-api` / `fe-connector-hive` / `fe-connector-hms` / `fe-connector-iceberg` / `fe-connector-hudi`。
- **⚠️ bash 工具默认 timeout 120s**：fe-core 全量编译 > 2min → **务必**把 `timeout` 调到 ~580000ms（曾因忘调而被 SIGTERM 打断）。**后台/管道 exit 不可信**——读 LOG 的 `BUILD SUCCESS` 行 + surefire `Tests run=/Failures=/Errors=`。改代码后 commit 前 fresh recompile 杜绝 stale `.class`。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**。⚠️ `Mockito.mock(接口)` **不跑 default 方法**（会返 null）——给接口加 default 方法后，凡 mock 该接口并只 stub 旧方法的测试都要补 stub 新方法（A 即遇此：`getScanPlanProvider(handle)` 在 bare mock 上返 null，须 stub arg 重载）。checkstyle **禁 static import**、**扫 test 源**。
- 连接器禁 import fe-core：`bash tools/check-connector-imports.sh`（**repo 根**，非 `fe/`）。**HMS `HiveVersionUtil` 命中 = 误报非违规**（memory）。
- **cwd 会被 harness 重置** → 一律绝对路径。**⚠️ `/mnt/disk1` 紧**（~82% used）；**勿用 worktree 隔离编译 agent**（复制整仓，盘不够）。

# 🧠 起步必读

1. **`plan-doc/tasks/P7-cutover-scope-map-2026-07-06.md`**（6 子批全图 + keystone + 删序 + 每子批开放决策 + Trino 对比）+ 本文顶部 🎯/🚀。
2. **样板**：`P5-paimon-migration.md`（翻闸+删 legacy 全流程）；`P6-iceberg-migration.md` + `P6.6-iceberg-flip-blockers-tasklist.md`（净室复审 + 能力孪生审计 + GSON replay 范式）。委派/缝模板 = 本轮 A 的设计文档。
3. **铁律**：fe-core 不得新增 `if(hive/iceberg/hudi)`/`instanceof HMSExternal*`/`switch(dlaType)`/引擎名判别（新缝走中立 SPI / ConnectorCapability）；fe-core 不解析属性（memory `catalog-spi-no-property-parsing-in-fecore`）；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）；插件跨边界 pin TCCL（memory `catalog-spi-plugin-tccl-classloader-gotcha`）；history_schema_info nested 名 lowercase（memory）。
4. **memory 相关项**：`handoff-discipline-per-phase`、`clean-room-adversarial-review-pref`、`ask-user-explain-in-chinese-first`、`session-handoff-at-30pct-context`、`doris-build-verify-gotchas`、`catalog-spi-fe-core-test-infra`、`catalog-spi-tracking-issue`。

---

## 背景：跨连接器删除排序（**F 最硬约束**，B/C 为其解绑）

`datasource/hive/` **删不掉**，直到以下非-hive 消费者全 retype 到 generic（否则编译断）：`datasource/hudi/HudiUtils`/`HudiScanNode`（extends `HiveScanNode`）/`HudiExternalMetaCache`（C 解绑）；`datasource/iceberg/source/IcebergHMSSource`、`statistics/HMSAnalysisTask`（`setTable(HMSExternalTable)`）、`statistics/util/StatisticsUtil.getIcebergColumnStats`(iceberg SDK)、`datasource/systable/IcebergSysTable`（B 解绑）。P6 #64688 删的是原生 iceberg，但 iceberg-on-HMS 仍走 fe-core，故 `datasource/iceberg/` 还保活 23 个 HMS-iceberg 支撑类——B 把它们切到连接器路径后，F 才能删。同理 `datasource/hudi/`、`datasource/hive/` 在 C/E/F 范围。整条 catalog-SPI 阶段链已合入 upstream `branch-catalog-spi`：P0 #63582 · P1 #63641 · P2 #64096 · P3 #64143 · P4 #64300 · P5 #64446+#64653 · P3b #64655 · P6 #64688。
