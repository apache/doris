# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里，见下「起步必读」）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸）。

---

# 🎯 当前状态（2026-07-10）

catalog-SPI 迁移的剩余工作 = **HMS 翻闸**。权威计划 = **`tasks/hms-cutover-execution-plan-2026-07-10.md`**（4 阶段 + DONE 账本 + 已签字决策 + 硬门；**起步必读 #1，行号信 HEAD 不信文档**）。四阶段：

- **Phase 0 连接器休眠补齐** ✅ **DONE**（读 SPI + iceberg/hudi 兄弟委派 + 整条 hudi 线）。
- **Phase 1 翻闸前 fe-core 建设（进行中，真正剩余主活）** — 连接器自持缓存(D2) ✅ → 事件管道 Model B ✅ → **4 个耦合缝(S1–S4) + W6 ✅（代码落地，欠净室复审）** → 剩余(复审 → 原子翻闸前置项)。
- **Phase 2 原子翻闸**（`SPI_READY += hms` + GSON 重映射 + 死臂/删除 + 4 守卫改接新子系统）→ **Phase 3 删除旧代码（最后做）** → **Phase 4 e2e/硬门**。

**⭐ 本轮（2026-07-10 晚）= Phase 1 第三项「翻闸耦合缝」全部落地（4 缝 + W6），停在复审前。**
权威设计 = **`tasks/hive-coupling-seams-step-design-2026-07-10.md`**（**起步必读**；4 缝 TODO 已全勾 + 2 处发现的后续项 + e2e 欠账）。本轮侦察 = `wf_ecf9f292-892`（4 缝读者 + 完备性审查，HEAD 逐条证）。**用户本轮 2 处决策（全选 FULL PARITY）**：① iceberg-on-HMS 统计资格机制 = **做法C**（Hive 转交时反射兄弟连接器能力集，Trino 表重定向语义，顺带关 Top-N/嵌套列裁剪隐患）；② 分桶 Hive 采样 NDV 估算 = **做法2**（透传分桶列完全对齐）。

- **✅ S1 partition_values** `166515cdc88`（上一轮）。
- **✅ S4 auto-analyze 按表门 + 兄弟能力反射** `89c6f9454bb`：侦察坐实 iceberg-on-HMS 的能力从 **hive 连接器**解析(非兄弟)——单去 hive 连接器级 flag 会静默回归 iceberg-on-HMS。修法=`supportsColumnAutoAnalyze()`→`hasScanCapability`、去 hive 连接器级 flag(排除 hudi-on-HMS)、plain-hive 按表发标记、**委派分支 `reflectSiblingScanCapabilities` 把兄弟连接器能力集反射成按表标记**(做法C)。休眠。4 套件绿。
- **✅ S2 hudi_meta 连接器驱动** `d8f2d01978a`：中立 `getMetadataTableRows` SPI(`List<List<String>>`)+`SUPPORTS_METADATA_TABLE`+`HudiConnector` 全活时间线(TCCL pin)+双臂 `hudiMetadataResult`(HMS 臂下沉到 `HudiExternalMetaCache.getTimelineRows`)。**MetadataGenerator 摆脱 `org.apache.hudi`**。休眠。
- **✅ S3 sample-analyze 完整移植** `8469a033abd`：`SUPPORTS_SAMPLE_ANALYZE`(plain-hive 按表)+`canSample`/`isSamplingPartition` 加臂+`PluginDrivenSampleAnalysisTask`(逐字移植 doSample/getSampleInfo/needLimit)+`ConnectorStatisticsOps.listFileSizes` SPI+`getChunkSizes` override+**分桶列透传**(`DISTRIBUTION_COLUMNS_KEY`)+`StatisticsAutoCollector` force-FULL 收窄 `&& !supportsSampleAnalyze()`。休眠。
- **✅ W6 写路径 TCCL = 核实假警报**：兄弟自带 `TcclPinningConnectorContext`，无未固定路径。软化注释 `f53a71f5260`(纯文档)。

**⭐ 下一步 = 净室对抗复审（本轮按用户要求"停在复审前"，未开工）：**
1. **净室对抗复审跨全部 seam commit（S1–S4 + W6）**：memory `clean-room-adversarial-review-pref`——多 agent 先独立判断、后交叉核对；重点核 ① S4 做法C 反射是否漏/误透能力(只应影响 `hasScanCapability` 消费者)、② S3 移植 doSample 字节等价 + `listFileSizes`/`getChunkSizes` TCCL 方向 + 分桶列大小写、③ S2 双臂无破 4 p2 套件(HMS 臂仍活)、④ 全部对 paimon/iceberg 字节+成本不变。修确认发现。
2. **勾设计文档 TODO / 登记 e2e 欠账**：本轮已做（execution-plan §2.3/§2.5/§5 + 设计文档 TODO + 本 HANDOFF）。
3. **2 处本轮发现的后续项（非 3 缝、勿丢，见设计文档「Discovered follow-ups」）**：① 删除步 build-break——`StatisticsAutoCollector.java:36`/`StatisticsCache.java:44` 误 import `org.apache.hudi.common.util.VisibleForTesting`(注解)，hudi 出 fe-core classpath 时会挂，删除步换 guava/doris `@VisibleForTesting`(现无害未改)；② 分区级 FULL analyze——`ExternalAnalysisTask.doFull` 不做旧 `HMSAnalysisTask.doPartitionTable` 分区级(所有插件表通性、非 S3 引入)，若要分区级外表 analyze 另起一步。

**⚠ 与本步解耦、仍待用户拍板的 3 处 D2 缓存复审项**（上一步遗留，见旧 HANDOFF/review2 报告；本轮未动）：① 自发 DROP/CREATE TABLE 不清连接器缓存(HIGH)；② 分区缓存容量单位悄变(MEDIUM)；③ paimon/iceberg `latestSnapshotCache` 现网同形删建洞(线上活性 bug)。下个 session 可择机连同上面 seam 一起或单独找用户定。

**⚠ 翻闸/e2e 欠账（非静默，勿丢）**：所有连接器休眠步(读/写-拒/schema-evolution/缓存/跨加载器委派/本步 4 缝)只在翻闸后 live，须异构 HMS docker e2e 断言（清单见 execution plan §4/§5 + `hive-coupling-seams-step-design` e2e-owed 段 + memory `hms-iceberg-delegation-needs-e2e`）。删除排序最硬约束：`datasource/hive|hudi|iceberg/` ~90 个 HMS 支撑类删不掉直到翻闸切消费者（execution plan §2.4/§3）。

**⚠ 并行 session 风险**：曾有两个 session 同工作树互相 amend；起步先查 `git log`/`git status`/运行中 maven/近 90s mtime 再动手。

---

# 🧠 起步必读（读文档，别炒 git log 历史）

1. **权威翻闸计划** = `tasks/hms-cutover-execution-plan-2026-07-10.md`（4 阶段 + DONE 账本 + 已签字决策 + 原子翻闸集 + 硬门）。
2. **本步（翻闸耦合缝）设计** = `tasks/hive-coupling-seams-step-design-2026-07-10.md`（4 缝 + 3 决策 + TODO；S1 已勾）。**起步先读，再对照 HEAD 核行号**。
3. 已完成的前两项 Phase-1 步：`tasks/hive-connector-cache-step-design-2026-07-10.md`（D2 缓存）、`tasks/hive-event-pipeline-step-design-2026-07-10.md`（事件管道）。
4. **补充权威**：`tasks/hms-cutover-retype-design-2026-07-07.md`（原子翻闸模型 + §6 D1–D6 决策）；`tasks/hms-cutover-sibling-connector-decomposition-2026-07-08.md`（兄弟委派 + CCE/TCCL 硬约束）；hudi = `tasks/hudi-on-hms-delegation-plan-2026-07-08.md` + `tasks/hudi-schema-evolution-step-design-2026-07-09.md`。
5. **样板**：`tasks/P5-paimon-migration.md`、`tasks/P6-iceberg-migration.md`（净室复审 + 能力孪生 + GSON replay 范式）。
6. 完成工作明细 = `git log`（commit message 详尽）；勿在 HANDOFF 里重述。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi`）。PR base = `branch-catalog-spi`，**squash 合并**。
- **公开 tracking issue = apache/doris#65185**（进度按已合入 `branch-catalog-spi` PR 口径）。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**：工作树有大量历史遗留 scratch（`*.bak` / `regression-conf.groovy` 明文 key / `.audit-scratch/` / `conf.cmy/` / `META-INF/` / `docker/...` / `plan-doc/reviews/P5-*` / `.claude/` / `failed-cases.out`——**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat|fix|doc](catalog) …` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每子步/每条 fix = 独立 commit**；HANDOFF + 设计文档单独 commit（与 code 分开）。上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

# ⚙️ 操作须知（构建/测试，复用）

- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`（**漏 `-am`→假错 `${revision}`**）。连接器：`-pl fe-connector/<mod> -am`。单测加 `-Dtest=<Class>`。
- **验证信 LOG 不信 exit**：`-q` + `| tail` 会吞掉 `BUILD SUCCESS` 且管道 exit=tail 的(不可信)；改**重定向到文件**跑（不加 `-q`），再 grep `BUILD SUCCESS`/`[ERROR].*\.java:`/checkstyle `You have N violations`（memory `doris-build-verify-gotchas`）。test-compile 的 `validate` 阶段已跑 checkstyle（本轮见 fe-core `0 violations`）。
- **build 中 ANTLR `mismatched input '->'`/`super::` 噪音 = 非致命**（某 codegen/工具 Java 语法解析器不识 lambda/方法引用；本轮 BUILD SUCCESS 照出）。
- **import gate**：`bash tools/check-connector-imports.sh`（repo 根跑）。HMS `HiveVersionUtil` 命中 = 误报（memory `catalog-spi-hms-hiveversionutil-gate-false-positive`）。
- **⚠️ bash 默认 timeout 120s**：fe-core 全量编译 ~3min+ → 后台跑 + 读 LOG。**⚠️ `/mnt/disk1` 盘紧(~82%)；勿用 worktree 隔离编译 agent**。cwd 会被重置 → 绝对路径。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**（`mock(接口)` 不跑 default 方法返 null；`mockStatic(Env)` 惯用）。checkstyle 禁 static import、扫 test 源（memory `catalog-spi-fe-core-test-infra`）。

# 🔒 铁律（fe-core 约束）

- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。能力按表判要用 `hasScanCapability`/`PER_TABLE_CAPABILITIES_KEY`（连接器发标记，fe-core 不看格式/dlaType）。
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector，均插件侧；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL** 到连接器 classloader（扫描线程/写-DDL 引擎线程/iceberg worker 池/事件轮询后台线程；memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
- `history_schema_info` 嵌套字段名逐层 lowercase（否则 BE SIGABRT；memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable` 是 paimon/iceberg **实时**基类：改其共享方法须对二者字节+成本双不变（本轮 S1 即因此把 `getNameToPartitionValues` 做成独立方法、不动 `getNameToPartitionItems`；memory `plugindriven-mvcc-table-is-live-not-dormant`）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `memory-keep-only-general-or-requested`。
