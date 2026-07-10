# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里，见下「起步必读」）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸）。

---

# 🎯 当前状态（2026-07-10）

catalog-SPI 迁移的剩余工作 = **HMS 翻闸**。权威计划 = **`tasks/hms-cutover-execution-plan-2026-07-10.md`**（4 阶段 + DONE 账本 + 已签字决策 + 硬门；**起步必读 #1，行号信 HEAD 不信文档**）。四阶段：

- **Phase 0 连接器休眠补齐** ✅ **DONE**（读 SPI + iceberg/hudi 兄弟委派 + 整条 hudi 线）。
- **Phase 1 翻闸前 fe-core 建设（进行中，真正剩余主活）** — 连接器自持缓存(D2) ✅ → 事件管道 Model B ✅ → 4 个耦合缝(S1–S4)+W6 ✅ → 耦合缝净室复审 ✅ → **缓存失效收尾 3 项 ✅（本轮：用户拍板 + 已修 + 已验）** → 剩余（本轮 3 修的净室对抗复核，正在跑 → 原子翻闸前置项）。
- **Phase 2 原子翻闸**（`SPI_READY += hms` + GSON 重映射 + 死臂/删除 + 4 守卫改接新子系统）→ **Phase 3 删除旧代码（最后做）** → **Phase 4 e2e/硬门**。

**⭐ 本轮（2026-07-10）= 连接器缓存第二轮净室复审留下的 4 个"缓存失效"发现，逐一对照 HEAD 核实后收尾。**
先做的独立核实（`wf_daed84c1-4ea`）有一处**重要更正**：其中一个 HIGH（REFRESH 刷不到 iceberg 兄弟快照缓存）**在 HEAD 已经修好了**（`forEachBuiltSibling` 已接进 invalidate\*），撤销无需处理。剩下 3 项按用户拍板落地，**每项独立 commit + 独立构建验证（BUILD SUCCESS + 0 checkstyle）**：

- **✅ D1（fe-core 通用钩子，线上改动）** `3b66982fedf`：Doris 自执行 `DROP/CREATE TABLE`、`DROP DATABASE` 从不清连接器自持缓存（只有 REFRESH 清）→ **同名 drop+recreate 命中老表快照/元数据（最长 24h TTL）**。休眠的 hive（翻闸后触发）+ **线上的 paimon/iceberg（现在就触发）** 一次修好：在 `PluginDrivenExternalCatalog` 的 drop/create-table、dropDb 里统一调 `connector.invalidate*`（REMOTE 名，照搬 Trino"谁改动谁清"）；`createDb` 有意不接。`PluginDrivenExternalCatalogDdlRoutingTest` 56 绿。
- **✅ D1 补丁（paimon 第二层缓存）** `7b8fed012be`：`PaimonSchemaAtMemo`（时间旅行 schema memo）原本无 invalidate 入口 → 补 `invalidate/invalidateAll` 并接进 `PaimonConnector.invalidateTable/invalidateAll`，补齐 paimon 侧的 drop+recreate 洞（iceberg 无需：manifest 按路径 key + 快照缓存已被同一钩子清）。`PaimonSchemaAtMemoTest` 5 绿。
- **✅ D2（分区缓存容量单位）** `982db925659`：`CachingHmsClient` 分区对象缓存原按"整条请求名列表"做 key（100000 变成"10 万个列表"而非"10 万分区对象"，可撑爆 FE 堆）→ 改成**每分区一个 value key**（对齐 Trino + 老实现），批量取分区做部分命中 + 只补缺失；正确性与名字解析保真度无关（store 永远按分区真实 values，误解析只退化成重取，绝不错/漏）。休眠。`CachingHmsClientTest` 15 绿。

设计文档（**起步必读**，含正确性论证 + e2e 欠账）= **`tasks/hive-cache-invalidation-followups-design-2026-07-10.md`**（§6 状态：3 修全勾 + 复审在跑）。

**⭐ 下一步：**
1. **本轮 3 修的净室对抗复核（正在后台跑，`wf_fe6ddef4-777`）** — 4 维独立冷读（D1/D1补丁/D2/完备性）+ 每发现 3-lens 对抗核实。**下个 session 起步先取它的结论**：无存活发现即收尾；有则按 severity 修（D1/D1补丁是线上路径，务必核对 paimon/iceberg 行为/成本；见 memory `plugindriven-mvcc-table-is-live-not-dormant`）。
2. **原子翻闸前置项**（Phase 2 集：`SPI_READY += hms`、GSON `HMSExternalTable`→`PluginDrivenMvccExternalTable` 重映射、4 守卫改接新子系统、死臂/删除排序）——见 execution-plan §2/§3。**翻闸前 fe-core 建设的外部输入已全部消化**（缓存决策已拍板落地），复核通过即可开 Phase 2。
3. **设计文档「Discovered follow-ups」2 项欠账（勿丢）**：① 删除步把 `StatisticsAutoCollector.java`/`StatisticsCache.java` 的 `org.apache.hudi...VisibleForTesting` 换 guava/doris；② 分区级 FULL analyze 缺失（所有插件表通性、非本轮引入）。

**⚠ 翻闸/e2e 欠账（非静默，勿丢）**：所有连接器休眠步 + 本轮 3 修只在翻闸后 live（hive）；**paimon/iceberg 的 drop+recreate 修复现在就 live，可即刻 e2e 回归**（`DROP TABLE t; CREATE TABLE t(新schema/位置); SELECT` 无需 REFRESH 见新表；paimon 时间旅行复用 schemaId 见新 schema）。异构 HMS docker e2e 清单见 `hive-cache-invalidation-followups-design` §5 + execution plan §4/§5 + memory `hms-iceberg-delegation-needs-e2e`。删除排序最硬约束：`datasource/hive|hudi|iceberg/` ~90 个 HMS 支撑类删不掉直到翻闸切消费者（execution plan §2.4/§3）。

**⚠ 并行 session 风险**：曾有两个 session 同工作树互相 amend；起步先查 `git log`/`git status`/运行中 maven/近 90s mtime 再动手（本轮见到的 `gq` 用户 maven 是**别的工作树**，非本树）。

---

# 🧠 起步必读（读文档，别炒 git log 历史）

1. **权威翻闸计划** = `tasks/hms-cutover-execution-plan-2026-07-10.md`（4 阶段 + DONE 账本 + 已签字决策 + 原子翻闸集 + 硬门）。
2. **本轮（缓存失效收尾）设计** = `tasks/hive-cache-invalidation-followups-design-2026-07-10.md`（D1 fe-core 钩子 + D2 每分区 keying + 正确性论证 + §6 状态 + e2e 欠账）。**起步先读，再对照 HEAD 核行号**。
3. 已完成的前几项 Phase-1 步：`tasks/hive-connector-cache-step-design-2026-07-10.md`（D2 缓存本体）、`tasks/hive-event-pipeline-step-design-2026-07-10.md`（事件管道）、`tasks/hive-coupling-seams-step-design-2026-07-10.md`（4 耦合缝）。第二轮缓存复审报告 = `plan-doc/reviews/hive-connector-cache-cleanroom-review2-2026-07-10.md`（本轮 3 修的来源；注意 §2.1 已在 HEAD 修好、§2.3 说"Trino 用 weigher"经复核为**不准确**——Trino 也是每分区一条目按数量封顶）。
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

- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`（**漏 `-am`→假错 `${revision}`**）。连接器：`-pl :fe-connector-<mod> -am`。单测加 `-Dtest=<Class>`。
- **⚠️ paimon 模块必须用 `install`（不是 `test`）验证**：`fe-connector-paimon` 的 `HiveConf` 来自 `fe-connector-paimon-hive-shade`，其 maven-shade 绑在 `package` 阶段；`mvn test` 停在 `test` 阶段→shade jar 没产出→paimon 编译报 `package org.apache.hadoop.hive.conf does not exist`（**误导：非你的改动**）。用 `-pl :fe-connector-paimon -am install`。hms/hive 无此坑（hive-common 是普通依赖）。
- **验证信 LOG 不信 exit**：`-q` + `| tail` 会吞掉 `BUILD SUCCESS`；改**重定向到文件**跑（不加 `-q`），再 grep `BUILD SUCCESS`/`[ERROR].*\.java:`/`Tests run:`/checkstyle `You have N violations`（memory `doris-build-verify-gotchas`）。
- **build 中 ANTLR `mismatched input '->'`/`super::` 噪音 = 非致命**。
- **import gate**：`bash tools/check-connector-imports.sh`（repo 根跑）。HMS `HiveVersionUtil` 命中 = 误报（memory `catalog-spi-hms-hiveversionutil-gate-false-positive`）。
- **⚠️ bash 默认 timeout 120s**：全量编译 ~3min+ → 后台跑 + 读 LOG。**⚠️ `/mnt/disk1` 盘紧；勿用 worktree 隔离编译 agent**。cwd 会被重置 → 绝对路径。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**（`PluginDrivenExternalCatalogDdlRoutingTest` 用 `mock(Connector.class)` 直接 verify invalidate\*）。checkstyle 禁 static import、扫 test 源。

# 🔒 铁律（fe-core 约束）

- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。本轮 D1 钩子调 `connector.invalidateTable/invalidateDb` 是**通用 SPI 调用**（no-op 默认），不违反。
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector，均插件侧；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL** 到连接器 classloader（扫描线程/写-DDL 引擎线程/iceberg worker 池/事件轮询后台线程；memory `catalog-spi-plugin-tccl-classloader-gotcha`）。invalidate\* 是直接方法派发（非按名反射），照搬 RefreshManager 现有调用不需 pin。
- `history_schema_info` 嵌套字段名逐层 lowercase（否则 BE SIGABRT；memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable` 是 paimon/iceberg **实时**基类：改其共享方法须对二者字节+成本双不变（memory `plugindriven-mvcc-table-is-live-not-dormant`）。**本轮 D1 是有意的线上行为改动（修 drop+recreate 洞），非字节中性——已配净室复审 + e2e 欠账把关。**

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `memory-keep-only-general-or-requested`。
