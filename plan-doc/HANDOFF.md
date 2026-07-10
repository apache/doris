# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里，见下「起步必读」）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸）。

---

# 🎯 当前状态（2026-07-10）

catalog-SPI 迁移的剩余工作 = **HMS 翻闸**。权威计划 = **`tasks/hms-cutover-execution-plan-2026-07-10.md`**（4 阶段 + DONE 账本 + 已签字决策 + 硬门；**起步必读 #1，行号信 HEAD 不信文档**）。四阶段：

- **Phase 0 连接器休眠补齐** ✅ **DONE**（读 SPI + iceberg/hudi 兄弟委派 + 整条 hudi 线）。
- **Phase 1 翻闸前 fe-core 建设（进行中，真正剩余主活）** — 连接器自持缓存(D2) ✅ → 事件管道 Model B ✅ → **3 个 loud-break 耦合缝 + W6（本步，进行中）** → 剩余。
- **Phase 2 原子翻闸**（`SPI_READY += hms` + GSON 重映射 + 死臂/删除 + 4 守卫改接新子系统）→ **Phase 3 删除旧代码（最后做）** → **Phase 4 e2e/硬门**。

**⭐ 本轮（2026-07-10 晚）= Phase 1 第三项「翻闸耦合缝」开工：HEAD 对齐侦察 + 3 处用户决策(全选逐位一致) + 第一缝落地。**
权威设计 = **`tasks/hive-coupling-seams-step-design-2026-07-10.md`**（**起步必读**；含 4 缝 HEAD 锚点 + 修法 + 用户决策 + TODO + e2e 欠账）。侦察 = `wf_dfe1cb86-df4`（4 缝读者 + 完备性审查；完备性审查确认 4 缝之外无未护 loud break，另抓出 auto-analyze 静默扩大缝）。

- **W6 写路径 TCCL = 已核实为假警报，无需改代码**：iceberg-on-HMS 写委派穿过 iceberg 兄弟已绑定的 `TcclPinningConnectorContext`（`IcebergConnector.java:174`，线程无关）。只欠 e2e。可选：软化 `HiveConnector.java:206-208` 过度谨慎注释（纯文档）。
- **用户 3 处决策（2026-07-10，全选 FULL PARITY）**：① 时间线函数 `hudi_meta` = **保留(改造连接器驱动)**（侦察推翻"删"倾向——4 个 p2 hudi 套件在用）；② Hive `ANALYZE … WITH SAMPLE` = **完整移植**（今天可用，不移植翻闸后会 `DdlException`）；③ 后台列自动统计 = **按表细分排除 hudi-on-HMS**（复刻旧 `HIVE||ICEBERG` 门）。
- **✅ 已落地：S1「partition_values() 通用插件臂」** commit `166515cdc88`（3 个 fe-core 文件；镜像 `$partitions`：闸门放行 `PluginDrivenExternalCatalog`+`PLUGIN_EXTERNAL_TABLE`、加 `PluginDrivenExternalTable` 臂无 HMS 强转、`getTableColumns` 上提基类；新增 SPI `PluginDrivenExternalTable.getNameToPartitionValues`（**独立方法**、不动 live `getNameToPartitionItems` 保 paimon/iceberg 字节+成本不变）；`MetadataGenerator` 加 `PLUGIN_EXTERNAL_TABLE` 臂 + HMS/插件共用 `partitionValuesRows` 构建器保 `HIVE_DEFAULT_PARTITION`→NULL）。**对 paimon/iceberg 是加法(此前抛错的 TVF 现可用)**、对 hive 休眠至翻闸。fe-core BUILD SUCCESS + 0 checkstyle + import-gate 干净。功能校验 e2e-owed。

**⭐ 下一步 = 本步剩余 3 缝 + 收尾（权威=`hive-coupling-seams-step-design-2026-07-10.md` TODO）：**
1. **S4 auto-analyze 按表门**（最小，但**先侦察**）：`PluginDrivenExternalTable.supportsColumnAutoAnalyze()` 由连接器级 `getCapabilities().contains(SUPPORTS_COLUMN_AUTO_ANALYZE)` 改为 `hasScanCapability(...)`（对 native iceberg/paimon 无变——它们仍连接器级声明）；hive 连接器从 `getCapabilities` 的 EnumSet 去掉该 flag，改在 `HiveConnectorMetadata.getTableSchema` 按表发标记（`PER_TABLE_CAPABILITIES_KEY`，precedent `:402-414` 的 TOPN 标记）。**隐藏深度须先查清**：翻闸后 iceberg-on-HMS / hudi-on-HMS 表的 `SUPPORTS_COLUMN_AUTO_ANALYZE` 是从 **hive 连接器**解析还是从**委派的兄弟连接器**解析？旧门 admit `dlaType==ICEBERG`。若 iceberg-on-HMS 已经从 iceberg 兄弟(连接器级声明)拿到，则 hive 只需对 plain-hive 发标记、对 hudi 不发，"排除 hudi"就落到"hudi 兄弟是否声明该 flag"。**动码前先在真实代码里把这个解析路径查清**（`HiveConnector` 的 3-way 路由 / 兄弟委派）。
2. **S2 hudi_meta 连接器驱动**（保留）：加中立"元数据行"SPI（镜像 `ConnectorProcedureResult` 行返回）+ `HudiConnector` 实现（时间线数据已在连接器侧：`HudiMetaClientExecutor`/`getActiveTimeline().getInstants()`）；重写 `MetadataGenerator.hudiMetadataResult`（gate 改通用插件/能力型、去 `HMSExternalTable` 强转、fe-core 摆脱 `org.apache.hudi` timeline import `:128-129`）。休眠。委派须 pin TCCL。parity = 4 个 p2 套件行(e2e)。旧 body 无论如何删除步移除。
3. **S3 sample-analyze 完整移植**（最大）：新 `ConnectorCapability.SUPPORTS_SAMPLE_ANALYZE`（hive **按表**发给 plain-hive 表——旧 `dlaType==HIVE`，排除 iceberg/hudi-on-HMS；native iceberg/paimon 不发保持现拒绝）+ `AnalysisManager.canSample`/`AnalyzeTableCommand.isSamplingPartition` 加 `PluginDrivenExternalTable.supportsSampleAnalyze()` 臂（走 `hasScanCapability`）+ `PluginDrivenExternalTable.createAnalysisTask` 返回可抽样任务(移植 `HMSAnalysisTask.doSample`+`getSampleInfo`+`needLimit`) + `getChunkSizes` override(经新 `Connector` chunk-sizes SPI 拿原始字节长度，type-math 留 fe-core)。非休眠单测端到端(发真抽样 SQL)→ e2e-owed。铁律：能力**按表**(连接器级会误 admit iceberg/hudi-on-HMS)。
4. **收尾**：（可选）软化 W6 注释；**净室对抗复审**跨全部 seam commit（memory `clean-room-adversarial-review-pref`）；把 e2e 欠账登记进 execution-plan §4；勾选设计文档 TODO；更新本 HANDOFF。

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
