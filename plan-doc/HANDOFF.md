# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸 → 删遗留代码）。

---

# 🆕 下一个 session = **删除阶段主体已完成（原子删除死簇已合入）→ 剩 e2e 欠账 + 用户全量回归 + PR 收尾**

> **删除阶段进度勾选 = `plan-doc/tasks/datasource-deletion-tasklist.md`**（阶段 0–6 全绿，仅剩 e2e/用户回归两项）。原子删除清单存档 = `plan-doc/tasks/batch3-delete-manifest-2026-07-14.md`（HEAD-verified，含 109 删文件 + 16 耦合删改 + 45 测试删/改 + 保留澄清）。**行号信 HEAD 不信文档**。
>
> **✅ 删旧代码全流程已完成**（用户 2026-07-13 批次方案 + 2026-07-14 清单批准执行）：
> - Batch 1 删前置（`ed46d48a354`/`c2f99712e8e`/`417f7fa6481`：分区值 fail-loud · 事件旧面板拆除 · §4-B ACID 安全直切）。
> - Batch 2 切死臂（`1e75d5023e5`/`b1aa9b763c6`：datasource + nereids/statistics/tvf 全部 live 文件死分支清零）。
> - **Batch 3 原子删死簇 = `6f7ff7f3f6b`**（172 文件，+39/−37100）：109 主源整删（hive 52 + hudi 15 + iceberg 23 + connectivity 5 + systable 1 + Nereids sink/scan 族 10 + planner/statistics/transaction 4 + 补丁版 `HiveMetaStoreClient` 1）+ 16 保留文件耦合删改 + 31 测试整删 + 14 测试改。
> - **守门全绿**：fe-core `test-compile`（含 test 源）BUILD SUCCESS · checkstyle 0（26 模块）· import-gate exit 0 · 回放单测 `Hms/Iceberg/PaimonGsonCompatReplayTest` 9/9（旧类名标签回放仍解析为 `PluginDriven*`）。
>
> **本 session 核验存档**：删前核验 `.claude/wf-batch3-delete-verify.js`（run `wf_d1815c18-1be`，122-agent：52 主源 + 68 测试逐文件分类 + gson/反射回放专项 + 完备性 critic）；施改 `.claude/wf-batch3-apply-edits.js`（run `wf_51715436-f16`，31-agent 精确删改）。
>
> **🔑 删除阶段两条关键澄清（务必记住，勿回退）**：
> 1. `DistributionSpecHiveTableSinkHashPartitioned`/`UnPartitioned`（`nereids/properties`）**名带 HiveTableSink 但是活类,保留**——活的通用连接器写路径 `PhysicalConnectorTableSink` + `PhysicalProperties.SINK_RANDOM_PARTITIONED` 在用；命名误导是既存债，改名单列。
> 2. `GsonUtils` 里 `"HMSExternalCatalog"`/`"HMSExternalTable"` 等是 **compat 字符串标签**，`registerCompatibleSubtype` 指向活 `PluginDrivenExternalCatalog`/`PluginDrivenMvccExternalTable`——**这些行不可删**（承接旧 image 回放），删旧类不碰它。
>
> **⏭ 删除线剩余（收尾）**：① e2e 欠账（ACID 分区表读分区列值 + 分区有序值 paimon/iceberg/hive/hudi 含非字符串 NULL 分区，与删除正交，用户真集群自跑）；② 用户自跑翻闸 hms 全量回归，删前后逐位一致；③ PR 收尾（拓扑多 commit → 最终 squash，base = `branch-catalog-spi`）。
>
> **⏭ 单列后续（用户定，不并入本轮）**：① 第二处哨兵泄漏 `TablePartitionValues.HIVE_DEFAULT_PARTITION` ← 活 `MetadataGenerator`（`partition_values` TVF）；② iceberg AWS 属性簇 + maven 依赖（`iceberg-aws`/`s3tables`）pom 裁剪（现可做，iceberg 文件已删）；③ jdbc `client|util` streaming/CDC 子系统迁移。**既存债**：`IcebergMergeCommand`/`IcebergUpdateCommand` 仍 iceberg 命名活类；`Column.ICEBERG_ROWID_COL`（勿新铸 `Column._row_id`）。

---

# 📌 历史 / e2e 欠账（已收口任务，仅存指针）

> P7.1–P7.4 + 翻闸（Phase 2）+ HIVEFS + hive e2e round-1/2 + #65185 复核修复（H/M/L 全系列）+ TeamCity #991951 均已 DONE，明细见 `git log` 与 `plan-doc/tasks/` 各设计文档。**各修的 live-gated e2e 仍欠用户真集群自跑**——完整 e2e 矩阵见 `hms-cutover-execution-plan-2026-07-10.md §4/§5`（memory `hms-iceberg-delegation-needs-e2e`）。删旧代码理论零行为差，e2e 欠账与本删除任务正交。

---

# 📦 分支 / Commit 须知

- **工作分支 = `catalog-spi-11-hive`**（off `branch-catalog-spi`）。PR base = `branch-catalog-spi`，**squash 合并**。
- **公开 tracking issue = apache/doris#65185**（进度按已合入 `branch-catalog-spi` PR 口径）。
- **⚠️ path-whitelist `git add`，严禁 `git add -A`**：工作树大量历史遗留 scratch（`*.bak` / `regression-conf.groovy` 明文 key / `.audit-scratch/` / `conf.cmy/` / `META-INF/` / `docker/...` / `plan-doc/reviews/P5-*` / `.claude/` / `failed-cases.out`——**非本线程产物，勿混入任何 commit**）。
- commit message：`[feat|fix|doc](catalog) …` + 末尾 `Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>` + `Claude-Session: …`。**每子步/每条 fix = 独立 commit**；HANDOFF + 设计文档单独 commit（与 code 分开）。上下文超 30% 找干净节点交接（memory `session-handoff-at-30pct-context`）。

# ⚙️ 操作须知（构建/测试，复用）

- maven：`mvn -o -f /mnt/disk1/yy/git/wt-catalog-spi/fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`（**漏 `-am`→假错 `${revision}`**）。连接器：`-pl :fe-connector-<mod> -am`。**靶向单测**加 `-Dtest=<Class> -DfailIfNoTests=false`（`-am` + `-Dtest` 会因上游模块无匹配测试报 “No tests were executed!” 假失败）。
- **⚠️ paimon 模块必须用 `install`（不是 `test`）验证**（shade jar 绑 `package` 阶段）；hms/hive 无此坑。
- **验证信 LOG 不信 exit**：后台 task 通知的 exit code 是 wrapper 的（本轮见过 rc=1 但通知报 exit 0）；重定向到文件跑（不加 `-q`），grep `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Tests run:`/`You have N Checkstyle`（memory `doris-build-verify-gotchas`）。
- **⚠️ bash 默认 timeout 120s**：全量编译 ~6min → 后台跑 + 读 LOG。**⚠️ `/mnt/disk1` 盘紧；勿用 worktree 隔离编译 agent**。cwd 会被重置 → 绝对路径。
- **连接器测试无 Mockito**（真 recording fake）；**fe-core 测有 Mockito**。checkstyle 禁 static import、扫 test 源、`UnusedImports` 会 fail build。

# 🔒 铁律（fe-core 约束）

- **【删旧代码铁律 A｜fe-core 只出不进】**：删旧代码期间 fe-core **不得新增/搬入**任何数据源直接相关代码（具体源的列名/常量/格式判别/属性解析）；源相关归各 connector 插件，fe-core 只保留 connector-agnostic 通用 SPI（memory `fe-core-source-isolation-iron-rules`）。
- **【删旧代码铁律 B｜禁 deletion-scaffolding 式搬迁】**：不得为「删 A 能编译过」把 A 的逻辑就近挪进 fe-core util。遇此情形**停手**，重分析真实归属，出方案交用户 review（memory `fe-core-source-isolation-iron-rules`）。
- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。（例外：事件源**类型探测** `getType()=="hms"` 对齐旧 poller，非源判别式违规。）
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL**（memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
- `history_schema_info` 嵌套字段名逐层 lowercase（memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable` 是 paimon/iceberg/**翻闸后 hms** 实时基类（memory `plugindriven-mvcc-table-is-live-not-dormant`）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `fe-core-source-isolation-iron-rules` · `memory-keep-only-general-or-requested`。
