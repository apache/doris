# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸 → 删遗留代码）。

---

# 🆕 下一个 session = **删除阶段进行中 · Batch 1 已完成 → 下一个是 Batch 2（切断所有死臂）**

> **📋 施工蓝图（唯一权威） = `plan-doc/tasks/datasource-deletion-batch-plan-2026-07-13.md`**（用户 2026-07-13 review 通过；含 §4-B ACID 定论 + 4 批次序 + 每批 file:line + 用户决策）。进度勾选 = `plan-doc/tasks/datasource-deletion-tasklist.md`（**每完成一项即勾 `[x]` 随 commit**）。抽取合规重分析 = `datasource-deletion-extraction-reanalysis-2026-07-13.md`。**行号信 HEAD 不信文档**。
>
> **本 session 核验存档**：`.claude/wf-p75-deletion-batch-verify.js`（run `wf_749d6834-e98`，15-agent HEAD 重核 + ACID 对抗复核）。DELETE/EDIT 逐文件重核结论已收进批次方案 §2/§3；EDIT 精确行号在核验结果里（scratchpad ephemeral，批次方案已收纳要点，动手前仍以 HEAD 为准重核）。
>
> **✅ Batch 1（删前置）已完成三 commit**（fe-core test-compile BUILD SUCCESS + 0 checkstyle 已核）：
> - `ed46d48a354` **分区值 fail-loud**：`PluginDrivenMvccExternalTable.toListPartitionItem` 删 `HiveUtil.toPartitionValues` 回退，连接器空值在容错 try/catch **之外** `checkState` 硬报错（4 连接器已全接线）。**删 `HiveUtil` 的最后一处 live 引用已清**。
> - `c2f99712e8e` **事件旧面板拆除**：`Env` 去 `MetastoreEventsProcessor` 全套面（field/init/getter/`start()`/import）；`ExternalMetaIdMgr` 切死 else 臂 127-130；`ExternalMetaIdMgrTest` 改测活路径 `MetastoreEventSyncDriver`。
> - `417f7fa6481` **§4-B ACID 安全直切**：`FileQueryScanNode` 删死 `instanceof HiveSplit` 臂 + import。§4-B 定论=安全直切（连接器恒填分区值 + `HiveScanRange.populateRangeParams` 无条件重建 columns-from-path，四重独立保险；**无需**连接器 ACID 中立位）。
> - ⚠ **欠 e2e（用户自跑，live-gated）**：① ACID 分区表读分区列值正确；② 分区有序值（paimon+iceberg+hive+hudi，含非字符串列真 NULL 分区）。与删除正交，收尾统一补。
>
> **✅ 用户 2026-07-13 拍板（最高优先级，见批次方案 §5）**：① 总批次序认可；② §4-B 安全直切、不加连接器位、补 ACID e2e；③ **`HiveVersionUtil` + fe-core 补丁版 `HiveMetaStoreClient` 两个都删**（fe-connector-hms 已自带副本，fe-core 两份不再使用，删完为零引用孤儿，随 Batch 3）；④ `getBrokerAddresses`/`PlanType` 孤儿枚举/`StatisticsAutoCollector` 的 hudi-jar `VisibleForTesting` 注解换源 = 随批清理；⑤ **Batch 3 死簇一个大删除提交**；⑥ 拓扑多 commit → 最终 squash。
>
> **✅ Batch 2 datasource 块已完成一 commit**（`1e75d5023e5`，test-compile BUILD SUCCESS + 0 checkstyle 已核）：切 `CatalogMgr` 两分区臂 / `ExternalMetaCacheRouteResolver` HMS 路由臂 / `RefreshManager` 两臂 / `CatalogConnectivityTestCoordinator` Hive-Glue 臂（自包含死臂，均顺清 import）。⚠ `ExternalMetaCacheRouteResolverTest` 现断言已删的 HMS 路由（与 `ExternalMetaCacheMgr` 引擎缓存机制纠缠）→ **随 Batch 3 test-linkage 删**。
>
> **🔴 Batch-2/Batch-3 边界铁律（本 session 核出，务必遵守）**：Batch 2 只切「活方法里的死分支」，顺清**仅**被该臂用的 import/local/私有方法；**不得**在 Batch 2 删「使用者含仍存在的待删类」的声明——`ExternalMetaCacheMgr.hive()/hudi()/iceberg()` 访问器+注册+常量+import、`Env.hiveTransactionMgr` field/init/getter/import、`BaseExternalTableDataSink.getTFileFormatType/getTFileCompressType/supportedFileFormatTypes`、`StatisticsUtil.getHiveRowCount/getTotalSizeFromHMS`、`CreateTableInfo.validateIcebergRowLineageColumns(int)`、`PhysicalPlanTranslator` sink-visitor 方法、`SinkVisitor`/`RelationVisitor` 默认方法——**随 Batch 3 与被删类原子一起删**（其使用者是死 `HiveScanNode`/`HiveTableSink` 等，Batch 2 删会编译断）。详见 `datasource-deletion-tasklist.md` 阶段 3 抬头。
>
> **✅ Batch 2 剩余已完成一 commit**（`b1aa9b763c6`，test-compile BUILD SUCCESS + 0 checkstyle）：nereids/statistics/tvf **22 文件死臂全切**（19-agent 并行工作流 `.claude/wf-p75-batch2-deadarm-cuts.js` + 3 刁钻文件人工做 + 通读 diff 复核 + 补修 2 处漏清 import）。**Batch 2（切死臂）至此全部完成**——所有 live 文件对删除集的死分支引用已清零，仅剩「Batch-3-coupled 声明」（见上铁律）+ 删除集内部互引 + 测试。
>
> **下一个 = Batch 3（原子删死簇，一个大删除提交，用户定）**。删前先用中文出删除文件清单给用户过目。内容 = ① §1 全部删除文件（hive 顶层含 `HiveUtil`/`HiveExternalMetaCache` + hive/event + hive/source + hudi + iceberg 含 `IcebergUtils` + infra，~106）+ ② trap-tier（§1.6）+ 连锁 impl-rule（`LogicalHiveTableSinkToPhysicalHiveTableSink`/`LogicalHudiScanToPhysicalHudiScan` + `RuleSet` 注册）+ ③ **两个补丁类**（fe-core `datasource/hive/HiveVersionUtil` + `org/apache/hadoop/hive/metastore/HiveMetaStoreClient`）+ ④ 同批移除 Batch-3-coupled 声明（`ExternalMetaCacheMgr` 引擎缓存注册/访问器/常量/import · `Env.hiveTransactionMgr` field/init/getter/import · `BaseExternalTableDataSink.getTFileFormatType/getTFileCompressType/supportedFileFormatTypes`+`PluginDrivenTableSink` override · `StatisticsUtil.getHiveRowCount/getRowCountFromParameters/getTotalSizeFromHMS`+import · `CreateTableInfo.validateIcebergRowLineageColumns(int)`+`IcebergUtils` import · `PhysicalPlanTranslator.visitPhysicalHudiScan`+`visitPhysicalHiveTableSink`+`DistributionSpecHiveTableSink*`+`directoryLister` 面+相关 import · `SinkVisitor`/`RelationVisitor` 默认方法 · `PlanType` 孤儿枚举=可选）+ ⑤ 测试联动（§4-C：删 3 个 `@Disabled`（`HmsCatalogTest`/`HmsQueryCacheTest`/`HiveDDLAndDMLPlanTest`）+ `MetastoreEventFactoryTest`/`HiveScanNodeTest`/`HiveTableSinkTest`/`HMSAnalysisTaskTest`/`Hudi*Test`/`Iceberg*Test`/`IcebergSysTableResolverTest`/`HiveUtilTest`/**`ExternalMetaCacheRouteResolverTest`**（断言已删 HMS 路由）等 + 改 `TestHMSCachedClient`/`OlapInsertExecutorTest` 桩）。删除集自成闭环→整簇原子删（一个大 commit）；每步 test-compile 必绿。**完整删除/测试清单见本 session 核验 `wf_749d6834-e98` 结果 + 批次方案 §1/§3/§4-C**。→ **Batch 4 守门**（test-compile 含测试源 + checkstyle 0 + import-gate + 回放单测 + 用户自跑全量回归）。
>
> **⚠ 动手前重核**：Batch 1 改了 5 个文件（`Env`/`ExternalMetaIdMgr`/`FileQueryScanNode`/`PluginDrivenMvccExternalTable`/`ExternalMetaIdMgrTest`），行号已变；批次方案 §3 的其余文件未被 Batch 1 触及，但仍以 HEAD grep 为准。
>
> **⏭ 单列后续（用户定，不并入本轮）**：第二处哨兵泄漏 `TablePartitionValues.HIVE_DEFAULT_PARTITION:47` ← 活 `MetadataGenerator:2166`（`partition_values` TVF）——本 session 核实仍活；须自己的活性判定。**既存债**：`IcebergMergeCommand`/`IcebergUpdateCommand` 仍 iceberg 命名活类；`Column.ICEBERG_ROWID_COL`（勿当 sanctioned 家、勿新铸 `Column._row_id`）。

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
