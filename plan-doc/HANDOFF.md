# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸 → 删遗留代码）。

---

# 🆕 下一个 session = **步 2 全部委派已完成 → 转入删前置缺口（阶段 1）+ 删除（阶段 3–5）**

> **📋 进度追踪 = `plan-doc/tasks/datasource-deletion-tasklist.md`**（唯一勾选清单，**每完成一项即勾 `[x]` 并随 commit 更新**；用户 2026-07-13 要求）。
>
> **起步必读（两份）**：① **`plan-doc/tasks/datasource-deletion-extraction-reanalysis-2026-07-13.md`**（§3 抽取的合规重分析，**用户已 review 通过**，14-agent recon run `wf_6b4ce713-04f` + 人工复核）；② `plan-doc/tasks/P7.5-datasource-deletion-plan.md`（施工蓝图，§3 处置列已作废改指重分析、§5 步 2 已改为 2a/2b）。**行号信 HEAD 不信文档**。
>
> **✅ 2026-07-13 用户拍板（最高优先级）**：认可「零 fe-core 源相关新增」的合规重分析取代原「搬中立家」方案。四组处置：① hive 分区名解析 = 2 纯删 + 1 经 `ConnectorPartitionInfo` 加**两并行列表**（有序值 + 现有空标志）委派，4 连接器填值；② hive LZO = **纯删**（消费点全死、连接器早自带）；③ hive 默认分区哨兵 = 查询路径经现有 `ConnectorScanRange.populateRangeParams` 委派（`HiveScanRange` 加 columnsFromPath 重置，**窄** `.equals` 非 `normalize()`）+ 加载路径改指现有中立常量 `ConnectorPartitionValues.HIVE_DEFAULT_PARTITION`；④ iceberg 行血缘 = 常量纯删 + 保留列身份用**逐列中立布尔位 `reservedPassthrough`**（经 `ConnectorColumnConverter:80-90` 跨界重贴，仿现有 invisible/uniqueId）+ 建表校验经现有 `ConnectorTableOps.createTable` 委派（**接受**时机/异常/文案变化，不加前置钩子）。
>
> **进度**：**步 2（全部委派）已完成三组**——
> ① **分区有序值委派**（commit `49254f1d429`）：`ConnectorPartitionInfo` 加 `orderedPartitionValues`，4 连接器填值，fe-core `toListPartitionItem` 改用（带回退）。**删 `HiveUtil` 前仍须** fail-loud + e2e（否则 `listLatestPartitions:277` try/catch 静默吞分区）。
> ② **hive 默认分区哨兵**（commit `feddf050190` 连接器+测试 / `c05bb01798e` fe-core）：`HiveScanRange.populateRangeParams` 重建 `columnsFromPath`（镜像 iceberg，窄 `HIVE_DEFAULT_PARTITION.equals`），hive 现自持哨兵→NULL；fe-core `FilePartitionUtils` 三处改。字节不变已核。**删 `HiveExternalMetaCache` 前仍须** hive 真 NULL 分区 e2e。
> ③ **iceberg 行血缘逐列标记**（commit `1ca679e0820` 基建位 / `9fa915b290b` 连接器 / `3364966cdd4` fe-core 消费者；三步各自 install/test 绿 0 checkstyle，8 recon+3 对抗核实 run `wf_55fc07e0-70e`）：中立 `reservedPassthrough` 位经 `ConnectorColumn`→`ConnectorColumnConverter`→`Column`（`Column` 用**非持久**字段=无 `@SerializedName`，仿 `isCompoundKey`+拷贝构造补行+不进 equals）；iceberg 连接器给 v3 行血缘列设位；建表 v3 保留名拒绝下沉 `IcebergConnectorMetadata.createTable`（新 `IcebergSchemaBuilder.getEffectiveFormatVersion` 全优先级，`IF NOT EXISTS` 命中已存在表→静默 no-op=用户签字接受）；fe-core `IcebergMergeCommand`(7)/`IcebergUpdateCommand`/`BindExpression`(从 `sink.getCols()` 派生保留名) 改读位、`CreateTableInfo` 删 engine gate。**注**：`CreateTableInfo.validateIcebergRowLineageColumns(int)`+`IcebergUtils` 行血缘成员**仍留**（被死 `IcebergMetadataOps:358`/`IcebergScanNode:833` 引用），随删除阶段一并删；行血缘 e2e（建表拒绝+MERGE/UPDATE 透传）欠账待补。
>
> **下一个 = 阶段 1 删前置缺口 + 阶段 3–5 删除**（步 2 委派已全绿，删旧代码不再有委派阻塞）：
> - **阶段 1**：① 迁移-hive ACID 分区列的连接器中立信号（`ConnectorScanRange` 加默认 false `isAcid()`，`FileQueryScanNode` 死 `instanceof HiveSplit` 臂改读它；计划 §4-B，唯一正确性隐患）；② 事件管道 legacy 拆除前置编辑（`Env` 去 `MetastoreEventsProcessor` 全套面 + `ExternalMetaIdMgr` 切死 else 臂；计划 §4-A）——与阶段 3 切臂可合并。
> - **阶段 3–5**：切死臂（`datasource/` 内外）+ 清 `Env` 两套面 → 删 trap-tier + hive/hudi/iceberg 循环单元（含 `HiveUtil`/`HiveExternalMetaCache`/`IcebergUtils`/`IcebergMetadataOps`/`IcebergScanNode`）+ `HMSAnalysisTask` → 测试源联动 → 守门。**删除顺序见计划 §5，每步 fe-core `test-compile` 必绿**。
> - **删旧代码前用中文出删除批次方案给用户 review**（哪些文件一批、拓扑序、每批验证点）。**每完成一项勾 `datasource-deletion-tasklist.md`**。
>
> **⏭ 单列紧邻后续（用户定，不并入本轮）**：第二处同款哨兵泄漏 `TablePartitionValues.HIVE_DEFAULT_PARTITION:47` ← 活消费者 `MetadataGenerator:2166`（`partition_values` TVF）；须自己的活性判定。**既存债非本轮**：`IcebergMergeCommand`/`IcebergUpdateCommand` 仍 iceberg 命名活类、`Column.ICEBERG_ROWID_COL:63`（勿当 sanctioned 家、勿新铸 `Column._row_id`）。
>
> **一句话**：翻闸早已完成（`SPI_READY_TYPES` 含 hms/iceberg/paimon/max_compute/jdbc/es/trino-connector；paimon/mc/es 遗留目录早删）。P7.5 = 收尾删 **hive+hudi+iceberg 循环单元 + trap-tier**（`nereids`/`planner`/`statistics`/`transaction` 里的遗留写/扫描链）≈ **106 文件**，切 **34 dead-arm + 13 import-only** 死臂（~30 存活文件），并做 **§3 合规委派**（连接器侧补数据/能力位 + fe-core 消费者改委派，再删原文件）。
>
> **⚠ 三个前置缺口（用户特别问的“遗漏前置工作”，均须删前处理，详见计划 §4）**：
> - **A 事件拆除**：`MetastoreEventSyncDriver`（通用）+ 插件 `HmsEventSource/HmsEventParser` 已是活路径；只剩 legacy 拆除——删 `hive/event/` 前须先 EDIT `Env.java`（去 `MetastoreEventsProcessor` 全套面含 `start():2089`）+ `ExternalMetaIdMgr.java`（切死 else 臂 127-130）。
> - **B ⚠ 迁移-hive ACID 分区列解析（唯一正确性隐患）**：`FileQueryScanNode` 的 `instanceof HiveSplit`→`isACID` 臂喂 `FilePartitionUtils` 路径剥列（`delta_xxx` 层 `pathCount=3`）。迁移 hive 发 `PluginDrivenSplit`（非 `HiveSplit`）→ `isACID` 恒 false；若连接器 `HiveScanRange` 对 ACID 表**不填** `getPartitionValues()`（回落路径解析），切臂会**静默错剥分区列**。**P7.5 第一步就查 `fe-connector-hive` 是否总填 partition values**；缺则先补连接器中立 ACID 信号（仿 `supportsTableSample` opt-in），别盲切。
> - **C 测试联动**：~27 测试文件 import 删除单元 → `test-compile` 是验收门；删 3 个 `@Disabled`（`HmsCatalogTest`/`HmsQueryCacheTest`/`HiveDDLAndDMLPlanTest`）+ `MetastoreEventFactoryTest` + 改 `ExternalMetaIdMgrTest`/`TestHMSCachedClient`。
>
> **明确排除（KEEP，非 P7.5）**：`jdbc/client|util`（14，BLOCKED——被 streaming/CDC `StreamingJobUtils`/`PostgresResourceValidator`/`cdc_stream` TVF 活用）· `property/`（51 全 KEEP；iceberg AWS 属性簇+maven 依赖裁剪=**独立后续 pom-cleanup**，排在 iceberg 文件删除之后）· `lakesoul/odbc/kafka/kinesis/doris/test`（未迁移）· 持久化面（GSON tag/`InitDatabaseLog.Type`/`buildDatabaseForInit` case/`TableFormatType`/thrift）· 全部 `PluginDriven*`/split 框架/中立转换器/`MetastoreEventSyncDriver`。**连接器无跨界依赖**（唯一 grep 命中=vendored 同 FQN 副本，已知误报）。
>
> **删除顺序（拓扑，计划 §5）**：① 验 §4-B ACID → ② 抽取 4 组活成员+重指向 import → ③ 切所有死臂+清 `Env` 两套面+`ExternalMetaIdMgr` 死臂 → ④ 删 trap-tier + 循环单元（hive/hudi/iceberg 机械整删）+ `HMSAnalysisTask` → ⑤ 测试联动 → ⑥ 守门。**每步 fe-core `test-compile` 必绿**（Rule 4/12）。
>
> **待用户 review 决策（计划 §8）**：范围边界确认 · §4-B ACID 前置做法 · PR 打包（拓扑多 commit→squash）· iceberg AWS 属性/依赖延后。

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

- **【删旧代码铁律 A｜fe-core 只出不进】**：删旧代码期间 fe-core **不得新增/搬入**任何数据源直接相关代码（具体源的列名/常量/格式判别/属性解析）；源相关归各 connector 插件，fe-core 只保留 connector-agnostic 通用 SPI。系 `catalog-spi-plugindriven-no-source-specific-code` + `catalog-spi-no-property-parsing-in-fecore` 在删除阶段的**单调化强化**（memory `fe-core-source-isolation-iron-rules`）。
- **【删旧代码铁律 B｜禁 deletion-scaffolding 式搬迁】**：不得为「删 A 能编译过」把 A 的逻辑就近挪进 fe-core util（原计划 §3 即此模式，**已作废**）。遇此情形**停手**，重分析真实归属（源相关→插件 SPI 委派 / 真通用→留框架），出方案交用户 review（memory `fe-core-source-isolation-iron-rules`）。
- fe-core **不得**新增 `if(hive/iceberg/hudi)` / `instanceof HMSExternal*` / `switch(dlaType)` / 引擎名判别；通用 SPI 节点 connector-agnostic（memory `catalog-spi-plugindriven-no-source-specific-code`）。（例外：事件源**类型探测** `getType()=="hms"` 对齐旧 poller，非源判别式违规。）
- fe-core **不解析属性**（storage→fe-filesystem、meta→fe-connector；memory `catalog-spi-no-property-parsing-in-fecore`）。
- 跨插件/跨边界**须 pin TCCL**（memory `catalog-spi-plugin-tccl-classloader-gotcha`）。
- `history_schema_info` 嵌套字段名逐层 lowercase（memory `catalog-spi-history-schema-info-lowercase-nested-names`）。
- `PluginDrivenMvccExternalTable`/`PluginDrivenExternalTable` 是 paimon/iceberg/**翻闸后 hms** 实时基类（memory `plugindriven-mvcc-table-is-live-not-dormant`）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `memory-keep-only-general-or-requested`。
