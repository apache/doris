# 🤝 Session Handoff

> **滚动文档**：每次 session 结束**覆盖式更新**，**只保留下一个 session 必须的上下文**；完成的工作明细**不落这里**（在 `git log` + `tasks/` 设计文档里）。协作规范：[AGENT-PLAYBOOK.md](./AGENT-PLAYBOOK.md)。
> **范围** = catalog-spi **主线**（HMS 翻闸 → 删遗留代码）。

---

# 🆕 下一个 session = **删除阶段主体已完成（原子删除死簇已合入）→ 剩 e2e 欠账 + 用户全量回归 + PR 收尾**

> **🔄 2026-07-15 rebase：本分支已 `git pull --rebase upstream-apache branch-catalog-spi` 到最新（新 base tip = `c0865b021b0`，原 `e0c392b9be0`）。** 上游把 `branch-catalog-spi` 整体 rebase 到更新 master（`f052d9da44e` → `30a44d61a8a`，约 23 个 master 提交）并新增 5 个 commit（`$position_deletes` 连接器移植）。我方 **388 commit 全部重放**；23 个旧上游副本由 `pull --rebase` 的 **fork-point 机制自动丢弃**（它内部调 `git merge-base --fork-point` 读 remote-tracking reflog 找到 force-push 前的旧 tip `e0c392b9be0`）。**⚠️ 别改用裸 `git rebase <upstream>`**：命令行显式给 upstream 时它默认 `--no-fork-point`，会从 merge-base 重放 391 个、含 3 个上游已有的巨型 PR（#64300/#64655/#64688，patch-id 因上游 rebase 而对不上 → 不会被自动丢弃），实测单个即 29 冲突（AA/UD 型）。**冲突 3 处已解**：① `plan-doc/HANDOFF.md`（取本线 blob——滚动文档=覆盖式契约；**一解掐断 140 个 commit 的级联**）；② 3 个 **modify/delete**（`IcebergSplit` / `IcebergSysTable` / `IcebergSysTableResolverTest`：上游改、我方原子删 → `git rm`；实证依据=连接器对二者引用**全是 javadoc 注释**，`import org.apache.doris.datasource` 计数 **0**，唯一真引用者 `IcebergSysTableResolverTest` 本身也在同批删除内）；③ `IcebergScanPlanProvider` import（上游 `Types.NestedField`+`ScanTaskUtil` ↔ 我方 `Types`，三者均有实用 → **全留**）。**守门全绿**：fe-core `test-compile`（含 test 源）SUCCESS · iceberg 连接器 `test-compile` SUCCESS · checkstyle 0（26+16 模块）· iceberg 连接器 **1004 单测绿**（0 fail/0 error；含上游 `$position_deletes` 三测 `IcebergPartitionUtilsTest` 59/59 · `IcebergScanPlanProviderTest` 95/95 · `IcebergConnectorMetadataSysTableTest` 22/22 全过）。**「差之差」已验**：11 个重叠文件中 7 个的我方净改动 rebase 前后**逐字节一致**，差异**恰好只有上述 4 处手工解**，无第五处。备份 tag `backup/pre-rebase-cb95884db75` = `cb95884db75`（rebase 前 HEAD，可回滚）。**⚠️ 本轮 388 个 commit 的 SHA 全部改写 → 本文档中引用的旧 SHA 一律失效**（如下段 `bb7779537b5` 现为 `1c474e60951`），**按 commit message 主题查 `git log` 为准**。
>
> **⬇ 继承自上游的翻闸门 = `$position_deletes` e2e 未跑**：upstream #65135 给**旧 fe-core iceberg 子系统**加了 `$position_deletes` 系统表（该子系统已被我方原子删除），上游遂在连接器内重新移植（**fe-core/SPI 零改动**，全在 `fe-connector-iceberg`：`IcebergConnectorMetadata` 暴露 + `IcebergScanRange` 第三形状 + `IcebergPartitionUtils.getPartitionDataObjectJson` + `IcebergScanPlanProvider` 规划分支）。**这是两条线唯一的真实语义碰撞源**，代码+单测已随 base 落地并在本分支全绿；**但 e2e 从未跑过**——`regression-test/suites/external_table_p0/iceberg/test_iceberg_position_deletes_sys_table.groovy`（659 行）+ `test_iceberg_sys_table.groovy`，需 iceberg REST+MinIO+**Spark** compose（两套件数据由 Spark 写）。**这是上游自认的唯一剩余翻闸门，现随 base 一并继承 → 并入下述 e2e 欠账批次同跑。** 权威设计 = `plan-doc/tasks/designs/P6.6-position-deletes-connector-port-design.md`。
> 
> **✅ #65502 FE 迁移已入连接器 = `bb7779537b5`**（iceberg 跨 schema 演进行级删除的 initial-default 发送）。连接器侧 `IcebergSchemaUtils.buildField` 现发每字段 iceberg initialDefault（二进制 UUID/BINARY/FIXED 走 Base64+`is_base64`，余走 Doris 串形，timestamp→DATETIMEV2 空格；透传 `enable.mapping.timestamp_tz`）；`IcebergScanPlanProvider` 普通 dict **外科式**补 identifier-field 键列（equality-delete 写方按 identifier 建键）而非发全部列——**保留剪枝优化 CI #969249**（用户 2026-07-14 选“外科式”），仅“有 equality-delete 但无 identifier”时回退全 schema。BE 侧 `TField.initial_default_value(_is_base64)` 随 rebase 已在 base 且已消费。**992 单测绿 · checkstyle 0**。**e2e 欠账**：iceberg equality-delete 跨 schema 演进真集群回归（与下述 e2e 欠账同批，用户自跑）。

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
> **⏭ 单列后续（用户定，不并入本轮）**：① 第二处哨兵泄漏 `TablePartitionValues.HIVE_DEFAULT_PARTITION` ← 活 `MetadataGenerator`（`partition_values` TVF）；② iceberg AWS 属性簇 + maven 依赖（`iceberg-aws`/`s3tables`）pom 裁剪（现可做，iceberg 文件已删）——**已并入下述独立任务空间的阶段 1（HCS-12），别双头开工**；③ jdbc `client|util` streaming/CDC 子系统迁移。
>
> **🆕 ④⑤ = 2026-07-15 rebase 引入的 2 个集成缺口（对抗 review `wf_38387e2d-e85` 16-agent/5-lens 确认，均 severity=low；两侧各自正确、合并后才出现，git 不标记、编译过、单测全绿 —— 「git 说没冲突 ≠ 语义没坏」第 4 次复现）**：
> - **④ `IcebergScanPlanProvider:1419` `$position_deletes` 字典分支丢 `enable.mapping.timestamp_tz`**：上游该分支传 **4 参**，绑到我方 #65502 留下的向后兼容重载 `IcebergSchemaUtils:132`（补 `enableTimestampTz=false`）；而我方另 3 个分支（1376/1385/1388）均传 **5 参**含该 flag——同方法注释「Thread it into EVERY dict branch」在 4 行后即被违反。该重载 **rebase 前零调用者**（死码），现被上游新调用点静默吸收。**⚠️ 两个 verifier 对「BE 是否真读到该字节」结论相反**，未裁决；双方同意触发条件窄（tz-mapping catalog + v3 initialDefault + TIMESTAMPTZ 列 + `$position_deletes` 查询 + delete file 缺该列，五者同时成立；v2 表 `initialDefault()` 恒 null → 分支惰性）。**建议修法**：把 `enableTimestampTz` 穿进 1419 调用，**并删掉那个 4 参重载**——future 上游新调用点会**编译报错**而非静默绑 false（fail loud）。
> - **⑤ `IcebergScanPlanProvider:297` `scannedPartitionCount` 现在对 `$position_deletes` 触发**：我方 FIX-L12 的 `selectedPartitionNum` 能力按 `IcebergScanRange.partitionDataJson` 取 distinct，而上游新的第三种 range 形状（`buildPositionDeleteRange`）**恰好也填了该字段** → 元数据表现在会被算分区数，喂给 `EXPLAIN partition=N/M` 与 `sql_block_rule` 分区治理；`PluginDrivenScanNode:1213` 无 `isSystemTable` 守卫。**NEW_BASE 与 OLD_HEAD 均不会出现此组合。** 撞用户 2026-07-13 签字的 `selectedPartitionNum` 设计决策（memory `catalog-spi-selected-partition-num-sdk-distinct`）→ **语义该如何定由用户先拍板**，再决定是否加 sys-table 守卫。
>
> **🆕 独立任务空间（2026-07-14 立项，与本主线并行、不混流）= `plan-doc/hive-catalog-shade-removal/`**：剔除 fe-core/fe-common 对 `hive-catalog-shade`（127MB）的依赖。**关键反直觉结论**：该依赖**不是**冗余、**现在删不掉**——fe-core `src/main` 仍有 **26 个文件** import hive 类（38 文件 vendored Glue 客户端树 + DLF `ProxyMetaStoreClient` + 5 个 HiveConf 属性类 + `DefaultConnectorContext` + `RangerHiveAuditHandler`），且 `fe-common` 的 shade 是 **`provided`**（删了 fe-core 依赖 → **编译绿、运行炸**，且在**每个外表 catalog** 路径上）。该空间自带 HANDOFF/design/tasklist/progress，**新 session 从它的 `HANDOFF.md` 进**。**既存债**：`IcebergMergeCommand`/`IcebergUpdateCommand` 仍 iceberg 命名活类；`Column.ICEBERG_ROWID_COL`（勿新铸 `Column._row_id`）。

---

# 📌 历史 / e2e 欠账（已收口任务，仅存指针）

> P7.1–P7.4 + 翻闸（Phase 2）+ HIVEFS + hive e2e round-1/2 + #65185 复核修复（H/M/L 全系列）+ TeamCity #991951 均已 DONE，明细见 `git log` 与 `plan-doc/tasks/` 各设计文档。**各修的 live-gated e2e 仍欠用户真集群自跑**——完整 e2e 矩阵见 `hms-cutover-execution-plan-2026-07-10.md §4/§5`（memory `hms-iceberg-delegation-needs-e2e`）。删旧代码理论零行为差，e2e 欠账与本删除任务正交。

> **🔀 旁支（非主线）：trino 子插件目录改名已完成 = `3aebe84ec85`**，设计 `plan-doc/trino-plugin-dir-rename-design.md`。Trino 自带插件的用户投放点 `plugins/connectors/` → `plugins/trino_plugins/`（FE+BE 对称）；新框架的 `plugins/connector/`（单数）**原地不动**。9 单测绿 + 两条关键 case 变异验证。**PR 收尾时必须捞起的两笔**：① **需 release note** —— 默认值变更用户可见（老部署靠三级 fallback 零感知，新装投放点改名，doris-website 的 trino-connector 安装文档须同步）；② **BE 未跑全量构建**（`config.cpp:1543` 仅改字面量）+ **fallback 无 e2e**（回归环境走显式配置，天然绕开 fallback → **现有回归跑绿不构成 fallback 的证据**）。

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

## 2026-07-14 — fe-core `datasource/connectivity` 迁出（已完成）

**结论**：该包在本分支已是**死代码**（`createMetaTester()` 返回 no-op → `getTestLocation()` 恒 null → S3/Minio 探针不可达；HDFS 探针是空 TODO）。
唯一入口 `ExternalCatalog.checkWhenCreating()` 被 `PluginDrivenExternalCatalog` 完全 override，只有 `type=doris|test` 够得着。
故**删除**而非搬迁（搬过去仍是死码）；真正要做的是**补回**被 P6/删旧码时一并删掉的 10 个 meta 探针的能力。方案见 `plan-doc/connectivity-migration-plan.md`。

- `8057f45d0f2` fe-core 删整包 8 文件 + `checkWhenCreating()` 留空 + 删 `DEFAULT_TEST_CONNECTION`；
  `HiveConnector.testConnection()`（HMS 探针，此前 `test_connection=true` 在 hms 目录上**静默空转**）；
  `IcebergConnector` meta 探针从 REST-only 扩到 HMS/Glue/S3Tables（钉死 upstream `createMetaTester()` 的分支集合，hadoop 不探）。
- `c8f05143dc1` BE 存储探针回填：`ConnectorContext.testBackendStorageConnectivity(int, Map)` 通用回调（fe-core 不解释 payload，无 fs 类型分支）
  + `DefaultConnectorContext` 实现 thrift 往返 + iceberg 在 FE 侧 HEAD 成功后同步调用 + **两个 `TcclPinningConnectorContext` 补委派**（不补则生产静默跳过，单测抓到）
  + BE `s3_connectivity_tester.cpp` 补 `test_location` 的 `end()` 检查。

**行为变化（唯一一处，须写进 PR 描述）**：`CREATE CATALOG type=doris|test + test_connection=true` 原本抛
`IllegalArgumentException("Unknown metastore type value 'doris'")`（`checkWhenCreating` 急切调 `getMetastoreProperties()`，
对无 metastore 的目录必抛且不被 catch）；现在成功。无套件覆盖，属潜伏 bug 修复。

**未验证 / 待办**：
- `external_table_p2/test_connection/test_connectivity.groovy`：Test 1.1/2.1（坏 HMS uri 应抛 `"HMS"` + `"connectivity test failed"`）
  在补能力**之前**应当是红的（hive/iceberg-on-HMS 无 meta 探针）；补完应转绿。**需外部环境实跑确认**（p2 未跑）。
- `fe-connector-paimon` 本地编译不了（`fe-connector-paimon-hive-shade` build 失败，干净树复现）→ 其装饰器委派那一个文件**本地未编译验证**，靠 CI。
- BE 未编译（用户指示 BE 只改代码）。

# 🗂 memory 相关项

`handoff-discipline-per-phase` · `clean-room-adversarial-review-pref` · `ask-user-explain-in-chinese-first` · `session-handoff-at-30pct-context` · `doris-build-verify-gotchas` · `catalog-spi-fe-core-test-infra` · `catalog-spi-plugindriven-no-source-specific-code` · `plugindriven-mvcc-table-is-live-not-dormant` · `catalog-spi-tracking-issue` · `hms-iceberg-delegation-needs-e2e` · `concurrent-sessions-shared-worktree-hazard` · `fe-core-source-isolation-iron-rules` · `memory-keep-only-general-or-requested`。
