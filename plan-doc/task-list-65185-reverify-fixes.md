# Task List — #65185 复核修复系列(2026-07-11 起)

> **来源(证据/推理详见)**：`plan-doc/reviews/catalog-spi-review-65185-reverify-2026-07-11.md`。本文件是**跟踪表**,只记「做什么 / 改哪 / 怎么验 / 进度」;每条的完整证据、失败场景、对抗结论去 reverify 文档对应小节(§2 高 / §3 中 / §4 设计 / §5 已登记 / §6 不适用)。
> **本系列范围**：reverify 中判定为「真实/活跃」且需**改代码**的条目。已登记/验收偏差(reverify §5)与不适用/已修/parity(reverify §6)**不在本系列**,见文末「明确不做」。
>
> **处理纪律(AGENT-PLAYBOOK 单任务循环,每条一遍)**：
> 起步先读 `HANDOFF.md` + 本表 → 选一条 → **对 HEAD 复核现码**(reverify 行号可能又漂了) → 设计(`plan-doc/tasks/designs/FIX-<id>-design.md`) → 设计红队 → 实现 → 实现自验 → build + 靶向 UT → **独立 commit** → summary → 勾掉本表 → **更新 HANDOFF + commit**(memory `handoff-discipline-per-phase`)。
> **每条一个独立 commit**;**path-whitelist `git add`,禁 `git add -A`**(工作树大量遗留 scratch,见 HANDOFF §分支须知)。上下文超 30% 找干净节点交接(memory `session-handoff-at-30pct-context`)。
>
> **构建/验证备忘(memory `doris-build-verify-gotchas` + HANDOFF §操作须知)**：
> - fe-core：`mvn -o -f fe/pom.xml -pl fe-core -am test-compile -Dmaven.build.cache.enabled=false`(漏 `-am`→假 `${revision}` 错)。
> - 连接器:`-pl :fe-connector-<mod> -am`;靶向 UT 加 `-Dtest=<Class> -DfailIfNoTests=false`。
> - **⚠ paimon 模块必须 `install`/`package`(shade jar 绑 package 阶段);hms/hive/hudi/iceberg/maxcompute 无此坑**。
> - 连接器**无 Mockito**(真 recording fake);**fe-core 有 Mockito**;checkstyle 禁 static import、扫 test 源、`UnusedImports` fail build。
> - `bash tools/check-connector-imports.sh` 须 exit 0(连接器不得 import fe-core)。
> - **信 LOG 不信 exit**:后台 task 通知的 exit code 是 wrapper 的;重定向到文件 grep `BUILD SUCCESS`/`BUILD FAILURE`/`[ERROR].*\.java:`/`Tests run:`/`You have N Checkstyle`。全量编译 ~6min→后台跑。
> - **e2e 多为 live-gated**(hudi/iceberg/s3/odps 需真集群);无法本地跑的须显式登记为 gated,别静默略过(Rule 12)。异构 `type=hms` 目录 e2e 见 memory `hms-iceberg-delegation-needs-e2e`。
> - **铁律**:fe-core 不得新增 `if(hive/iceberg/hudi)`/`instanceof HMSExternal*`/源名判别;不解析属性(memory `catalog-spi-plugindriven-no-source-specific-code`、`catalog-spi-no-property-parsing-in-fecore`)。通用节点 connector-agnostic。

---

## 进度总表

Legend：⬜ todo / 🔄 in progress / ✅ done / ⏸ 挂起(需决策/live)

| # | id | 严重度 | 模块 | 一句话 | 设计 | 实现 | build+UT | 状态 |
|---|----|-------|------|--------|------|------|----------|------|
| 1 | **H1** | 🔴高 | hudi**+hive** | 分区名不 unescape→丢行 | ✅ | ✅ | ✅ | ✅ |
| 2 | **H2** | 🔴高 | hudi**+hive** | datetime 分区谓词 ISO→0 行 | ✅ | ✅ | ✅ | ✅ |
| 3 | **H3** | 🔴高 | hudi | HMS 名当存储路径→非 hive-style 带 filter 0 split | ✅ | ✅ | ✅ | ✅ |
| 4 | **H4** | 🔴高 | hudi | 混大小写 Avro→JNI 崩 | ✅ | ✅ | ✅ | ✅ |
| 5 | **M1** | 🟠中 | fe-core | TABLESAMPLE 插件路径静默全表扫 | ⬜ | ⬜ | ⬜ | ⬜ |
| 6 | **M2** | 🟠中 | hive | 翻闸 hive 丢批量/异步 split | ✅ | ✅ | ✅ | ✅ |
| 7 | **M3** | 🟠中 | fe-core | MC batch 闸门 `!=NOT_PRUNED`→`!isPruned` | ⬜ | ⬜ | ⬜ | ⬜ |
| 8 | **M4** | 🟠中 | maxcompute | MC 分区值缓存删除→每查询全量 listPartitions | ✅ | ✅ | ✅ | ✅ |
| 9 | **M5** | 🟠中 | iceberg | computeRowCount 丢 equality-delete gate | ✅ | ✅ | ✅ | ✅ |
| 10 | **M6** | 🟠中 | iceberg | s3tables 无显式凭证硬失败(丢默认链) | ✅ | ✅ | ✅ | ✅ |
| 11 | **M7** | 🟠中 | iceberg | REST vended-cred region 别名收窄 | ✅ | ✅ | ✅ | ✅ |
| 12 | **M8** | 🟠中(运营) | build/docs | 升级只换 lib 不部署 plugins→首访抛 | ⬜ | ⬜ | ⬜ | ⬜ |
| 13 | **L1** | 🟡低 | tools | import-gate 三洞 | ⬜ | ⬜ | ⬜ | ⬜ |
| 14 | **L2** | 🟡低 | fe-core | 翻闸 hive 丢 SQL 缓存资格 + COUNTER 停增 | ⬜ | ⬜ | ⬜ | ⏸ 需决策 |
| 15 | **L3** | 🟡低 | trino | 元数据事务从不 commit/close | ⬜ | ⬜ | ⬜ | ⬜ |
| 16 | **L4** | 🟡低 | trino | plugin.dir 首胜单例(fail-loud) | ⬜ | ⬜ | ⬜ | ⬜ |
| 17 | **L5** | 🟡低 | trino | listTableNames 丢去重 | ⬜ | ⬜ | ⬜ | ⬜ |
| 18 | **L6** | 🟡低 | trino | guard 字段发布顺序→瞬时 NPE | ⬜ | ⬜ | ⬜ | ⬜ |
| 19 | **L7** | 🟡低 | kerberos | UGI.setConfiguration 无 guard(丢 first-writer) | ⬜ | ⬜ | ⬜ | ⬜ |
| 20 | **L8** | 🟡低 | kerberos | doAs 吞 interrupt 不 restore | ⬜ | ⬜ | ⬜ | ⬜ |
| 21 | **L9** | 🟡低 | maxcompute | 谓词下推全有全无 | ⬜ | ⬜ | ⬜ | ⬜ |
| 22 | **L10** | 🟡低 | fe-core | EXPLAIN 节点名 VPluginDrivenScanNode | ⬜ | ⬜ | ⬜ | ⏸ 需决策 |
| 23 | **L11** | 🟡低 | paimon | JNI/COUNT file_format 用表级默认 | ⬜ | ⬜ | ⬜ | ⬜ |
| 24 | **L12** | 🟡低 | fe-core/paimon | selectedPartitionNum 语义(登记或对齐) | ⬜ | ⬜ | ⬜ | ⏸ 需决策 |
| 25 | **L13** | 🟡低 | paimon | to-Paimon 丢嵌套 nullability | ⬜ | ⬜ | ⬜ | ⬜ |
| 26 | **L14** | 🟡低 | paimon | ignore_split_type 静默 no-op | ⬜ | ⬜ | ⬜ | ⬜ |
| 27 | **L15** | 🟡低 | fe-core | PAIMON_SCAN_METRICS 悬空常量 | ⬜ | ⬜ | ⬜ | ⬜ |
| 28 | **L16** | 🟡低 | iceberg | 快照/schema 缓存偏斜(防御性 union) | ⬜ | ⬜ | ⬜ | ⬜ |
| 29 | **L17** | 🟡低 | fe-core/iceberg | 同表多版本 version-blind schema 绑定 | ⬜ | ⬜ | ⬜ | ⬜ |
| 30 | **L18** | 🟡低 | iceberg | 未知/v3 类型静默 UNSUPPORTED | ⬜ | ⬜ | ⬜ | ⬜ |
| 31 | **L19** | 🟡低 | fe-core/iceberg | partition_columns 魔法键撞名→误判分区 | ⬜ | ⬜ | ⬜ | ⬜ |
| 32 | **L20** | 🟡低 | maxcompute | EQ 发 `==`(对齐 `=` 或 live A/B) | ⬜ | ⬜ | ⬜ | ⏸ 或 live |
| — | **D-系列** | ⚪设计债 | — | 见文末「设计债跟踪」(多为 P8 前置/需一次性重构) | — | — | — | ⏸ |

---

## 建议批次(独立;按 blast radius 从小到大 + 高危优先)

- **批次 0(高危,先做)**：`H4`(单行 lowercase,最小)→ `H1`+`H3`(建议先做 `D-PRUNE` 抽共享 helper,再一处修 H1/H3)→ `H2`。全部需 hudi 剪枝/schema 回归测试(现有 parity 测试抓不到)。
- **批次 1(中危·连接器局部)**：`M5`→`M7`→`M6`(iceberg)、`M4`(mc)、`M2`(hive)。
- **批次 2(中危·fe-core 通用节点,blast radius 较大,单测充分后再动)**：`M3`、`M1`。
- **批次 3(运营/门禁)**：`M8`(发布工具+文档,无 fe 编译)、`L1`(gate)。
- **批次 4(低危·连接器)**：`L3–L6`(trino)、`L7/L8`(kerberos)、`L9/L20`(mc)、`L11/L13/L14`(paimon)、`L18`(iceberg)。
- **批次 5(需决策,先问用户再动)**：`L2`(SQL 缓存:恢复 vs 登记)、`L10`(EXPLAIN 名:登记 vs 按连接器声明)、`L12`(selectedPartitionNum:登记 vs 对齐)。
- **批次 6(设计债/P8)**：`D-系列`,择机或随 P8。`D-PRUNE` 因承载 H1/H3,提前到批次 0。

> 决策类(⏸)条目**先在 session 里用中文讲清背景+选项问用户**(memory `ask-user-explain-in-chinese-first`),别擅自选一路实现。

---

## 🔴 高危(H1–H4)

### H1 — hudi 分区名不 unescape → 丢行 · reverify §2 H1
- [x] **H1**(hive+hudi 两份) · DONE `39a279e7c26`(设计 `designs/FIX-H1-design.md`)
- **现码**:`fe-connector-hudi/.../HudiConnectorMetadata.java:1054-1064`(`parsePartitionName` 无 unescape)+ `:1067-1078`(`matchesPredicates` 裸字符串比);候选名 `:247` ← `ThriftHmsClient.listPartitionNames:214-218`(原样 Hive 转义)。
- **Fix**:存值前 unescape(复用 `HudiScanPlanProvider.unescapePathName`,提升可见性;或 Hive `FileUtils.unescapePathName`),对齐 legacy `HudiExternalMetaCache.loadPartitionNames:231`。**首选落在 `D-PRUNE` 抽出的共享 helper**。
- **Files**:`HudiConnectorMetadata.java`(或共享 `HmsStylePartitionPruner`)。
- **Test intent**:`HudiPartitionPruningTest` 加 HMS 名 `ts=2024-01-01 10%3A00%3A00` + 谓词 `ts='2024-01-01 10:00:00'`,断言命中(RED:被剪)。live e2e gated。

### H2 — hudi datetime 分区谓词 ISO 化 → 0 行 · reverify §2 H2
- [x] **H2**(hive+hudi 两份) · DONE `cf540eebc3c`(设计 `designs/FIX-H2-design.md`，含设计红队 SOUND)
- **现码**:`ExprToConnectorExpressionConverter.convertDateLiteral:309-322`(非 DATE→`LocalDateTime`)+ `HudiConnectorMetadata.extractLiteralValue:1030-1036`(`String.valueOf`→`2024-01-01T10:00`)+ `matchesPredicates:1067` 字符串比 HMS `2024-01-01 10:00:00`。
- **Fix**:时间类型不做字符串剪枝——`extractLiteralValue` 按 Hive 规范文本(空格分隔、全 `HH:mm:ss[.ffffff]`)渲染 `LocalDateTime`;更优:`matchesPredicates` type-aware 或对时间列退回 fe-core typed Nereids 剪枝。
- **Files**:`HudiConnectorMetadata.java`(时间渲染逻辑;必要时连带 `extractLiteralValue`)。
- **Test intent**:DATETIME 分区列 + `= '2024-01-01 10:00:00'` 断言命中(RED:整表剪到 0)。
- 注:此条对抗验证 agent 未完成,但机制确定 + 与 H1/H3 同源(均 CONFIRMED),保留高危。

### H3 — hudi HMS 名当存储路径 → 非 hive-style 带 filter 0 split · reverify §2 H3
- [x] **H3**(hudi-only) · DONE `9c6fc584eb9`(设计 `designs/FIX-H3-design.md`，含最终对抗复审 CORRECT&COMPLETE + 残余登记)
- **现码**:`HudiConnectorMetadata.applyFilter:244-267`(无条件用 hive-style HMS 名作 `prunedPartitionPaths`)→ `HudiScanPlanProvider.resolvePartitions:587-590` → `collectCowSplits:394`/`collectMorSplits:429` 喂 `fsView.getLatestBaseFilesBeforeOrOn`(期望 Hudi 相对存储路径)。`use_hive_sync` 感知只在列举路 `collectPartitions:641`,`applyFilter` 绕过。
- **Fix**:`applyFilter` 候选分区源 `use_hive_sync_partition` 感知:`!useHiveSyncPartition`→对 `listAllPartitionPaths`(`2024/01`)剪枝;`useHiveSyncPartition`→`FSUtils.getRelativePartitionPath(basePath, location)` 相对化。
- **Files**:`HudiConnectorMetadata.java`(applyFilter 候选源)。
- **Test intent**:非 hive-style 表断言「带 filter 分区集 == 不带 filter」(RED:带 filter 0 split)。live e2e gated。

### H4 — hudi 混大小写 Avro → JNI/MOR reader 崩 · reverify §2 H4
- [x] **H4**(最小,建议先做) · DONE `03f4c12dffa`(设计 `designs/FIX-H4-design.md`)
- **现码**:`HudiScanPlanProvider.planScan:180-181` `.map(Schema.Field::name)`(原始大小写)→ `HudiScanRange:220` → BE `HadoopHudiJniScanner.initRequiredColumnsAndTypes:227-229` 对 lowercase `requiredField` 精确 `containsKey`→throw。
- **Fix**:`planScan:181` 改 `.map(f -> f.name().toLowerCase(java.util.Locale.ROOT))`(与 `HudiConnectorMetadata.avroSchemaToColumns:905`、`HudiSchemaUtils:137` 一致);列类型顺序不变。
- **Files**:`HudiScanPlanProvider.java`。
- **Test intent**:`HudiSchemaParityTest` 加 JNI 列名断言(混大小写源→lowercase 列表)。MOR/JNI live e2e gated。

---

## 🟠 中危(M1–M8)

### M1 — TABLESAMPLE 插件路径静默全表扫 · reverify §3 M1
- [ ] **M1**
- **现码**:`PhysicalPlanTranslator.visitPhysicalFileScan:812-821`(只 `setSelectedPartitions`,不转发 `getTableSample()`;legacy 转发臂 `:837-840` 死)+ `PluginDrivenScanNode.getSplits:998-1019`(零 tableSample)+ `ConnectorScanPlanProvider.planScan:119`(无采样参数)。
- **Fix**:(1)translator 插件臂在 `setSelectedPartitions` 后镜像 legacy 转发 `setTableSample`;(2)`PluginDrivenScanNode.getSplits` 实现**通用 connector-agnostic** 按 split 大小采样(仿 `HiveScanNode:448-458`,操作通用 `PluginDrivenSplit` 大小,**不按源分支**)。
- **Files**:`fe-core/.../PhysicalPlanTranslator.java`、`fe-core/.../PluginDrivenScanNode.java`。
- **Test intent**:强化 `test_hive_tablesample_p0.groovy` 断言采样后基数(非仅 EXPLAIN 子串)。

### M2 — 翻闸 hive 丢批量/异步 split · reverify §3 M2
- [x] **M2** · DONE `702153885ab`（设计 `designs/FIX-M2-design.md`；实现经 impl-subagent + 独立 diff 复核 + 全模块 test）。**两** override（非照抄 MaxCompute）：`supportsBatchScan`(分区∧非事务)+`planScanForPartitionBatch`(scope 到 batch 防重复 split)；ACID 刻意排除(同 `isTransactional()` accessor)。**登记两偏差**：BATCH-ACID-SYNC(永久)、BATCH-UNPRUNED-SYNC(由 M3 解)。`HiveScanBatchModeTest` 4/4 + hive 模块 284/284 绿。e2e live-gated。
- **现码**:`HiveScanPlanProvider.java:62`(自称 No batch;不 override `supportsBatchScan`/`streamingSplitEstimate`)→ `PluginDrivenScanNode.computeBatchMode:1085-1113` 恒非 batch。legacy `HiveScanNode.isBatchMode:283-294` 按 `prunedPartitions.size()>=1024` 异步。
- **Fix**:override `HiveScanPlanProvider.supportsBatchScan`(分区表 true)+ `planScanForPartitionBatch`,仿 `MaxComputeScanPlanProvider`;无需改 fe-core。若决定接受回归,须登记 + 大分区 e2e 真值门。
- **Files**:`fe-connector-hive/.../HiveScanPlanProvider.java`。
- **Test intent**:单测断言分区表 `supportsBatchScan`;大分区异步 split live e2e gated。

### M3 — MC batch 闸门 `!=NOT_PRUNED`→`!isPruned` · reverify §3 M3
- [ ] **M3**
- **现码**:`PluginDrivenScanNode.shouldUseBatchMode:1136` `!selectedPartitions.isPruned`;应 `== SelectedPartitions.NOT_PRUNED`。`ExternalTable.initSelectedPartitions:447` 无谓词返 `isPruned=false` 非哨兵。行内 `:1129-1132` 注释误称 parity。仅 MC opt-in(`MaxComputeScanPlanProvider.supportsBatchScan:254`)。
- **Fix**:`:1136` 改回 `== NOT_PRUNED` 早退语义;订正 `:1129-1132` 注释。
- **Files**:`fe-core/.../PluginDrivenScanNode.java`。
- **Test intent**:纯 helper 单测覆盖 `(isPruned=false, 非 NOT_PRUNED, size≥阈值)` 应 →batch(RED:返回 false)。

### M4 — MC 分区值缓存删除 · reverify §3 M4
- [x] **M4** · DONE `c553c3c7696` + TTL parity 修 `fca288424fc`（设计 `designs/FIX-M4-design.md`；实现经 impl-subagent + 独立 build/test/zip 核验 + 最终对抗复核）。新 `MaxComputePartitionCache`=`HiveFileListingCache` 结构副本（共享 `fe-connector-cache`，contextual-only+manual-miss flag 字节一致），keyed(db,table)，持于 `MaxComputeDorisConnector`、注入 metadata、三方法走它、4 个 REFRESH 钩子刷；pom 加 `fe-connector-cache`+Caffeine 2.9.3。**复核纠正 TTL 默认 86400→600s（对齐旧版 `external_cache_refresh_time_minutes*60`）**。`MaxComputePartitionCacheTest` 9/9 + 模块 113/113、0 checkstyle、import 门 0、**插件 zip 恰含单个 caffeine-2.9.3.jar**。e2e live-gated。
- **现码**:`PluginDrivenExternalTable.getNameToPartitionItems:780-781`(每次 `listPartitions`)→ `MaxComputeConnectorMetadata.listPartitions:256-273` → `McStructureHelper.getPartitions:112-118`(裸 ODPS SDK)。fe-core+连接器**两层无缓存**。
- **Fix**:maxcompute 连接器内加 TTL/容量 Caffeine(仿 `CachingHmsClient`/`HiveFileListingCache`),keyed `(project,db,table)`,`REFRESH` 失效。**不**在 fe-core 加二级缓存。若延后须登记 CACHE-P1。
- **Files**:`fe-connector-maxcompute/.../MaxComputeConnectorMetadata.java`(+ 新缓存类)。
- **Test intent**:连接器单测:同表两次 `listPartitions` 只一次 SDK 往返(recording fake 计数)。

### M5 — iceberg computeRowCount 丢 equality-delete gate · reverify §3 M5
- [x] **M5** · DONE `84f580c9075`（设计 `designs/FIX-M5-design.md`；recon+红队 SOUND_WITH_CHANGES）。根因=parity 目标被上游 #64648 移动（非误读）；恢复护栏对齐当前旧版（**用户 2026-07-11 签字推翻先前「不 gate」决定**）；连接器局部无 fe-core；3 处 P6.6-FIX-H4 stale 文档已批注 SUPERSEDED；`IcebergConnectorMetadataStatisticsTest` 7/7 绿（含倒置后的 gate 断言，RED-able）。e2e live-gated。
- **现码**:`IcebergConnectorMetadata.computeRowCount:631-646`(无 gate 减法)vs COUNT(*) 下推 `IcebergScanPlanProvider.getCountFromSummary:1572-1574`(保留 gate)。legacy `IcebergUtils.getCountFromSummary:231-233` gate 到 UNKNOWN。javadoc+单测前提为假。
- **Fix**:加 `TOTAL_EQUALITY_DELETES` 常量,`computeRowCount` 减法前 `null||!="0"→返回 -1(UNKNOWN)`;订正 javadoc `:624-629`;重写 `equalityDeletesDoNotGateTableStatistics:195-210` 断言 UNKNOWN。
- **Files**:`fe-connector-iceberg/.../IcebergConnectorMetadata.java` + 同名测试。
- **Test intent**:100 records + 5 equality-delete → rowCount UNKNOWN(RED:当前锁死 100)。

### M6 — iceberg s3tables 无显式凭证硬失败 · reverify §3 M6
- [x] **M6** · DONE `03bd4f58187`（设计 `designs/FIX-M6-design.md`）。反转硬性要求=region 唯一必需（存储或 props）、无存储凭证回退 `DefaultCredentialsProvider`；新 `resolveS3Region`(factory)+`resolveS3TablesRegion`(connector 静态门)；`buildS3TablesClient(Optional,region)` 重签名；**companion 升为必做**=无存储臂发 data-plane `client.region`。测试 reframe 1 + gate 4 + companion 1（RED-able），`IcebergConnectorTest` 19/19 + `IcebergCatalogFactoryTest` 63/63 绿。依赖 M7 拓宽别名。e2e live-gated。
- **现码**:`IcebergConnector.createS3TablesCatalog:735-739`(`chosenS3` 空即 throw)← `S3FileSystemProvider.supports:64-74`(要 `hasCredential`)。legacy 用 `DefaultCredentialsProvider` 默认链。
- **Fix**:`chosenS3` 空但能从原始 props 解析 region 时(复用 M7 拓宽别名),用 `DefaultCredentialsProvider`+`Region.of(...)` 建 client 而非抛;仅无 storage 且无 region 才 fail-loud。
- **Files**:`fe-connector-iceberg/.../IcebergConnector.java`。
- **Test intent**:region+warehouse-only s3tables props 建 catalog 不抛(RED:抛)。live S3 e2e gated。

### M7 — iceberg REST vended-cred region 别名收窄 · reverify §3 M7
- [x] **M7** · DONE `f6de950e5bd`（设计 `designs/FIX-M7-design.md`）。`S3_REGION_ALIASES` 4→10 逐字节对齐 fe-core `S3Properties` isRegionField 集（同声明序）；注释按红队更正为「S3 子集、OSS/COS/OBS/Minio 刻意排除」（不再过度声称 getRegionFromProperties 精确镜像）；新测覆盖 `AWS_REGION`/`iceberg.rest.signing-region`（RED-able）。`IcebergCatalogFactoryTest` 62/62 绿。e2e live-gated。
- **现码**:`IcebergCatalogFactory.java:83` `S3_REGION_ALIASES` 仅 4 个 → `appendS3FileIO:187-194` 唯一 region 源。丢 `AWS_REGION`/`iceberg.rest.signing-region`/`rest.signing-region` 等。
- **Fix**:拓宽别名对齐 legacy `getRegionFromProperties`(至少加上述 3 个 + `REGION`/`glue.region`/`aws.glue.region`);连接器侧复制列表(不 import fe-core);订正 `:78-83` 注释。
- **Files**:`fe-connector-iceberg/.../IcebergCatalogFactory.java`。
- **Test intent**:仅 `AWS_REGION` 的 props → `CLIENT_REGION` 被发(RED:null)。

### M8 — 升级只换 lib 不部署 plugins → 首访抛 · reverify §3 M8
- [ ] **M8**(无 fe 编译;发布工具 + 文档)
- **现码**:`PluginDrivenExternalCatalog.java:135` throw;`CatalogFactory` degraded 只护启动 `:119-127`。翻闸把 blast radius 扩到全部 type=hms 目录。
- **Fix**:主线 = 发布/升级工具把连接器 jar 部署到 `connector_plugin_root`(build.sh/部署步骤)+ 响亮 release note。代码侧可选防御:replay 后聚合 ERROR 枚举所有 degraded 目录。**保留** first-access throw,**不**加 legacy fallback。
- **Files**:`build.sh`/部署脚本、release-note/升级文档;(可选)`fe-core/.../CatalogFactory.java` 或 replay 收尾处聚合日志。
- **Test intent**:升级文档步骤评审;(可选)degraded 聚合日志单测。

---

## 🟡 低危(L1–L20)

> 每条一行「现码 → fix」,详见 reverify §1 表 + 对应正文。⏸ 三条(L2/L10/L12)先问用户。

- [ ] **L1** import-gate 三洞 · `tools/check-connector-imports.sh:48,50`:grep 改 `^import (static )?${FORBIDDEN}[.]`;`FORBIDDEN` 补 `persist|transaction|fs|statistics|mysql|service`;glob 覆盖 `src/test/java`。保留 `is_vendored()`(HiveVersionUtil FP)。
- [ ] **L2** ⏸ 翻闸 hive 丢 SQL 缓存资格 · `CacheAnalyzer.java:308/316/319`(instanceof HiveScanNode)+ `BindRelation.java:887`(instanceof HMSExternalTable)。**需决策**:加 `ConnectorCapability` 让 `PluginDrivenScanNode`/`PluginDrivenExternalTable` 被识别并恢复缓存 **vs** 登记为 fail-safe 验收偏差(`enable_hive_sql_cache` 默认关)。
- [ ] **L3** trino 事务从不 commit/close · `TrinoConnectorDorisMetadata.java`(6 处)+ `TrinoScanPlanProvider.java:112`(scan 站有意保持):元数据 6 站 try/finally `commit(txn)`(read-only 廉价)。scan 站不动。
- [ ] **L4** trino plugin.dir 首胜单例 · `TrinoBootstrap.java:136,316`:单例已存在且 pluginDir 不同时 fail-loud 抛(而非静默用旧);或删 per-catalog 分支只认全局 config。
- [ ] **L5** trino listTableNames 丢去重 · `TrinoConnectorDorisMetadata.java:98`:收集加 `.distinct()`/LinkedHashSet 保序;不复原 prefix 过滤(冗余)。
- [ ] **L6** trino guard 字段发布顺序 NPE · `TrinoDorisConnector.java:176`:`doInitialize` 把 guard 字段 `trinoConnector` 赋值移到**最后**(volatile happens-before);或原子发布不可变 holder。
- [ ] **L7** kerberos UGI.setConfiguration 无 guard · `HadoopKerberosAuthenticator.java:53-59,83,115`:port master first-writer-wins(仅首次 setConfiguration,进程锁序列化,已匹配则 skip+WARN);refresh 分支不重跑 initializeAuthConfig。注:metastore 路 parity,仅 fe-filesystem HDFS 数据路是真变。
- [ ] **L8** kerberos doAs 吞 interrupt · `HadoopAuthenticator.java:34`:`throw new IOException(e)` 前加 `Thread.currentThread().interrupt();`。一行。
- [ ] **L9** MC 谓词下推全有全无 · `MaxComputePredicateConverter.java:87-97,117-123`:`convertFilter` 若根是 `ConnectorAnd` 则逐 top-level conjunct 独立 try/catch(丢+log 失败)再 AND 幸存者;**不**对 OR/嵌套 AND 逐子容错。
- [ ] **L10** ⏸ EXPLAIN 节点名 · `PluginDrivenScanNode.java:170,320`。**需决策**:登记 display-only 验收偏差(cheapest,`CONNECTOR:` 行已披露)**vs** 加连接器声明的 legacy `*_SCAN_NODE` 名(连 `Connector.getLegacyEngineName` 一起,见 D-ENGINE)。注:报告的 `connectorType.toUpperCase()+"_SCAN_NODE"` 一行修法**不够**(hms 会出 `VHMS_SCAN_NODE` 非 `VHIVE_/VICEBERG_`)。
- [ ] **L11** paimon file_format · `PaimonScanPlanProvider.java:812-813,848-849`:JNI DataSplit + COUNT 路按首数据文件后缀 `getFileFormatBySuffix("/"+dataFiles().get(0).fileName()).orElse(defaultFileFormat)`(仿 native 臂 `:540`/legacy)。注:默认 JNI reader 不消费 file_format,影响窄(仅 opt-in cpp reader)。
- [ ] **L12** ⏸ selectedPartitionNum 语义 · `PluginDrivenScanNode.java:297-303`。**需决策**:登记为「paimon/iceberg 对齐 MC/hive 的 Nereids-剪枝数」验收偏差(推荐)**vs** 连接器回报 SDK-distinct(重,不推荐)。同步 paimon/iceberg EXPLAIN `partition=N/M` 回归期望。**勿**在通用节点按源分支。
- [ ] **L13** paimon 嵌套 nullability · `PaimonTypeMapping.java:254,257-259,269-281`:发 4-arg `DataField(id,name,type.copy(isChildNullable),comment)`;ARRAY 元素/MAP value `.copy(...)`。仿 `IcebergSchemaBuilder`/legacy。comment 半已登记 DV-035c。
- [ ] **L14** paimon ignore_split_type no-op · `PaimonScanPlanProvider.java:407-451`:读 `ignore_split_type`(经 `ConnectorSession.getSessionProperties()`)按被忽略 reader 类型跳 split;或退休变量+登记。
- [ ] **L15** PAIMON_SCAN_METRICS 悬空 · `SummaryProfile.java:158,218,277`:删三处死引用(P5 已验收弃 paimon FE scan metrics);或加 connector-neutral scan-metrics SPI(feature,非必需)。
- [ ] **L16** iceberg 缓存偏斜(部分已修) · `IcebergScanPlanProvider.java:1077-1108`:hasSnapshotPin 臂把 field-id dict 建成「pinned schema ∪ requested latest columns」超集(传 requestedLowerNames 而非 `emptyList`),两向都超集;或 fe-core 侧 query-begin pin 解析 pinnedSchema 到 pinned schemaId(对齐 time-travel 臂 `:377-387`)。
- [ ] **L17** iceberg 同表多版本 version-blind 绑定 · `PluginDrivenMvccExternalTable.java:475-485`:per-reference schema 绑定 version-aware(用 `StatementContext.getSnapshot(table, tableSnapshot, scanParams)` 的 pinnedSchema,fallback latest);与 L16 同根,建议一并。窄触发 + fail-loud。
- [ ] **L18** iceberg 未知/v3 类型静默 UNSUPPORTED · `IcebergTypeMapping.java:91,143`:两 default 臂改 `throw DorisConnectorException("Cannot transform unknown type: "+...)`,保留显式 TIME/VARIANT UNSUPPORTED;或接受更松并登记。
- [ ] **L19** partition_columns 魔法键撞名 · `PluginDrivenExternalTable.java:512,703-714` + `IcebergConnectorMetadata.java:409-445`:putAll 前从源 `table.properties()` 剔除保留键;iceberg buildTableSchema 在分区/非分区分支前移除已存在的 `partition_columns`,使非分区表不继承用户 `partition_columns`。
- [ ] **L20** ⏸ MC EQ `==` · `MaxComputePredicateConverter.java:145-146`:直接 `case EQ: opDesc = "=";` 对齐 SDK/legacy 消除不确定性(推荐);或 live ODPS A/B 确认容忍。顺带补 IN 方向回归测试(P4-3-IN 已修但缺测)。

---

## ⚪ 设计债跟踪(D-系列;多为 P8 前置或一次性重构,择机)

> 详见 reverify §4。均为真实设计张力,按现行铁律非「须修 bug」;择机或随 P8。**D-PRUNE 因承载 H1/H3,提前到高危批次。**

- [ ] **D-PRUNE**(承载 H1/H3,优先)· 抽 `HiveConnectorMetadata:1995-2093` 与 `HudiConnectorMetadata:980-1078` 逐字节相同的 7 方法 EQ/IN 剪枝块到共享 `HmsStylePartitionPruner`(fe-connector-api pushdown 或 fe-connector-metastore-hms util),H1/H3 的 unescape/相对化修复一处落地,防连接器专属分歧。
- [ ] **D-ENGINE** · `Connector.getLegacyEngineName()`/`getLegacyTableTypeName()` SPI 收口三处引擎名 switch(`PluginDrivenExternalTable.getEngine:1182`/`getEngineTableTypeName:1220`/`CreateTableInfo.pluginCatalogTypeToEngine:927`)。连 L10 EXPLAIN 名可一起。
- [ ] **D-SHOWCREATE-MASK**(安全) · `Env.java:4897-4907` 插件 PROPERTIES 改走 `new DatasourcePrintableMap<>(props," = ",true,true,hidePassword)`(~5 行,纵深防御;当前无可达泄漏)。
- [ ] **D-SENSITIVE-KEY**(安全) · `DatasourcePrintableMap.SENSITIVE_KEY:57-75` 的连接器键折进 `registerSensitiveKeys(...)` SPI 聚合;至少 iceberg REST 键移连接器注册。
- [ ] **D-DML-REGISTRY** · `RowLevelDmlRegistry:37` 加 fail-loud 契约检查(非 iceberg 连接器声明 DELETE/MERGE 时拒绝)+ 刷新过时 javadoc;或连接器提供 transform SPI。
- [ ] **D-TCCL** · 抽 `Tccl.pin(loader,Runnable)` 到 fe-connector-spi/support,iceberg/paimon/hive(内联 `HiveConnectorMetadata:798-805,832-845`)共用。
- [ ] **D-HIVECONF** · 合并 hive 三处 `buildHadoopConf`(`HiveConnector:621`/`HiveScanPlanProvider:444`/`HiveConnectorMetadata:967`)为一私有 helper;长期共享 `HadoopConfBuilder`。
- [ ] **D-FAKESTUB**(测试) · 加共享 `AbstractFakeHmsClient` 测试夹具,hive/hudi ~11 份桩收敛。
- [ ] **D-MAGICKEY** · SHOW CREATE 子句换结构化 `ConnectorTableDdl`(transform+sort 作数据,fe-core 单一 altitude 渲染);至少把 `partition_columns`/`primary_keys` 提为声明常量。
- [ ] **D-SPI-FACET** · 4.2 stringly-typed(transform/bucket enum 化)+ 4.3 连接器专属能力收进 capability-discovered facet;登记为验收架构张力。
- [ ] **D-D2-MICROS** · `ConnectorMvccPartitionView.getNewestUpdateTimeMillis:113` 重命名 `getNewestUpdateMarker()`(去伪单位,零行为变更)。
- [ ] **D-D3-PATHCONTRACT** · `applyRewriteFileScope:204`/`applyTopnLazyMaterialization:226` 引 `ConnectorFilePath` token / debug 全 schema 断言;或登记验收。
- [ ] **D-P2-PRECREATE** · `TrinoDorisConnector:78` 急切 preCreateValidation 登记 deviations-log(或改 best-effort)。
- [ ] **D-P8** · P8 前置(SPI_READY_TYPES 字符串门、data-cache allowlist、getEngine switch、TableType.ICEBERG_EXTERNAL_TABLE 残引、PlanNode instanceof 死臂)统一随 P8/Phase-3 处理。**当前对 hive 均无错误行为**,勿提前动。

---

## 明确不做(避免误改,详见 reverify §5/§6)

- **已登记/验收偏差(§5)**:P4-5(DV-018 INSERT 吞 refresh)、P4-6(DV-034 DB errno)、P4-SHOWPART:limit(DV-021)、P4-SHOWPART:where、P6-7(benign 超集)、P6-8(latent 不可达)、P6-10(DV-049③)。→ 保持,除非用户要重开决策。
- **不适用/已修/parity(§6)**:P0-1/2/3/4、P0-6(×2)、P1-1、P1-3-iceberg 死臂、**P3-hudi COW/MOR(HD-A4 已修)**、**P3b-1(证伪)**、**P3b-3(证伪)**、P4-3-IN(已修,仅缺测→并入 L20)、P5-7(parity)、P6-S:cap-enum、ENGINE-SWITCH:i3(已修)、P8:printNested 死臂。→ **不要据此改代码**。

---

## 每轮结束更新(滚动)

> 每完成一条:此处记 `<id> DONE <commit> — 一句话` + 勾总表 + 更新 `HANDOFF.md`。

**⭐ 批次 0 范围决策(用户 2026-07-11 签字)**：对抗 agent 已证实 **H1/H2 不是 hudi 独有——`HiveConnectorMetadata` 逐字节相同的剪枝块(parsePartitionName/matchesPredicates/extractLiteralValue)同样静默丢行**(fe-core 算出正确 typed 分区集但 hive `planScan` 丢弃、只认 applyFilter 那份 bug 结果)。用户选 **「两份就地各修」**(选项 2)：H1/H2 在 hive 和 hudi **两份副本各自就地修**(不抽共享 helper),H3/H4 仅 hudi。**D-PRUNE 抽取继续延后**为 ⚪ 设计债(reverify §4 DUPLICATION:partition-prune)。

- **H4** DONE `03f4c12dffa` — lowercase JNI reader 列名(hudi-only;`jniColumnNames` helper + UT)。
- **H1** DONE `39a279e7c26` — hive+hudi 剪枝 `parsePartitionName` unescape 值(两份就地各修;widen `unescapePathName`;直接单测跨 H3 稳定 + hive e2e applyFilter 测)。
- **H2** DONE `cf540eebc3c` — hive+hudi `extractLiteralValue` 对 `LocalDateTime` 渲 Hive-canonical 空格文本(`hiveDateTimeString` helper;设计红队 SOUND+实证 fixture `run17.hql`;H1+H2 复合 e2e 测)。
- **H3** DONE `9c6fc584eb9` — hudi `applyFilter` 候选源 `use_hive_sync_partition` 感知(非 hive-sync 走 `listAllPartitionPaths` 相对路径+新 `prunePartitionPaths`,hive-sync 保留 HMS 名臂;`matchesPredicates` 提 static)。测迁移到 stub executor + 位置式/hive-sync/直接 helper 新测。
- **批次 0 test-hardening** DONE `f0ee2ab06d2` — DATE 非回归测(hive+hudi)+ hive `.1` 微秒去尾零(补 H2 设计 item 6 + 复审软点)。
- **批次 0 全量对抗复审(3 skeptic)= CLEAN**:四修正确/复合/无回归、范围完整、10 新测均可 RED。**登记残余(非本批修)**:`use_hive_sync_partition=true`+`hive_style_partitioning=false` 表 hive-sync 臂仍喂 HMS 名给 fsView→带 filter 0 split(同 H3 类、另一臂、pre-existing 与 legacy parity);D-PRUNE/相对化 location 时一并评估。见 `designs/FIX-H3-design.md` §最终对抗复审。
- **⭐ 批次 0(H1–H4)全部 DONE。** 下一步见文首建议批次:批次 1(M5→M7→M6/M4/M2 连接器局部)。**所有 H1–H4 e2e 均 live-gated**(含转义值/DATETIME/非-hive-style/MOR-JNI 混大小写读),须真集群回归(memory `hms-iceberg-delegation-needs-e2e`)。

---

**⭐ 批次 1(M5→M7→M6/M4/M2 连接器局部)进行中**:recon+对抗红队一轮扫全 5 条(workflow `wf_40498e52-19f`,5 recon+5 红队),全部机制 HEAD 确认、verdict SOUND / SOUND_WITH_CHANGES(无 UNSOUND)。要点:M7 只需更正注释措辞(别名数组本身正确);M6 须把「data-plane client.region」companion 从可选升为必做 + 注意测试 UnusedImports;M4 最干净(SOUND,唯 Caffeine 2.9.3 版本一致性待 build-verify);M2 **非** trivial「照抄 MaxCompute」——hive `planScan` 非 partition-set-scoped,必须**额外** override `planScanForPartitionBatch` 否则 batch 重复 split(红队证实),另需登记 ACID→sync + 未过滤扫描→sync 两条偏差。

- **M5** DONE `84f580c9075`(code) — iceberg 表级行数恢复 equality-delete 护栏,对齐当前旧版(上游 #64648 移动了 parity 目标)。**推翻先前签字的「不 gate」决定,用户 2026-07-11 签字**;3 处 P6.6-FIX-H4 文档批注 SUPERSEDED;`IcebergConnectorMetadataStatisticsTest` 7/7 绿。e2e live-gated(equality-delete 表 SHOW TABLE STATS=UNKNOWN,独立 iceberg + iceberg-on-HMS 同表同结果)。
- **M7** DONE `f6de950e5bd`(code) — iceberg REST vended-cred `client.region` 别名 4→10 拓宽,逐字节对齐 fe-core `S3Properties` isRegionField 集;注释按红队更正为「S3 子集」。`IcebergCatalogFactoryTest` 62/62 绿。e2e live-gated(region 经 `AWS_REGION` 写提交不报 Unable to load region)。
- **M6** DONE `03bd4f58187`(code) — iceberg s3tables 无绑定存储不再硬失败:region 唯一必需(存储或 props),凭证回退 `DefaultCredentialsProvider` 默认链;companion 无存储臂发 data-plane `client.region`。`IcebergConnectorTest` 19/19 + `IcebergCatalogFactoryTest` 63/63 绿。**iceberg 子组(M5/M7/M6)全 DONE**。e2e live-gated(EC2 instance-profile s3tables 目录 CREATE+list 不抛)。
- **M4** DONE `c553c3c7696`(code) — maxcompute 连接器内 `MaxComputePartitionCache`(HiveFileListingCache 结构副本)恢复被删的分区值缓存,消除每规划一次全量 ODPS listPartitions;4 REFRESH 钩子刷;pom+Caffeine 2.9.3。9/9+模块 113/113 绿,插件 zip 单 caffeine 版本。e2e live-gated。
- **M2** DONE `702153885ab`(code) — hive 补 batch 通路:`supportsBatchScan`(分区∧非事务)+`planScanForPartitionBatch`(scope 到 batch,防重复 split——hive `planScan` 非 partition-scoped 故须额外 override)。**登记 BATCH-ACID-SYNC(永久)+BATCH-UNPRUNED-SYNC(M3 解)**。4/4+hive 模块 284/284 绿。e2e live-gated。
- **批次 1 最终对抗复核(5 per-fix skeptic + 1 cross-cut,`wf_542c60b9-001`)= M5/M6/M7/M2/cross-cut CLEAN；M4 命中 1 medium 缺陷已修**:M4 TTL 默认误抄 hive 文件缓存 knob(86400s)而非旧版 MaxCompute 分区缓存 knob(`external_cache_refresh_time_minutes*60`=600s),144x 过陈;经核旧版删除 commit `1da88365e85^`+Config 默认证实,已改 600s(**`fca288424fc`**,9/9 仍绿)。capacity 10000 巧合正确(旧版 `max_hive_partition_table_cache_num`)。
- **⭐ 批次 1(M5/M7/M6/M4/M2)全部 DONE + 最终复核 CLEAN。** 5 连接器局部修全绿(iceberg 3 + mc 1 + hive 1),各配 RED-able 单测 + 独立 code/doc commit。**e2e 全 live-gated**(equality-delete 统计/vended-region/s3tables 默认链/mc 分区缓存往返/hive 大分区异步),须真集群回归(memory `hms-iceberg-delegation-needs-e2e`)。**下一步=批次 2(M3→M1,fe-core 通用节点,blast radius 较大)**;M3 顺带解 M2 的 BATCH-UNPRUNED-SYNC 残余。
