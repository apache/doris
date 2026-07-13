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
| 5 | **M1** | 🟠中 | fe-core+hive | TABLESAMPLE 插件路径静默全表扫 | ✅ | ✅ | ✅ | ✅ |
| 6 | **M2** | 🟠中 | hive | 翻闸 hive 丢批量/异步 split | ✅ | ✅ | ✅ | ✅ |
| 7 | **M3** | 🟠中 | fe-core | MC batch 闸门 `!=NOT_PRUNED`→`!isPruned` | ✅ | ✅ | ✅ | ✅ |
| 8 | **M4** | 🟠中 | maxcompute | MC 分区值缓存删除→每查询全量 listPartitions | ✅ | ✅ | ✅ | ✅ |
| 9 | **M5** | 🟠中 | iceberg | computeRowCount 丢 equality-delete gate | ✅ | ✅ | ✅ | ✅ |
| 10 | **M6** | 🟠中 | iceberg | s3tables 无显式凭证硬失败(丢默认链) | ✅ | ✅ | ✅ | ✅ |
| 11 | **M7** | 🟠中 | iceberg | REST vended-cred region 别名收窄 | ✅ | ✅ | ✅ | ✅ |
| 12 | **M8** | 🟠中(运营) | build/docs | 升级只换 lib 不部署 plugins→首访抛 | ⬜ | ⬜ | ⬜ | ⏸ 跳过(用户 07-12;侦察见下) |
| 13 | **L1** | 🟡低 | tools | import-gate 三洞**+第4洞** | ✅ | ✅ | ✅ | ✅ |
| 14 | **L2** | 🟡低 | fe-core | 翻闸 hive 丢 SQL 缓存资格 + COUNTER 停增 | ✅ | ✅ | ✅ | ✅ |
| 15 | **L3** | 🟡低 | trino | 元数据事务从不 commit/close | ✅ | ✅ | ✅ | ✅ |
| 16 | **L4** | 🟡低 | trino | plugin.dir 首胜单例(fail-loud) | ✅ | ✅ | ✅ | ✅ |
| 17 | **L5** | 🟡低 | trino | listTableNames 丢去重 | ✅ | ✅ | ✅ | ✅ |
| 18 | **L6** | 🟡低 | trino | guard 字段发布顺序→瞬时 NPE | ✅ | ✅ | ✅ | ✅ |
| 19 | **L7** | 🟡低 | kerberos | UGI.setConfiguration 无 guard(丢 first-writer) | ✅ | ✅ | ✅ | ✅ |
| 20 | **L8** | 🟡低 | kerberos | doAs 吞 interrupt 不 restore | ✅ | ✅ | ✅ | ✅ |
| 21 | **L9** | 🟡低 | maxcompute | 谓词下推全有全无 | ✅ | ✅ | ✅ | ✅ |
| 22 | **L10** | 🟡低 | fe-core | EXPLAIN 节点名 VPluginDrivenScanNode | — | — | — | ✅ 登记 DV-050 |
| 23 | **L11** | 🟡低 | paimon | JNI/COUNT file_format 用表级默认 | ✅ | ✅ | ✅ | ✅ |
| 24 | **L12** | 🟡低 | fe-core/paimon/iceberg | selectedPartitionNum 语义(能力 SPI 回报真实数) | ✅ | ✅ | ✅ | ✅ |
| 25 | **L13** | 🟡低 | paimon | to-Paimon 丢嵌套 nullability | ✅ | ✅ | ✅ | ✅ |
| 26 | **L14** | 🟡低 | paimon | ignore_split_type 静默 no-op | ✅ | ✅ | ✅ | ✅ |
| 27 | **L15** | 🟡低 | fe-core | PAIMON_SCAN_METRICS 悬空常量 | ✅ | ✅ | ✅ | ✅ |
| 28 | **L16** | 🟡低 | iceberg | 快照/schema 缓存偏斜(防御性 union) | ✅ | ✅ | ✅ | ✅ 复核 REFUTED+防呆注释 |
| 29 | **L17** | 🟡低 | fe-core/iceberg | 同表多版本 version-blind schema 绑定 | ✅ | ✅ | ✅ | ✅ 用户签字 fail-loud+TODO |
| 30 | **L18** | 🟡低 | iceberg | 未知/v3 类型静默 UNSUPPORTED | ✅ | ✅ | ✅ | ✅ 用户签字 accept(DV-051) |
| 31 | **L19** | 🟡低 | fe-core/iceberg | partition_columns 魔法键撞名→误判分区 | ✅ | ✅ | ✅ | ✅ |
| 32 | **L20** | 🟡低 | maxcompute | EQ 发 `==`(对齐 `=` 或 live A/B) | ⬜ | ⬜ | ⬜ | ⏸ 或 live |
| — | **D-系列** | ⚪设计债 | — | 见文末「设计债跟踪」(多为 P8 前置/需一次性重构) | — | — | — | ⏸ |

---

## 建议批次(独立;按 blast radius 从小到大 + 高危优先)

- **批次 0(高危,先做)**：`H4`(单行 lowercase,最小)→ `H1`+`H3`(建议先做 `D-PRUNE` 抽共享 helper,再一处修 H1/H3)→ `H2`。全部需 hudi 剪枝/schema 回归测试(现有 parity 测试抓不到)。
- **批次 1(中危·连接器局部)**：`M5`→`M7`→`M6`(iceberg)、`M4`(mc)、`M2`(hive)。
- **批次 2(中危·fe-core 通用节点,blast radius 较大,单测充分后再动)**：`M3`、`M1`。
- **批次 3(运营/门禁)**：`M8`(发布工具+文档,无 fe 编译)、`L1`(gate)。
- **批次 4(低危·连接器)**：`L3–L6`(trino)、`L7/L8`(kerberos)、`L9/L20`(mc)、`L11/L13/L14`(paimon)、`L18`(iceberg)。
- **批次 5(需决策,先问用户再动)**：~~`L2`(SQL 缓存)~~ **DONE `c9a86337906` 恢复缓存**、~~`L10`(EXPLAIN 名)~~ **DONE 用户签字 accept [DV-050]**、~~`L12`(selectedPartitionNum)~~ **DONE `e5de7aedcd5` 用户签字选项 B=能力 SPI 回报真实数**;**余** `L20`(MC EQ 写法)。
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
- [x] **M1** · DONE `17b432dc1e1`（设计 `designs/FIX-M1-design.md`；设计红队 `wf_32decfa0-349` 3 lens **推翻原"通用采样"方案**为 UNSOUND → 改**连接器 opt-in**，**用户 2026-07-11 签字"只修 hive"**）。**scope 更正=hive-only 回归**（只有 hive 曾采样；其它连接器 legacy 从不采样）。原方案缺陷=`Split.getLength()` 语义因连接器而异（MaxCompute 默认 byte_size/Paimon JNI range 报 -1，MaxCompute row_offset 报行数），盲目按字节采样出乱结果。修法=SPI `ConnectorScanPlanProvider.supportsTableSample()` 默认 false + `HiveScanPlanProvider` override true；translator 插件臂通用转发 tableSample；`PluginDrivenScanNode` 仅在 `applySample`(能力开)时 `sampleSplits`（legacy `selectFiles` 端口，通用 `Split#getLength()`），非支持连接器 no-op+WARN（非静默）。两 gate 门（COUNT 抑制、batch 抑制）挂 `applySample`。`PluginDrivenScanNodeTableSampleTest` 6/6 + BatchMode 12/12 + hive 285/285，0 checkstyle，import 门净。e2e live-gated（docker-hive 结果不变式已加；强基数缩减须多文件 fixture 真集群验）。
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
- [x] **M3** · DONE `6963de4124f`（设计 `designs/FIX-M3-design.md`；设计红队 `wf_811e6242-d8b` 3 lens：BLAST-RADIUS SOUND / LEGACY-PARITY SOUND_WITH_CHANGES / COMPLETENESS SOUND_WITH_CHANGES，1 blocker+1 major 已解）。`:1136` `!isPruned`→`== NOT_PRUNED`（对齐 legacy `MaxComputeScanNode:227` + sibling `displayPartitionCounts:298`；git `1da88365e85^` 取证 + 全 producer 枚举证闭合）。**反转** pinning 测试 `testUnprocessedPruningNeverBatches`→`testNoPredicatePartitionedTableBatches`（assertTrue）。**解 M2 的 BATCH-UNPRUNED-SYNC**。**登记 supersession**：D-035/DV-019 的 LP-1「`!isPruned` 等价」判定被推翻（已补 SUPERSEDED 批注）。**docker-hive golden**`test_hive_partitions:200``(approximate)inputSplitNum` `60→6`（**用户 2026-07-11 签字：SPI 统一分区数口径**；batch 模式 `selectedSplitNum=numApproximateSplits=分区数`；对齐 MaxCompute/Trino）。`PluginDrivenScanNodeBatchModeTest` 12/12 绿。e2e live-gated（docker-hive，本地不可跑；sweep 确认全 regression 仅此 1 处 `(approximate)` 断言受影响）。
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
- [ ] **M8** ⏸ **用户 2026-07-12 要求跳过**（运营/文档欠账,非代码 bug;不 silently drop,登记待办）。
  **侦察结论(留档)**:`build.sh:1069-1083` 已把每个连接器 zip 解到 `output/fe/plugins/connector/<mod>/`,故**非构建缺口**;
  缺口在**升级流程**——运营若只替换 `fe/lib/` 而漏拷新的 `fe/plugins/connector/` 目录,FE 重启 replay 时全部 `type=hms`
  目录进 degraded(`CatalogFactory.java:119-127`)→首访抛(`PluginDrivenExternalCatalog.java:148-150`)。修法=升级文档/release-note
  响亮提示新增 `fe/plugins/connector/` + (**可选**)replay 收尾聚合 ERROR 枚举 degraded 目录(触碰 fe-core,需编译)。
  **保留** first-access throw,不加 legacy fallback。回来做时:先用中文讲清「仅文档」vs「文档+可选防御码」让用户拍板。
- **现码**:`PluginDrivenExternalCatalog.java:135` throw;`CatalogFactory` degraded 只护启动 `:119-127`。翻闸把 blast radius 扩到全部 type=hms 目录。
- **Fix**:主线 = 发布/升级工具把连接器 jar 部署到 `connector_plugin_root`(build.sh/部署步骤)+ 响亮 release note。代码侧可选防御:replay 后聚合 ERROR 枚举所有 degraded 目录。**保留** first-access throw,**不**加 legacy fallback。
- **Files**:`build.sh`/部署脚本、release-note/升级文档;(可选)`fe-core/.../CatalogFactory.java` 或 replay 收尾处聚合日志。
- **Test intent**:升级文档步骤评审;(可选)degraded 聚合日志单测。

---

## 🟡 低危(L1–L20)

> 每条一行「现码 → fix」,详见 reverify §1 表 + 对应正文。⏸ 三条(L2/L10/L12)先问用户。

- [x] **L1** DONE `88aa55b831b`(设计 `designs/FIX-L1-design.md`;设计红队 `wf_643c11b4-3fe` 3 lens 全 SOUND_WITH_CHANGES)。补 3 洞(static / +6 包 / test 源)**+ 红队发现的第 4 洞**:4 条白名单 `grep -v` 按**整行**匹配(`.`≡`/`)→连接器命名空间文件(608 个,全根在 `org.apache.doris.connector.**`)里的违规 import 被**按路径抑制**,门禁对其结构性失明。修法=候选 grep 加宽(static+test glob+6 包)+**白名单锚定到 import 目标**(`:import ...` 非整行)+ fqn sed 剥 `static`(修 static-vendored 误报,红队证 E3 属正确性非装饰)。新增自测 `tools/check-connector-imports.test.sh`(8 违规/2 vendored skip/3 allow;GREEN 于新、RED 于旧,真树 exit 0)。保留 `is_vendored()`(HiveVersionUtil FP)。**观察**:grep-denylist 固有盲区(新增内部包漏登记/FQN-无-import 内联/间距)登记设计债,根治须 allowlist/ArchUnit(Trino 先例)。
- [x] **L2** DONE `c9a86337906`(独立工作线,设计 `FIX-querycache-spi-design.md`;4 文件 + 3 测试类 8 测,fe-core BUILD SUCCESS 0 checkstyle)· **决策已定=恢复缓存**(选「加能力」路,非登记偏差):把 `CacheAnalyzer`/`BindRelation`/`SqlCacheContext`/`NereidsSqlCacheManager` 四文件六处源名分支换成 **connector-agnostic `MTMVRelatedTableIf` 能力** + 其 data-tied token `getNewestUpdateVersionOrTime()`(hive=max transient_lastDdlTime、iceberg/paimon=单调 snapshot 版本;token≤0 fail-safe 标 unsupported 不 pin 假常量),遵铁律无 `instanceof HMSExternalTable/HiveScanNode`。`enable_hive_sql_cache` 默认关不变。**COUNTER 部分**:dead 的 source-specific `COUNTER_QUERY_HIVE_TABLE` bump **有意移除**(非静默——commit 明载,通用外部缓存路不该 bump hive 专属计数)。e2e live-gated(翻闸 hive 表结果缓存命中/失效)。
- [x] **L3** DONE `4cd63c6911a`(设计 `FIX-L3-design.md`;对抗复审 `agent a182a049f` SOUND_WITH_CHANGES）· trino 6 个 FE-only 元数据站 try/finally 经 `releaseQuietly` 释放事务(commit + swallow-log 防 mask);scan 站不动(txnHandle 序列化发 BE 须保持打开)。UT 受 `io.trino.Session` 构造墙阻,登记 e2e live-gated。
- [x] **L4** DONE `e27602d4ab6`(设计 `FIX-L4-design.md`;并发对抗复审 `agent a28dc47095` SOUND）· `TrinoBootstrap` 存 pluginDir,`getInstance` 不同 dir fail-loud 抛(canonicalize best-effort 兜同物理目录异拼写)。UT 受重构造墙阻,登记。
- [x] **L5** DONE `be96adf76ba`(设计 `FIX-L5-design.md`）· `listTableNames` 加 `.distinct()` 复原 legacy LinkedHashSet 保序去重;不复原冗余 prefix 过滤。
- [x] **L6** DONE `18048f7f217`(设计 `FIX-L6-design.md`;并发对抗复审同上 SOUND）· `doInitialize` guard 字段 `trinoConnector` 移到最后赋值(安全发布,闭合半初始化瞬时 NPE 窗口);全字段已 volatile,重排即足。
- [x] **L7** DONE `9e4f2992382`(设计 `FIX-L7-design.md`;对抗复审 `agent a3e2a8a6` SOUND，对照 hadoop-common 3.4.2 字节码验证）· `initializeAuthConfig` 恢复 first-writer-wins(静态字段记首个 auth 方式,首写者设全局、后续+刷票跳过、真不匹配才 WARN)。**刻意不用 master 的 `getLoginUser()`**(会建进程级 login user,Doris 有意规避;本类是唯一非测 setConfiguration 调用者故静态字段=真全局)。刷票跳过=恢复 master parity。
- [x] **L8** DONE `59697ce3fc7`(设计 `FIX-L8-design.md`）· `doAs` catch(InterruptedException) 内 `throw` 前加 `Thread.currentThread().interrupt();`(恢复中断标志)。
- [x] **L9** DONE `017d1af1894`(设计 `FIX-L9-design.md`;converter 纯函数,**RED-able UT**）· `convert` 特判根 `ConnectorAnd`:逐 top-level conjunct 独立转(抽 `convertOne`),丢+log 失败者、AND 幸存者;OR/NOT/嵌套 AND 保持全有全无(只根 AND 处正/单调位置丢 conjunct=超集,BE 重滤;OR 丢 disjunct=子集会漏行)。加 5 UT(3 RED-able+2 guard),21/21 绿。perf-only。

- [x] **L10** DONE（**用户 2026-07-12 签字 accept**,登记 [DV-050],**无代码**）· EXPLAIN 外部表扫描节点显示通用名 `VPluginDrivenScanNode`(`PluginDrivenScanNode:173` super label)。**决策=接受为 display-only 偏差**(非加 `Connector.getLegacyEngineName` SPI 恢复旧名):`getNodeExplainString:325` 已附 `CONNECTOR: <catalog type>` 披露数据源、regression 黄金文件全树仅 1 处引用且已是新名、且与 Trino 一致(统一 `TableScan` 通用名 + 连接器作属性)。否决 Option B(catalog type 拼名会误出 `VHMS_SCAN_NODE`;改动面大)。关联 D-ENGINE 择机随 P8。
- [x] **L11** DONE `4a8650bd062`(设计 `FIX-L11-design.md`;红队 `wf_05574ccb-bd2` 3 lens SOUND/SOUND_WITH_CHANGES）· JNI DataSplit + COUNT 路改按**首数据文件后缀**取 file_format(新 package-private `dataSplitFileFormat`,legacy `getFileFormat(getPathString())` parity),回退表级默认;顺补 `getFileFormatBySuffix` 的 `.avro` 臂(legacy `FileFormatUtils` parity,inert on native)。**call-site RED 测**(`Table.copy` 令表默认 parquet≠磁盘 .orc,断言 JNI+COUNT range 携 "orc")——原 helper 孤立测不守护接线(红队 MAJOR)。69/69 绿、0 checkstyle、gate 净。默认 JNI reader 不消费 file_format,影响窄(仅 opt-in cpp reader)。e2e live-gated(cpp reader over 混/改格式表)。
- [x] **L12** DONE `e5de7aedcd5`(设计 `designs/FIX-L12-design.md`;summary `FIX-L12-summary.md`;设计 3-lens 对抗红队 `wf_f1524868-4b8` 全 SOUND_WITH_CHANGES,major/minor 全折入)· **用户 2026-07-13 签字选项 B**(连接器回报 SDK-distinct 真实扫描分区数,非登记偏差)。新 opt-in SPI `ConnectorScanPlanProvider.scannedPartitionCount`(默认 empty,仿 `supportsTableSample`)+ fe-core 纯 helper `resolveSelectedPartitionNum`(`!countPushdown && present` 用连接器数,否则 Nereids;getSplits 覆写)+ paimon(distinct `getPartitionValues()`)/iceberg(distinct `specId|partitionDataJson`,新 `IcebergScanRange.getScannedPartitionKey`)override;hive/MC 不 override 保留 Nereids(本就相等)。**通用节点无源分支**(铁律)。8/8+5/5+4/4 RED-able UT,paimon 356/356+iceberg 978/978 全绿,0 checkstyle,import 门净。登记残余:COUNT(*) 保守 Nereids、iceberg binary/null-bucket undercount、streaming-iceberg(显式开 batch+≥1024 文件)inert=legacy parity、query-cache 消费者 benign。**e2e live-gated**(隐藏/transform 分区 <1024 文件 batch-off `WHERE ts` 单日→`partition=1/M`)。
- [x] **L13** DONE `ced4775b844`(设计 `FIX-L13-design.md`;红队 3 lens 全 SOUND）· `toPaimonType` 对 ARRAY 元素/MAP value/STRUCT 字段加 `.copy(type.isChildNullable(i))`(MAP key 保持 `.copy(false)`),恢复 legacy `DorisToPaimonTypeVisitor` 嵌套 nullability parity。**scope=仅 nullability**:comment 丢=DV-035 M10.1 已接受偏差、field-id 顺序 parity 均不动。`.copy(true)` 对默认可空子类型逐字节恒等(既有 parity 测保持绿)。3 新 RED-able 测(经 `.type().isNullable()` 断言,非 DataField equals)。type-mapping+schema-builder 26/26 绿。e2e live-gated(建嵌套 NOT NULL 表 DESCRIBE/SDK 读回)。
- [x] **L14** DONE `478718aca6f`(设计 `FIX-L14-design.md`;红队 3 lens SOUND/SOUND_WITH_CHANGES）· null-tolerant `resolveIgnoreSplitType(session)`(镜像 `isCppReaderEnabled`,红队 MINOR)+三 legacy `continue` 位:`IGNORE_JNI` drops nonDataSplit+DataSplit-JNI 臂、`IGNORE_NATIVE` drops native 臂、count 臂不检、`IGNORE_PAIMON_CPP` 保持 no-op(=legacy parity,全树 grep 证 legacy 从不引用)。默认 NONE 逐字节不变。3 新 live-planScan RED 测(IGNORE_JNI/IGNORE_NATIVE 各证跳 split+IGNORE_PAIMON_CPP==NONE);nonDataSplit IGNORE_JNI 位离线不可测→留 E2E-only。69/69 绿。e2e live-gated(真集群 SET ignore_split_type 断言跳分片)。
- [x] **L15 → 被 scan-metrics 功能取代**(用户 2026-07-13 追加要求恢复功能,非删死引用):先删 `b2cdf971889`(判为死引用),后**复核发现是插件迁移丢的真功能**(paimon+iceberg 都丢;iceberg 的 `ICEBERG_SCAN_METRICS` 仅靠死 legacy `IcebergScanNode` 续命,我先前「iceberg 活」判断有误)→ **改为恢复**:`8d4209865a3` 建**连接器中立 scan-metrics SPI**(`ConnectorScanProfile`+`collectScanProfiles`;paimon `PaimonMetricRegistry`+`PaimonScanMetrics` pull、iceberg `IcebergScanProfileReporter` push;fe-core 纯静态 `writeScanProfilesInto` 写 profile;**复活** `PAIMON_SCAN_METRICS` 常量)。设计 3-lens 红队 `wf_0f803c49-7bb` 全 SOUND_WITH_CHANGES(2 blocker=iceberg streaming 泄漏改挂 planScanInternal、DebugUtil 自搬,+ majors 全折入)。设计/summary=`designs/FIX-scan-metrics-spi-{design,summary}.md`。UT fe-core 4/4+paimon 4/4+iceberg 4/4,全模块 paimon 360+iceberg 982+fe-core 94 绿,0 checkstyle,import 门净。e2e live-gated(profile 含 X Scan Metrics 组)。
- [x] **L16** DONE `76afd6f2e80`(复核 REFUTED+防呆注释;设计 `designs/FIX-L16-design.md`)· 复核判定**已消除/benign**:`IcebergScanPlanProvider` hasSnapshotPin 臂现已传 `Collections.emptyList()`(全量 pinned schema,非 requestedLowerNames 超集,自首 commit 即如此);残留「快照 id vs schema id 偏斜」构造不出——pin 原子取 `(snapshotId,schemaId)`+iceberg `schemas()` 只增+两条取 schema 路径(`pinnedSchema` dict / `getTableSchema` slot)用同一选择器同一静默回退。**无功能改动**,仅加交叉引用防呆注释锁住「两处回退必须一致」不变量(否则 BE `children.at()` SIGABRT)。残留结构隐患=L17 同根(逐引用版本感知)。
- [x] **L17** DONE `3627556db34`(设计 `designs/FIX-L17-design.md`;3-lens 红队 `wf_f7b69cf7-ec8` 推翻 analysis-time 方案→改 scan-node)· **用户 2026-07-13 签字:先 fail-loud + 记 TODO 重构**。红队证 analysis-time `getSchemaCacheValue` 守卫**顺序依赖+被 plain-ref/MTMV default pin 遮蔽+@incr 漏**(同一 query 抛或静默 skew 视绑定序)→改**逐引用 scan-node 守卫**:`PluginDrivenScanNode.pinMvccSnapshot` 解析版本感知 pin 后,校验每个 bound tuple 列在**本引用 pinned schema** 可解析(有 field-id 按 id、否则按 name),否则抛。确定性+完整(catch 自连接/latest-遮蔽/@incr/MTMV)+无误报(`t@old JOIN t@latest` 各 tuple 匹配自身版本→不抛)。静态可测 helper,7 RED-able UT。TODO=`D-MVCC-VERSION-SCHEMA`(逐引用版本感知 schema 绑定,仿 Trino 版本作用域 handle)。残余:嵌套 field-id-only 重编号看不见(iceberg id 稳定不触发)。
- [x] **L18** DONE `1c9c99c7767`(设计 `designs/FIX-L18-design.md`;登记 **DV-051**)· **用户 2026-07-13 签字 accept「统一映射 UNSUPPORTED」**(非抛):`IcebergTypeMapping.fromIcebergType/fromPrimitive` 两 `default` 臂保持把 Doris 无法表示的 iceberg 类型(v3 primitives TIMESTAMP_NANO/GEOMETRY/GEOGRAPHY/UNKNOWN + non-primitive VARIANT)映射 UNSUPPORTED→表能加载、该列 present-but-unqueryable、其它列可用。**背离** legacy(抛 `IllegalArgumentException`)与 Trino(抛 NOT_SUPPORTED),但用户选 graceful degradation。**无功能改动**;加澄清注释+守护测试 `unknownAndV3TypesDegradeToUnsupportedByDesign`(未来改抛→red);写方向 `toIcebergPrimitive` 仍抛(CREATE 不静默接受不可 round-trip 类型)。
- [x] **L19** DONE(初版 `01668779fd9` **已被 `2c58d8342c1` 取代**;设计 `designs/FIX-L19-design.md`〔SUPERSEDED〕+ `designs/DESIGN-reserved-connector-keys-framework.md`)· **用户否决静默 `remove`**(丢用户数据无信号、后来者加保留键无提示)→改**给所有保留控制键统一加 `__internal.` 前缀**(与既有 `show.*`/`connector.*` 同机制,集中为 `ConnectorTableSchema` 常量:`__internal.partition_columns`/`__internal.primary_keys`/`__internal.show.*`/`__internal.connector.*` + `RESERVED_CONTROL_KEYS` 集)。撞名**由构造消除**——用户裸 `partition_columns` 作普通用户属性正常透传(不删、SHOW CREATE 正常显示),永不被当控制键。**用户裁决只重命名、不加校验**(前缀够独特)。保留键全 **FE-only**(不发 BE,BE 走 `path_partition_keys`)、非持久化、SHOW CREATE 剥离→重命名零 BE/序列化/golden 影响。删三连接器 L19 `remove`;fe-core `getTableProperties` 改 `RESERVED_CONTROL_KEYS.contains`(未来加键自动剥离)。iceberg 50+hive 23+paimon 12/19/43+fe-core 8/36 全绿,0 checkstyle,import 门净。**新增 UT 证撞名消除**(裸用户键与保留键共存/透传)。e2e live-gated(非分区表设 `partition_columns` 用户属性不误判 + SHOW CREATE 显示该属性)。
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
- [ ] **D-MAGICKEY** · SHOW CREATE 子句换结构化 `ConnectorTableDdl`(transform+sort 作数据,fe-core 单一 altitude 渲染);至少把 `partition_columns`/`primary_keys` 提为声明常量。**L19 已在连接器侧(iceberg/hive/paimon)`remove` 保留键防撞名**;此项为长期结构化收口。
- [ ] **D-MVCC-VERSION-SCHEMA**(承接 L17 用户 TODO,2026-07-13)· 让 schema 绑定端到端**按引用版本感知**(仿 Trino:版本作用域 `ConnectorTableHandle` / 逐引用列句柄解析),使同一 `TableIf` 在一条语句里能带两套 schema——「同表多版本跨结构变更自连接」**能跑通**而非报错,移除 L17 fail-loud 守卫的限制。现状=`getSchemaCacheValue()` 无引用参数、从共享 `TableIf` 版本盲绑定;L17 已在 `PluginDrivenScanNode.pinMvccSnapshot` 加逐引用 fail-loud 守卫(tuple 列须在版本感知 pinned schema 解析,否则抛)。Nereids 层改动,择机。**残余**:嵌套 field-id-only 重编号守卫看不见(iceberg id 稳定,实际不触发)。
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

---

**⭐ 批次 2(M3→M1,fe-core 通用节点)全部 DONE**

- **M3** DONE `6963de4124f`(code) — batch 闸门 `!isPruned`→`== NOT_PRUNED`:无谓词大分区表(MaxCompute + 翻闸 hive)恢复异步 batch split(legacy `MaxComputeScanNode:227` 的 `!= NOT_PRUNED` + sibling `displayPartitionCounts` 双证;git `1da88365e85^` 取证 + 全 producer 枚举证闭合)。**顺带解 M2 的 BATCH-UNPRUNED-SYNC**。设计红队 `wf_811e6242-d8b`(3 lens:BLAST-RADIUS SOUND / LEGACY-PARITY SOUND_WITH_CHANGES / COMPLETENESS SOUND_WITH_CHANGES)命中 1 blocker(docker-hive golden 未 reconcile)+ 1 major(overturn 前次签字 LP-1/D-035「等价」未登记)均已解:**反转** pinning 测试(`testUnprocessedPruningNeverBatches`→`testNoPredicatePartitionedTableBatches`,assertTrue)、**登记 supersession**(D-035/DV-019 补 SUPERSEDED 批注)、**docker-hive golden** `test_hive_partitions:200` `(approximate)inputSplitNum` `60→6`(**用户 2026-07-11 签字:SPI 统一分区数口径**,非 hive 专属 split-count 估算;对齐 MaxCompute/Trino「引擎层统一报分片」)。`PluginDrivenScanNodeBatchModeTest` **12/12** 绿、fe-core test-compile BUILD SUCCESS 0 checkstyle。**e2e live-gated**(docker-hive `(approximate)inputSplitNum=6` + MaxCompute 无谓词 ≥阈值分区表进 batch;sweep 确认全 regression 仅此 1 处 `(approximate)` 断言受影响,maxcompute p2 只断言 `partition=N/M`/结果不受影响)。**⚠ 构建坑**:本轮 UT 一度被并行 session 的 `be-java-extensions package -am -T 1C` 构建污染共享 target 报「cannot access 生成类」假失败(非本码);待其结束后干净重跑 12/12(memory `concurrent-sessions-shared-worktree-hazard`)。
- **M1** DONE `17b432dc1e1`(code) — TABLESAMPLE 翻闸 hive 静默全表扫修复。**红队 `wf_32decfa0-349` 推翻原"通用采样"方案(UNSOUND)**:`Split.getLength()` 对 MaxCompute 默认 byte_size / Paimon JNI range 报 -1、对 MaxCompute row_offset 报行数 → 盲目按字节采样出乱结果(全表或 1 split)。**scope 更正=hive-only 回归**(只有 hive 曾采样)。→ 改**连接器 opt-in**(`ConnectorScanPlanProvider.supportsTableSample()` 默认 false、仅 `HiveScanPlanProvider` true;**用户 2026-07-11 签字 scope=hive-only**)。translator 插件臂通用转发 + `PluginDrivenScanNode.sampleSplits`(仅 `applySample` 时,legacy `selectFiles` 端口)+ 非支持连接器 no-op+WARN(非静默)+ 两 gate 门(COUNT 抑制/batch 抑制,挂 `applySample`)。对齐 Trino `applySample` + 上一批 `supportsBatchScan` opt-in 先例。`PluginDrivenScanNodeTableSampleTest` 6/6 + BatchMode 12/12 + hive 285/285 绿、0 checkstyle、import 门净。e2e live-gated(docker-hive 结果不变式已加;强基数缩减须多文件 fixture 真集群验)。
- **⭐ 批次 2(M3→M1)全部 DONE。** 2 条 fe-core 通用节点修复,各配 RED-able 单测 + 独立 code/doc commit + 设计红队(各 3 lens;**M1 红队捕获并推翻 UNSOUND 原方案 → 触发用户 scope 决策**)。**下一步 = 批次 3(M8 发布工具/文档 + L1 import 门禁)**;之后批次 4(低危连接器)… ⏸ 决策类(L2/L10/L12/L20)先问用户。

---

**⭐ 批次 3 = L1 DONE;M8 用户跳过(2026-07-12)**

- **L1** DONE `88aa55b831b`(code+test) — import 门禁补 3 洞 + **红队发现的第 4 洞**(白名单按整行匹配→连接器命名空间文件违规 import 被路径抑制,门禁对 608 个连接器实现文件结构性失明)。修法=候选 grep 加宽(static/test/6 包)+ 白名单锚定 import 目标 + fqn 剥 static;新增 `tools/check-connector-imports.test.sh`(RED 于旧/GREEN 于新/真树 exit 0)。设计红队 `wf_643c11b4-3fe` 3 lens 全 SOUND_WITH_CHANGES,发现全部逐条复现并折入。**非 live**(纯工具,无 e2e 欠账)。
- **M8** ⏸ **用户 2026-07-12 明确要求跳过**(转做 L 系列)。侦察结论已留档(build.sh 已部署插件,缺口在升级流程+文档,含一个「仅文档 vs 文档+可选防御码」的用户决策)。**不 silently drop**——保留在表中待办,回来做时先中文讲清再拍板。
- **下一步(用户指向 L 系列)**:直接可做的低危连接器 L 条=`L3–L9`(trino/kerberos/mc)、`L11/L13/L14`(paimon)、`L15–L19`(paimon/iceberg 杂项)。⏸ 决策类 `L2/L10/L12/L20` 须先中文讲清背景+选项问用户再动(memory `ask-user-explain-in-chinese-first`)。

---

**⭐ 批次 4·trino 子群(L3/L4/L5/L6)全 DONE**（各独立 code commit + 设计文档；trino 局部,不碰 fe-core,import 门净）：
- **L3** `4cd63c6911a` — 6 元数据站释放事务(releaseQuietly,masking-safe);scan 站不动。对抗复审 `a182a049f` SOUND_WITH_CHANGES(折入 masking-safe 守卫)。
- **L5** `be96adf76ba` — listTableNames `.distinct()` 复原去重。
- **L6** `18048f7f217` — guard 字段最后发布,闭合并发瞬时 NPE。并发对抗复审 `a28dc47095` SOUND。
- **L4** `e27602d4ab6` — plugin.dir 不同 fail-loud(canonicalize 兜异拼写)。同上复审 SOUND(折入 canonicalize minor)。
- **共性**:trino UT 受 `io.trino.Session`/重构造墙阻,均 build-compile + 对抗复审 + e2e live-gated 兜底(非静默;显式登记 UT-wall)。

**⭐ 批次 4·kerberos 子群(L7/L8)全 DONE**（`fe-kerberos` 独立模块,非连接器,import 门禁不适用;各独立 commit）：
- **L7** `9e4f2992382` — `UGI.setConfiguration` first-writer-wins guard(恢复 #64655 删掉的 master guard 意图,静态字段跟踪首个 auth 方式,避 `getLoginUser` 进程级登录副作用)。对抗复审 `a3e2a8a6` SOUND(对照 hadoop 字节码)。
- **L8** `59697ce3fc7` — `doAs` 恢复中断标志(一行)。
- fe-kerberos UT 11/11 绿(含 AuthenticationTest);L7 e2e live-gated(双 kerberos HDFS 目录)。

**⭐ 批次 4·maxcompute L9 DONE** `017d1af1894` — 谓词下推从「全有全无」改为顶层 AND 逐 conjunct 容错(超集正确、BE 重滤)。**首个有 RED-able UT 的 L 条**(converter 纯函数):5 新 UT(3 RED-able+2 guard,证 OR/嵌套 AND 不容错),21/21 绿。perf-only。

**⭐ 批次 4·paimon 子群(L11/L13/L14)全 DONE**（fe-connector-paimon 局部,各独立 code commit + 统一设计红队 `wf_05574ccb-bd2`：3 设计 × 3 lens = 9 agent,无 UNSOUND;设计文档 `FIX-L11/L13/L14-design.md` 含红队折入）：
- **L11** `4a8650bd062` — JNI/COUNT range file_format 改按首数据文件后缀取(legacy `getFileFormat(getPathString())` parity),补 `.avro` 臂;call-site RED 测(`Table.copy` 令表默认≠磁盘后缀,红队 MAJOR)。
- **L13** `ced4775b844` — `toPaimonType` 嵌套 nullability `.copy(isChildNullable)`(ARRAY/MAP-value/STRUCT-field),scope 仅 nullability(comment=DV-035 M10.1 已接受);`.copy(true)` 默认恒等。
- **L14** `478718aca6f` — honor `ignore_split_type`(null-tolerant helper + 三 legacy continue 位;`IGNORE_PAIMON_CPP` no-op=legacy parity);3 live-planScan RED 测,nonDataSplit 位 E2E-only。
- **共性**:全走单任务循环(复核现码→设计→红队→实现→build+靶向 UT→独立 commit)。**paimon 构建坑复现**:`mvn test` 因 hive-shade 模块 shade 绑 package→`HiveConf` NoClassDefFound 假失败,改 `package` 阶段即绿(memory `doris-build-verify-gotchas`)。模块靶向 UT 全绿(scan 69/69 + type-mapping/schema-builder 26/26)、0 checkstyle、import gate 净。**e2e 全 live-gated**(cpp reader 混格式 / 嵌套 NOT NULL DDL / SET ignore_split_type 跳分片)。
- **下一步 = iceberg/杂项 L15–L19**(L15 paimon-metrics 悬空、L16/L17 iceberg 缓存/version-blind、L18 iceberg 未知类型、L19 partition_columns 撞名)。

---

**⭐ L2–L10 复核对账（2026-07-12,按 commit log 逐条核实）**：
- **L3–L9 已完成**（顶部进度表此前漏勾,已同步 `a691d0264f5`）——7 commit 全在分支:trino `4cd63c6911a`/`e27602d4ab6`/`be96adf76ba`/`18048f7f217`、kerberos `9e4f2992382`/`59697ce3fc7`、mc `017d1af1894`。
- **L2 已完成** `c9a86337906`（独立工作线,今日）——SQL 结果缓存资格用 `MTMVRelatedTableIf` 能力恢复(选「加能力」非登记偏差)、`COUNTER_QUERY_HIVE_TABLE` dead bump 有意移除;4 文件+3 测试类。标 done `83674a8c1ec`。
- **L10 已定** = **用户 2026-07-12 签字 accept**,登记 [DV-050],**无代码**：EXPLAIN 通用节点名 `VPluginDrivenScanNode` 接受为 display-only 偏差(`CONNECTOR:` 行已披露 + 黄金文件已适配 + 对齐 Trino 通用节点名做法)。
- **净结果**：L2–L10 全部收口(done 或 accept)。决策类**仅余 L12/L20**,动前先中文讲清问用户。⏸ 决策类先问用户。

---

**⭐ L15 DONE `b2cdf971889`(2026-07-13)** — 删 `SummaryProfile` 三处 `PAIMON_SCAN_METRICS` 死引用(P5 后无 reporter 填充,对照活的 iceberg metrics 保留)。零行为变更、无新测、非 live;fe-core BUILD SUCCESS + 0 checkstyle。设计/summary = `designs/FIX-L15-{design,summary}.md`。**⚠ 后被 scan-metrics 功能取代(见下)——复核发现该功能实为插件迁移真丢、且 iceberg 同丢,遂改为恢复。**

**⭐ scan-metrics 功能恢复 DONE `8d4209865a3`(2026-07-13,用户追加)** — 用户质疑 L15「删死引用」是否丢了功能;复核参考仓库 master 证实:老版 `PaimonScanMetricsReporter`+`PaimonMetricRegistry` 是真功能,插件迁移**paimon+iceberg 都丢**(已登记偏差 T02/T29),iceberg 的 `ICEBERG_SCAN_METRICS` 仅靠死 legacy `IcebergScanNode` 续命。用户要求在 connector SPI 框架**统一恢复**:新 `ConnectorScanProfile`+`collectScanProfiles` SPI、paimon pull(port registry)+iceberg push(reporter 挂同步 planScanInternal 路避泄漏)、fe-core 纯静态写 profile、复活常量、DebugUtil 格式化器自搬。设计 3-lens 红队 `wf_0f803c49-7bb` 全 SOUND_WITH_CHANGES(2 blocker+majors 全折入);UT 12 全绿、全模块 paimon 360+iceberg 982+fe-core 94 绿。设计/summary=`designs/FIX-scan-metrics-spi-{design,summary}.md`。e2e live-gated。

**⭐ L12 DONE `e5de7aedcd5`(2026-07-13,用户签字选项 B)** — selectedPartitionNum 用连接器 SDK-distinct 真实扫描分区数(能力 SPI opt-in `scannedPartitionCount` + fe-core 纯 helper `resolveSelectedPartitionNum` + paimon/iceberg override,通用节点无源分支);设计经 3-lens 对抗红队(`wf_f1524868-4b8`)全 SOUND_WITH_CHANGES,major(iceberg streaming inert=legacy parity、iceberg 跨 spec 值撞加 specId 解、query-cache 第三消费者 benign)+ minor 全折入。8/8+5/5+4/4 RED-able UT、paimon 356/356+iceberg 978/978 全绿、0 checkstyle、import 门净。设计/summary=`designs/FIX-L12-{design,summary}.md`。**决策类仅余 L20**。

<details><summary>L12 侦察原始记录(2026-07-13,workflow `wf_6c516483-c34`,4 agent recon)</summary>

- **确认 iceberg 也受影响**:iceberg 读实走 `PluginDrivenScanNode`(legacy `IcebergScanNode.java` 已死;`PhysicalPlanTranslator:812-829` instanceof `PluginDrivenExternalTable` 臂)。故 L12 覆盖 paimon+iceberg 两者。
- **旧→新语义**:旧 = **SDK-distinct**(连接器 SDK 实际规划 split 后的 distinct 原生分区数;paimon `partitionInfoMaps.size()` keyed `dataSplit.partition()`、iceberg `partitionMapInfos.size()` keyed `file().partition()`);新 = **Nereids-剪枝分区项数**(`selectedPartitions.size()`,仅按声明分区列在 `LogicalFilter` 下剪,见不到 SDK manifest/residual/bucket/transform 剪枝)。新数 **≥** 旧数。
- **背离场景**:仅当 SDK 能剪而 Nereids 不能——iceberg 隐藏/transform 分区(`days(ts)` + WHERE ts:新 30/30 vs 旧 1/30)、paimon 非分区列 manifest 剪枝(WHERE id=999:新 2/2 vs 旧 1/2)。identity 分区 + 无谓词时两者相等。
- **爆炸半径 = 0 黄金 EXPLAIN 断言**:paimon/iceberg 无任何 `partition=N/M` 黄金(它们断言 split-count 指标 `inputSplitNum`/`paimonNativeReadSplits`,不受影响)。3 个 `sql_block_rule partition_num` 测(iceberg/paimon/hive)均**identity 分区 + 无谓词**→两选项同值→**两选项都不破测**。
- **Trino 对照**:Trino EXPLAIN **无**引擎统一的 per-scan 分区计数;分区计量完全 connector-owned(row estimate 由连接器 `getTableStatistics` 报,分区 guard 如 hive `max-partitions-per-scan` 在连接器内执行)。据此 Trino 更贴近 **选项 B**(连接器经能力 SPI 回报 SDK-distinct,不违铁律——能力 SPI 本身即反分支机制,仿现有 `supportsTableSample`/`Split.getLength()` opt-in)。
- **张力(Rule 7)**:task list 原推荐 A(登记 Nereids 数为验收偏差,零代码);但 Trino 与「该数还喂 `sql_block_rule` 治理」角度支持 B(A 在隐藏分区下**高报**实际扫描分区数=治理 false-positive over-block)。已用中文向用户讲清背景+两选项+推荐 → **用户选 B**。

</details>

---

**⭐ L16–L19 全部 DONE(2026-07-13,iceberg 杂项收尾)** — 4 recon agent(`wf_0aee6600-244`)复核 HEAD + 独立读码;两条需用户决策(L17/L18)已用中文讲清背景+Trino 对照+选项问用户。commits:`01668779fd9`(L19 三连接器 remove 保留键)· `1c9c99c7767`(L18 accept UNSUPPORTED+守护测试,DV-051)· `76afd6f2e80`(L16 复核 REFUTED+防呆注释)· `3627556db34`(L17 scan-node fail-loud 守卫,3-lens 红队 `wf_f7b69cf7-ec8` 推翻 analysis-time 方案)。
- **L19** — partition_columns 撞名:**复核扩 scope 到 iceberg+hive+paimon**(同 bug 三份;hudi/mc 免疫)。producer-side `remove`(fe-core 无法区分连接器发 vs 用户透传,铁律禁解析连接器键义)。iceberg 50/50+hive 23/23 UT 绿(各 2 RED-able)、paimon package 编译绿、import 门净;paimon coreOptions 路现有 fake 非 DataTable 不可单测→检视+e2e-gated 登记。
- **L18** — 未知/v3 类型:**用户签字 accept graceful degradation**(非抛,登记 DV-051);无功能改动+守护测试锁意图。IcebergTypeMappingReadTest 11/11。
- **L16** — 缓存偏斜:**复核判 REFUTED/benign**(超集写法已不存在+偏斜构造不出:原子 pin+append-only schemas+对称回退);仅加防呆注释锁不变量。IcebergScanPlanProviderTest 88/88。
- **L17** — 同表多版本 version-blind:**用户签字 fail-loud + TODO 重构**(`D-MVCC-VERSION-SCHEMA`)。红队证 analysis-time 守卫顺序依赖+default-pin/MTMV/@incr 遮蔽→改**逐引用 scan-node 守卫**(tuple 列须在本引用 pinned schema 可解析,有 id 按 id 否则 name)。确定性+完整+无误报。SchemaGuardTest 7/7 + MvccPin 3/3 + MvccExternalTable 59/59。
- **e2e 全 live-gated**(真集群):L19 非分区表 SET TBLPROPERTIES('partition_columns'=真列)不误判分区;L18 v3 GEOMETRY 列表可加载、该列查报错、其它列可用;L16 无(纯注释);L17 iceberg `t FOR VERSION AS OF v1 a JOIN t FOR VERSION AS OF v2 b` 跨 ALTER →抛清晰错(非 BE 崩)。
- **决策类仅余 L20**(MC EQ `==`)。设计文档 `designs/FIX-L16/L17/L18/L19-design.md`。
